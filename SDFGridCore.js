// SDFGrid.js — dense 1024×1024 per-layer Float32 overlay with zero-template base,
// nucleus-centered alignment, SVG SDF, and Storage Buckets persistence.
//
// Dense overlay: one Float32Array per layer, length = 1024 * 1024 * F (F = #fields).
// First creation of a layer clones a zero template from the base store (base_zero)
// and applies any existing sparse cell data center-aligned; zeros remain as padding.
//
// IDB inside a Storage Bucket named after UID (lowercased, sanitized):
//   DB: 'SDFFieldDB'  (version 7)
//   Stores:
//     'meta'                : layout, global schema, per-layer nuclei
//       - 'layout'          : { w,h,layers, denseW,denseH, shapeType, gw,gh,gd }
//       - 'schema'          : { id, fields: string[] }
//       - `z:${z}`          : { cx, cy, w, h, rule }
//     'base'                : per-layer Int16 SDF (key = z)    [kept for SDF usage]
//     'base_zero'           : Float32 zero template buffers     [NEW]
//         key = `sid:${schemaId}`  -> ArrayBuffer(1024*1024*F*4)
//     'overlay_layers'      : per-layer Float32 dense, key = z
//     'overlay_layers_meta' : per-layer schema version { sid, fields }, key = z
//
// Console helpers exposed: SDF_layerInfo(uid,z), SDF_readCell(uid,z,x,y), SDF_centerCell(uid,z)
//
// Dependencies: THREE, utils.js (safeNum, clamp, lsSet, lsGet, updateRegistrySaved, logicKey, stateKey, blobsKey)
//               svgParser.js (SVGPathParser.parseSVGPaths), logicPresets.js (presetCode)

import { safeNum } from './utils.js';
import { SVGPathParser } from './svgParser.js';
import { presetCode } from './logicPresets.js';
import { DENSE_W, DENSE_H, STORE_META } from './SDFGridConstants.js';
import { normalizeUID, normalizeBucketName, arraysEqual } from './SDFGridUtil.js';
import { openBucketLC, openFieldDB, idbGet, idbPut } from './SDFGridStorage.js';
import { pickNucleusByDirection } from './SDFGridNucleus.js';
import { saveState, saveLogic, saveBlobs, loadState, loadLogic, loadBlobs, applyBlobs } from './SDFGridPersistence.js';
import { compileLogic } from './SDFGridLogic.js';
import { createInterpolatedShapes, sdf, sdfGrad } from './SDFGridShape.js';
import {
  _ensureZeroTemplate, _ensureBaseSDF, getBaseDistance, _denseIdx, _ensureDenseLayer,
  _mapCellToDense, _applySparseIntoDense, setDenseFromCell, addDenseFromCell,
  sampleDenseForCell, _flushDirtyLayers
} from './SDFGridLayers.js';
import { updateParticles } from './SDFGridParticles.js';
import { visualizeGrid, _valueToColor, updateVisualization } from './SDFGridVisualization.js';

const PARSE_SVG = SVGPathParser?.parseSVGPaths || null;

export class SDFGrid{
  constructor(uid, scene, params){
    this.uid   = normalizeUID(uid);
    this.scene = scene;

    const pos = params?.position || {x:0,y:0,z:0};
    this.state = {
      gridWidth: params.gridWidth, gridHeight: params.gridHeight, gridDepth: params.gridDepth,
      cellsX: params.cellsX, cellsY: params.cellsY, cellsZ: params.cellsZ,
      fidelity: params.fidelity, shapeType: params.shapeType, customSVGPath: params.customSVGPath
    };
    this.position = new THREE.Vector3(safeNum(pos.x,0), safeNum(pos.y,0), safeNum(pos.z,0));
    this.effectiveCellsZ = this.state.cellsZ * this.state.fidelity;

    // scene actors
    this.gridGroup = null;
    this.instancedMesh = null;

    // legacy sparse backing
    this.blobArray = [];
    this.dataTable = {};
    this.envVariables = params.envVariables || ['O2','CO2','H2O'];

    // svg
    this.svgShapes = [];
    this.interpolatedShapes = [];

    // nuclei per layer (logical grid coords)
    this._nuclei = new Array(this.effectiveCellsZ);

    // buckets
    this.bucketNameLC = normalizeBucketName(this.uid);
    this._bucket = null;
    this._db     = null;

    // overlay schema
    const initialFields = Array.isArray(params.fieldNames)&&params.fieldNames.length ? params.fieldNames.slice() : this.envVariables.slice();
    this.schema = { id: 1, fieldNames: initialFields, index: new Map(initialFields.map((n,i)=>[n,i])) };
    this.fieldForViz = params.fieldForViz || (initialFields.includes('O2') ? 'O2' : initialFields[0]);

    // caches and batching
    this._layerCache = new Map(); // z -> Float32Array (dense)
    this._dirtyLayers = new Set();
    this._flushHandle = null;

    // stats
    this._maxField = Object.create(null);
    this._maxO2 = 1;

    this._lastBlobSave = 0;
    this._lastDispersionUpdate = 0;
    this.trailStrength = params.trailStrength || 1.0;
    this.decayRate     = params.decayRate     || 0.1;

    this._disposed = false;
    this._rev = 0;

    // logic
    const L = SDFGrid.loadLogic(this.uid);
    this.logic = L || { enabled:false, preset:'Attract', forceScale:1.0, code: presetCode('Attract'), compiled:null, compileError:null };
    this.compileLogic(this.logic.code);

    // nuclei seed in logical space
    {
      const w=this.state.cellsX, h=this.state.cellsY, dir=params?.propagationDir||{x:1,y:0};
      const pick=()=>pickNucleusByDirection(w,h,dir);
      for (let z=0; z<this.effectiveCellsZ; z++) this._nuclei[z]=pick();
    }

    // init
    this.initializeGrid();
    this.applyBlobs(SDFGrid.loadBlobs(this.uid));

    if (this.bucketNameLC && navigator.storageBuckets){
      this._initBuckets(params?.propagationDir).then(()=>{ if(!this._disposed) this.visualizeGrid(); });
    } else {
      this.visualizeGrid();
    }

    // expose console helpers
    SDFGrid._instances ??= new Map();
    SDFGrid._instances.set(this.uid, this);
    if (typeof window !== 'undefined'){
      window.SDF_readCell   = SDFGrid.readCell.bind(SDFGrid);
      window.SDF_layerInfo  = SDFGrid.layerInfo.bind(SDFGrid);
      window.SDF_centerCell = SDFGrid.centerCell.bind(SDFGrid);
    }
  }

  // ---------- Schema ----------
  async evolveSchema(newFieldNames){
    if (!Array.isArray(newFieldNames) || !newFieldNames.length) return this.schema.id;
    if (arraysEqual(newFieldNames, this.schema.fieldNames)) return this.schema.id;
    this.schema = { id: this.schema.id + 1, fieldNames: newFieldNames.slice(), index: new Map(newFieldNames.map((n,i)=>[n,i])) };
    if (this._db) await idbPut(this._db, STORE_META, 'schema', { id:this.schema.id, fields:this.schema.fieldNames });
    // zero template will auto-regenerate for new schemaId on first use
    return this.schema.id;
  }

  // ---------- Bucket/DB init ----------
  async _initBuckets(dir){
    this._bucket = await openBucketLC(this.bucketNameLC);
    if (!this._bucket){ console.warn('Storage Buckets unavailable'); return; }
    this._db = await openFieldDB(this._bucket);

    // layout with dense dims
    const layoutVal = {
      w:this.state.cellsX, h:this.state.cellsY, layers:this.effectiveCellsZ,
      denseW:DENSE_W, denseH:DENSE_H,
      shapeType:this.state.shapeType||'',
      gw:this.state.gridWidth, gh:this.state.gridHeight, gd:this.state.gridDepth
    };
    await idbPut(this._db, STORE_META, 'layout', layoutVal);

    // global schema
    const curSchema = await idbGet(this._db, STORE_META, 'schema');
    if (!curSchema || !arraysEqual(curSchema.fields||[], this.schema.fieldNames)){
      await idbPut(this._db, STORE_META, 'schema', { id:this.schema.id, fields:this.schema.fieldNames });
    } else {
      this.schema.id = curSchema.id|0;
      this.schema.fieldNames = curSchema.fields.slice();
      this.schema.index = new Map(this.schema.fieldNames.map((n,i)=>[n,i]));
    }

    // nuclei meta (logical coords)
    const w=this.state.cellsX, h=this.state.cellsY;
    for (let z=0; z<this.effectiveCellsZ; z++){
      if (this._disposed) return;
      const key=`z:${z}`;
      const m=await idbGet(this._db, STORE_META, key);
      if (!m){
        const n=this._nuclei[z] || pickNucleusByDirection(w,h,dir||{x:1,y:0});
        await idbPut(this._db, STORE_META, key, {cx:n.x, cy:n.y, w, h, rule:'dir'});
      } else {
        this._nuclei[z]={x:m.cx,y:m.cy};
      }
    }

    // ensure zero template for current schema exists
    await this._ensureZeroTemplate();
  }
  // ---------- Nucleus and centers ----------
  getNucleus(z){
    const zi=Math.min(Math.max(z|0,0), this.effectiveCellsZ-1);
    const n=this._nuclei[zi];
    if (n && Number.isInteger(n.x) && Number.isInteger(n.y)) return {x:n.x,y:n.y,z:zi};
    return {x:(this.state.cellsX>>1)-1, y:(this.state.cellsY>>1)-1, z:zi};
  }
  centerCellIndex(z,mode='nucleus'){
    const zi=Math.min(Math.max(z|0,0), this.effectiveCellsZ-1);
    if (mode==='nucleus') return this.getNucleus(zi);
    return {x:(this.state.cellsX>>1)-1, y:(this.state.cellsY>>1)-1, z:zi};
  }

  // ---------- Legacy sparse persistence ----------
  toStateJSON(){
    return {
      state:this.state,
      position:{x:this.position.x,y:this.position.y,z:this.position.z},
      effectiveCellsZ:this.effectiveCellsZ, ts:Date.now(), uid:this.uid,
      envVariables:this.envVariables, trailStrength:this.trailStrength, decayRate:this.decayRate
    };
  }
  // ---------- Grid init ----------
  initializeGrid(){
    const sizeX=this.state.gridWidth/this.state.cellsX;
    const sizeY=this.state.gridHeight/this.state.cellsY;
    const sizeZ=this.state.gridDepth/this.effectiveCellsZ;

    this.blobArray=[]; this.dataTable={};

    for (let z=0; z<this.effectiveCellsZ; z++){
      const yz=[]; for(let y=0; y<this.state.cellsY; y++){ const xz=[]; for(let x=0; x<this.state.cellsX; x++) xz.push([]); yz.push(xz); }
      this.blobArray.push(yz);
    }
    if (this.state.shapeType==='custom' && PARSE_SVG){
      this.svgShapes = PARSE_SVG(this.state.customSVGPath);
      this.createInterpolatedShapes();
    } else {
      this.svgShapes=[]; this.interpolatedShapes=[];
    }
    for (let z2=0; z2<this.effectiveCellsZ; z2++){
      for (let y2=0; y2<this.state.cellsY; y2++){
        for (let x2=0; x2<this.state.cellsX; x2++){
          const cx=x2*sizeX + sizeX/2 - this.state.gridWidth/2 + this.position.x;
          const cy=y2*sizeY + sizeY/2 - this.state.gridHeight/2 + this.position.y;
          const cz=z2*sizeZ + sizeZ/2 - this.state.gridDepth/2 + this.position.z;
          if (this.sdf(new THREE.Vector3(cx,cy,cz), z2) >= 0) this.blobArray[z2][y2][x2]=null;
        }
      }
    }
  }
  // ---------- Update grid (dimensions/shape change) ----------
  async updateGrid(params){
    const oldDataTable={...this.dataTable};
    const oldPos=this.position.clone();
    const oX=this.state.cellsX,oY=this.state.cellsY,oZ=this.state.cellsZ,oF=this.state.fidelity;
    const oW=this.state.gridWidth,oH=this.state.gridHeight,oD=this.state.gridDepth;

    // state
    this.state.gridWidth  = params.gridWidth  || this.state.gridWidth;
    this.state.gridHeight = params.gridHeight || this.state.gridHeight;
    this.state.gridDepth  = params.gridDepth  || this.state.gridDepth;
    this.state.cellsX     = params.cellsX     || this.state.cellsX;
    this.state.cellsY     = params.cellsY     || this.state.cellsY;
    this.state.cellsZ     = params.cellsZ     || this.state.cellsZ;
    this.state.fidelity   = params.fidelity   || this.state.fidelity;
    this.state.shapeType  = params.shapeType  || this.state.shapeType;
    this.state.customSVGPath = params.customSVGPath || this.state.customSVGPath;

    if (Array.isArray(params.fieldNames) && params.fieldNames.length){
      await this.evolveSchema(params.fieldNames);
      this.fieldForViz = this.fieldForViz && this.schema.index.has(this.fieldForViz) ? this.fieldForViz : this.schema.fieldNames[0];
    }

    this.effectiveCellsZ = this.state.cellsZ * this.state.fidelity;

    // reseed nuclei
    this._nuclei=new Array(this.effectiveCellsZ);
    { const w=this.state.cellsX,h=this.state.cellsY,dir=params?.propagationDir||{x:1,y:0}; const pick=()=>pickNucleusByDirection(w,h,dir); for(let z=0; z<this.effectiveCellsZ; z++) this._nuclei[z]=pick(); }

    // reset caches
    this._layerCache.clear();
    this._dirtyLayers.clear();
    if (this._flushHandle){ clearTimeout(this._flushHandle); this._flushHandle=null; }

    this.initializeGrid();

    // carry O2 sparse (legacy) roughly mapped
    const sXo=oW/oX, sYo=oH/oY, sZo=oD/(oZ*oF);
    const sXn=this.state.gridWidth/this.state.cellsX, sYn=this.state.gridHeight/this.state.cellsY, sZn=this.state.gridDepth/this.effectiveCellsZ;
    for (const k in oldDataTable){
      const [xO,yO,zO]=k.split(',').map(Number);
      const cx=xO*sXo+sXo/2 - oW/2 + oldPos.x;
      const cy=yO*sYo+sYo/2 - oH/2 + oldPos.y;
      const cz=zO*sZo+sZo/2 - oD/2 + oldPos.z;
      const xi=Math.floor((cx - (this.position.x - this.state.gridWidth/2))/sXn);
      const yi=Math.floor((cy - (this.position.y - this.state.gridHeight/2))/sYn);
      const zi=Math.floor((cz - (this.position.z - this.state.gridDepth/2))/sZn);
      if (xi>=0&&xi<this.state.cellsX && yi>=0&&yi<this.state.cellsY && zi>=0&&zi<this.effectiveCellsZ){
        const d=this.getCellData(xi,yi,zi), cur=d?(d.O2||0):0;
        this.setCellData(xi,yi,zi,{O2:cur}, true);
      }
    }

    // persist meta
    if (this._db){
      await idbPut(this._db, STORE_META, 'layout', {
        w:this.state.cellsX, h:this.state.cellsY, layers:this.effectiveCellsZ,
        denseW:DENSE_W, denseH:DENSE_H,
        shapeType:this.state.shapeType||'',
        gw:this.state.gridWidth, gh:this.state.gridHeight, gd:this.state.gridDepth
      });
      await idbPut(this._db, STORE_META, 'schema', { id:this.schema.id, fields:this.schema.fieldNames });
      for(let z=0; z<this.effectiveCellsZ; z++){
        const n=this._nuclei[z];
        await idbPut(this._db, STORE_META, `z:${z}`, {cx:n.x, cy:n.y, w:this.state.cellsX, h:this.state.cellsY, rule:'dir'});
      }
      await this._ensureZeroTemplate();
    }

    this.visualizeGrid();
    this.saveState();
    this.saveBlobs();
  }

  // ---------- Position ----------
  updatePosition(p){
    this.position.set(
      safeNum(p.x,this.position.x),
      safeNum(p.y,this.position.y),
      safeNum(p.z,this.position.z)
    );
    if (this.gridGroup){
      this.scene.remove(this.gridGroup);
      this.gridGroup.traverse(o=>{ if(o.geometry)o.geometry.dispose(); if(o.material)o.material.dispose(); });
    }
    this.gridGroup=null; this.instancedMesh=null;
    this.visualizeGrid();
    this.saveState();
  }

  // ---------- Misc ----------
  zLayerIndexFromWorldZ(zWorld){
    const fine=this.state.gridDepth/this.effectiveCellsZ;
    const zLocal=zWorld - (this.position.z - this.state.gridDepth/2);
    let zi=Math.floor(zLocal/fine);
    if (zi<0) zi=0; if (zi>=this.effectiveCellsZ) zi=this.effectiveCellsZ-1;
    return zi;
  }

  // legacy sparse getters
  getCellData(x,y,z){
    if (x<0||x>=this.state.cellsX||y<0||y>=this.state.cellsY||z<0||z>=this.effectiveCellsZ) return null;
    const key=`${x},${y},${z}`;
    return this.dataTable[key] || this.envVariables.reduce((o,k)=>{o[k]=0; return o;}, {});
  }
  setCellData(x,y,z,values,skipSave=false){
    if (x<0||x>=this.state.cellsX||y<0||y>=this.state.cellsY||z<0||z>=this.effectiveCellsZ) return false;
    const key=`${x},${y},${z}`;
    const cur=this.dataTable[key] || this.envVariables.reduce((o,k)=>{o[k]=0; return o;}, {});
    const upd={...cur, ...values};
    const allZero=this.envVariables.every(k => (upd[k]||0)===0);
    if (allZero) delete this.dataTable[key];
    else {
      this.dataTable[key]=upd;
      if (upd.O2) this._maxO2=Math.max(this._maxO2, upd.O2);
    }
    if (!skipSave) this.saveBlobs();
    return true;
  }

  updateDispersion(dt){
    const now=performance.now();
    if (now - this._lastDispersionUpdate < 1000) return;
    this._lastDispersionUpdate = now;

    const decay=Math.exp(-this.decayRate);
    let maxO2=1;
    for (const key in this.dataTable){
      const d=this.dataTable[key];
      if (d.O2){
        const v=d.O2*decay;
        if (v<0.01) delete this.dataTable[key];
        else { this.dataTable[key].O2=v; maxO2=Math.max(maxO2, v); }
      }
    }
    this._maxO2=maxO2;
  }

  setVisible(v){ if (this.gridGroup) this.gridGroup.visible=v; }
  dispose(){
    this._disposed=true;
    this._rev++;
    if (this._flushHandle){ clearTimeout(this._flushHandle); this._flushHandle=null; }
    if (this.gridGroup){
      this.scene.remove(this.gridGroup);
      this.gridGroup.traverse(o=>{ if(o.geometry)o.geometry.dispose(); if(o.material)o.material.dispose(); });
      this.gridGroup=null; this.instancedMesh=null;
    }
    this._layerCache.clear();
    this._dirtyLayers.clear();
    SDFGrid._instances?.delete(this.uid);
  }

  // ---------- Console helpers ----------
  static async layerInfo(uid, z){
    const m=this._instances?.get(uid); if(!m) return null;
    const arr = await m._ensureDenseLayer(z);
    return {
      uid: m.uid, z,
      denseW: DENSE_W, denseH: DENSE_H,
      fields: m.schema.fieldNames.slice(),
      bytes: arr.byteLength, floats: arr.length
    };
  }
  static async readCell(uid, z, x, y){
    const m=this._instances?.get(uid); if(!m) return null;
    const arr = await m._ensureDenseLayer(z);
    const F = m.schema.fieldNames.length;
    const { bx, by } = m._mapCellToDense(z, x, y);
    const base = ((by*DENSE_W)+bx)*F;
    const out = {};
    for (let i=0;i<F;i++) out[m.schema.fieldNames[i]] = arr[base+i] || 0;
    return out;
  }
  static centerCell(uid, z){
    const m=this._instances?.get(uid); if(!m) return null;
    return m.getNucleus(z);
  }
}

Object.assign(SDFGrid.prototype, {
  saveState,
  saveLogic,
  saveBlobs,
  applyBlobs,
  createInterpolatedShapes,
  sdf,
  sdfGrad,
  compileLogic,
  _ensureZeroTemplate,
  _ensureBaseSDF,
  getBaseDistance,
  _denseIdx,
  _ensureDenseLayer,
  _mapCellToDense,
  _applySparseIntoDense,
  setDenseFromCell,
  addDenseFromCell,
  sampleDenseForCell,
  _flushDirtyLayers,
  updateParticles,
  visualizeGrid,
  _valueToColor,
  updateVisualization
});

Object.assign(SDFGrid, {
  loadState,
  loadLogic,
  loadBlobs
});
