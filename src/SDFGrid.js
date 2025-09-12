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

import { safeNum, clamp, lsSet, lsGet, updateRegistrySaved, logicKey, stateKey, blobsKey } from '../utils.js';
import { SVGPathParser } from '../svgParser.js';
import { presetCode } from '../logicPresets.js';
import { normalizeUID, normalizeBucketName, openBucketLC, openFieldDB, idbGet, idbPut, arraysEqual, pickNucleusByDirection } from './helpers.js';
import { DENSE_W, DENSE_H, STORE_META, STORE_BASE, STORE_BASEZ, STORE_LAYER, STORE_LMETA } from './constants.js';

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

  // ---------- Zero template (Float32 zeros in base_zero) ----------
  async _ensureZeroTemplate(){
    if (!this._db) return null;
    const F=this.schema.fieldNames.length;
    const key=`sid:${this.schema.id}`;
    let buf = await idbGet(this._db, STORE_BASEZ, key);
    if (!buf){
      const zeros = new Float32Array(DENSE_W*DENSE_H*F); // all zeros
      await idbPut(this._db, STORE_BASEZ, key, zeros.buffer);
      buf = zeros.buffer;
    }
    return buf;
  }

  // ---------- Base SDF per layer (mm Int16) ----------
  async _ensureBaseSDF(z){
    if (!this._db) return null;
    const W=this.state.cellsX, H=this.state.cellsY;
    const key=z|0;
    const buf=await idbGet(this._db, STORE_BASE, key);
    if (buf) return new Int16Array(buf);

    // compute SDF once
    const sx=this.state.gridWidth/W, sy=this.state.gridHeight/H, sz=this.state.gridDepth/this.effectiveCellsZ;
    const halfW=this.state.gridWidth/2, halfH=this.state.gridHeight/2, halfD=this.state.gridDepth/2;
    const arr=new Int16Array(W*H);
    let c=0;
    for(let y=0;y<H;y++){
      for(let x=0;x<W;x++){
        const cx=x*sx+sx/2-halfW + this.position.x;
        const cy=y*sy+sy/2-halfH + this.position.y;
        const cz=z*sz+sz/2-halfD + this.position.z;
        const d=this.sdf(new THREE.Vector3(cx,cy,cz), z);
        const q=Math.max(-32767, Math.min(32767, Math.round(d*1000)));
        arr[y*W+x]=q;
        if ((++c & 0xFFFF)===0) await Promise.resolve();
      }
    }
    await idbPut(this._db, STORE_BASE, key, arr.buffer);
    return arr;
  }
  async getBaseDistance(z,x,y){
    const W=this.state.cellsX,H=this.state.cellsY;
    if(!this._db || x<0||y<0||x>=W||y>=H) return 0;
    const arr=await this._ensureBaseSDF(z);
    return arr ? arr[y*W+x]/1000.0 : 0;
  }

  // ---------- Dense overlay per layer (Float32 interleaved) ----------
  _denseIdx(F,xPix,yPix,fi){ return ((yPix*DENSE_W)+xPix)*F + fi; }

  // First creation path:
  // 1) Clone zero template Float32Array (all zeros) into per-layer array.
  // 2) Apply any existing sparse cell data center-aligned; keep zeros as padding.
  async _ensureDenseLayer(z){
    const key=z|0;
    if (this._layerCache.has(key)) return this._layerCache.get(key);

    const targetSchema = this.schema;
    if (!this._db){
      const arr=new Float32Array(DENSE_W*DENSE_H*targetSchema.fieldNames.length);
      this._layerCache.set(key,arr); return arr;
    }

    const lmeta = await idbGet(this._db, STORE_LMETA, key);
    const buf   = await idbGet(this._db, STORE_LAYER, key);

    if (!buf){
      // create from zero template
      const tmplBuf = await this._ensureZeroTemplate();
      const arr = new Float32Array(tmplBuf.slice(0)); // clone
      // apply any preexisting sparse cell data into dense (center-aligned)
      await this._applySparseIntoDense(z, arr);
      await idbPut(this._db, STORE_LAYER, key, arr.buffer);
      await idbPut(this._db, STORE_LMETA, key, { sid:targetSchema.id, fields:targetSchema.fieldNames });
      this._layerCache.set(key,arr); return arr;
    }

    // have data; check schema
    const curSid  = lmeta?.sid|0;
    const curList = lmeta?.fields || [];
    if (curSid === targetSchema.id && arraysEqual(curList, targetSchema.fieldNames)){
      const arr=new Float32Array(buf);
      this._layerCache.set(key,arr); return arr;
    }

    // up-convert dense to new schema
    const old = new Float32Array(buf);
    const Fold = curList.length;
    const Fnew = targetSchema.fieldNames.length;
    const out = new Float32Array(DENSE_W*DENSE_H*Fnew);
    const oldIdx = new Map(curList.map((n,i)=>[n,i]));

    for (let y=0;y<DENSE_H;y++){
      const rowOld = y*DENSE_W*Fold;
      const rowNew = y*DENSE_W*Fnew;
      for (let x=0;x<DENSE_W;x++){
        const baseOld = rowOld + x*Fold;
        const baseNew = rowNew + x*Fnew;
        for (const [name, fiNew] of targetSchema.index){
          const fiOld = oldIdx.get(name);
          if (fiOld!=null) out[baseNew+fiNew] = old[baseOld+fiOld];
        }
      }
    }
    await idbPut(this._db, STORE_LAYER, key, out.buffer);
    await idbPut(this._db, STORE_LMETA, key, { sid:targetSchema.id, fields:targetSchema.fieldNames });
    this._layerCache.set(key,out); return out;
  }

  // Map logical cell (x,y) to dense pixel (bx,by) using nucleus-centered alignment
  _mapCellToDense(z, x, y){
    const w=this.state.cellsX, h=this.state.cellsY;
    const nuc=this.getNucleus(z); // logical nucleus
    const dx = x - nuc.x;
    const dy = y - nuc.y;
    const sx = DENSE_W / Math.max(1,w);
    const sy = DENSE_H / Math.max(1,h);
    const baseC = { x: (DENSE_W>>1)-1, y: (DENSE_H>>1)-1 };
    const bx = Math.max(0, Math.min(DENSE_W-1, baseC.x + Math.round(dx * sx)));
    const by = Math.max(0, Math.min(DENSE_H-1, baseC.y + Math.round(dy * sy)));
    return { bx, by };
  }

  // Apply existing sparse cell data to dense layer on first creation
  async _applySparseIntoDense(z, arr){
    const F = this.schema.fieldNames.length;
    const applyFields = this.schema.fieldNames; // use current schema order
    // Walk sparse table only for this z
    for (const key in this.dataTable){
      const parts = key.split(',');
      const zi = Number(parts[2]||-1);
      if (zi !== (z|0)) continue;
      const x = Number(parts[0]), y = Number(parts[1]);
      if (x<0||x>=this.state.cellsX||y<0||y>=this.state.cellsY) continue;
      const { bx, by } = this._mapCellToDense(z, x, y);
      const base = this._denseIdx(F, bx, by, 0);
      const src = this.dataTable[key];
      for (let fi=0; fi<F; fi++){
        const name = applyFields[fi];
        const v = src[name] || 0;
        if (v!==0) arr[base+fi] = v;
      }
    }
  }

  async setDenseFromCell(z, xCell, yCell, values){
    const arr = await this._ensureDenseLayer(z);
    const F = this.schema.fieldNames.length;
    const { bx, by } = this._mapCellToDense(z, xCell, yCell);
    const base = this._denseIdx(F, bx, by, 0);
    for (const [name,v] of Object.entries(values)){
      const fi=this.schema.index.get(name); if (fi==null) continue;
      arr[base+fi] = v;
      this._maxField[name] = Math.max(this._maxField[name]||0, v||0);
      if (name==='O2') this._maxO2=Math.max(this._maxO2, v||0);
    }
    this._dirtyLayers.add(z|0);
    if (!this._flushHandle) this._flushHandle=setTimeout(()=>this._flushDirtyLayers(), 200);
  }

  async addDenseFromCell(z, xCell, yCell, values){
    const arr = await this._ensureDenseLayer(z);
    const F = this.schema.fieldNames.length;
    const { bx, by } = this._mapCellToDense(z, xCell, yCell);
    const base = this._denseIdx(F, bx, by, 0);
    for (const [name,inc] of Object.entries(values)){
      const fi=this.schema.index.get(name); if (fi==null) continue;
      const nxt = (arr[base+fi]||0) + inc;
      arr[base+fi] = nxt;
      this._maxField[name] = Math.max(this._maxField[name]||0, nxt);
      if (name==='O2') this._maxO2=Math.max(this._maxO2, nxt);
    }
    this._dirtyLayers.add(z|0);
    if (!this._flushHandle) this._flushHandle=setTimeout(()=>this._flushDirtyLayers(), 200);
  }

  async sampleDenseForCell(z, xCell, yCell, field){
    const fi = this.schema.index.get(field); if (fi==null) return 0;
    const arr = await this._ensureDenseLayer(z);
    const F = this.schema.fieldNames.length;
    const { bx, by } = this._mapCellToDense(z, xCell, yCell);
    return arr[this._denseIdx(F, bx, by, fi)] || 0;
  }

  async _flushDirtyLayers(){
    if (this._disposed){ this._flushHandle=null; return; }
    if (!this._db || !this._dirtyLayers.size){ this._flushHandle=null; return; }
    const zs = Array.from(this._dirtyLayers);
    this._dirtyLayers.clear();
    await Promise.all(zs.map(async z=>{
      const arr=this._layerCache.get(z|0);
      if (arr) await idbPut(this._db, STORE_LAYER, z|0, arr.buffer);
      await idbPut(this._db, STORE_LMETA, z|0, { sid:this.schema.id, fields:this.schema.fieldNames });
    }));
    this._flushHandle=null;
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
  saveState(){ lsSet(stateKey(this.uid), this.toStateJSON()); updateRegistrySaved(this.uid); }
  saveLogic(){ const payload={ enabled:this.logic.enabled, preset:this.logic.preset, forceScale:this.logic.forceScale, code:this.logic.code }; lsSet(logicKey(this.uid), payload); }
  saveBlobs(){
    const sparse=[];
    for (let z=0; z<this.effectiveCellsZ; z++){
      for (let y=0; y<this.state.cellsY; y++){
        for (let x=0; x<this.state.cellsX; x++){
          const cell=(this.blobArray[z] && this.blobArray[z][y] && this.blobArray[z][y][x]) ? this.blobArray[z][y][x] : null;
          const key=`${x},${y},${z}`; const data=this.dataTable[key];
          if ((cell && cell.length) || data){
            const particles = cell && cell.length ? cell.map(p=>({
              o:[p.offset.x,p.offset.y,p.offset.z],
              v:[p.velocity.x,p.velocity.y,p.velocity.z],
              q:[p.orientation.x,p.orientation.y,p.orientation.z,p.orientation.w],
              d:(p.d!=null?p.d:1),
              t:(p.t!=null?p.t:0)
            })) : [];
            sparse.push({ x,y,z, particles, data });
          }
        }
      }
    }
    lsSet(blobsKey(this.uid), {
      layout:{ w:this.state.gridWidth, h:this.state.gridHeight, d:this.state.gridDepth, cx:this.state.cellsX, cy:this.state.cellsY, cz:this.effectiveCellsZ },
      envVariables:this.envVariables, data:sparse, ts:Date.now(), uid:this.uid
    });
    updateRegistrySaved(this.uid);
  }
  static loadState(uid){ return lsGet(stateKey(uid)); }
  static loadLogic(uid){ return lsGet(logicKey(uid)); }
  static loadBlobs(uid){
    const saved=lsGet(blobsKey(uid));
    if (!saved || !saved.layout || !saved.data || !Array.isArray(saved.data)) return null;
    const valid=saved.data.filter(c=>c && typeof c.x==='number' && typeof c.y==='number' && typeof c.z==='number');
    return { layout:saved.layout, envVariables:saved.envVariables || ['O2','CO2','H2O'], data:valid };
  }
  applyBlobs(blobs){
    if (!blobs || !blobs.layout || !blobs.data ||
        blobs.layout.cx!==this.state.cellsX || blobs.layout.cy!==this.state.cellsY || blobs.layout.cz!==this.effectiveCellsZ){
      for (let z=0; z<this.effectiveCellsZ; z++){
        const yz=[]; for(let y=0; y<this.state.cellsY; y++){ const xz=[]; for(let x=0; x<this.state.cellsX; x++) xz.push([]); yz.push(xz); }
        this.blobArray[z]=yz;
      }
      return;
    }
    this.envVariables = blobs.envVariables || this.envVariables;
    this.dataTable = {};
    blobs.data.forEach(cell=>{
      if (!cell || typeof cell.x!=='number' || typeof cell.y!=='number' || typeof cell.z!=='number') return;
      const {x,y,z,particles,data} = cell;
      if (x>=0 && x<this.state.cellsX && y>=0 && y<this.state.cellsY && z>=0 && z<this.effectiveCellsZ){
        if (particles && Array.isArray(particles)){
          this.blobArray[z][y][x] = particles.map(p=>({
            offset:new THREE.Vector3(p.o[0],p.o[1],p.o[2]),
            velocity:new THREE.Vector3(p.v[0],p.v[1],p.v[2]),
            orientation:new THREE.Quaternion(p.q[0],p.q[1],p.q[2],p.q[3]),
            density:p.d!=null?p.d:1,
            time:p.t!=null?p.t:0
          }));
        }
        if (data && typeof data==='object'){
          const key=`${x},${y},${z}`; this.dataTable[key] = { ...data };
          if (data.O2) this._maxO2 = Math.max(this._maxO2, data.O2);
        }
      }
    });
  }

  // ---------- SVG SDF ----------
  createInterpolatedShapes(){
    if (!this.svgShapes.length) return;
    this.interpolatedShapes=[];
    if (this.svgShapes.length===1){
      for (let z=0; z<this.effectiveCellsZ; z++) this.interpolatedShapes.push({ vertices:this.svgShapes[0].vertices });
    } else {
      for (let z2=0; z2<this.effectiveCellsZ; z2++){
        const t=(this.effectiveCellsZ<=1)?0:(z2/(this.effectiveCellsZ-1));
        const scaled=t*(this.svgShapes.length-1);
        const lo=Math.floor(scaled), hi=Math.min(lo+1,this.svgShapes.length-1);
        const lt=scaled-lo;
        if (lt<=1e-6){ this.interpolatedShapes.push({ vertices:this.svgShapes[lo].vertices }); }
        else{
          const a=this.svgShapes[lo].vertices, b=this.svgShapes[hi].vertices;
          const N=Math.max(a.length,b.length), verts=new Array(N);
          for (let i=0;i<N;i++){
            const v1=a[i<a.length?i:a.length-1], v2=b[i<b.length?i:b.length-1];
            verts[i]=new THREE.Vector2(v1.x+(v2.x-v1.x)*lt, v1.y+(v2.y-v1.y)*lt);
          }
          this.interpolatedShapes.push({ vertices:verts });
        }
      }
    }
  }
  pointInPoly(p,V){
    if (V.length<3) return false;
    let inside=false, j=V.length-1;
    for (let i=0;i<V.length;i++){
      const yi=V[i].y, yj=V[j].y, xi=V[i].x, xj=V[j].x;
      const inter=((yi>p.y)!==(yj>p.y)) && (p.x < (xj-xi)*(p.y-yi)/((yj-yi)||1e-12) + xi);
      if (inter) inside=!inside; j=i;
    }
    return inside;
  }
  distToPoly(p,V){
    if (V.length<2) return Infinity;
    let md=Infinity;
    for (let i=0;i<V.length;i++){
      const j=(i+1)%V.length, v1=V[i], v2=V[j];
      const A=p.x-v1.x, B=p.y-v1.y, C=v2.x-v1.x, D=v2.y-v1.y;
      const l2=C*C+D*D; const t=l2?Math.max(0,Math.min(1,(A*C+B*D)/l2)):0;
      const qx=v1.x+t*C, qy=v1.y+t*D;
      const d=Math.hypot(p.x-qx,p.y-qy);
      if (d<md) md=d;
    }
    return md;
  }
  sdf(point,zLayerIndex){
    const rel=point.clone().sub(this.position);
    const halfX=this.state.gridWidth/2, halfY=this.state.gridHeight/2, halfZ=this.state.gridDepth/2;
    if (this.state.shapeType==='cube'){
      const q=new THREE.Vector3(Math.abs(rel.x)-halfX, Math.abs(rel.y)-halfY, Math.abs(rel.z)-halfZ);
      return Math.max(q.x,q.y,q.z);
    }
    if (this.state.shapeType==='sphere'){
      const r=Math.max(this.state.gridWidth, this.state.gridHeight, this.state.gridDepth)/4;
      return rel.length()-r;
    }
    if (this.state.shapeType==='custom' && this.state.customSVGPath && PARSE_SVG){
      if (!this.interpolatedShapes.length){
        if (!this.svgShapes.length) this.svgShapes = PARSE_SVG(this.state.customSVGPath);
        this.createInterpolatedShapes();
      }
      const zi=clamp(zLayerIndex|0, 0, this.effectiveCellsZ-1);
      const s=this.interpolatedShapes[zi];
      if (!s || !s.vertices || s.vertices.length<3){
        const q2=new THREE.Vector3(Math.abs(rel.x)-halfX, Math.abs(rel.y)-halfY, Math.abs(rel.z)-halfZ);
        return Math.max(q2.x,q2.y,q2.z);
      }
      const s2=Math.min(this.state.gridWidth, this.state.gridHeight)/2;
      const p2=new THREE.Vector2(rel.x/s2, rel.y/s2);
      const inside=this.pointInPoly(p2, s.vertices);
      const d2=this.distToPoly(p2, s.vertices);
      const sd2=inside ? -d2 : d2;
      const zDist=Math.abs(rel.z)-halfZ;
      return Math.max(sd2*s2, zDist);
    }
    return Infinity;
  }
  sdfGrad(point,zLayerIndex){
    const e=1e-2;
    const dx=this.sdf(new THREE.Vector3(point.x+e,point.y,point.z),zLayerIndex)-this.sdf(new THREE.Vector3(point.x-e,point.y,point.z),zLayerIndex);
    const dy=this.sdf(new THREE.Vector3(point.x,point.y+e,point.z),zLayerIndex)-this.sdf(new THREE.Vector3(point.x,point.y-e,point.z),zLayerIndex);
    const dz=this.sdf(new THREE.Vector3(point.x,point.y,point.z+e),zLayerIndex)-this.sdf(new THREE.Vector3(point.x,point.y,point.z-e),zLayerIndex);
    return new THREE.Vector3(dx,dy,dz).multiplyScalar(1/(2*e));
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

  // ---------- Logic compile ----------
  compileLogic(src){
    src = src || "";
    try{
      const f = new Function('THREE','"use strict";\n'+src+'\n;return (typeof applyForce==="function")?applyForce:null;')(THREE);
      if (typeof f!=='function') throw 0;
      this.logic.compiled=(ctx)=>f(ctx); this.logic.compileError=null; this.logic.code=src; return true;
    }catch(e1){
      try{
        const F=(0,eval)('(function(THREE){"use strict";'+src+';return (typeof applyForce==="function")?applyForce:null;})');
        const g=F(THREE); if (typeof g!=='function') throw 0;
        this.logic.compiled=(ctx)=>g(ctx); this.logic.compileError=null; this.logic.code=src; return true;
      }catch(e2){
        this.logic.compiled=null; this.logic.compileError=(e1?.message||String(e1))+' | '+(e2?.message||String(e2)); return false;
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

  // ---------- Particles -> fields ----------
  async updateParticles(particles, dt){
    if (this._disposed) return;
    const rev=this._rev;

    const sX=this.state.gridWidth/this.state.cellsX;
    const sY=this.state.gridHeight/this.state.cellsY;
    const sZ=this.state.gridDepth/this.effectiveCellsZ;

    // clear legacy arrays
    for (let z=0; z<this.effectiveCellsZ; z++)
      for (let y=0; y<this.state.cellsY; y++)
        for (let x=0; x<this.state.cellsX; x++)
          if (this.blobArray[z][y][x]!==null) this.blobArray[z][y][x].length=0;

    const updated = new Map();

    for (let i=0;i<particles.length;i++){
      const p=particles[i];
      const zi=this.zLayerIndexFromWorldZ(p.position.z);
      const sd=this.sdf(p.position, zi);
      const inside = sd<0;
      const grad = this.sdfGrad(p.position, zi);

      if (this.logic.enabled && this.logic.compiled){
        try{
          this.logic.compiled({ p, dt, sd, inside, grad, center:this.position, zIndex:zi, uid:this.uid, forceScale:(typeof this.logic.forceScale==='number'?this.logic.forceScale:1), state:this.state });
        }catch{
          const v=this.position.clone().sub(p.position); const L=v.length()||1e-6; p.velocity.addScaledVector(v,0.2*dt/L);
        }
      } else {
        const v=this.position.clone().sub(p.position); const L=v.length()||1e-6; p.velocity.addScaledVector(v,0.2*dt/L);
        if (inside) p.velocity.multiplyScalar(0.995);
      }

      if (inside){
        const x=Math.floor((p.position.x - (this.position.x - this.state.gridWidth/2))/sX);
        const y=Math.floor((p.position.y - (this.position.y - this.state.gridHeight/2))/sY);
        const z=Math.floor((p.position.z - (this.position.z - this.state.gridDepth/2))/sZ);
        if (x>=0&&x<this.state.cellsX && y>=0&&y<this.state.cellsY && z>=0&&z<this.effectiveCellsZ && this.blobArray[z][y][x]!==null){
          const k=`${x},${y},${z}`; if (!updated.has(k)) updated.set(k,{x,y,z,count:0}); updated.get(k).count++;
        }
      }
    }

    // apply to dense overlay (increment all fields equally by default)
    for (const [,c] of updated){
      const inc = this.trailStrength * c.count;
      const vals = Object.fromEntries(this.schema.fieldNames.map(n=>[n,inc]));
      await this.addDenseFromCell(c.z, c.x, c.y, vals);
    }
    if (!this._flushHandle && this._dirtyLayers.size) this._flushHandle=setTimeout(()=>this._flushDirtyLayers(), 200);

    this.updateDispersion(dt);
    if (this._disposed || this._rev!==rev) return;
    await this.updateVisualization();

    const now=performance.now();
    if (now - this._lastBlobSave > 2000){
      this.saveBlobs();
      this._lastBlobSave = now;
    }
  }

  // ---------- Visualization ----------
  visualizeGrid(){
    if (this._disposed) return;
    const rev=this._rev;

    if (this.gridGroup){
      this.scene.remove(this.gridGroup);
      this.gridGroup.traverse(o=>{ if(o.geometry)o.geometry.dispose(); if(o.material)o.material.dispose(); });
    }

    const group=new THREE.Group();

    const sizeX=this.state.gridWidth/this.state.cellsX;
    const sizeZ=this.state.gridDepth/this.state.cellsZ;
    const halfW=this.state.gridWidth/2, halfD=this.state.gridDepth/2;
    const yBase=this.position.y - this.state.gridHeight/2;
    const yStep=this.state.gridHeight/this.effectiveCellsZ;

    // wire mesh
    const geo=new THREE.BufferGeometry(), verts=[], norms=[], cols=[], idxs=[];
    const col=new THREE.Color();
    for (let zL=0; zL<=this.effectiveCellsZ; zL++){
      const y=yBase - zL*yStep;
      for (let i=0;i<=this.state.cellsX;i++){
        const x=this.position.x - halfW + i*sizeX;
        for (let j=0;j<=this.state.cellsZ;j++){
          const z=this.position.z - halfD + j*sizeZ;
          verts.push(x,y,z); norms.push(0,1,0);
          col.setRGB(1,1,1,THREE.SRGBColorSpace); cols.push(col.r,col.g,col.b);
        }
      }
    }
    const stride=this.state.cellsZ+1;
    for (let zL=0; zL<this.effectiveCellsZ; zL++){
      const off=zL*(this.state.cellsX+1)*(this.state.cellsZ+1);
      for (let i=0;i<this.state.cellsX;i++){
        for (let j=0;j<this.state.cellsZ;j++){
          const a=off+i*stride+(j+1), b=off+i*stride+j, c=off+(i+1)*stride+j, d=off+(i+1)*stride+(j+1);
          idxs.push(a,b,d, b,c,d);
        }
      }
    }
    geo.setIndex(idxs);
    geo.setAttribute('position', new THREE.Float32BufferAttribute(verts,3));
    geo.setAttribute('normal',   new THREE.Float32BufferAttribute(norms,3));
    geo.setAttribute('color',    new THREE.Float32BufferAttribute(cols,3));
    const mat=new THREE.MeshBasicMaterial({ vertexColors:true, side:THREE.DoubleSide, transparent:true, opacity:0.12, wireframe:true });
    group.add(new THREE.Mesh(geo,mat));

    // instanced cells
    const boxG=new THREE.BoxGeometry(sizeX, this.state.gridHeight/this.state.cellsY, this.state.gridDepth/this.effectiveCellsZ);
    const boxM=new THREE.MeshBasicMaterial({ opacity:0.2, transparent:true, wireframe:true });
    const maxInst=this.state.cellsX*this.state.cellsY*this.effectiveCellsZ;
    const imesh=new THREE.InstancedMesh(boxG, boxM, maxInst);
    let id=0; const map=new Map();

    const halfWidth=this.state.gridWidth/2, halfHeight=this.state.gridHeight/2, halfDepth=this.state.gridDepth/2;
    for (let z2=0; z2<this.effectiveCellsZ; z2++){
      for (let y2=0; y2<this.state.cellsY; y2++){
        for (let x2=0; x2<this.state.cellsX; x2++){
          const cx=x2*sizeX + sizeX/2 - halfWidth + this.position.x;
          const cy=y2*(this.state.gridHeight/this.state.cellsY) + (this.state.gridHeight/this.state.cellsY)/2 - halfHeight + this.position.y;
          const cz=z2*(this.state.gridDepth/this.effectiveCellsZ) + (this.state.gridDepth/this.effectiveCellsZ)/2 - halfDepth + this.position.z;
          if (this.sdf(new THREE.Vector3(cx,cy,cz), z2) < 0){
            imesh.setMatrixAt(id, new THREE.Matrix4().setPosition(cx,cy,cz));
            map.set(`${x2},${y2},${z2}`, id);
            id++;
          }
        }
      }
    }
    imesh.count=id;
    imesh.instanceMap=map;
    group.add(imesh);

    if (this._disposed || this._rev!==rev){
      group.traverse(o=>{ if(o.geometry)o.geometry.dispose(); if(o.material)o.material.dispose(); });
      return;
    }

    this.instancedMesh=imesh;
    this.gridGroup=group;
    this.scene.add(this.gridGroup);

    this.updateVisualization();
  }

  _valueToColor(norm){
    if (norm<=0) return new THREE.Color(0.2,0.2,1.0);
    if (norm<0.5){ const t=norm*2; return new THREE.Color(0.2*(1-t), 0.2+0.8*t, 1.0*(1-t)); }
    const t=(norm-0.5)*2; return new THREE.Color(0.8*t+0.2*(1-t), 1.0*(1-t), 0);
  }

  async updateVisualization(){
    if (this._disposed) return;
    const im=this.instancedMesh;
    if (!im || !im.instanceMap) return;

    const field=this.fieldForViz;
    const fi=this.schema.index.get(field) ?? 0;
    let max = this._maxField[field] || 0; if (max<=0) max=1;

    // ensure all referenced dense layers are resident
    const needZ=new Set();
    for (const [key] of im.instanceMap){ const z=Number(key.split(',')[2]); needZ.add(z); }
    await Promise.all(Array.from(needZ, z=>this._ensureDenseLayer(z)));

    const F=this.schema.fieldNames.length;

    for (const [key,id] of im.instanceMap){
      const [x,y,z]=key.split(',').map(Number);
      const { bx, by } = this._mapCellToDense(z, x, y);
      const arr=this._layerCache.get(z|0);
      const val = arr ? (arr[this._denseIdx(F,bx,by,fi)] || 0) : 0;
      const norm=Math.min(1,(val<=0?0:val)/max);
      im.setColorAt(id, this._valueToColor(norm));
    }
    if (im.instanceColor) im.instanceColor.needsUpdate=true;
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
