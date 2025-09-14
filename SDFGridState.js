import { safeNum } from './utils.js';
import { SVGPathParser } from './svgParser.js';
import { DENSE_W, DENSE_H, STORE_META } from './SDFGridConstants.js';
import { idbPut } from './SDFGridStorage.js';
import { pickNucleusByDirection } from './SDFGridNucleus.js';

const PARSE_SVG = SVGPathParser?.parseSVGPaths || null;

export function getNucleus(z){
  const zi=Math.min(Math.max(z|0,0), this.effectiveCellsZ-1);
  const n=this._nuclei[zi];
  if (n && Number.isInteger(n.x) && Number.isInteger(n.y)) return {x:n.x,y:n.y,z:zi};
  return {x:(this.state.cellsX>>1)-1, y:(this.state.cellsY>>1)-1, z:zi};
}

export function centerCellIndex(z,mode='nucleus'){
  const zi=Math.min(Math.max(z|0,0), this.effectiveCellsZ-1);
  if (mode==='nucleus') return this.getNucleus(zi);
  return {x:(this.state.cellsX>>1)-1, y:(this.state.cellsY>>1)-1, z:zi};
}

export function toStateJSON(){
  return {
    state:this.state,
    position:{x:this.position.x,y:this.position.y,z:this.position.z},
    effectiveCellsZ:this.effectiveCellsZ, ts:Date.now(), uid:this.uid,
    envVariables:this.envVariables, trailStrength:this.trailStrength, decayRate:this.decayRate
  };
}

export function initializeGrid(){
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

export async function updateGrid(params){
  const oldDataTable={...this.dataTable};
  const oldPos=this.position.clone();
  const oX=this.state.cellsX,oY=this.state.cellsY,oZ=this.state.cellsZ,oF=this.state.fidelity;
  const oW=this.state.gridWidth,oH=this.state.gridHeight,oD=this.state.gridDepth;

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

  this._nuclei=new Array(this.effectiveCellsZ);
  { const w=this.state.cellsX,h=this.state.cellsY,dir=params?.propagationDir||{x:1,y:0}; const pick=()=>pickNucleusByDirection(w,h,dir); for(let z=0; z<this.effectiveCellsZ; z++) this._nuclei[z]=pick(); }

  this._layerCache.clear();
  this._dirtyLayers.clear();
  if (this._flushHandle){ clearTimeout(this._flushHandle); this._flushHandle=null; }

  this.initializeGrid();

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
      const d=this.getCellData(xi,yi,zi);
      const idxO2=this.envVariables.indexOf('O2');
      const cur=d ? d[idxO2] || 0 : 0;
      const arr=new Array(this.envVariables.length).fill(0);
      if (idxO2!==-1) arr[idxO2]=cur;
      this.setCellData(xi,yi,zi,arr, true);
    }
  }

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

export function updatePosition(p){
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

export function zLayerIndexFromWorldZ(zWorld){
  const fine=this.state.gridDepth/this.effectiveCellsZ;
  const zLocal=zWorld - (this.position.z - this.state.gridDepth/2);
  let zi=Math.floor(zLocal/fine);
  if (zi<0) zi=0; if (zi>=this.effectiveCellsZ) zi=this.effectiveCellsZ-1;
  return zi;
}

export function getCellData(x,y,z){
  if (x<0||x>=this.state.cellsX||y<0||y>=this.state.cellsY||z<0||z>=this.effectiveCellsZ) return null;
  const key=`${x},${y},${z}`;
  return this.dataTable[key] || new Array(this.envVariables.length).fill(0);
}

export function setCellData(x,y,z,values,skipSave=false){
  if (x<0||x>=this.state.cellsX||y<0||y>=this.state.cellsY||z<0||z>=this.effectiveCellsZ) return false;
  const key=`${x},${y},${z}`;
  const F=this.envVariables.length;
  const cur=this.dataTable[key] ? this.dataTable[key].slice() : new Array(F).fill(0);
  if (Array.isArray(values)){
    for (let i=0;i<Math.min(F, values.length); i++) cur[i]=values[i];
  } else {
    for (let i=0;i<F;i++){
      const name=this.envVariables[i];
      const v=values[name];
      if (v!=null) cur[i]=v;
    }
  }
  const allZero=cur.every(v => (v||0)===0);
  if (allZero) delete this.dataTable[key];
  else {
    this.dataTable[key]=cur;
    const idxO2=this.envVariables.indexOf('O2');
    if (idxO2!==-1 && cur[idxO2]) this._maxO2=Math.max(this._maxO2, cur[idxO2]);
  }
  if (!skipSave) this.saveBlobs();
  return true;
}

export function updateDispersion(dt){
  const now=performance.now();
  if (now - this._lastDispersionUpdate < 1000) return;
  this._lastDispersionUpdate = now;

  const idxO2=this.envVariables.indexOf('O2');
  if (idxO2===-1) return;
  const decay=Math.exp(-this.decayRate);
  let maxO2=1;
  for (const key in this.dataTable){
    const arr=this.dataTable[key];
    const val=arr[idxO2] || 0;
    if (val){
      const v=val*decay;
      if (v<0.01) delete this.dataTable[key];
      else { arr[idxO2]=v; maxO2=Math.max(maxO2, v); }
    }
  }
  this._maxO2=maxO2;
}

export function setVisible(v){ if (this.gridGroup) this.gridGroup.visible=v; }

export function dispose(){
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
  this.constructor._instances?.delete(this.uid);
}
