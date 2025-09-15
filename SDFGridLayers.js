import { DENSE_W, DENSE_H, STORE_BASE, STORE_BASEZ, STORE_LAYER, STORE_LMETA, DEFAULT_QUADRANT_COUNT } from './SDFGridConstants.js';
import { arraysEqual } from './SDFGridUtil.js';
import { idbGet, idbPut } from './SDFGridStorage.js';
import { createSparseQuadrants } from './SDFGridQuadrants.js';

export async function _ensureZeroTemplate(){
  const count = this.quadrantCount || DEFAULT_QUADRANT_COUNT;
  if (!this._db) return createSparseQuadrants(count, this.envExpressions || []);
  const key=`sid:${this.schema.id}`;
  let tmpl=await idbGet(this._db, STORE_BASEZ, key);
  if (!tmpl){
    tmpl=createSparseQuadrants(count, this.envExpressions || []);
    await idbPut(this._db, STORE_BASEZ, key, tmpl);
  }
  return tmpl;
}

export async function _ensureBaseSDF(z){
  if (!this._db) return null;
  const W=this.state.cellsX, H=this.state.cellsY;
  const key=z|0;
  const buf=await idbGet(this._db, STORE_BASE, key);
  if (buf) return new Int16Array(buf);

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

export async function getBaseDistance(z,x,y){
  const W=this.state.cellsX,H=this.state.cellsY;
  if(!this._db || x<0||y<0||x>=W||y>=H) return 0;
  const arr=await this._ensureBaseSDF(z);
  return arr ? arr[y*W+x]/1000.0 : 0;
}

export function _denseIdx(F,xPix,yPix,fi){
  return ((yPix*DENSE_W)+xPix)*F + fi;
}

function _quadrantLayout(count){
  const cols=Math.ceil(Math.sqrt(count));
  const rows=Math.ceil(count/cols);
  const qW=Math.ceil(DENSE_W/cols);
  const qH=Math.ceil(DENSE_H/rows);
  return { cols, rows, qW, qH, total:cols*rows };
}

export function _mapDenseToQuadrant(layer, bx, by){
  const qcol=Math.min(layer.cols-1, Math.floor(bx/layer.qW));
  const qrow=Math.min(layer.rows-1, Math.floor(by/layer.qH));
  const qi=qrow*layer.cols+qcol;
  const qx=bx - qcol*layer.qW;
  const qy=by - qrow*layer.qH;
  return { qi, qx, qy };
}

export async function _ensureDenseLayer(z){
  const key=z|0;
  if (this._layerCache.has(key)) return this._layerCache.get(key);

  const targetSchema=this.schema;
  const tmpl=await this._ensureZeroTemplate();
  const qCount=this.quadrantCount || DEFAULT_QUADRANT_COUNT;
  const { cols, rows, qW, qH, total }=_quadrantLayout(qCount);
  const Fnew=targetSchema.fieldNames.length;
  const layer={ quads:new Array(total), cols, rows, qW, qH };

  if (!this._db){
    for(let i=0;i<total;i++) layer.quads[i]=new Float32Array(qW*qH*Fnew);
    this._layerCache.set(key,layer);
    return layer;
  }

  const lmeta=await idbGet(this._db, STORE_LMETA, key);
  const curSid=lmeta?.sid|0;
  const curList=lmeta?.fields || [];
  const curQ=lmeta?.qCount || qCount;
  const { qW:qW0, qH:qH0, total:tot0 }=_quadrantLayout(curQ);

  for(let qi=0; qi<total; qi++){
    const buf=await idbGet(this._db, STORE_LAYER, `${key},${qi}`);
    let arr;
    if (buf && curSid===targetSchema.id && arraysEqual(curList, targetSchema.fieldNames) && tot0===total && qW0===qW && qH0===qH){
      arr=new Float32Array(buf);
    } else if (buf){
      const old=new Float32Array(buf);
      const Fold=curList.length;
      arr=new Float32Array(qW*qH*Fnew);
      const oldIdx=new Map(curList.map((n,i)=>[n,i]));
      const cellCount=Math.min(old.length/Fold, qW*qH);
      for(let i=0;i<cellCount;i++){
        const baseOld=i*Fold;
        const baseNew=i*Fnew;
        for (const [name, fiNew] of targetSchema.index){
          const fiOld=oldIdx.get(name);
          if (fiOld!=null) arr[baseNew+fiNew]=old[baseOld+fiOld];
        }
      }
    } else {
      const quad=tmpl.quadrants[qi] || {};
      arr=new Float32Array(qW*qH*Fnew);
      const entries=Object.entries(quad);
      if (entries.length){
        for(let y=0;y<qH;y++){
          const row=y*qW*Fnew;
          for(let x=0;x<qW;x++){
            const base=row+x*Fnew;
            for(const [name,val] of entries){
              const fi=targetSchema.index.get(name);
              if (fi!=null) arr[base+fi]=val;
            }
          }
        }
      }
    }
    layer.quads[qi]=arr;
  }

  await this._applySparseIntoDense(z, layer);
  if (this._db){
    await Promise.all(layer.quads.map((arr,qi)=>idbPut(this._db, STORE_LAYER, `${key},${qi}`, arr.buffer)));
    await idbPut(this._db, STORE_LMETA, key, { sid:targetSchema.id, fields:targetSchema.fieldNames, qCount:layer.quads.length });
  }
  this._layerCache.set(key,layer);
  return layer;
}

export function _mapCellToDense(z, x, y){
  const w=this.state.cellsX, h=this.state.cellsY;
  const nuc=this.getNucleus(z);
  const dx=x - nuc.x;
  const dy=y - nuc.y;
  const sx=DENSE_W / Math.max(1,w);
  const sy=DENSE_H / Math.max(1,h);
  const baseC={ x:(DENSE_W>>1)-1, y:(DENSE_H>>1)-1 };
  const bx=Math.max(0, Math.min(DENSE_W-1, baseC.x + Math.round(dx * sx)));
  const by=Math.max(0, Math.min(DENSE_H-1, baseC.y + Math.round(dy * sy)));
  return { bx, by };
}

export async function _applySparseIntoDense(z, layer){
  const F=this.schema.fieldNames.length;
  const applyFields=this.schema.fieldNames;
  for (const key in this.dataTable){
    const parts=key.split(',');
    const zi=Number(parts[2]||-1);
    if (zi !== (z|0)) continue;
    const x=Number(parts[0]), y=Number(parts[1]);
    if (x<0||x>=this.state.cellsX||y<0||y>=this.state.cellsY) continue;
    const { bx, by } = this._mapCellToDense(z, x, y);
    const { qi, qx, qy } = _mapDenseToQuadrant(layer, bx, by);
    const arr=layer.quads[qi];
    const base=((qy*layer.qW)+qx)*F;
    const src=this.dataTable[key];
    for (let fi=0; fi<F; fi++){
      const name=applyFields[fi];
      const v=src[name] || 0;
      if (v!==0) arr[base+fi]=v;
    }
  }
}

export async function setDenseFromCell(z, xCell, yCell, values){
  const layer=await this._ensureDenseLayer(z);
  const F=this.schema.fieldNames.length;
  const { bx, by } = this._mapCellToDense(z, xCell, yCell);
  const { qi, qx, qy } = _mapDenseToQuadrant(layer, bx, by);
  const arr=layer.quads[qi];
  const base=((qy*layer.qW)+qx)*F;
  for (const [name,v] of Object.entries(values)){
    const fi=this.schema.index.get(name); if (fi==null) continue;
    arr[base+fi] = v;
    this._maxField[name] = Math.max(this._maxField[name]||0, v||0);
    if (name==='O2') this._maxO2=Math.max(this._maxO2, v||0);
  }
  if (!this._dirtyLayers.has(z|0)) this._dirtyLayers.set(z|0,new Set());
  this._dirtyLayers.get(z|0).add(qi);
  if (!this._flushHandle) this._flushHandle=setTimeout(()=>this._flushDirtyLayers(), 200);
}

export async function addDenseFromCell(z, xCell, yCell, values){
  const layer=await this._ensureDenseLayer(z);
  const F=this.schema.fieldNames.length;
  const { bx, by } = this._mapCellToDense(z, xCell, yCell);
  const { qi, qx, qy } = _mapDenseToQuadrant(layer, bx, by);
  const arr=layer.quads[qi];
  const base=((qy*layer.qW)+qx)*F;
  for (const [name,inc] of Object.entries(values)){
    const fi=this.schema.index.get(name); if (fi==null) continue;
    const nxt=(arr[base+fi]||0) + inc;
    arr[base+fi] = nxt;
    this._maxField[name] = Math.max(this._maxField[name]||0, nxt);
    if (name==='O2') this._maxO2=Math.max(this._maxO2, nxt);
  }
  if (!this._dirtyLayers.has(z|0)) this._dirtyLayers.set(z|0,new Set());
  this._dirtyLayers.get(z|0).add(qi);
  if (!this._flushHandle) this._flushHandle=setTimeout(()=>this._flushDirtyLayers(), 200);
}

export async function sampleDenseForCell(z, xCell, yCell, field){
  const fi=this.schema.index.get(field); if (fi==null) return 0;
  const layer=await this._ensureDenseLayer(z);
  const F=this.schema.fieldNames.length;
  const { bx, by } = this._mapCellToDense(z, xCell, yCell);
  const { qi, qx, qy } = _mapDenseToQuadrant(layer, bx, by);
  const arr=layer.quads[qi];
  return arr ? (arr[((qy*layer.qW)+qx)*F + fi] || 0) : 0;
}

export async function _flushDirtyLayers(){
  if (this._disposed){ this._flushHandle=null; return; }
  if (!this._db || !this._dirtyLayers.size){ this._flushHandle=null; return; }
  const entries=Array.from(this._dirtyLayers.entries());
  this._dirtyLayers.clear();
  await Promise.all(entries.map(async ([z,qset])=>{
    const layer=this._layerCache.get(z|0);
    if (!layer) return;
    await Promise.all(Array.from(qset).map(async qi=>{
      const arr=layer.quads[qi];
      if (arr) await idbPut(this._db, STORE_LAYER, `${z|0},${qi}`, arr.buffer);
    }));
    await idbPut(this._db, STORE_LMETA, z|0, { sid:this.schema.id, fields:this.schema.fieldNames, qCount:layer.quads.length });
  }));
  this._flushHandle=null;
}

