import { DENSE_W, DENSE_H, STORE_BASE, STORE_BASEZ, STORE_LAYER, STORE_LMETA, DEFAULT_QUADRANT_COUNT } from './SDFGridConstants.js';
import { arraysEqual } from './SDFGridUtil.js';
import { idbGet, idbPut } from './SDFGridStorage.js';
import { createSparseQuadrants } from './SDFGridQuadrants.js';

function quadrantLayout(count){
  const cols=Math.ceil(Math.sqrt(count));
  const rows=Math.ceil(count/cols);
  const qW=Math.ceil(DENSE_W/cols);
  const qH=Math.ceil(DENSE_H/rows);
  return {cols,rows,qW,qH};
}

function quadrantBounds(layout,i){
  const col=i%layout.cols; const row=Math.floor(i/layout.cols);
  const xStart=col*layout.qW; const yStart=row*layout.qH;
  const xEnd=Math.min(xStart+layout.qW, DENSE_W);
  const yEnd=Math.min(yStart+layout.qH, DENSE_H);
  return {xStart,yStart,xEnd,yEnd,w:xEnd-xStart,h:yEnd-yStart};
}

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

export async function _ensureQuadrantLayer(z,qIdx){
  const key=`${z}:${qIdx}`;
  if (this._quadrantCache.has(key)) return this._quadrantCache.get(key);

  const tmpl=await this._ensureZeroTemplate();
  const qCount=tmpl.quadrants.length;
  const layout=quadrantLayout(qCount);
  const bounds=quadrantBounds(layout,qIdx);
  const F=this.schema.fieldNames.length;
  let arr;

  if (!this._db){
    arr=new Float32Array(bounds.w*bounds.h*F);
  } else {
    const lmeta=await idbGet(this._db, STORE_LMETA, z|0);
    const buf=await idbGet(this._db, STORE_LAYER, key);
    if (buf){
      const curSid=lmeta?.sid|0;
      const curList=lmeta?.fields||[];
      if (curSid===this.schema.id && arraysEqual(curList,this.schema.fieldNames)){
        arr=new Float32Array(buf);
      } else {
        const old=new Float32Array(buf);
        const Fold=curList.length; const Fnew=this.schema.fieldNames.length;
        arr=new Float32Array(bounds.w*bounds.h*Fnew);
        const oldIdx=new Map(curList.map((n,i)=>[n,i]));
        for(let y=0;y<bounds.h;y++){
          const rowOld=y*bounds.w*Fold; const rowNew=y*bounds.w*Fnew;
          for(let x=0;x<bounds.w;x++){
            const baseOld=rowOld+x*Fold; const baseNew=rowNew+x*Fnew;
            for (const [name,fiNew] of this.schema.index){
              const fiOld=oldIdx.get(name); if (fiOld!=null) arr[baseNew+fiNew]=old[baseOld+fiOld];
            }
          }
        }
        await idbPut(this._db, STORE_LAYER, key, arr.buffer);
        await idbPut(this._db, STORE_LMETA, z|0, { sid:this.schema.id, fields:this.schema.fieldNames });
      }
    } else {
      arr=new Float32Array(bounds.w*bounds.h*F);
      const quad=tmpl.quadrants[qIdx]||{};
      const entries=Object.entries(quad);
      for(let y=0;y<bounds.h;y++){
        const row=y*bounds.w*F;
        for(let x=0;x<bounds.w;x++){
          const base=row+x*F;
          for (const [name,val] of entries){
            const fi=this.schema.index.get(name); if (fi!=null) arr[base+fi]=val;
          }
        }
      }
      for (const k in this.dataTable){
        const parts=k.split(',');
        const zi=Number(parts[2]||-1); if (zi!==(z|0)) continue;
        const cx=Number(parts[0]), cy=Number(parts[1]);
        if (cx<0||cy<0||cx>=this.state.cellsX||cy>=this.state.cellsY) continue;
        const { bx, by } = this._mapCellToDense(z,cx,cy);
        if (bx<bounds.xStart||bx>=bounds.xEnd||by<bounds.yStart||by>=bounds.yEnd) continue;
        const lx=bx-bounds.xStart, ly=by-bounds.yStart;
        const base=(ly*bounds.w+lx)*F; const src=this.dataTable[k];
        for (const [name,v] of Object.entries(src)){
          const fi=this.schema.index.get(name); if (fi!=null) arr[base+fi]=v||0;
        }
      }
      await idbPut(this._db, STORE_LAYER, key, arr.buffer);
      await idbPut(this._db, STORE_LMETA, z|0, { sid:this.schema.id, fields:this.schema.fieldNames });
    }
  }

  this._quadrantCache.set(key,arr);
  return arr;
}

export async function _ensureDenseLayer(z){
  const key=z|0;
  if (this._layerCache.has(key)) return this._layerCache.get(key);

  const tmpl=await this._ensureZeroTemplate();
  const qCount=tmpl.quadrants.length;
  const layout=quadrantLayout(qCount);
  const F=this.schema.fieldNames.length;
  const out=new Float32Array(DENSE_W*DENSE_H*F);

  for(let q=0;q<qCount;q++){
    const bounds=quadrantBounds(layout,q);
    const qa=await this._ensureQuadrantLayer(z,q);
    const w=bounds.w,h=bounds.h;
    for(let y=0;y<h;y++){
      const rowOut=(bounds.yStart+y)*DENSE_W*F + bounds.xStart*F;
      const rowQ=y*w*F;
      out.set(qa.subarray(rowQ,rowQ+w*F), rowOut);
    }
  }
  this._layerCache.set(key,out);
  return out;
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

export async function setDenseFromCell(z, xCell, yCell, values){
  const { bx, by } = this._mapCellToDense(z, xCell, yCell);
  const layout=quadrantLayout(this.quadrantCount || DEFAULT_QUADRANT_COUNT);
  const qx=Math.floor(bx/layout.qW);
  const qy=Math.floor(by/layout.qH);
  const qIdx=qy*layout.cols+qx;
  const bounds=quadrantBounds(layout,qIdx);
  const lx=bx-bounds.xStart, ly=by-bounds.yStart;
  const key=`${z}:${qIdx}`;
  const qArr=await this._ensureQuadrantLayer(z,qIdx);
  const F=this.schema.fieldNames.length;
  const base=(ly*bounds.w + lx)*F;
  for (const [name,v] of Object.entries(values)){
    const fi=this.schema.index.get(name); if (fi==null) continue;
    qArr[base+fi]=v;
    if (this._layerCache.has(z|0)){
      const arr=this._layerCache.get(z|0); arr[this._denseIdx(F,bx,by,fi)]=v;
    }
    this._maxField[name] = Math.max(this._maxField[name]||0, v||0);
    if (name==='O2') this._maxO2=Math.max(this._maxO2, v||0);
  }
  this._dirtyQuadrants.add(key);
  if (!this._flushHandle) this._flushHandle=setTimeout(()=>this._flushDirtyLayers(),200);
}

export async function addDenseFromCell(z, xCell, yCell, values){
  const { bx, by } = this._mapCellToDense(z, xCell, yCell);
  const layout=quadrantLayout(this.quadrantCount || DEFAULT_QUADRANT_COUNT);
  const qx=Math.floor(bx/layout.qW);
  const qy=Math.floor(by/layout.qH);
  const qIdx=qy*layout.cols+qx;
  const bounds=quadrantBounds(layout,qIdx);
  const lx=bx-bounds.xStart, ly=by-bounds.yStart;
  const key=`${z}:${qIdx}`;
  const qArr=await this._ensureQuadrantLayer(z,qIdx);
  const F=this.schema.fieldNames.length;
  const base=(ly*bounds.w + lx)*F;
  for (const [name,inc] of Object.entries(values)){
    const fi=this.schema.index.get(name); if (fi==null) continue;
    const nxt=(qArr[base+fi]||0)+inc;
    qArr[base+fi]=nxt;
    if (this._layerCache.has(z|0)){
      const arr=this._layerCache.get(z|0); arr[this._denseIdx(F,bx,by,fi)]=nxt;
    }
    this._maxField[name]=Math.max(this._maxField[name]||0,nxt);
    if (name==='O2') this._maxO2=Math.max(this._maxO2,nxt);
  }
  this._dirtyQuadrants.add(key);
  if (!this._flushHandle) this._flushHandle=setTimeout(()=>this._flushDirtyLayers(),200);
}

export async function sampleDenseForCell(z, xCell, yCell, field){
  const fi=this.schema.index.get(field); if (fi==null) return 0;
  const { bx, by } = this._mapCellToDense(z, xCell, yCell);
  const layout=quadrantLayout(this.quadrantCount || DEFAULT_QUADRANT_COUNT);
  const qx=Math.floor(bx/layout.qW);
  const qy=Math.floor(by/layout.qH);
  const qIdx=qy*layout.cols+qx;
  const bounds=quadrantBounds(layout,qIdx);
  const lx=bx-bounds.xStart, ly=by-bounds.yStart;
  const qArr=await this._ensureQuadrantLayer(z,qIdx);
  const F=this.schema.fieldNames.length;
  return qArr[(ly*bounds.w + lx)*F + fi] || 0;
}

export async function _flushDirtyLayers(){
  if (this._disposed){ this._flushHandle=null; return; }
  if (!this._db || !this._dirtyQuadrants.size){ this._flushHandle=null; return; }
  const keys=Array.from(this._dirtyQuadrants);
  this._dirtyQuadrants.clear();
  const zSet=new Set();
  await Promise.all(keys.map(async k=>{
    const arr=this._quadrantCache.get(k);
    if (arr) await idbPut(this._db, STORE_LAYER, k, arr.buffer);
    const z=Number(k.split(':')[0]); zSet.add(z);
  }));
  await Promise.all(Array.from(zSet, z=>idbPut(this._db, STORE_LMETA, z, { sid:this.schema.id, fields:this.schema.fieldNames })));
  this._flushHandle=null;
}

