import { DENSE_W, DENSE_H, STORE_BASE, STORE_BASEZ, STORE_LAYER, STORE_LMETA, DEFAULT_QUADRANT_COUNT } from './SDFGridConstants.js';
import { arraysEqual } from './SDFGridUtil.js';
import { idbGet, idbPut } from './SDFGridStorage.js';
import { createSparseQuadrants, denseFromQuadrants } from './SDFGridQuadrants.js';

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

export async function _ensureDenseLayer(z){
  const key=z|0;
  if (this._layerCache.has(key)) return this._layerCache.get(key);

  const targetSchema=this.schema;
  const F=targetSchema.fieldNames.length;
  if (!this._db){
    const arr=new Float32Array(DENSE_W*DENSE_H*F);
    this._layerCache.set(key,arr); return arr;
  }

  const lmeta=await idbGet(this._db, STORE_LMETA, key);
  const curSid=lmeta?.sid|0;
  const curList=lmeta?.fields || [];
  const qCount=this.quadrantCount || DEFAULT_QUADRANT_COUNT;
  const tmpl=await this._ensureZeroTemplate();
  const arr=denseFromQuadrants(tmpl, targetSchema);

  const promises=[];
  for(let qi=0; qi<qCount; qi++) promises.push(idbGet(this._db, STORE_LAYER, `${key}:${qi}`));
  const bufs=await Promise.all(promises);

  if (curSid === targetSchema.id && arraysEqual(curList, targetSchema.fieldNames)){
    for (let qi=0; qi<qCount; qi++){
      const buf=bufs[qi]; if(!buf) continue;
      const quad=new Float32Array(buf);
      const col=qi % this._qCols, row=Math.floor(qi/this._qCols);
      const xStart=col*this._qW, yStart=row*this._qH;
      const xEnd=Math.min(xStart+this._qW, DENSE_W);
      const yEnd=Math.min(yStart+this._qH, DENSE_H);
      const w=xEnd-xStart, h=yEnd-yStart;
      for(let y=0;y<h;y++){
        const srcRow=y*w*F;
        const dstRow=(yStart+y)*DENSE_W*F;
        for(let x=0;x<w;x++){
          const srcBase=srcRow+x*F;
          const dstBase=dstRow + (xStart+x)*F;
          arr.set(quad.subarray(srcBase,srcBase+F), dstBase);
        }
      }
    }
  } else if (curList.length){
    const Fold=curList.length;
    const oldIdx=new Map(curList.map((n,i)=>[n,i]));
    for (let qi=0; qi<qCount; qi++){
      const buf=bufs[qi]; if(!buf) continue;
      const old=new Float32Array(buf);
      const col=qi % this._qCols, row=Math.floor(qi/this._qCols);
      const xStart=col*this._qW, yStart=row*this._qH;
      const xEnd=Math.min(xStart+this._qW, DENSE_W);
      const yEnd=Math.min(yStart+this._qH, DENSE_H);
      const w=xEnd-xStart, h=yEnd-yStart;
      for(let y=0;y<h;y++){
        const rowOld=y*w*Fold;
        const rowNew=(yStart+y)*DENSE_W*F;
        for(let x=0;x<w;x++){
          const baseOld=rowOld + x*Fold;
          const baseNew=rowNew + (xStart+x)*F;
          for (const [name, fiNew] of targetSchema.index){
            const fiOld=oldIdx.get(name);
            if (fiOld!=null) arr[baseNew+fiNew] = old[baseOld+fiOld];
          }
        }
      }
    }
    // persist converted quadrants
    for (let qi=0; qi<qCount; qi++){
      const col=qi % this._qCols, row=Math.floor(qi/this._qCols);
      const xStart=col*this._qW, yStart=row*this._qH;
      const xEnd=Math.min(xStart+this._qW, DENSE_W);
      const yEnd=Math.min(yStart+this._qH, DENSE_H);
      const w=xEnd-xStart, h=yEnd-yStart;
      const slice=new Float32Array(w*h*F);
      for(let y=0;y<h;y++){
        const srcRow=(yStart+y)*DENSE_W*F;
        const dstRow=y*w*F;
        for(let x=0;x<w;x++){
          const srcBase=srcRow + (xStart+x)*F;
          const dstBase=dstRow + x*F;
          slice.set(arr.subarray(srcBase,srcBase+F), dstBase);
        }
      }
      await idbPut(this._db, STORE_LAYER, `${key}:${qi}`, slice.buffer);
    }
  }

  if (curSid !== targetSchema.id || !arraysEqual(curList, targetSchema.fieldNames)){
    await idbPut(this._db, STORE_LMETA, key, { sid:targetSchema.id, fields:targetSchema.fieldNames });
  }

  await this._applySparseIntoDense(z, arr);
  this._layerCache.set(key,arr); return arr;
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

export async function _applySparseIntoDense(z, arr){
  const F=this.schema.fieldNames.length;
  const applyFields=this.schema.fieldNames;
  for (const key in this.dataTable){
    const parts=key.split(',');
    const zi=Number(parts[2]||-1);
    if (zi !== (z|0)) continue;
    const x=Number(parts[0]), y=Number(parts[1]);
    if (x<0||x>=this.state.cellsX||y<0||y>=this.state.cellsY) continue;
    const { bx, by } = this._mapCellToDense(z, x, y);
    const base=this._denseIdx(F, bx, by, 0);
    const src=this.dataTable[key];
    for (let fi=0; fi<F; fi++){
      const name=applyFields[fi];
      const v=src[name] || 0;
      if (v!==0) arr[base+fi]=v;
    }
  }
}

export async function setDenseFromCell(z, xCell, yCell, values){
  const arr=await this._ensureDenseLayer(z);
  const F=this.schema.fieldNames.length;
  const { bx, by } = this._mapCellToDense(z, xCell, yCell);
  const base=this._denseIdx(F, bx, by, 0);
  for (const [name,v] of Object.entries(values)){
    const fi=this.schema.index.get(name); if (fi==null) continue;
    arr[base+fi] = v;
    this._maxField[name] = Math.max(this._maxField[name]||0, v||0);
    if (name==='O2') this._maxO2=Math.max(this._maxO2, v||0);
  }
  const qx = Math.floor(bx / this._qW);
  const qy = Math.floor(by / this._qH);
  const qi = qy * this._qCols + qx;
  let set = this._dirtyQuadrants.get(z|0);
  if (!set){ set = new Set(); this._dirtyQuadrants.set(z|0, set); }
  set.add(qi);
  if (!this._flushHandle) this._flushHandle=setTimeout(()=>this._flushDirtyLayers(), 200);
}

export async function addDenseFromCell(z, xCell, yCell, values){
  const arr=await this._ensureDenseLayer(z);
  const F=this.schema.fieldNames.length;
  const { bx, by } = this._mapCellToDense(z, xCell, yCell);
  const base=this._denseIdx(F, bx, by, 0);
  for (const [name,inc] of Object.entries(values)){
    const fi=this.schema.index.get(name); if (fi==null) continue;
    const nxt=(arr[base+fi]||0) + inc;
    arr[base+fi] = nxt;
    this._maxField[name] = Math.max(this._maxField[name]||0, nxt);
    if (name==='O2') this._maxO2=Math.max(this._maxO2, nxt);
  }
  const qx = Math.floor(bx / this._qW);
  const qy = Math.floor(by / this._qH);
  const qi = qy * this._qCols + qx;
  let set = this._dirtyQuadrants.get(z|0);
  if (!set){ set = new Set(); this._dirtyQuadrants.set(z|0, set); }
  set.add(qi);
  if (!this._flushHandle) this._flushHandle=setTimeout(()=>this._flushDirtyLayers(), 200);
}

export async function sampleDenseForCell(z, xCell, yCell, field){
  const fi=this.schema.index.get(field); if (fi==null) return 0;
  const arr=await this._ensureDenseLayer(z);
  const F=this.schema.fieldNames.length;
  const { bx, by } = this._mapCellToDense(z, xCell, yCell);
  return arr[this._denseIdx(F, bx, by, fi)] || 0;
}

export async function _flushDirtyLayers(){
  if (this._disposed){ this._flushHandle=null; return; }
  if (!this._db || !this._dirtyQuadrants.size){ this._flushHandle=null; return; }
  const entries=Array.from(this._dirtyQuadrants.entries());
  this._dirtyQuadrants.clear();
  const F=this.schema.fieldNames.length;
  const cols=this._qCols, qW=this._qW, qH=this._qH;
  await Promise.all(entries.map(async ([z, qs])=>{
    const arr=this._layerCache.get(z|0);
    if (!arr) return;
    for (const qi of qs){
      const col = qi % cols;
      const row = Math.floor(qi / cols);
      const xStart = col * qW;
      const yStart = row * qH;
      const xEnd = Math.min(xStart + qW, DENSE_W);
      const yEnd = Math.min(yStart + qH, DENSE_H);
      const w = xEnd - xStart;
      const h = yEnd - yStart;
      const slice = new Float32Array(w*h*F);
      for (let y=0; y<h; y++){
        const srcRow=(yStart+y)*DENSE_W*F;
        const dstRow=y*w*F;
        for (let x=0; x<w; x++){
          const srcBase=srcRow + (xStart+x)*F;
          const dstBase=dstRow + x*F;
          slice.set(arr.subarray(srcBase, srcBase+F), dstBase);
        }
      }
      await idbPut(this._db, STORE_LAYER, `${z}:${qi}`, slice.buffer);
    }
    await idbPut(this._db, STORE_LMETA, z|0, { sid:this.schema.id, fields:this.schema.fieldNames });
  }));
  this._flushHandle=null;
}

