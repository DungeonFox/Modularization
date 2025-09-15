import { DENSE_W, DENSE_H, STORE_BASE, STORE_BASEZ, STORE_LAYER, STORE_LMETA, DEFAULT_QUADRANT_COUNT } from './SDFGridConstants.js';
import { arraysEqual } from './SDFGridUtil.js';
import { idbGet, idbPut, idbDelete } from './SDFGridStorage.js';
import { createSparseQuadrants, denseFromQuadrants } from './SDFGridQuadrants.js';

const QUADRANT_WRITE_ATTEMPTS = 3;

function _normalizeQuadrantCount(ctx, desired){
  const current=Number.isFinite(ctx.quadrantCount) && ctx.quadrantCount>0 ? ctx.quadrantCount|0 : DEFAULT_QUADRANT_COUNT;
  let next=Number.isFinite(desired) && desired>0 ? desired|0 : current;
  if (!Number.isFinite(next) || next<=0) next=DEFAULT_QUADRANT_COUNT;
  if (ctx.quadrantCount !== next){
    ctx.quadrantCount = next;
    ctx._quadLayout = null;
    ctx._quadKeyWidth = null;
  }
  return ctx.quadrantCount || DEFAULT_QUADRANT_COUNT;
}

function _quadrantKeyWidth(ctx){
  if (ctx._quadKeyWidth) return ctx._quadKeyWidth;
  const qCount=ctx.quadrantCount || DEFAULT_QUADRANT_COUNT;
  const maxIndex=Math.max(0, qCount-1);
  const width=Math.max(1, String(maxIndex).length);
  ctx._quadKeyWidth = width;
  return width;
}

function _layerQuadrantKey(ctx, z, qi){
  const width=_quadrantKeyWidth(ctx);
  const layer=z|0;
  const idx=Math.max(0, qi|0);
  return `${layer},${idx.toString().padStart(width,'0')}`;
}

async function _getStoredQuadrant(ctx, z, qi){
  if (!ctx._db) return { buffer:null, legacyKey:null };
  const primaryKey=_layerQuadrantKey(ctx, z, qi);
  let buf=await idbGet(ctx._db, STORE_LAYER, primaryKey);
  if (buf) return { buffer:buf, legacyKey:null };
  const legacyKey=`${z|0},${qi|0}`;
  if (legacyKey===primaryKey) return { buffer:null, legacyKey:null };
  buf=await idbGet(ctx._db, STORE_LAYER, legacyKey);
  return { buffer:buf, legacyKey:buf ? legacyKey : null };
}

function _quadrantLayout(count){
  const cols=Math.ceil(Math.sqrt(count));
  const rows=Math.ceil(count/cols);
  const qW=Math.ceil(DENSE_W/cols);
  const qH=Math.ceil(DENSE_H/rows);
  return { cols, rows, qW, qH };
}

function _ensureLayout(ctx){
  ctx._quadLayout ||= _quadrantLayout(ctx.quadrantCount || DEFAULT_QUADRANT_COUNT);
  return ctx._quadLayout;
}

function _quadrantIndex(bx, by){
  const { cols, qW, qH } = _ensureLayout(this);
  const col=Math.floor(bx / qW);
  const row=Math.floor(by / qH);
  return row*cols + col;
}

function _sliceQuadrant(arr, qi, F){
  const { cols, qW, qH } = _ensureLayout(this);
  const col=qi%cols, row=Math.floor(qi/cols);
  const xStart=col*qW, yStart=row*qH;
  const xEnd=Math.min(xStart+qW, DENSE_W);
  const yEnd=Math.min(yStart+qH, DENSE_H);
  const qw=xEnd-xStart, qh=yEnd-yStart;
  const out=new Float32Array(qw*qh*F);
  let idx=0;
  for(let y=yStart;y<yEnd;y++){
    const rowBase=y*DENSE_W*F;
    for(let x=xStart;x<xEnd;x++){
      const base=rowBase + x*F;
      for(let fi=0;fi<F;fi++) out[idx++]=arr[base+fi];
    }
  }
  return out;
}

function _insertQuadrant(arr, qi, quad, F){
  const { cols, qW, qH } = _ensureLayout(this);
  const col=qi%cols, row=Math.floor(qi/cols);
  const xStart=col*qW, yStart=row*qH;
  const xEnd=Math.min(xStart+qW, DENSE_W);
  const yEnd=Math.min(yStart+qH, DENSE_H);
  const qw=xEnd-xStart;
  let idx=0;
  for(let y=yStart;y<yEnd;y++){
    const rowBase=y*DENSE_W*F;
    for(let x=xStart;x<xEnd;x++){
      const base=rowBase + x*F;
      for(let fi=0;fi<F;fi++) arr[base+fi]=quad[idx++];
    }
  }
}

async function _writeQuadrantWithRetry(ctx, z, qi, arr, F){
  if (!ctx._db) return;
  const key=_layerQuadrantKey(ctx, z, qi);
  for (let attempt=0; attempt<QUADRANT_WRITE_ATTEMPTS; attempt++){
    const quad=_sliceQuadrant.call(ctx, arr, qi, F);
    const expectedBytes=quad.byteLength;
    await idbPut(ctx._db, STORE_LAYER, key, quad.buffer);
    const stored=await idbGet(ctx._db, STORE_LAYER, key);
    const storedBytes=stored instanceof ArrayBuffer ? stored.byteLength : (stored?.byteLength ?? stored?.buffer?.byteLength ?? 0);
    if (storedBytes === expectedBytes) return;
  }
  throw new Error(`Failed to persist quadrant ${key} after ${QUADRANT_WRITE_ATTEMPTS} attempts`);
}

async function _storeQuadrantsSequential(ctx, z, arr, F, indices){
  if (!ctx._db) return;
  _ensureLayout(ctx);
  const qCount=ctx.quadrantCount || DEFAULT_QUADRANT_COUNT;
  const baseList=indices ? Array.from(indices) : Array.from({length:qCount},(_,i)=>i);
  const list=baseList.filter(i=>Number.isFinite(i) && i>=0 && i<qCount);
  if (!list.length) return;
  list.sort((a,b)=>a-b);
  let prev=-1;
  for (const qi of list){
    if (qi===prev) continue;
    prev=qi;
    await _writeQuadrantWithRetry(ctx, z, qi, arr, F);
  }
}

function _markDirty(ctx, z, bx, by){
  const qi=_quadrantIndex.call(ctx, bx, by);
  let set=ctx._dirtyLayers.get(z|0);
  if(!set){ set=new Set(); ctx._dirtyLayers.set(z|0,set); }
  set.add(qi);
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

export async function _ensureDenseLayer(z){
  const layer=z|0;
  if (this._layerCache.has(layer)) return this._layerCache.get(layer);

  const targetSchema=this.schema;
  const Fnew=targetSchema.fieldNames.length;

  if (!this._db){
    const arr=new Float32Array(DENSE_W*DENSE_H*Fnew);
    this._layerCache.set(layer,arr); return arr;
  }

  const tmpl=await this._ensureZeroTemplate();
  const tmplCount=Array.isArray(tmpl?.quadrants) ? tmpl.quadrants.length : 0;
  const qCount=_normalizeQuadrantCount(this, tmplCount || this.quadrantCount || DEFAULT_QUADRANT_COUNT);
  _ensureLayout(this);

  const lmeta=await idbGet(this._db, STORE_LMETA, layer);
  const buffers=new Array(qCount);
  const missing=[];
  const legacyIndices=[];
  const legacyDeleteKeys=[];
  for(let i=0;i<qCount;i++){
    const { buffer, legacyKey } = await _getStoredQuadrant(this, layer, i);
    buffers[i]=buffer;
    if (!buffer) missing.push(i);
    if (legacyKey){
      legacyIndices.push(i);
      legacyDeleteKeys.push(legacyKey);
    }
  }

  const allMissing=missing.length===qCount;

  if (allMissing){
    const arr=denseFromQuadrants(tmpl, targetSchema);
    await this._applySparseIntoDense(z, arr);
    await _storeQuadrantsSequential(this, layer, arr, Fnew);
    await idbPut(this._db, STORE_LMETA, layer, { sid:targetSchema.id, fields:targetSchema.fieldNames });
    if (legacyDeleteKeys.length){
      const deleteKeys=Array.from(new Set(legacyDeleteKeys));
      await Promise.all(deleteKeys.map(key=>idbDelete(this._db, STORE_LAYER, key)));
    }
    this._layerCache.set(layer,arr); return arr;
  }

  const curSid=lmeta?.sid|0;
  const curList=lmeta?.fields || [];
  if (curSid === targetSchema.id && arraysEqual(curList, targetSchema.fieldNames)){
    const needTemplate=missing.length || legacyIndices.length;
    const arr=needTemplate ? denseFromQuadrants(tmpl, targetSchema) : new Float32Array(DENSE_W*DENSE_H*Fnew);
    for(let i=0;i<qCount;i++){
      const buf=buffers[i]; if(!buf) continue;
      _insertQuadrant.call(this, arr, i, new Float32Array(buf), Fnew);
    }
    if (needTemplate){
      const rewrite=new Set([...missing, ...legacyIndices]);
      if (missing.length) await this._applySparseIntoDense(z, arr);
      await _storeQuadrantsSequential(this, layer, arr, Fnew, rewrite);
      await idbPut(this._db, STORE_LMETA, layer, { sid:targetSchema.id, fields:targetSchema.fieldNames });
      if (legacyDeleteKeys.length){
        const deleteKeys=Array.from(new Set(legacyDeleteKeys));
        await Promise.all(deleteKeys.map(key=>idbDelete(this._db, STORE_LAYER, key)));
      }
    }
    this._layerCache.set(layer,arr); return arr;
  }

  const Fold=curList.length;
  const oldIdx=new Map(curList.map((n,i)=>[n,i]));
  const arr=denseFromQuadrants(tmpl, targetSchema);
  const { cols, qW, qH } = _ensureLayout(this);

  for(let qi=0; qi<qCount; qi++){
    const buf=buffers[qi]; if(!buf) continue;
    const quadOld=new Float32Array(buf);
    const col=qi%cols, row=Math.floor(qi/cols);
    const xStart=col*qW, yStart=row*qH;
    const xEnd=Math.min(xStart+qW, DENSE_W);
    const yEnd=Math.min(yStart+qH, DENSE_H);
    const qw=xEnd-xStart;
    let idx=0;
    for(let y=yStart;y<yEnd;y++){
      const rowBase=y*DENSE_W*Fnew;
      for(let x=xStart;x<xEnd;x++){
        const baseNew=rowBase + x*Fnew;
        const baseOld=idx*Fold;
        for (const [name, fiNew] of targetSchema.index){
          const fiOld=oldIdx.get(name);
          if (fiOld!=null) arr[baseNew+fiNew]=quadOld[baseOld+fiOld];
        }
        idx++;
      }
    }
  }

  await _storeQuadrantsSequential(this, layer, arr, Fnew);
  await idbPut(this._db, STORE_LMETA, layer, { sid:targetSchema.id, fields:targetSchema.fieldNames });
  if (legacyDeleteKeys.length){
    const deleteKeys=Array.from(new Set(legacyDeleteKeys));
    await Promise.all(deleteKeys.map(key=>idbDelete(this._db, STORE_LAYER, key)));
  }
  this._layerCache.set(layer,arr); return arr;
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
  _markDirty(this, z, bx, by);
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
  _markDirty(this, z, bx, by);
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
  if (!this._db || !this._dirtyLayers.size){ this._flushHandle=null; return; }
  const entries=Array.from(this._dirtyLayers.entries());
  this._dirtyLayers.clear();
  _ensureLayout(this);
  await Promise.all(entries.map(async ([z,set])=>{
    const arr=this._layerCache.get(z|0);
    if (!arr) return;
    const F=this.schema.fieldNames.length;
    const indices=Array.from(set);
    if (indices.length){
      await _storeQuadrantsSequential(this, z|0, arr, F, indices);
    }
    await idbPut(this._db, STORE_LMETA, z|0, { sid:this.schema.id, fields:this.schema.fieldNames });
  }));
  this._flushHandle=null;
}

