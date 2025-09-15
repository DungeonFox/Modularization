import { DENSE_W, DENSE_H, STORE_BASE, STORE_BASEZ, STORE_LAYER, STORE_LMETA, DEFAULT_QUADRANT_COUNT } from './SDFGridConstants.js';
import { arraysEqual } from './SDFGridUtil.js';
import { idbGet, idbPut } from './SDFGridStorage.js';
import {
  createSparseQuadrants,
  denseFromQuadrants,
  quadrantIndexFromXY,
  applyQuadrantBuffer,
  extractQuadrantBuffer,
  quadrantBounds
} from './SDFGridQuadrants.js';

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
  const key = z|0;
  if (this._layerCache.has(key)) return this._layerCache.get(key);

  const targetSchema = this.schema;
  const tmpl = await this._ensureZeroTemplate();
  const arr = denseFromQuadrants(tmpl, targetSchema);

  if (!this._db){
    await this._applySparseIntoDense(z, arr);
    this._layerCache.set(key, arr); return arr;
  }

  const qCount = this.quadrantCount || DEFAULT_QUADRANT_COUNT;
  const lmeta = await idbGet(this._db, STORE_LMETA, key);
  const curSid = lmeta?.sid|0;
  const curList = lmeta?.fields || [];
  const sameSchema = curSid === targetSchema.id && arraysEqual(curList, targetSchema.fieldNames);
  const oldIdx = sameSchema ? null : new Map(curList.map((n,i)=>[n,i]));

  for (let qi=0; qi<qCount; qi++){
    const buf = await idbGet(this._db, STORE_LAYER, `${key}:${qi}`);
    if (!buf) continue;
    if (sameSchema){
      applyQuadrantBuffer(arr, targetSchema, buf, qi, qCount);
    } else {
      const src = new Float32Array(buf);
      const { xStart, yStart, xEnd, yEnd } = quadrantBounds(qi, qCount);
      const width = xEnd - xStart;
      const height = yEnd - yStart;
      const Fold = curList.length;
      const Fnew = targetSchema.fieldNames.length;
      for (let y=0; y<height; y++){
        const rowOld = y * width * Fold;
        const rowNew = (yStart + y) * DENSE_W * Fnew;
        for (let x=0; x<width; x++){
          const baseOld = rowOld + x * Fold;
          const baseNew = rowNew + (xStart + x) * Fnew;
          for (const [name, fiNew] of targetSchema.index){
            const fiOld = oldIdx.get(name);
            if (fiOld != null) arr[baseNew + fiNew] = src[baseOld + fiOld];
          }
        }
      }
    }
  }

  await this._applySparseIntoDense(z, arr);
  this._layerCache.set(key, arr); return arr;
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
  const qCount=this.quadrantCount || DEFAULT_QUADRANT_COUNT;
  const qi=quadrantIndexFromXY(bx, by, qCount);
  let set=this._dirtyLayers.get(z|0); if (!set){ set=new Set(); this._dirtyLayers.set(z|0,set); }
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
  const qCount=this.quadrantCount || DEFAULT_QUADRANT_COUNT;
  const qi=quadrantIndexFromXY(bx, by, qCount);
  let set=this._dirtyLayers.get(z|0); if (!set){ set=new Set(); this._dirtyLayers.set(z|0,set); }
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
  if (!this._db || !this._dirtyLayers.size){ this._flushHandle=null; return; }
  const entries=Array.from(this._dirtyLayers.entries());
  this._dirtyLayers.clear();
  const qCount=this.quadrantCount || DEFAULT_QUADRANT_COUNT;
  await Promise.all(entries.map(async ([z,set])=>{
    const arr=this._layerCache.get(z|0);
    if (arr){
      for (const qi of set){
        const buf=extractQuadrantBuffer(arr, this.schema, qi, qCount).buffer;
        await idbPut(this._db, STORE_LAYER, `${z}:${qi}`, buf);
      }
      await idbPut(this._db, STORE_LMETA, z|0, { sid:this.schema.id, fields:this.schema.fieldNames });
    }
  }));
  this._flushHandle=null;
}

