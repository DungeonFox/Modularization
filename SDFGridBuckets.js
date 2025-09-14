import { DENSE_W, DENSE_H, STORE_META } from './SDFGridConstants.js';
import { arraysEqual } from './SDFGridUtil.js';
import { openBucketLC, openFieldDB, idbGet, idbPut } from './SDFGridStorage.js';
import { pickNucleusByDirection } from './SDFGridNucleus.js';

export async function _initBuckets(dir){
  this._bucket = await openBucketLC(this.bucketNameLC);
  if (!this._bucket){ console.warn('Storage Buckets unavailable'); return; }
  this._db = await openFieldDB(this._bucket);

  const layoutVal = {
    w:this.state.cellsX, h:this.state.cellsY, layers:this.effectiveCellsZ,
    denseW:DENSE_W, denseH:DENSE_H,
    shapeType:this.state.shapeType||'',
    gw:this.state.gridWidth, gh:this.state.gridHeight, gd:this.state.gridDepth
  };
  await idbPut(this._db, STORE_META, 'layout', layoutVal);

  const curSchema = await idbGet(this._db, STORE_META, 'schema');
  if (!curSchema || !arraysEqual(curSchema.fields||[], this.schema.fieldNames)){
    await idbPut(this._db, STORE_META, 'schema', { id:this.schema.id, fields:this.schema.fieldNames });
  } else {
    this.schema.id = curSchema.id|0;
    this.schema.fieldNames = curSchema.fields.slice();
    this.schema.index = new Map(this.schema.fieldNames.map((n,i)=>[n,i]));
  }

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

  await this._ensureZeroTemplate();

  // Persist any overlay data captured before buckets were initialized
  if (this._dirtyLayers?.size) await this._flushDirtyLayers();
}
