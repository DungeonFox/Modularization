import { DENSE_W, DENSE_H } from './SDFGridConstants.js';

export async function layerInfo(uid, z){
  const m=this._instances?.get(uid); if(!m) return null;
  const arr = await m._ensureDenseLayer(z);
  return {
    uid: m.uid, z,
    denseW: DENSE_W, denseH: DENSE_H,
    fields: m.schema.fieldNames.slice(),
    bytes: arr.byteLength, floats: arr.length
  };
}

export async function readCell(uid, z, x, y){
  const m=this._instances?.get(uid); if(!m) return null;
  const arr = await m._ensureDenseLayer(z);
  const F = m.schema.fieldNames.length;
  const { bx, by } = m._mapCellToDense(z, x, y);
  const base = ((by*DENSE_W)+bx)*F;
  const out = {};
  for (let i=0;i<F;i++) out[m.schema.fieldNames[i]] = arr[base+i] || 0;
  return out;
}

export function centerCell(uid, z){
  const m=this._instances?.get(uid); if(!m) return null;
  return m.getNucleus(z);
}
