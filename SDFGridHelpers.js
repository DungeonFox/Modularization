// Helper functions and constants for SDFGrid

// Fixed dense resolution per layer
export const DENSE_W = 1024;
export const DENSE_H = 1024;

// IDB in Storage Buckets
export const IDB_NAME    = 'SDFFieldDB';
export const IDB_VERSION = 7;

export const STORE_META  = 'meta';
export const STORE_BASE  = 'base';        // Int16 SDF per-layer (kept)
export const STORE_BASEZ = 'base_zero';   // Float32 zero template per schemaId
export const STORE_LAYER = 'overlay_layers';
export const STORE_LMETA = 'overlay_layers_meta';

export function normalizeUID(u){
  if (typeof u === 'string') return u;
  if (u && typeof u === 'object'){
    if (typeof u.uid === 'string') return u.uid;
    if (typeof u.id  === 'string') return u.id;
  }
  return String(u ?? 'grid');
}

export function normalizeBucketName(x){
  const s = normalizeUID(x).toLowerCase()
    .replace(/[^a-z0-9-]/g,'-')
    .replace(/^-+|-+$/g,'')
    .slice(0,63);
  return s.length>=3 ? s : `g-${Date.now().toString(36)}`;
}

export async function openBucketLC(nameLC){
  if (!nameLC || !navigator.storageBuckets) return null;
  return navigator.storageBuckets.open(nameLC);
}

export function openFieldDB(bucket){
  return new Promise((res,rej)=>{
    const req=bucket.indexedDB.open(IDB_NAME, IDB_VERSION);
    req.onupgradeneeded=e=>{
      const db=e.target.result;
      if (!db.objectStoreNames.contains(STORE_META))  db.createObjectStore(STORE_META);
      if (!db.objectStoreNames.contains(STORE_BASE))  db.createObjectStore(STORE_BASE);
      if (!db.objectStoreNames.contains(STORE_LAYER)) db.createObjectStore(STORE_LAYER);
      if (!db.objectStoreNames.contains(STORE_LMETA)) db.createObjectStore(STORE_LMETA);
      if (!db.objectStoreNames.contains(STORE_BASEZ)) db.createObjectStore(STORE_BASEZ); // NEW
    };
    req.onsuccess=()=>res(req.result);
    req.onerror =()=>rej(req.error);
  });
}

export function idbGet(db,store,key){
  return new Promise((res,rej)=>{
    const tx=db.transaction(store,'readonly'), st=tx.objectStore(store);
    const rq=st.get(key); rq.onsuccess=()=>res(rq.result ?? null); rq.onerror=()=>rej(rq.error);
  });
}

export function idbPut(db,store,key,val){
  return new Promise((res,rej)=>{
    const tx=db.transaction(store,'readwrite'), st=tx.objectStore(store);
    const rq=st.put(val,key); rq.onsuccess=()=>res(true); rq.onerror=()=>rej(rq.error);
  });
}

export function arraysEqual(a,b){
  if (a===b) return true; if (!a||!b) return false; if (a.length!==b.length) return false;
  for (let i=0;i<a.length;i++) if (a[i]!==b[i]) return false; return true;
}

// nucleus selection for even dims via propagation direction
export function pickNucleusByDirection(w,h,dir){
  const pivot={x:(w-1)/2,y:(h-1)/2}, cx=w>>1, cy=h>>1;
  const C=[{x:cx-1,y:cy-1},{x:cx-1,y:cy},{x:cx,y:cy-1},{x:cx,y:cy}];
  const dlen=Math.hypot(dir?.x||0,dir?.y||0)||1, dx=(dir?.x||0)/dlen, dy=(dir?.y||0)/dlen;
  let best=C[0], score=-Infinity;
  for(const c of C){
    const ccx=c.x+0.5, ccy=c.y+0.5, ox=ccx-pivot.x, oy=ccy-pivot.y, olen=Math.hypot(ox,oy)||1;
    const s=(ox/olen)*dx + (oy/olen)*dy;
    if(s>score){ score=s; best=c; }
  }
  return best;
}

