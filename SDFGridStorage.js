// IndexedDB and Storage Bucket helpers for SDFGrid
import { IDB_NAME, IDB_VERSION, STORE_META, STORE_BASE, STORE_LAYER, STORE_LMETA, STORE_BASEZ } from './SDFGridConstants.js';

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
      if (!db.objectStoreNames.contains(STORE_BASEZ)) db.createObjectStore(STORE_BASEZ);
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
