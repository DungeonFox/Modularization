import { lsSet, lsGet, updateRegistrySaved, logicKey, stateKey, blobsKey } from './utils.js';

export function saveState(){
  lsSet(stateKey(this.uid), this.toStateJSON());
  updateRegistrySaved(this.uid);
}

export function saveLogic(){
  const payload={ enabled:this.logic.enabled, preset:this.logic.preset, forceScale:this.logic.forceScale, code:this.logic.code };
  lsSet(logicKey(this.uid), payload);
}

export function saveBlobs(){
  const sparse=[];
  for (let z=0; z<this.effectiveCellsZ; z++){
    for (let y=0; y<this.state.cellsY; y++){
      for (let x=0; x<this.state.cellsX; x++){
        const cell=(this.blobArray[z] && this.blobArray[z][y] && this.blobArray[z][y][x]) ? this.blobArray[z][y][x] : null;
        const key=`${x},${y},${z}`; const data=this.dataTable[key];
        if ((cell && cell.length) || data){
          const particles = cell && cell.length ? cell.map(p=>({
            o:[p.offset.x,p.offset.y,p.offset.z],
            v:[p.velocity.x,p.velocity.y,p.velocity.z],
            q:[p.orientation.x,p.orientation.y,p.orientation.z,p.orientation.w],
            d:(p.d!=null?p.d:1),
            t:(p.t!=null?p.t:0)
          })) : [];
          const entry={ x,y,z, particles };
          if (data) entry.data=Array.from(data);
          sparse.push(entry);
        }
      }
    }
  }
  lsSet(blobsKey(this.uid), {
    layout:{ w:this.state.gridWidth, h:this.state.gridHeight, d:this.state.gridDepth, cx:this.state.cellsX, cy:this.state.cellsY, cz:this.effectiveCellsZ },
    envVariables:this.envVariables, data:sparse, ts:Date.now(), uid:this.uid
  });
  updateRegistrySaved(this.uid);
}

export function loadState(uid){ return lsGet(stateKey(uid)); }
export function loadLogic(uid){ return lsGet(logicKey(uid)); }
export function loadBlobs(uid){
  const saved=lsGet(blobsKey(uid));
  if (!saved || !saved.layout || !saved.data || !Array.isArray(saved.data)) return null;
  const valid=saved.data.filter(c=>c && typeof c.x==='number' && typeof c.y==='number' && typeof c.z==='number');
  return { layout:saved.layout, envVariables:saved.envVariables || ['O2','CO2','H2O'], data:valid };
}

export function applyBlobs(blobs){
  if (!blobs || !blobs.layout || !blobs.data ||
      blobs.layout.cx!==this.state.cellsX || blobs.layout.cy!==this.state.cellsY || blobs.layout.cz!==this.effectiveCellsZ){
    for (let z=0; z<this.effectiveCellsZ; z++){
      const yz=[]; for(let y=0; y<this.state.cellsY; y++){ const xz=[]; for(let x=0; x<this.state.cellsX; x++) xz.push([]); yz.push(xz); }
      this.blobArray[z]=yz;
    }
    return;
  }
  this.envVariables = blobs.envVariables || this.envVariables;
  this.dataTable = {};
  blobs.data.forEach(cell=>{
    if (!cell || typeof cell.x!=='number' || typeof cell.y!=='number' || typeof cell.z!=='number') return;
    const {x,y,z,particles,data} = cell;
    if (x>=0 && x<this.state.cellsX && y>=0 && y<this.state.cellsY && z>=0 && z<this.effectiveCellsZ){
      if (particles && Array.isArray(particles)){
        this.blobArray[z][y][x] = particles.map(p=>({
          offset:new THREE.Vector3(p.o[0],p.o[1],p.o[2]),
          velocity:new THREE.Vector3(p.v[0],p.v[1],p.v[2]),
          orientation:new THREE.Quaternion(p.q[0],p.q[1],p.q[2],p.q[3]),
          density:p.d!=null?p.d:1,
          time:p.t!=null?p.t:0
        }));
      }
      if (data){
        const arr=new Float32Array(this.schema.fieldNames.length);
        if (Array.isArray(data)){
          for(let i=0;i<Math.min(data.length, arr.length); i++) arr[i]=data[i]||0;
        } else if (typeof data==='object'){
          for (const [name,val] of Object.entries(data)){
            const fi=this.schema.index.get(name);
            if (fi!=null) arr[fi]=val;
          }
        }
        const key=`${x},${y},${z}`;
        this.dataTable[key] = arr;
        const o2Idx=this.schema.index.get('O2');
        if (o2Idx!=null && arr[o2Idx]) this._maxO2 = Math.max(this._maxO2, arr[o2Idx]);
      }
    }
  });
}
