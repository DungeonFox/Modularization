export async function updateParticles(particles, dt){
  if (this._disposed) return;
  const rev=this._rev;

  const sX=this.state.gridWidth/this.state.cellsX;
  const sY=this.state.gridHeight/this.state.cellsY;
  const sZ=this.state.gridDepth/this.effectiveCellsZ;

  for (let z=0; z<this.effectiveCellsZ; z++)
    for (let y=0; y<this.state.cellsY; y++)
      for (let x=0; x<this.state.cellsX; x++)
        if (this.blobArray[z][y][x]!==null) this.blobArray[z][y][x].length=0;

  const updated=new Map();

  for (let i=0;i<particles.length;i++){
    const p=particles[i];
    const zi=this.zLayerIndexFromWorldZ(p.position.z);
    const sd=this.sdf(p.position, zi);
    const inside=sd<0;
    const grad=this.sdfGrad(p.position, zi);

    if (this.logic.enabled && this.logic.compiled){
      try{
        this.logic.compiled({ p, dt, sd, inside, grad, center:this.position, zIndex:zi, uid:this.uid, forceScale:(typeof this.logic.forceScale==='number'?this.logic.forceScale:1), state:this.state });
      }catch{
        const v=this.position.clone().sub(p.position); const L=v.length()||1e-6; p.velocity.addScaledVector(v,0.2*dt/L);
      }
    } else {
      const v=this.position.clone().sub(p.position); const L=v.length()||1e-6; p.velocity.addScaledVector(v,0.2*dt/L);
      if (inside) p.velocity.multiplyScalar(0.995);
    }

    if (inside){
      const x=Math.floor((p.position.x - (this.position.x - this.state.gridWidth/2))/sX);
      const y=Math.floor((p.position.y - (this.position.y - this.state.gridHeight/2))/sY);
      const z=Math.floor((p.position.z - (this.position.z - this.state.gridDepth/2))/sZ);
      if (x>=0&&x<this.state.cellsX && y>=0&&y<this.state.cellsY && z>=0&&z<this.effectiveCellsZ && this.blobArray[z][y][x]!==null){
        const k=`${x},${y},${z}`; if (!updated.has(k)) updated.set(k,{x,y,z,count:0}); updated.get(k).count++;
      }
    }
  }

  for (const [,c] of updated){
    const inc=this.trailStrength * c.count;
    const vals=Object.fromEntries(this.schema.fieldNames.map(n=>[n,inc]));
    await this.addDenseFromCell(c.z, c.x, c.y, vals);
  }

  this.updateDispersion(dt);
  if (this._disposed || this._rev!==rev) return;
  await this.updateVisualization();

  const now=performance.now();
  if (now - this._lastBlobSave > 2000){
    this.saveBlobs().catch(()=>{});
    this._lastBlobSave = now;
  }
}

