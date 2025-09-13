export function visualizeGrid(){
  if (this._disposed) return;
  const rev=this._rev;

  if (this.gridGroup){
    this.scene.remove(this.gridGroup);
    this.gridGroup.traverse(o=>{ if(o.geometry)o.geometry.dispose(); if(o.material)o.material.dispose(); });
  }

  const group=new THREE.Group();

  const sizeX=this.state.gridWidth/this.state.cellsX;
  const sizeZ=this.state.gridDepth/this.state.cellsZ;
  const halfW=this.state.gridWidth/2, halfD=this.state.gridDepth/2;
  const yBase=this.position.y - this.state.gridHeight/2;
  const yStep=this.state.gridHeight/this.effectiveCellsZ;

  const geo=new THREE.BufferGeometry(), verts=[], norms=[], cols=[], idxs=[];
  const col=new THREE.Color();
  for (let zL=0; zL<=this.effectiveCellsZ; zL++){
    const y=yBase - zL*yStep;
    for (let i=0;i<=this.state.cellsX;i++){
      const x=this.position.x - halfW + i*sizeX;
      for (let j=0;j<=this.state.cellsZ;j++){
        const z=this.position.z - halfD + j*sizeZ;
        verts.push(x,y,z); norms.push(0,1,0);
        col.setRGB(1,1,1,THREE.SRGBColorSpace); cols.push(col.r,col.g,col.b);
      }
    }
  }
  const stride=this.state.cellsZ+1;
  for (let zL=0; zL<this.effectiveCellsZ; zL++){
    const off=zL*(this.state.cellsX+1)*(this.state.cellsZ+1);
    for (let i=0;i<this.state.cellsX;i++){
      for (let j=0;j<this.state.cellsZ;j++){
        const a=off+i*stride+(j+1), b=off+i*stride+j, c=off+(i+1)*stride+j, d=off+(i+1)*stride+(j+1);
        idxs.push(a,b,d, b,c,d);
      }
    }
  }
  geo.setIndex(idxs);
  geo.setAttribute('position', new THREE.Float32BufferAttribute(verts,3));
  geo.setAttribute('normal',   new THREE.Float32BufferAttribute(norms,3));
  geo.setAttribute('color',    new THREE.Float32BufferAttribute(cols,3));
  const mat=new THREE.MeshBasicMaterial({ vertexColors:true, side:THREE.DoubleSide, transparent:true, opacity:0.12, wireframe:true });
  group.add(new THREE.Mesh(geo,mat));

  const boxG=new THREE.BoxGeometry(sizeX, this.state.gridHeight/this.state.cellsY, this.state.gridDepth/this.effectiveCellsZ);
  const boxM=new THREE.MeshBasicMaterial({ opacity:0.2, transparent:true, wireframe:true });
  const maxInst=this.state.cellsX*this.state.cellsY*this.effectiveCellsZ;
  const imesh=new THREE.InstancedMesh(boxG, boxM, maxInst);
  let id=0; const map=new Map();

  const halfWidth=this.state.gridWidth/2, halfHeight=this.state.gridHeight/2, halfDepth=this.state.gridDepth/2;
  for (let z2=0; z2<this.effectiveCellsZ; z2++){
    for (let y2=0; y2<this.state.cellsY; y2++){
      for (let x2=0; x2<this.state.cellsX; x2++){
        const cx=x2*sizeX + sizeX/2 - halfWidth + this.position.x;
        const cy=y2*(this.state.gridHeight/this.state.cellsY) + (this.state.gridHeight/this.state.cellsY)/2 - halfHeight + this.position.y;
        const cz=z2*(this.state.gridDepth/this.effectiveCellsZ) + (this.state.gridDepth/this.effectiveCellsZ)/2 - halfDepth + this.position.z;
        if (this.sdf(new THREE.Vector3(cx,cy,cz), z2) < 0){
          imesh.setMatrixAt(id, new THREE.Matrix4().setPosition(cx,cy,cz));
          map.set(`${x2},${y2},${z2}`, id);
          id++;
        }
      }
    }
  }
  imesh.count=id;
  imesh.instanceMap=map;
  group.add(imesh);

  if (this._disposed || this._rev!==rev){
    group.traverse(o=>{ if(o.geometry)o.geometry.dispose(); if(o.material)o.material.dispose(); });
    return;
  }

  this.instancedMesh=imesh;
  this.gridGroup=group;
  this.scene.add(this.gridGroup);

  this.updateVisualization();
}

export function _valueToColor(norm){
  if (norm<=0) return new THREE.Color(0.2,0.2,1.0);
  if (norm<0.5){ const t=norm*2; return new THREE.Color(0.2*(1-t), 0.2+0.8*t, 1.0*(1-t)); }
  const t=(norm-0.5)*2; return new THREE.Color(0.8*t+0.2*(1-t), 1.0*(1-t), 0);
}

export async function updateVisualization(){
  if (this._disposed) return;
  const im=this.instancedMesh;
  if (!im || !im.instanceMap) return;

  const field=this.fieldForViz;
  const fi=this.schema.index.get(field) ?? 0;
  let max=this._maxField[field] || 0; if (max<=0) max=1;

  const needZ=new Set();
  for (const [key] of im.instanceMap){ const z=Number(key.split(',')[2]); needZ.add(z); }
  await Promise.all(Array.from(needZ, z=>this._ensureDenseLayer(z)));

  for (const [key,id] of im.instanceMap){
    const [x,y,z]=key.split(',').map(Number);
    const { bx, by } = this._mapCellToDense(z, x, y);
    const arr=this._layerCache.get(z|0);
    const val = arr?.[by]?.[bx]?.[fi] || 0;
    const norm=Math.min(1,(val<=0?0:val)/max);
    im.setColorAt(id, this._valueToColor(norm));
  }
  if (im.instanceColor) im.instanceColor.needsUpdate=true;
}

