import { clamp } from './utils.js';
import { SVGPathParser } from './svgParser.js';

const PARSE_SVG = SVGPathParser?.parseSVGPaths || null;

function pointInPoly(p, V){
  if (V.length<3) return false;
  let inside=false, j=V.length-1;
  for (let i=0;i<V.length;i++){
    const yi=V[i].y, yj=V[j].y, xi=V[i].x, xj=V[j].x;
    const inter=((yi>p.y)!==(yj>p.y)) && (p.x < (xj-xi)*(p.y-yi)/((yj-yi)||1e-12) + xi);
    if (inter) inside=!inside; j=i;
  }
  return inside;
}

function distToPoly(p, V){
  if (V.length<2) return Infinity;
  let md=Infinity;
  for (let i=0;i<V.length;i++){
    const j=(i+1)%V.length, v1=V[i], v2=V[j];
    const A=p.x-v1.x, B=p.y-v1.y, C=v2.x-v1.x, D=v2.y-v1.y;
    const l2=C*C+D*D; const t=l2?Math.max(0,Math.min(1,(A*C+B*D)/l2)):0;
    const qx=v1.x+t*C, qy=v1.y+t*D;
    const d=Math.hypot(p.x-qx,p.y-qy);
    if (d<md) md=d;
  }
  return md;
}

export function createInterpolatedShapes(){
  if (!this.svgShapes.length) return;
  this.interpolatedShapes=[];
  if (this.svgShapes.length===1){
    for (let z=0; z<this.effectiveCellsZ; z++) this.interpolatedShapes.push({ vertices:this.svgShapes[0].vertices });
  } else {
    for (let z2=0; z2<this.effectiveCellsZ; z2++){
      const t=(this.effectiveCellsZ<=1)?0:(z2/(this.effectiveCellsZ-1));
      const scaled=t*(this.svgShapes.length-1);
      const lo=Math.floor(scaled), hi=Math.min(lo+1,this.svgShapes.length-1);
      const lt=scaled-lo;
      if (lt<=1e-6){ this.interpolatedShapes.push({ vertices:this.svgShapes[lo].vertices }); }
      else{
        const a=this.svgShapes[lo].vertices, b=this.svgShapes[hi].vertices;
        const N=Math.max(a.length,b.length), verts=new Array(N);
        for (let i=0;i<N;i++){
          const v1=a[i<a.length?i:a.length-1], v2=b[i<b.length?i:b.length-1];
          verts[i]=new THREE.Vector2(v1.x+(v2.x-v1.x)*lt, v1.y+(v2.y-v1.y)*lt);
        }
        this.interpolatedShapes.push({ vertices:verts });
      }
    }
  }
}

export function sdf(point, zLayerIndex){
  const rel=point.clone().sub(this.position);
  const halfX=this.state.gridWidth/2, halfY=this.state.gridHeight/2, halfZ=this.state.gridDepth/2;
  if (this.state.shapeType==='cube'){
    const q=new THREE.Vector3(Math.abs(rel.x)-halfX, Math.abs(rel.y)-halfY, Math.abs(rel.z)-halfZ);
    return Math.max(q.x,q.y,q.z);
  }
  if (this.state.shapeType==='sphere'){
    const r=Math.max(this.state.gridWidth, this.state.gridHeight, this.state.gridDepth)/4;
    return rel.length()-r;
  }
  if (this.state.shapeType==='custom' && this.state.customSVGPath && PARSE_SVG){
    if (!this.interpolatedShapes.length){
      if (!this.svgShapes.length) this.svgShapes = PARSE_SVG(this.state.customSVGPath);
      this.createInterpolatedShapes();
    }
    const zi=clamp(zLayerIndex|0, 0, this.effectiveCellsZ-1);
    const s=this.interpolatedShapes[zi];
    if (!s || !s.vertices || s.vertices.length<3){
      const q2=new THREE.Vector3(Math.abs(rel.x)-halfX, Math.abs(rel.y)-halfY, Math.abs(rel.z)-halfZ);
      return Math.max(q2.x,q2.y,q2.z);
    }
    const s2=Math.min(this.state.gridWidth, this.state.gridHeight)/2;
    const p2=new THREE.Vector2(rel.x/s2, rel.y/s2);
    const inside=pointInPoly(p2, s.vertices);
    const d2=distToPoly(p2, s.vertices);
    const sd2=inside ? -d2 : d2;
    const zDist=Math.abs(rel.z)-halfZ;
    return Math.max(sd2*s2, zDist);
  }
  return Infinity;
}

export function sdfGrad(point,zLayerIndex){
  const e=1e-2;
  const dx=this.sdf(new THREE.Vector3(point.x+e,point.y,point.z),zLayerIndex)-this.sdf(new THREE.Vector3(point.x-e,point.y,point.z),zLayerIndex);
  const dy=this.sdf(new THREE.Vector3(point.x,point.y+e,point.z),zLayerIndex)-this.sdf(new THREE.Vector3(point.x,point.y-e,point.z),zLayerIndex);
  const dz=this.sdf(new THREE.Vector3(point.x,point.y,point.z+e),zLayerIndex)-this.sdf(new THREE.Vector3(point.x,point.y,point.z-e),zLayerIndex);
  return new THREE.Vector3(dx,dy,dz).multiplyScalar(1/(2*e));
}
