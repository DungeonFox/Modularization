// Logic for selecting nuclei positions based on propagation direction
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
