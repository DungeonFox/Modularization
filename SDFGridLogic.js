export function compileLogic(src){
  src = src || "";
  try{
    const f = new Function('THREE','"use strict";\n'+src+'\n;return (typeof applyForce==="function")?applyForce:null;')(THREE);
    if (typeof f!=='function') throw 0;
    this.logic.compiled=(ctx)=>f(ctx); this.logic.compileError=null; this.logic.code=src; return true;
  }catch(e1){
    try{
      const F=(0,eval)('(function(THREE){"use strict";'+src+';return (typeof applyForce==="function")?applyForce:null;})');
      const g=F(THREE); if (typeof g!=='function') throw 0;
      this.logic.compiled=(ctx)=>g(ctx); this.logic.compileError=null; this.logic.code=src; return true;
    }catch(e2){
      this.logic.compiled=null; this.logic.compileError=(e1?.message||String(e1))+' | '+(e2?.message||String(e2)); return false;
    }
  }
}
