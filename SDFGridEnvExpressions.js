export function envNamesFromModule(mod){
  if (!mod) return [];
  const quads = Array.isArray(mod.quadrants) ? mod.quadrants : [mod];
  const set = new Set();
  for (const q of quads){
    if (!q) continue;
    for (const name of Object.keys(q)) set.add(name);
  }
  return Array.from(set);
}

export function envHashFromModule(expr){
  const out = {};
  if (!expr) return out;
  for (const [name, val] of Object.entries(expr)){
    if (typeof val === 'function'){
      try {
        out[name] = Number(val()) || 0;
      } catch {
        out[name] = 0;
      }
    } else if (typeof val === 'number'){
      out[name] = val;
    } else if (typeof val === 'string'){
      try {
        out[name] = Number(new Function(`return (${val});`)()) || 0;
      } catch {
        out[name] = 0;
      }
    } else {
      out[name] = 0;
    }
  }
  return out;
}

export function quadrantHashesFromModule(count, mod){
  const names = envNamesFromModule(mod);
  const baseZero = Object.fromEntries(names.map(n=>[n,0]));
  const quads = Array.isArray(mod?.quadrants) ? mod.quadrants : [];
  const out = [];
  if (quads.length){
    for (let i=0; i<count; i++){
      const expr = quads[i] || {};
      out.push({ ...baseZero, ...envHashFromModule(expr) });
    }
  } else {
    const hash = { ...baseZero, ...envHashFromModule(mod) };
    for (let i=0; i<count; i++) out.push({ ...hash });
  }
  return out;
}
