// General utility helpers for SDFGrid
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

export function arraysEqual(a,b){
  if (a===b) return true; if (!a||!b) return false; if (a.length!==b.length) return false;
  for (let i=0;i<a.length;i++) if (a[i]!==b[i]) return false; return true;
}
