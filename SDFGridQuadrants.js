import { DENSE_W, DENSE_H, DEFAULT_QUADRANT_COUNT } from './SDFGridConstants.js';
import { parseEnvExpression } from './SDFGridEnvExpressions.js';

// Quantize environment variables using the Pareto principle (top 20% retained)
export function quantizePareto(env){
  const entries = Object.entries(env || {});
  if (!entries.length) return {};
  const sorted = entries.sort((a,b)=>Math.abs(b[1])-Math.abs(a[1]));
  const keep = Math.ceil(entries.length * 0.2);
  const out = {};
  for (let i=0; i<keep; i++){
    const [k,v] = sorted[i];
    out[k] = v;
  }
  return out;
}

// Create sparse quadrants from serialized environment expressions
// Each expression resolves to an object whose keys become the quadrant variables
// with zero as the default value. Expressions can differ per quadrant.
export function createSparseQuadrants(count = DEFAULT_QUADRANT_COUNT, envExprs = []){
  const quads = [];
  for (let i=0; i<count; i++){
    const expr = envExprs[i] ?? envExprs[0] ?? {};
    const tmpl = parseEnvExpression(expr);
    const q = {};
    for (const k of Object.keys(tmpl)) q[k] = 0;
    quads.push(q);
  }
  return { quadrants: quads };
}

// Reconstruct a dense Float32Array layer from a quadrant template
export function denseFromQuadrants(template, schema){
  const F = schema.fieldNames.length;
  const arr = new Float32Array(DENSE_W * DENSE_H * F);
  if (!template?.quadrants || !template.quadrants.length) return arr;
  const totalPixels = DENSE_W * DENSE_H;
  const qCount = template.quadrants.length;
  const areaPerQ = Math.ceil(totalPixels / qCount);
  let pix = 0;
  for (const quad of template.quadrants){
    const entries = Object.entries(quad);
    const end = Math.min(pix + areaPerQ, totalPixels);
    for (; pix < end; pix++){
      const base = pix * F;
      for (const [name, val] of entries){
        const fi = schema.index.get(name);
        if (fi != null) arr[base + fi] = val;
      }
    }
  }
  return arr;
}
