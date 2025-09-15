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
  const quads = template?.quadrants || [];
  const qCount = quads.length;
  if (!qCount) return arr;

  // Lay out quadrants across the 1024×1024 layer in a near-square grid
  const cols = Math.ceil(Math.sqrt(qCount));
  const rows = Math.ceil(qCount / cols);
  const qW = Math.ceil(DENSE_W / cols);
  const qH = Math.ceil(DENSE_H / rows);

  for (let i=0; i<qCount; i++){
    const quad = quads[i];
    const entries = Object.entries(quad);
    const col = i % cols;
    const row = Math.floor(i / cols);
    const xStart = col * qW;
    const yStart = row * qH;
    const xEnd = Math.min(xStart + qW, DENSE_W);
    const yEnd = Math.min(yStart + qH, DENSE_H);

    for (let y=yStart; y<yEnd; y++){
      const rowBase = y * DENSE_W * F;
      for (let x=xStart; x<xEnd; x++){
        const base = rowBase + x * F;
        for (const [name, val] of entries){
          const fi = schema.index.get(name);
          if (fi != null) arr[base + fi] = val;
        }
      }
    }
  }
  return arr;
}
