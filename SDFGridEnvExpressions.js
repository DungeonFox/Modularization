export function serializeEnvHash(env){
  return JSON.stringify(env || {});
}

export function parseEnvExpression(expr){
  if (typeof expr === 'string'){
    try { return JSON.parse(expr); } catch { return {}; }
  }
  if (expr && typeof expr === 'object') return expr;
  return {};
}

export function envExpressionFromModule(mod){
  const obj = mod?.default ?? mod;
  return serializeEnvHash(obj);
}
