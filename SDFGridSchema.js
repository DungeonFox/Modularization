import { arraysEqual } from './SDFGridUtil.js';
import { STORE_META } from './SDFGridConstants.js';
import { idbPut } from './SDFGridStorage.js';

export async function evolveSchema(newFieldNames){
  if (!Array.isArray(newFieldNames) || !newFieldNames.length) return this.schema.id;
  if (arraysEqual(newFieldNames, this.schema.fieldNames)) return this.schema.id;
  this.schema = {
    id: this.schema.id + 1,
    fieldNames: newFieldNames.slice(),
    index: new Map(newFieldNames.map((n,i)=>[n,i]))
  };
  if (this._db) await idbPut(this._db, STORE_META, 'schema', { id:this.schema.id, fields:this.schema.fieldNames });
  return this.schema.id;
}
