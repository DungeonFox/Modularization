// SDFGrid.js — dense 1024×1024 per-layer Float32 overlay with sparse quadrant base,
// nucleus-centered alignment, SVG SDF, and Storage Buckets persistence.
//
// Dense overlay: one Float32Array per layer, length = 1024 * 1024 * F (F = #fields).
// First creation of a layer clones a quantized quadrant template from the base store (base_zero)
// and applies any existing sparse cell data center-aligned; zeros remain as padding.
//
// IDB inside a Storage Bucket named after UID (lowercased, sanitized):
//   DB: 'SDFFieldDB'  (version 7)
//   Stores:
//     'meta'                : layout, global schema, per-layer nuclei
//       - 'layout'          : { w,h,layers, denseW,denseH, shapeType, gw,gh,gd }
//       - 'schema'          : { id, fields: string[] }
//       - `z:${z}`          : { cx, cy, w, h, rule }
//     'base'                : per-layer Int16 SDF (key = z)    [kept for SDF usage]
//     'base_zero'           : sparse quadrant templates        [NEW]
//         key = `sid:${schemaId}`  -> { quadrants: Array }
//     'overlay_layers'      : per-layer Float32 dense, key = z
//     'overlay_layers_meta' : per-layer schema version { sid, fields }, key = z
//
// Console helpers exposed: SDF_layerInfo(uid,z), SDF_readCell(uid,z,x,y), SDF_centerCell(uid,z)
//
// Dependencies: THREE, utils.js (safeNum, clamp, lsSet, lsGet, updateRegistrySaved, logicKey, stateKey, blobsKey)
//               svgParser.js (SVGPathParser.parseSVGPaths), logicPresets.js (presetCode)

import { safeNum } from './utils.js';
import { presetCode } from './logicPresets.js';
import { normalizeUID, normalizeBucketName } from './SDFGridUtil.js';
import { pickNucleusByDirection } from './SDFGridNucleus.js';
import { saveState, saveLogic, saveBlobs, loadState, loadLogic, loadBlobs, applyBlobs } from './SDFGridPersistence.js';
import { compileLogic } from './SDFGridLogic.js';
import { createInterpolatedShapes, sdf, sdfGrad } from './SDFGridShape.js';
import { DEFAULT_QUADRANT_COUNT } from './SDFGridConstants.js';
import {
  _ensureZeroTemplate, _ensureBaseSDF, getBaseDistance, _denseIdx, _ensureDenseLayer,
  _mapCellToDense, _applySparseIntoDense, setDenseFromCell, addDenseFromCell,
  sampleDenseForCell, _flushDirtyLayers
} from './SDFGridLayers.js';
import { updateParticles } from './SDFGridParticles.js';
import { visualizeGrid, _valueToColor, updateVisualization } from './SDFGridVisualization.js';
import { evolveSchema } from './SDFGridSchema.js';
import { _initBuckets } from './SDFGridBuckets.js';
import {
  getNucleus, centerCellIndex, toStateJSON, initializeGrid, updateGrid, updatePosition,
  zLayerIndexFromWorldZ, getCellData, setCellData, updateDispersion, setVisible, dispose
} from './SDFGridState.js';
import { layerInfo, readCell, centerCell } from './SDFGridConsole.js';

export class SDFGrid{
  constructor(uid, scene, params){
    this.uid   = normalizeUID(uid);
    this.scene = scene;

    const pos = params?.position || {x:0,y:0,z:0};
    this.state = {
      gridWidth: params.gridWidth, gridHeight: params.gridHeight, gridDepth: params.gridDepth,
      cellsX: params.cellsX, cellsY: params.cellsY, cellsZ: params.cellsZ,
      fidelity: params.fidelity, shapeType: params.shapeType, customSVGPath: params.customSVGPath
    };
    this.position = new THREE.Vector3(safeNum(pos.x,0), safeNum(pos.y,0), safeNum(pos.z,0));
    this.effectiveCellsZ = this.state.cellsZ * this.state.fidelity;

    // scene actors
    this.gridGroup = null;
    this.instancedMesh = null;

    // legacy sparse backing
    this.blobArray = [];
    this.dataTable = {};
    this.envVariables = params.envVariables || ['O2','CO2','H2O'];
    this.envTemplate  = Object.fromEntries(this.envVariables.map(n=>[n,0]));
    this.quadrantCount = params?.quadrantCount || DEFAULT_QUADRANT_COUNT;

    // svg
    this.svgShapes = [];
    this.interpolatedShapes = [];

    // nuclei per layer (logical grid coords)
    this._nuclei = new Array(this.effectiveCellsZ);

    // buckets
    this.bucketNameLC = normalizeBucketName(this.uid);
    this._bucket = null;
    this._db     = null;

    // overlay schema
    const initialFields = Array.isArray(params.fieldNames)&&params.fieldNames.length ? params.fieldNames.slice() : this.envVariables.slice();
    this.schema = { id: 1, fieldNames: initialFields, index: new Map(initialFields.map((n,i)=>[n,i])) };
    this.fieldForViz = params.fieldForViz || (initialFields.includes('O2') ? 'O2' : initialFields[0]);

    // caches and batching
    this._layerCache = new Map(); // z -> Float32Array (dense)
    this._dirtyLayers = new Set();
    this._flushHandle = null;

    // stats
    this._maxField = Object.create(null);
    this._maxO2 = 1;

    this._lastBlobSave = 0;
    this._lastDispersionUpdate = 0;
    this.trailStrength = params.trailStrength || 1.0;
    this.decayRate     = params.decayRate     || 0.1;

    this._disposed = false;
    this._rev = 0;

    // logic
    const L = SDFGrid.loadLogic(this.uid);
    this.logic = L || { enabled:false, preset:'Attract', forceScale:1.0, code: presetCode('Attract'), compiled:null, compileError:null };
    this.compileLogic(this.logic.code);

    // nuclei seed in logical space
    {
      const w=this.state.cellsX, h=this.state.cellsY, dir=params?.propagationDir||{x:1,y:0};
      const pick=()=>pickNucleusByDirection(w,h,dir);
      for (let z=0; z<this.effectiveCellsZ; z++) this._nuclei[z]=pick();
    }

    // init
    this.initializeGrid();
    this.applyBlobs(SDFGrid.loadBlobs(this.uid));

    if (this.bucketNameLC && navigator.storageBuckets){
      this._initBuckets(params?.propagationDir).then(()=>{ if(!this._disposed) this.visualizeGrid(); });
    } else {
      this.visualizeGrid();
    }

    // expose console helpers
    SDFGrid._instances ??= new Map();
    SDFGrid._instances.set(this.uid, this);
    if (typeof window !== 'undefined'){
      window.SDF_readCell   = SDFGrid.readCell.bind(SDFGrid);
      window.SDF_layerInfo  = SDFGrid.layerInfo.bind(SDFGrid);
      window.SDF_centerCell = SDFGrid.centerCell.bind(SDFGrid);
    }
  }
}

Object.assign(SDFGrid.prototype, {
  saveState,
  saveLogic,
  saveBlobs,
  applyBlobs,
  createInterpolatedShapes,
  sdf,
  sdfGrad,
  compileLogic,
  _ensureZeroTemplate,
  _ensureBaseSDF,
  getBaseDistance,
  _denseIdx,
  _ensureDenseLayer,
  _mapCellToDense,
  _applySparseIntoDense,
  setDenseFromCell,
  addDenseFromCell,
  sampleDenseForCell,
  _flushDirtyLayers,
  updateParticles,
  visualizeGrid,
  _valueToColor,
  updateVisualization,
  evolveSchema,
  _initBuckets,
  getNucleus,
  centerCellIndex,
  toStateJSON,
  initializeGrid,
  updateGrid,
  updatePosition,
  zLayerIndexFromWorldZ,
  getCellData,
  setCellData,
  updateDispersion,
  setVisible,
  dispose
});

Object.assign(SDFGrid, {
  loadState,
  loadLogic,
  loadBlobs,
  layerInfo,
  readCell,
  centerCell
});
