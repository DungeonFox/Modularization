// Constants for SDFGrid configuration and storage
export const DENSE_W = 1024;
export const DENSE_H = 1024;

export const IDB_NAME    = 'SDFFieldDB';
export const IDB_VERSION = 7;

export const STORE_META  = 'meta';
export const STORE_BASE  = 'base';        // Int16 SDF per-layer (kept)
// Sparse quadrant template per schemaId
export const STORE_BASEZ = 'base_zero';
export const STORE_LAYER = 'overlay_layers';
export const STORE_LMETA = 'overlay_layers_meta';

// Default number of quadrants for environment quantization
export const DEFAULT_QUADRANT_COUNT = 16;
