'use strict';

const fs = require('fs');
const path = require('path');

const STATE_PATH = path.join(__dirname, 'activeTables.json');

const DEFAULT_STATE = {
  urls: [],
  meta: {
    ts: 0,
    finalCount: 0,
    lastMs: 0,
    lastError: null,
    lastErrorCode: null,
    lastRunId: null,
    source: null,
  },
  refreshing: false,
};

const activeTables = {
  urls: [],
  meta: { ...DEFAULT_STATE.meta },
  refreshing: false,
};

function readJsonSafe(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

function writeJsonAtomic(filePath, obj) {
  const tmp = `${filePath}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2), 'utf8');
  fs.renameSync(tmp, filePath);
}

function sanitizeUrls(value) {
  if (!Array.isArray(value)) return [];
  const out = [];
  for (const raw of value) {
    if (typeof raw !== 'string') continue;
    const url = raw.trim();
    if (!url) continue;
    out.push(url);
  }
  return Array.from(new Set(out));
}

function snapshot() {
  return {
    urls: [...activeTables.urls],
    meta: { ...activeTables.meta },
    refreshing: false,
  };
}

function persist() {
  writeJsonAtomic(STATE_PATH, snapshot());
}

function load() {
  const parsed = readJsonSafe(STATE_PATH);
  if (!parsed || typeof parsed !== 'object') {
    persist();
    return;
  }

  activeTables.urls = sanitizeUrls(parsed.urls);
  activeTables.meta = {
    ...DEFAULT_STATE.meta,
    ...(parsed.meta && typeof parsed.meta === 'object' ? parsed.meta : {}),
  };
  activeTables.refreshing = false;
}

function setFromIncoming(payload = {}) {
  const nextUrls = sanitizeUrls(payload.urls);
  const nextMeta = payload.meta && typeof payload.meta === 'object' ? payload.meta : {};

  activeTables.urls = nextUrls;
  activeTables.meta = {
    ...DEFAULT_STATE.meta,
    ...nextMeta,
    ts: Date.now(),
    source: nextMeta.source || 'remote_agent',
  };
  activeTables.refreshing = false;

  persist();
  return snapshot();
}

load();

module.exports = {
  STATE_PATH,
  activeTables,
  snapshot,
  setFromIncoming,
  load,
  persist,
};