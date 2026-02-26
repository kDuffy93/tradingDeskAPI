'use strict';

const fs = require('fs');
const path = require('path');

const STATE_PATH = path.join(__dirname, 'refreshCommand.json');

const store = {
  command: null,
  updatedAt: 0,
};
const ALLOWED_STATUS = new Set(['pending', 'started', 'completed', 'failed', 'skipped']);
const ALLOWED_COMMAND_KEY = new Set(['refresh_now']);

function safeText(value, { max = 256, fallback = '' } = {}) {
  if (value === undefined || value === null) return fallback;
  let s = String(value).trim();
  if (!s) return fallback;
  s = s.replace(/[\u0000-\u001f\u007f]+/g, ' ');
  if (s.length > max) s = s.slice(0, max);
  return s;
}

function safeInt(value, { min = 0, max = Number.MAX_SAFE_INTEGER, fallback = null } = {}) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  const i = Math.floor(n);
  if (i < min) return min;
  if (i > max) return max;
  return i;
}

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

function cloneCommand(cmd) {
  if (!cmd) return null;
  return { ...cmd };
}

function sanitizeStoredCommand(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const id = safeText(raw.id, { max: 96, fallback: '' });
  if (!id) return null;
  const status = safeText(raw.status, { max: 32, fallback: 'pending' }).toLowerCase();
  const hasExplicitCommandKey = raw.commandKey !== undefined && raw.commandKey !== null && String(raw.commandKey).trim() !== '';
  const commandKey = safeText(raw.commandKey, { max: 64, fallback: 'refresh_now' }).toLowerCase();
  if (hasExplicitCommandKey && !ALLOWED_COMMAND_KEY.has(commandKey)) return null;
  const source = safeText(raw.source, { max: 64, fallback: 'api' });
  return {
    id,
    commandKey,
    schemaVersion: 1,
    status: ALLOWED_STATUS.has(status) ? status : 'pending',
    requestedAt: safeInt(raw.requestedAt, { min: 0, max: 9999999999999, fallback: 0 }) || 0,
    updatedAt: safeInt(raw.updatedAt, { min: 0, max: 9999999999999, fallback: 0 }) || 0,
    requestedBy: safeText(raw.requestedBy, { max: 128, fallback: 'frontend' }),
    agentId: raw.agentId == null ? null : safeText(raw.agentId, { max: 128, fallback: '' }) || null,
    note: raw.note == null ? null : safeText(raw.note, { max: 256, fallback: '' }) || null,
    error: raw.error == null ? null : safeText(raw.error, { max: 1024, fallback: '' }) || null,
    onlineCount:
      raw.onlineCount === undefined || raw.onlineCount === null
        ? null
        : safeInt(raw.onlineCount, { min: 0, max: 100000, fallback: null }),
    lastRunId: raw.lastRunId == null ? null : safeText(raw.lastRunId, { max: 128, fallback: '' }) || null,
    source,
    startedAt:
      raw.startedAt === undefined || raw.startedAt === null
        ? undefined
        : safeInt(raw.startedAt, { min: 0, max: 9999999999999, fallback: undefined }),
    finishedAt:
      raw.finishedAt === undefined || raw.finishedAt === null
        ? undefined
        : safeInt(raw.finishedAt, { min: 0, max: 9999999999999, fallback: undefined }),
  };
}

function snapshot() {
  return {
    command: cloneCommand(store.command),
    updatedAt: store.updatedAt || 0,
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
  store.command = sanitizeStoredCommand(parsed.command);
  store.updatedAt = safeInt(parsed.updatedAt, { min: 0, max: 9999999999999, fallback: 0 }) || 0;
}

function makeCommandId() {
  return `cmd-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function getCurrentCommand() {
  return cloneCommand(store.command);
}

function getPendingCommand() {
  if (!store.command || store.command.status !== 'pending') return null;
  return cloneCommand(store.command);
}

function createRefreshCommand({ requestedBy = 'frontend' } = {}) {
  const now = Date.now();
  const cmd = {
    id: makeCommandId(),
    commandKey: 'refresh_now',
    schemaVersion: 1,
    status: 'pending',
    requestedAt: now,
    updatedAt: now,
    requestedBy: safeText(requestedBy, { max: 128, fallback: 'frontend' }),
    agentId: null,
    note: null,
    error: null,
    onlineCount: null,
    lastRunId: null,
    source: 'api',
  };
  store.command = cmd;
  store.updatedAt = now;
  persist();
  return cloneCommand(cmd);
}

function updateCommandStatus(id, status, patch = {}) {
  const cmd = store.command;
  const safeId = safeText(id, { max: 96, fallback: '' });
  const safeStatus = safeText(status, { max: 32, fallback: '' }).toLowerCase();
  if (!cmd || !safeId || cmd.id !== safeId) return null;
  if (!ALLOWED_STATUS.has(safeStatus) || safeStatus === 'pending') return null;

  const now = Date.now();
  cmd.status = safeStatus;
  cmd.updatedAt = now;
  cmd.agentId = patch.agentId ? safeText(patch.agentId, { max: 128, fallback: '' }) : cmd.agentId;
  cmd.note = patch.note ? safeText(patch.note, { max: 256, fallback: '' }) : cmd.note;
  cmd.error = patch.error ? safeText(patch.error, { max: 1024, fallback: '' }) : null;
  cmd.lastRunId = patch.lastRunId ? safeText(patch.lastRunId, { max: 128, fallback: '' }) : cmd.lastRunId;
  if (patch.onlineCount !== undefined && patch.onlineCount !== null) {
    const n = safeInt(patch.onlineCount, { min: 0, max: 100000, fallback: null });
    cmd.onlineCount = n === null ? cmd.onlineCount : n;
  }
  if (cmd.status === 'started' && !cmd.startedAt) {
    cmd.startedAt = now;
  }
  if (cmd.status === 'completed' || cmd.status === 'failed' || cmd.status === 'skipped') {
    cmd.finishedAt = now;
  }

  store.updatedAt = now;
  persist();
  return cloneCommand(cmd);
}

load();

module.exports = {
  STATE_PATH,
  snapshot,
  getCurrentCommand,
  getPendingCommand,
  createRefreshCommand,
  updateCommandStatus,
};
