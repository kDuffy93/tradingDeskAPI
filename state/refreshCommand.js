'use strict';

const fs = require('fs');
const path = require('path');

const STATE_PATH = path.join(__dirname, 'refreshCommand.json');

const store = {
  command: null,
  updatedAt: 0,
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

function cloneCommand(cmd) {
  if (!cmd) return null;
  return { ...cmd };
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
  store.command = parsed.command && typeof parsed.command === 'object' ? parsed.command : null;
  store.updatedAt = Number(parsed.updatedAt || 0);
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
    status: 'pending',
    requestedAt: now,
    updatedAt: now,
    requestedBy: String(requestedBy || 'frontend'),
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
  if (!cmd || !id || cmd.id !== id) return null;

  const now = Date.now();
  cmd.status = String(status || cmd.status);
  cmd.updatedAt = now;
  cmd.agentId = patch.agentId ? String(patch.agentId) : cmd.agentId;
  cmd.note = patch.note ? String(patch.note) : cmd.note;
  cmd.error = patch.error ? String(patch.error) : null;
  cmd.lastRunId = patch.lastRunId ? String(patch.lastRunId) : cmd.lastRunId;
  if (patch.onlineCount !== undefined && patch.onlineCount !== null) {
    const n = Number(patch.onlineCount);
    cmd.onlineCount = Number.isFinite(n) ? n : cmd.onlineCount;
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

