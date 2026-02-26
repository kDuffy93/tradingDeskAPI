'use strict';

const express = require('express');
const router = express.Router();

const {
  snapshot,
  setFromIncoming,
  STATE_PATH,
} = require('../state/activeTables');
const rawFrameQueue = require('../state/rawFrameQueue');
const {
  snapshot: refreshSnapshot,
  getCurrentCommand,
  getPendingCommand,
  createRefreshCommand,
  updateCommandStatus,
} = require('../state/refreshCommand');

const AGENT_KEY = String(process.env.BJSPY_AGENT_KEY || '');
const MAX_ACTIVE_URLS = 2000;
const MAX_RAW_FRAME_PUSH_ITEMS = 200;
const MAX_RAW_FRAME_ACK_IDS = 1000;

function safeText(value, { max = 256, fallback = '' } = {}) {
  if (value === undefined || value === null) return fallback;
  let s = String(value).trim();
  if (!s) return fallback;
  s = s.replace(/[\u0000-\u001f\u007f]+/g, ' ');
  if (s.length > max) s = s.slice(0, max);
  return s;
}

function safeInt(value, { min = 0, max = Number.MAX_SAFE_INTEGER, fallback = 0 } = {}) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  const i = Math.floor(n);
  if (i < min) return min;
  if (i > max) return max;
  return i;
}

function normalizeHttpUrl(raw) {
  try {
    const u = new URL(String(raw || '').trim());
    if (!['http:', 'https:'].includes(u.protocol)) return '';
    u.hash = '';
    const s = u.toString();
    return s.length <= 2048 ? s : '';
  } catch {
    return '';
  }
}

function sanitizeUrlList(value, { maxItems = MAX_ACTIVE_URLS } = {}) {
  if (!Array.isArray(value)) return [];
  const out = [];
  const seen = new Set();
  for (const raw of value) {
    if (out.length >= maxItems) break;
    const u = normalizeHttpUrl(raw);
    if (!u || seen.has(u)) continue;
    seen.add(u);
    out.push(u);
  }
  return out;
}

function sanitizeActiveIncoming(payload) {
  const src = payload && typeof payload === 'object' && !Array.isArray(payload) ? payload : {};
  const meta = src.meta && typeof src.meta === 'object' && !Array.isArray(src.meta) ? src.meta : {};
  return {
    urls: sanitizeUrlList(src.urls),
    meta: {
      ts: safeInt(meta.ts, { min: 0, max: 9999999999999, fallback: 0 }),
      finalCount: safeInt(meta.finalCount, { min: 0, max: 100000, fallback: 0 }),
      lastMs: safeInt(meta.lastMs, { min: 0, max: 60 * 60 * 1000, fallback: 0 }),
      lastError: meta.lastError ? safeText(meta.lastError, { max: 512, fallback: '' }) : null,
      lastErrorCode: meta.lastErrorCode ? safeText(meta.lastErrorCode, { max: 64, fallback: '' }) : null,
      lastRunId: meta.lastRunId ? safeText(meta.lastRunId, { max: 128, fallback: '' }) : null,
      source: safeText(meta.source, { max: 64, fallback: 'remote_agent' }),
    },
    refreshing: !!src.refreshing,
  };
}

function isLoopbackAddress(value) {
  const ip = String(value || '').trim().toLowerCase();
  if (!ip) return false;
  return ip === '::1' || ip === '127.0.0.1' || ip === '::ffff:127.0.0.1' || ip === 'localhost';
}

function isLocalRequest(req) {
  return (
    isLoopbackAddress(req.ip) ||
    isLoopbackAddress(req.socket?.remoteAddress) ||
    isLoopbackAddress(req.connection?.remoteAddress)
  );
}

function requireAgentKey(req, res, next) {
  if (!AGENT_KEY) {
    if (isLocalRequest(req)) return next();
    return res.status(503).json({ ok: false, error: 'agent_key_not_configured' });
  }
  const got = String(req.get('x-agent-key') || '');
  if (got === AGENT_KEY) return next();
  return res.status(401).json({ ok: false, error: 'unauthorized' });
}

router.get('/active', function (req, res) {
  res.json(snapshot());
});

router.post('/active', requireAgentKey, function (req, res) {
  const payload = sanitizeActiveIncoming(req.body);
  const state = setFromIncoming(payload);

  return res.status(202).json({
    ok: true,
    acceptedAt: Date.now(),
    count: state.urls.length,
    statePath: STATE_PATH,
  });
});

function normalizeCommandStatus(value) {
  const status = String(value || '').trim().toLowerCase();
  if (status === 'started') return 'started';
  if (status === 'completed') return 'completed';
  if (status === 'failed') return 'failed';
  if (status === 'skipped') return 'skipped';
  return null;
}

function handleRefreshRequest(req, res) {
  const existing = getPendingCommand();
  if (existing) {
    return res.status(202).json({
      ok: true,
      reused: true,
      command: existing,
    });
  }

  const requestedBy = safeText(req.body?.requestedBy || req.ip || 'frontend', { max: 128, fallback: 'frontend' });
  const command = createRefreshCommand({ requestedBy });
  return res.status(202).json({
    ok: true,
    reused: false,
    command,
  });
}

router.post('/refresh-request', requireAgentKey, handleRefreshRequest);
router.get('/refresh-request', requireAgentKey, handleRefreshRequest);

router.get('/refresh-status', function (req, res) {
  res.json({
    ok: true,
    ...refreshSnapshot(),
  });
});

router.get('/refresh-command', requireAgentKey, function (req, res) {
  res.json({
    ok: true,
    command: getPendingCommand(),
    current: getCurrentCommand(),
  });
});

router.post('/refresh-command/ack', requireAgentKey, function (req, res) {
  const id = safeText(req.body?.id, { max: 96, fallback: '' });
  const status = normalizeCommandStatus(req.body?.status);
  if (!id || !status) {
    return res.status(400).json({ ok: false, error: 'invalid_command_payload' });
  }

  const updated = updateCommandStatus(id, status, {
    agentId: safeText(req.body?.agentId || req.ip || 'agent', { max: 128, fallback: 'agent' }),
    note: req.body?.note ? safeText(req.body?.note, { max: 256, fallback: '' }) : null,
    error: req.body?.error ? safeText(req.body?.error, { max: 1024, fallback: '' }) : null,
    lastRunId: req.body?.lastRunId ? safeText(req.body?.lastRunId, { max: 128, fallback: '' }) : null,
    onlineCount: req.body?.onlineCount === undefined ? undefined : safeInt(req.body?.onlineCount, { min: 0, max: 100000, fallback: 0 }),
  });

  if (!updated) {
    return res.status(404).json({ ok: false, error: 'command_not_found' });
  }

  return res.json({
    ok: true,
    command: updated,
  });
});

router.get('/health', function (req, res) {
  const state = snapshot();
  const rawStats = rawFrameQueue.snapshotStats();
  res.json({
    ok: true,
    ts: Date.now(),
    tablesOnline: state.urls.length,
    lastSyncTs: state.meta.ts || 0,
    lastError: state.meta.lastError || null,
    rawFrameQueue: {
      queueCount: Number(rawStats.queueCount || 0),
      claimedCount: Number(rawStats.claimedCount || 0),
      unclaimedCount: Number(rawStats.unclaimedCount || 0),
      lastPushAt: Number(rawStats.stats?.lastPushAt || 0) || null,
      lastClaimAt: Number(rawStats.stats?.lastClaimAt || 0) || null,
      lastAckAt: Number(rawStats.stats?.lastAckAt || 0) || null,
    },
  });
});

router.get('/raw-frame/stats', requireAgentKey, function (req, res) {
  res.json(rawFrameQueue.snapshotStats());
});

router.post('/raw-frame/push', requireAgentKey, function (req, res) {
  const payload = req.body && typeof req.body === 'object' ? req.body : {};
  const items = Array.isArray(payload.items)
    ? payload.items.slice(0, MAX_RAW_FRAME_PUSH_ITEMS).filter((x) => x && typeof x === 'object')
    : [];
  if (!items.length) {
    return res.status(400).json({ ok: false, error: 'items_required' });
  }

  const out = rawFrameQueue.enqueueMany(items, {
    recentTtlMs: safeInt(payload.recentTtlMs, { min: 1000, max: 60 * 60 * 1000, fallback: 10 * 60 * 1000 }),
  });

  return res.status(202).json({
    ok: true,
    ts: Date.now(),
    ...out,
  });
});

router.post('/raw-frame/claim', requireAgentKey, function (req, res) {
  const payload = req.body && typeof req.body === 'object' ? req.body : {};
  const out = rawFrameQueue.claimMany({
    limit: safeInt(payload.limit, { min: 1, max: 500, fallback: 100 }),
    consumer: safeText(payload.consumer || req.ip || 'be', { max: 80, fallback: 'be' }),
    claimMs: safeInt(payload.claimMs, { min: 5000, max: 10 * 60 * 1000, fallback: 60000 }),
  });
  return res.json(out);
});

router.post('/raw-frame/ack', requireAgentKey, function (req, res) {
  const payload = req.body && typeof req.body === 'object' ? req.body : {};
  const ids = Array.isArray(payload.ids)
    ? payload.ids.slice(0, MAX_RAW_FRAME_ACK_IDS).map((id) => safeText(id, { max: 128, fallback: '' })).filter(Boolean)
    : [];
  if (!ids.length) {
    return res.status(400).json({ ok: false, error: 'ids_required' });
  }
  const out = rawFrameQueue.ackMany({
    ids,
    consumer: safeText(payload.consumer || req.ip || '', { max: 80, fallback: '' }),
  });
  return res.json(out);
});

module.exports = router;
