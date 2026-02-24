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

function requireAgentKey(req, res, next) {
  if (!AGENT_KEY) return next();
  const got = String(req.get('x-agent-key') || '');
  if (got === AGENT_KEY) return next();
  return res.status(401).json({ ok: false, error: 'unauthorized' });
}

router.get('/active', function (req, res) {
  res.json(snapshot());
});

router.post('/active', requireAgentKey, function (req, res) {
  const payload = req.body && typeof req.body === 'object' ? req.body : {};
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

  const requestedBy = req.body?.requestedBy || req.ip || 'frontend';
  const command = createRefreshCommand({ requestedBy });
  return res.status(202).json({
    ok: true,
    reused: false,
    command,
  });
}

router.post('/refresh-request', handleRefreshRequest);
router.get('/refresh-request', handleRefreshRequest);

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
  const id = String(req.body?.id || '').trim();
  const status = normalizeCommandStatus(req.body?.status);
  if (!id || !status) {
    return res.status(400).json({ ok: false, error: 'invalid_command_payload' });
  }

  const updated = updateCommandStatus(id, status, {
    agentId: req.body?.agentId || req.ip || 'agent',
    note: req.body?.note,
    error: req.body?.error,
    lastRunId: req.body?.lastRunId,
    onlineCount: req.body?.onlineCount,
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
  const items = Array.isArray(payload.items) ? payload.items : [];
  if (!items.length) {
    return res.status(400).json({ ok: false, error: 'items_required' });
  }

  const out = rawFrameQueue.enqueueMany(items, {
    recentTtlMs: payload.recentTtlMs,
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
    limit: payload.limit,
    consumer: payload.consumer || req.ip || 'be',
    claimMs: payload.claimMs,
  });
  return res.json(out);
});

router.post('/raw-frame/ack', requireAgentKey, function (req, res) {
  const payload = req.body && typeof req.body === 'object' ? req.body : {};
  const ids = Array.isArray(payload.ids) ? payload.ids : [];
  if (!ids.length) {
    return res.status(400).json({ ok: false, error: 'ids_required' });
  }
  const out = rawFrameQueue.ackMany({
    ids,
    consumer: payload.consumer || req.ip || '',
  });
  return res.json(out);
});

module.exports = router;
