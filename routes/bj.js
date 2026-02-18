'use strict';

const express = require('express');
const router = express.Router();

const {
  snapshot,
  setFromIncoming,
  STATE_PATH,
} = require('../state/activeTables');
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
  res.json({
    ok: true,
    ts: Date.now(),
    tablesOnline: state.urls.length,
    lastSyncTs: state.meta.ts || 0,
    lastError: state.meta.lastError || null,
  });
});

module.exports = router;
