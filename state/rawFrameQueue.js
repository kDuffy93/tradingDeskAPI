'use strict';

const store = {
  byId: Object.create(null),
  order: [],
  recentByKey: Object.create(null),
  stats: {
    accepted: 0,
    deduped: 0,
    acked: 0,
    claimed: 0,
    rejected: 0,
    lastPushAt: 0,
    lastClaimAt: 0,
    lastAckAt: 0,
    lastSeq: 0,
  },
};

const DEFAULT_RECENT_TTL_MS = 10 * 60 * 1000;
const DEFAULT_CLAIM_MS = 60 * 1000;
const MAX_QUEUE_ITEMS = 200000;

function nowMs() {
  return Date.now();
}

function makeId() {
  return `rf-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function pruneRecent(ttlMs = DEFAULT_RECENT_TTL_MS, now = nowMs()) {
  const ttl = Math.max(1000, Number(ttlMs || DEFAULT_RECENT_TTL_MS));
  for (const [key, ts] of Object.entries(store.recentByKey)) {
    const n = Number(ts || 0);
    if (!n || now - n > ttl) delete store.recentByKey[key];
  }
}

function purgeMissingOrder() {
  if (!Array.isArray(store.order) || store.order.length === 0) return;
  store.order = store.order.filter((id) => !!store.byId[id]);
}

function cleanupExpiredClaims(now = nowMs()) {
  for (const item of Object.values(store.byId)) {
    if (!item || !item.claim) continue;
    const until = Number(item.claim.claimUntil || 0);
    if (until > 0 && until <= now) {
      item.claim = null;
    }
  }
}

function sanitizePayload(input = {}) {
  const payload = input && typeof input === 'object' ? input : {};
  const frame = payload.frame && typeof payload.frame === 'object' ? payload.frame : null;
  const frameData = frame && frame.data && typeof frame.data === 'object' ? frame.data : null;

  const idTable = frameData ? String(frameData.idTable || '') : String(payload.idTable || '');
  const idDeck = frameData && Number.isFinite(Number(frameData.idDeck)) ? Number(frameData.idDeck) : null;
  const idRound = frameData && Number.isFinite(Number(frameData.idRound)) ? Number(frameData.idRound) : null;
  const logsLen =
    frameData && Array.isArray(frameData.logs) ? frameData.logs.length : Number.isFinite(Number(payload.logsLen)) ? Number(payload.logsLen) : 0;
  const lastLog =
    frameData && Array.isArray(frameData.logs) && frameData.logs.length
      ? frameData.logs[frameData.logs.length - 1]
      : payload.lastLog && typeof payload.lastLog === 'object'
        ? payload.lastLog
        : null;

  const frameKey = String(payload.frameKey || '').trim() || [
    idTable,
    idDeck ?? '',
    idRound ?? '',
    logsLen,
    lastLog && lastLog.m ? String(lastLog.m) : '',
    lastLog && lastLog.s ? String(lastLog.s) : '',
    lastLog && lastLog.d ? String(lastLog.d) : '',
  ].join('|');

  return {
    source: String(payload.source || 'local-be'),
    sourceUrl: String(payload.sourceUrl || payload.url || ''),
    tableUrl: String(payload.tableUrl || payload.url || ''),
    tableId: String(payload.tableId || ''),
    receivedAt: Number.isFinite(Number(payload.receivedAt)) ? Number(payload.receivedAt) : nowMs(),
    frameKey,
    roundKey: String(payload.roundKey || [idTable, idDeck ?? '', idRound ?? ''].join('|')),
    idTable,
    idDeck,
    idRound,
    logsLen,
    frame: frame ? frame : null,
    rawPayload: payload,
  };
}

function enqueueMany(items = [], { recentTtlMs = DEFAULT_RECENT_TTL_MS } = {}) {
  const now = nowMs();
  pruneRecent(recentTtlMs, now);
  cleanupExpiredClaims(now);
  purgeMissingOrder();

  const accepted = [];
  const deduped = [];
  const rejected = [];
  const arr = Array.isArray(items) ? items : [items];

  for (const raw of arr) {
    const item = sanitizePayload(raw);
    if (!item.frame || !item.frame.data || item.frameKey.length < 3) {
      store.stats.rejected += 1;
      rejected.push({ reason: 'invalid_frame', frameKey: item.frameKey || '' });
      continue;
    }

    if (item.frameKey && store.recentByKey[item.frameKey]) {
      store.stats.deduped += 1;
      deduped.push({ frameKey: item.frameKey, reason: 'recent_key' });
      continue;
    }

    if (store.order.length >= MAX_QUEUE_ITEMS) {
      store.stats.rejected += 1;
      rejected.push({ reason: 'queue_full', frameKey: item.frameKey });
      continue;
    }

    const id = makeId();
    const seq = (store.stats.lastSeq || 0) + 1;
    store.stats.lastSeq = seq;
    const record = {
      id,
      seq,
      createdAt: now,
      claim: null,
      payload: item,
    };
    store.byId[id] = record;
    store.order.push(id);
    if (item.frameKey) store.recentByKey[item.frameKey] = now;
    store.stats.accepted += 1;
    accepted.push({
      id,
      seq,
      frameKey: item.frameKey,
      roundKey: item.roundKey,
      idTable: item.idTable,
      idRound: item.idRound,
      logsLen: item.logsLen,
      createdAt: now,
    });
  }

  store.stats.lastPushAt = now;
  return { accepted, deduped, rejected, acceptedCount: accepted.length, dedupedCount: deduped.length, rejectedCount: rejected.length };
}

function claimMany({ limit = 100, consumer = 'be', claimMs = DEFAULT_CLAIM_MS } = {}) {
  const now = nowMs();
  cleanupExpiredClaims(now);
  const max = Math.min(1000, Math.max(1, parseInt(limit, 10) || 100));
  const leaseMs = Math.max(5000, parseInt(claimMs, 10) || DEFAULT_CLAIM_MS);
  const who = String(consumer || 'be').slice(0, 80);
  const items = [];

  for (const id of store.order) {
    if (items.length >= max) break;
    const rec = store.byId[id];
    if (!rec) continue;
    if (rec.claim && Number(rec.claim.claimUntil || 0) > now) continue;
    rec.claim = {
      consumer: who,
      claimedAt: now,
      claimUntil: now + leaseMs,
      attempts: Number(rec.claim?.attempts || 0) + 1,
    };
    items.push({
      id: rec.id,
      seq: rec.seq,
      createdAt: rec.createdAt,
      claim: { ...rec.claim },
      payload: rec.payload,
    });
  }

  store.stats.claimed += items.length;
  store.stats.lastClaimAt = now;
  return {
    ok: true,
    ts: now,
    count: items.length,
    consumer: who,
    claimMs: leaseMs,
    items,
  };
}

function ackMany({ ids = [], consumer = '' } = {}) {
  const now = nowMs();
  const who = String(consumer || '').slice(0, 80);
  const arr = Array.isArray(ids) ? ids : [];
  let acked = 0;
  let missing = 0;
  let denied = 0;

  for (const rawId of arr) {
    const id = String(rawId || '').trim();
    if (!id) continue;
    const rec = store.byId[id];
    if (!rec) {
      missing += 1;
      continue;
    }
    if (who && rec.claim && rec.claim.consumer && rec.claim.consumer !== who) {
      denied += 1;
      continue;
    }
    delete store.byId[id];
    acked += 1;
  }

  if (acked > 0) {
    purgeMissingOrder();
    store.stats.acked += acked;
    store.stats.lastAckAt = now;
  }

  return { ok: true, ts: now, acked, missing, denied, requested: arr.length };
}

function snapshotStats() {
  const now = nowMs();
  cleanupExpiredClaims(now);
  purgeMissingOrder();
  let claimedCount = 0;
  let oldestCreatedAt = 0;
  let newestCreatedAt = 0;
  for (const id of store.order) {
    const rec = store.byId[id];
    if (!rec) continue;
    if (rec.claim && Number(rec.claim.claimUntil || 0) > now) claimedCount += 1;
    const createdAt = Number(rec.createdAt || 0);
    if (createdAt) {
      if (!oldestCreatedAt || createdAt < oldestCreatedAt) oldestCreatedAt = createdAt;
      if (!newestCreatedAt || createdAt > newestCreatedAt) newestCreatedAt = createdAt;
    }
  }
  return {
    ok: true,
    ts: now,
    queueCount: store.order.length,
    claimedCount,
    unclaimedCount: Math.max(0, store.order.length - claimedCount),
    oldestCreatedAt: oldestCreatedAt || null,
    newestCreatedAt: newestCreatedAt || null,
    stats: { ...store.stats },
  };
}

module.exports = {
  enqueueMany,
  claimMany,
  ackMany,
  snapshotStats,
};

