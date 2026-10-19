// One endpoint, ?a=poll|sheet|part|scan|put|commit. Data lives in Upstash Redis:
//   sheet:<i> = the uploaded CSV, gzipped + base64 by the browser, split in parts (Vercel caps bodies at 4.5 MB)
//   sheetn = part count, ver = upload id
//   packed = hash AWB -> ISO time
//   unmatched = hash AWB -> first time it was scanned/pasted while not in the sheet (cleared on replace)
//   punmatched = hash '<panel>	<AWB>' -> first time it was rejected by a panel-wise scan (cleared on replace)
//   log = list: 'v:<ver>' first, then each newly packed AWB in scan order; phones poll only the tail of it
// The browser checks "is this AWB in the sheet" itself, so the server only records packs.
const URL_ = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const MAX_PARTS = 20, MAX_PART_CHARS = 4_000_000;
// KEY_PREFIX is unset in production; tests run this same code against throwaway 'test:' keys.
const K = k => (process.env.KEY_PREFIX || '') + k;

// KEYS[3] (optional) = 'n:<nonce>', a set of the AWBs this request packed, kept 5 min. The phone retries
// with the same nonce when a reply is lost, and gets 'packed' again instead of a false 'already'.
const SCAN_LUA = `if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 1 then
  redis.call('RPUSH', KEYS[2], ARGV[1])
  if KEYS[3] then redis.call('SADD', KEYS[3], ARGV[1]) redis.call('EXPIRE', KEYS[3], 300) end
  return {1, ARGV[2]} end
local at = redis.call('HGET', KEYS[1], ARGV[1])
if KEYS[3] and redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then return {1, at} end
return {0, at}`;

const MAX_BULK = 2000;
const BULK_LUA = `local out, at, nk = {}, ARGV[#ARGV], KEYS[3]
for i = 1, #ARGV - 1 do
  if redis.call('HSETNX', KEYS[1], ARGV[i], at) == 1 then
    redis.call('RPUSH', KEYS[2], ARGV[i]) if nk then redis.call('SADD', nk, ARGV[i]) end out[i] = 1
  elseif nk and redis.call('SISMEMBER', nk, ARGV[i]) == 1 then out[i] = 1 else out[i] = 0 end
end
if nk then redis.call('EXPIRE', nk, 300) end
return out`;
// Lua keys for a scan: packed, log and, when the phone sent a valid nonce, its retry set.
const scanKeys = n => /^[a-z0-9]{6,32}$/.test(String(n || '')) ? [3, K('packed'), K('log'), K('n:' + n)] : [2, K('packed'), K('log')];

const N = s =>String(s || '').replace(/[`'\s]/g, '').toUpperCase();

async function redis(path, body) {
  const r = await fetch(URL_ + path, { method: 'POST', headers: { Authorization: 'Bearer ' + TOKEN }, body: JSON.stringify(body) });
  const j = await r.json();
  if (!r.ok || j.error) throw new Error('redis: ' + (j.error || r.status));
  if (Array.isArray(j)) for (const x of j) if (x.error) throw new Error('redis: ' + x.error);
  return j;
}
const cmd = async c => (await redis('', c)).result;

module.exports = async (req, res) => {
  res.setHeader('cache-control', 'no-store');
  try {
    const a = req.query.a;

    // Everything after position from-1. The client compares the first item with the last one it holds:
    // a mismatch means the sheet was replaced.
    if (a === 'poll') {
      const from = Math.max(0, (parseInt(req.query.from, 10) || 0) - 1);
      return res.json({ items: await cmd(['LRANGE', K('log'), from, -1]) });
    }
    if (a === 'sheet') {
      const [ver, n] = await redis('/pipeline', [['GET', K('ver')], ['GET', K('sheetn')]]);
      return res.json({ ver: ver.result, n: Number(n.result) || 0 });
    }
    // A part never changes for a given ver, so browsers and the CDN may cache it forever.
    if (a === 'part') {
      const i = parseInt(req.query.i, 10);
      const [ver, gz] = await redis('/pipeline', [['GET', K('ver')], ['GET', K('sheet:') + i]]);
      if (ver.result !== req.query.v || typeof gz.result !== 'string') return res.status(409).json({ error: 'sheet changed, reload' });
      res.setHeader('cache-control', 'public, max-age=31536000, s-maxage=31536000, immutable');
      return res.json({ gz: gz.result });
    }

    // Unmatched: AWBs scanned or pasted that are not in the sheet. Hash AWB -> first time seen.
    // GET lists them; POST { awbs } saves new ones (the first time seen is kept).
    // With a panel (POST { awbs, panel } / GET &p=1) it is the per-panel list instead: items are [panel, AWB, time].
    if (a === 'unmatched') {
      const per = req.method === 'POST' ? typeof (req.body || {}).panel === 'string' : !!req.query.p;
      if (req.method !== 'POST') {
        const flat = (await cmd(['HGETALL', K(per ? 'punmatched' : 'unmatched')])) || [], items = [];
        for (let i = 0; i < flat.length; i += 2) items.push(per ? [...flat[i].split('	'), flat[i + 1]] : [flat[i], flat[i + 1]]);
        return res.json({ items });
      }
      const awbs = [...new Set((Array.isArray((req.body || {}).awbs) ? req.body.awbs : []).map(N).filter(Boolean))];
      if (!awbs.length || awbs.length > MAX_BULK) return res.status(400).json({ error: `send 1 to ${MAX_BULK} AWBs` });
      const panel = per ? String(req.body.panel).replace(/	/g, ' ').slice(0, 100) : '';
      if (per && !panel) return res.status(400).json({ error: 'empty panel' });
      const at = new Date().toISOString();
      const r = await redis('/pipeline', awbs.map(x => per ? ['HSETNX', K('punmatched'), panel + '	' + x, at] : ['HSETNX', K('unmatched'), x, at]));
      return res.json({ saved: r.filter(x => x.result === 1).length });
    }

    // Pack time of every packed AWB, flat [awb, unix seconds, ...] to keep the reply small.
    // ponytail: whole hash in one reply (~3 MB at 1 lakh packed, Vercel caps at 4.5 MB); HSCAN pages if it grows past that
    if (a === 'times') {
      const flat = (await cmd(['HGETALL', K('packed')])) || [];
      for (let i = 1; i < flat.length; i += 2) flat[i] = Math.round(Date.parse(flat[i]) / 1000);
      return res.json({ items: flat });
    }

    if (req.method !== 'POST') return res.status(405).json({ error: 'POST only' });
    const body = req.body || {};

    if (a === 'scan') {
      const awb = N(body.awb);
      if (!awb) return res.status(400).json({ error: 'empty AWB' });
      // One round trip, atomic: mark packed + append to the log, or return when it was packed before.
      const [isNew, at] = await cmd(['EVAL', SCAN_LUA, ...scanKeys(body.nonce), awb, new Date().toISOString()]);
      return res.json({ status: isNew ? 'packed' : 'already', awb, at });
    }

    // Bulk paste: many AWBs in one atomic call; returns 1 (newly packed) or 0 (already packed) per AWB.
    if (a === 'scanmany') {
      const awbs = [...new Set((Array.isArray(body.awbs) ? body.awbs : []).map(N).filter(Boolean))];
      if (!awbs.length || awbs.length > MAX_BULK) return res.status(400).json({ error: `send 1 to ${MAX_BULK} AWBs` });
      const flags = await cmd(['EVAL', BULK_LUA, ...scanKeys(body.nonce), ...awbs, new Date().toISOString()]);
      return res.json({ awbs, packed: awbs.filter((_, i) => flags[i] === 1), already: awbs.filter((_, i) => flags[i] !== 1) });
    }

    // Upload = put every part under a temporary id, then commit swaps them in at once.
    const id = String(body.id || '');
    if ((a === 'put' || a === 'commit') && !/^[a-z0-9]{6,20}$/.test(id)) return res.status(400).json({ error: 'bad upload id' });

    if (a === 'put') {
      const i = body.i;
      if (!Number.isInteger(i) || i < 0 || i >= MAX_PARTS) return res.status(400).json({ error: 'bad part number' });
      if (typeof body.gz !== 'string' || !body.gz || body.gz.length > MAX_PART_CHARS) return res.status(400).json({ error: 'bad part' });
      await cmd(['SET', K(`up:${id}:${i}`), body.gz, 'EX', 3600]);
      return res.json({ ok: true });
    }

    if (a === 'commit') {
      const n = body.n;
      if (!Number.isInteger(n) || n < 1 || n > MAX_PARTS) return res.status(400).json({ error: 'bad part count' });
      const parts = Array.from({ length: n }, (_, i) => K(`up:${id}:${i}`));
      const have = await cmd(['EXISTS', ...parts]);
      if (have !== n) return res.status(400).json({ error: `only ${have} of ${n} parts arrived, upload again` });
      const ver = String(Date.now());
      const old = Array.from({ length: MAX_PARTS }, (_, i) => K('sheet:') + i);
      // mode 'add': the uploaded sheet already contains the old rows plus the new ones, so packed marks and
      // the log stay; the new 'v:' entry in the log tells other phones to load the bigger sheet.
      const keep = body.mode === 'add';
      await redis('/multi-exec', [
        keep ? ['DEL', K('sheet'), K('sheetgz'), K('awbs'), ...old] : ['DEL', K('packed'), K('log'), K('unmatched'), K('punmatched'), K('sheet'), K('sheetgz'), K('awbs'), ...old],
        ...parts.map((p, i) => ['RENAME', p, K('sheet:') + i]),
        ...parts.map((_, i) => ['PERSIST', K('sheet:') + i]), // RENAME keeps the 1 h expiry of the temp key
        ['SET', K('sheetn'), String(n)],
        ['SET', K('ver'), ver],
        ['RPUSH', K('log'), 'v:' + ver],
      ]);
      return res.json({ ver });
    }

    res.status(400).json({ error: 'unknown action' });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
};
