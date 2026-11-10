import { gzipSync } from 'node:zlib';
import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import { setTimeout as sleep } from 'node:timers/promises';

const address = '0xe2ce6ab80874fa9fa2aae65d277dd6b8e65c9de0';
const topic = '0xa800a2038683844fac66747f771bfdfae862eb28b16bcfa387afa9fbacce8ff7';
const filter = {address, topics:[topic]};
const hex = n => '0x' + n.toString(16);
const key = l => [l.blockHash,l.transactionHash,Number(l.logIndex)].join(':').toLowerCase();
const wsURL = 'wss://bsc-rpc.publicnode.com';
const referenceURLs = ['https://public.1rpc.io/bnb','https://bsc-rpc.publicnode.com'];
const quantile = (xs,p) => xs.length ? Math.round(xs.toSorted((a,b)=>a-b)[Math.ceil(p*xs.length)-1]) : null;
let rid = 0;

function grade(reference, seen, heads) {
  const timing = {beforeNextHead:0, beforePlus2Head:0, atOrAfterPlus2Head:0, missingNextHead:0, missingPlus2Head:0};
  let missed = 0;
  const logMinusOwnHead = [], plus2Slack = [];
  for (const l of reference.values()) {
    const t = seen.get(key(l)), h = Number(l.blockNumber);
    if (t === undefined) { missed++; continue; }
    const own = heads.get(h), next = heads.get(h+1), plus2 = heads.get(h+2);
    if (own) logMinusOwnHead.push(t-own.at);
    if (!next) timing.missingNextHead++;
    else if (t < next.at) timing.beforeNextHead++;
    if (!plus2) timing.missingPlus2Head++;
    else {
      plus2Slack.push(plus2.at-t);
      timing[t < plus2.at ? 'beforePlus2Head' : 'atOrAfterPlus2Head']++;
    }
  }
  return {referenceBuys:reference.size, received:reference.size-missed, missed, ...timing,
    logMinusOwnHeadMs:{p50:quantile(logMinusOwnHead,.5),p90:quantile(logMinusOwnHead,.9)},
    plus2HeadSlackMs:{p10:quantile(plus2Slack,.1),p50:quantile(plus2Slack,.5)}};
}

async function rpc(url,method,params) {
  const id = ++rid;
  const res = await fetch(url,{method:'POST',headers:{'content-type':'application/json'},
    body:JSON.stringify({jsonrpc:'2.0',id,method,params}),signal:AbortSignal.timeout(8000)});
  if (!res.ok) throw new Error('http_' + res.status);
  const reply = await res.json();
  if (reply.error || reply.id !== id || reply.result === undefined) throw new Error('rpc_' + (reply.error?.code ?? 'bad_reply'));
  return reply.result;
}

async function capture() {
  const seen = new Map(), heads = new Map(), subs = new Map();
  const removed = new Set(), conflictingHeads = new Set();
  let duplicates = 0, malformed = 0, socketError = false, closed = false, chainOK = false;
  const ws = new WebSocket(wsURL);
  try {
    ws.addEventListener('open', () => {
      for (const [id,method,params] of [
        [1,'eth_chainId',[]],[2,'eth_subscribe',['newHeads']],[3,'eth_subscribe',['logs',filter]]
      ]) ws.send(JSON.stringify({jsonrpc:'2.0',id,method,params}));
    });
    ws.addEventListener('error', () => { socketError = true; });
    ws.addEventListener('close', () => { closed = true; });
    ws.addEventListener('message', ({data}) => {
      const at = performance.now();
      try {
        const m = JSON.parse(data);
        if (m.id === 1) { chainOK = Number(m.result) === 56; return; }
        if ((m.id === 2 || m.id === 3) && typeof m.result === 'string') { subs.set(m.result,m.id); return; }
        if (!m.params || !subs.has(m.params.subscription)) return;
        const row = m.params.result;
        if (subs.get(m.params.subscription) === 2) {
          const h = Number(row.number);
          if (!Number.isSafeInteger(h) || !row.hash) throw new Error('head_shape');
          if (heads.has(h) && heads.get(h).hash !== row.hash) conflictingHeads.add(h);
          else if (!heads.has(h)) heads.set(h,{at,hash:row.hash});
          return;
        }
        if (row.address?.toLowerCase() !== address || row.topics?.length !== 1 || row.topics[0].toLowerCase() !== topic ||
            !row.blockHash || !row.transactionHash || !Number.isSafeInteger(Number(row.logIndex)) ||
            !Number.isSafeInteger(Number(row.blockNumber))) throw new Error('log_shape');
        const k = key(row);
        if (row.removed) { removed.add(k); return; }
        if (seen.has(k)) duplicates++;
        else seen.set(k,at);
      } catch { malformed++; }
    });
    for (let i=0; i<100 && !(chainOK && subs.size === 2); i++) {
      if (closed || socketError) break;
      await sleep(100);
    }
    if (!chainOK || subs.size !== 2) throw new Error('websocket_chain_or_subscription_failed');
    const start = new Date().toISOString();
    const seconds = Number(process.argv[2] ?? 120);
    await sleep(seconds*1000);
    const end = new Date().toISOString();
    const heights = [...heads.keys()].sort((a,b)=>a-b);
    if (heights.length < 8) throw new Error('head_coverage_too_thin');
    const lo = heights[0]+3, hi = heights.at(-1)-3;
    for (const k of removed) seen.delete(k);
    for (const h of conflictingHeads) heads.delete(h);
    const missingHeads = Array.from({length:hi-lo+1},(_,i)=>lo+i).filter(h=>!heads.has(h)).length;
    return {seen,heads,lo,hi,info:{start,end,seconds,headNotifications:heights.length,missingHeads,duplicates,
      malformed,removed:removed.size,conflictingHeads:conflictingHeads.size,socketError,disconnected:closed}};
  } finally { if (ws.readyState < WebSocket.CLOSING) ws.close(); }
}

async function reference(refURL,lo,hi) {
  const logs = new Map(), failures = [];
  if (Number(await rpc(refURL,'eth_chainId',[])) !== 56) throw new Error('reference_wrong_chain');
  for (let a=lo; a<=hi; a+=15) {
    const b = Math.min(hi,a+14);
    try {
      const rows = await rpc(refURL,'eth_getLogs',[{...filter,fromBlock:hex(a),toBlock:hex(b)}]);
      if (!Array.isArray(rows) || rows.length >= 10000) throw new Error('reference_shape_or_cap');
      const chunk = new Map();
      for (const row of rows) {
        const n = Number(row.blockNumber);
        if (row.address?.toLowerCase() !== address || row.topics?.length !== 1 || row.topics[0].toLowerCase() !== topic ||
            n<a || n>b || !Number.isSafeInteger(n) || !row.blockHash || !row.transactionHash ||
            !Number.isSafeInteger(Number(row.logIndex)) || row.removed) throw new Error('reference_row_shape');
        chunk.set(key(row),row);
      }
      for (const [k,v] of chunk) logs.set(k,v);
    } catch (e) { failures.push([a,b,e.message]); break; }
    await sleep(800);
  }
  return {logs,failures};
}

if (process.argv[2] === '--self-test') {
  const log = {blockHash:'A',transactionHash:'B',logIndex:'0x0',blockNumber:'0xa'};
  const missing = {...log,logIndex:'0x1'};
  const late = {...log,logIndex:'0x2'};
  const ref = new Map([[key(log),log],[key(missing),missing],[key(late),late]]);
  const result = grade(ref,new Map([[key(log),9],[key(late),14]]),
    new Map([[10,{at:8}],[11,{at:10}],[12,{at:13}]]));
  assert.equal(result.missed,1);
  assert.equal(result.received,2);
  assert.equal(result.beforeNextHead,1);
  assert.equal(result.beforePlus2Head,1);
  assert.equal(result.atOrAfterPlus2Head,1);
} else {
  const {seen,heads,lo,hi,info} = await capture();
  const packed = gzipSync(JSON.stringify({seen:[...seen],heads:[...heads],lo,hi,info})).toString('base64');
  for (let i=0; i<packed.length; i+=3000) console.log('CAPTURE_B64 '+packed.slice(i,i+3000));
  const unavailable = [];
  let refURL, logs, failures;
  for (const candidate of referenceURLs) {
    try {
      const r = await reference(candidate,lo,hi);
      if (r.failures.length) { unavailable.push([candidate,r.failures]); continue; }
      refURL=candidate; ({logs,failures}=r); break;
    } catch (e) { unavailable.push([candidate,e.message]); }
  }
  if (!logs) {
    console.log('REFERENCE_UNAVAILABLE '+JSON.stringify({window:[lo,hi],...info,websocketDistinct:seen.size,unavailable}));
    throw new Error('reference_unavailable');
  }
  const chainHashMismatches = new Set();
  for (const row of logs.values()) {
    const n=Number(row.blockNumber), head=heads.get(n);
    if (head && head.hash.toLowerCase() !== row.blockHash.toLowerCase()) chainHashMismatches.add(n);
  }
  for (const n of chainHashMismatches) heads.delete(n);
  console.log('PROBE_RESULT '+JSON.stringify({providers:{websocket:wsURL,reference:refURL},window:[lo,hi],
    ...info, websocketDistinct:seen.size, referenceFailures:failures,unavailable,chainHashMismatches:[...chainHashMismatches],
    ...grade(logs,seen,heads)}));
}
