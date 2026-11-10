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
const referenceURLs = [
  'https://endpoints.omniatech.io/v1/bsc/mainnet/public',
  'https://bnb.rpc.subquery.network/public',
  'https://bnb-mainnet.g.alchemy.com/public',
  'https://public.1rpc.io/bnb',
  'https://bsc.drpc.org'
];
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
  const removed = new Set(), conflictingHeads = new Set(), logHeights = new Map();
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
        else { seen.set(k,at); logHeights.set(k,Number(row.blockNumber)); }
      } catch { malformed++; }
    });
    for (let i=0; i<100 && !(chainOK && subs.size === 2); i++) {
      if (closed || socketError) break;
      await sleep(100);
    }
    if (!chainOK || subs.size !== 2) throw new Error('websocket_chain_or_subscription_failed');
    const startAt = performance.now();
    const start = new Date().toISOString();
    const seconds = Number(process.argv[2] ?? 1800);
    await sleep(seconds*1000);
    const endAt = performance.now();
    const end = new Date().toISOString();
    await sleep(6000);
    const window = [...heads].filter(([,h])=>h.at>=startAt && h.at<endAt).sort(([a],[b])=>a-b);
    if (window.length < 8) throw new Error('head_coverage_too_thin');
    const lo = window[0][0], hi = window.at(-1)[0];
    const firstHeadDelayMs = Math.round(window[0][1].at-startAt);
    const endHeadAgeMs = Math.round(endAt-window.at(-1)[1].at);
    for (const k of removed) seen.delete(k);
    for (const h of conflictingHeads) heads.delete(h);
    const missingHeads = Array.from({length:hi-lo+1},(_,i)=>lo+i).filter(h=>!heads.has(h)).length;
    return {seen,heads,lo,hi,logHeights,info:{start,end,seconds,headNotifications:window.length,firstHeadDelayMs,endHeadAgeMs,missingHeads,duplicates,
      malformed,removed:removed.size,conflictingHeads:conflictingHeads.size,socketError,disconnected:closed}};
  } finally { if (ws.readyState < WebSocket.CLOSING) ws.close(); }
}

async function reference(lo,hi) {
  const logs = new Map(), gaps = [], coverage = [], errors = [];
  const providers = [];
  for (const url of referenceURLs) {
    try {
      if (Number(await rpc(url,'eth_chainId',[])) !== 56) throw new Error('wrong_chain');
      providers.push({url,cap:200,blockedUntil:0});
    } catch (e) { errors.push([url,'chain_id',e.message]); }
  }
  let attempts = 0;
  async function read(a,b) {
    for (const provider of providers) {
      if (b-a+1 > provider.cap || Date.now() < provider.blockedUntil || attempts >= 240) continue;
      attempts++;
      try {
        const rows = await rpc(provider.url,'eth_getLogs',[{...filter,fromBlock:hex(a),toBlock:hex(b)}]);
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
        coverage.push([a,b,provider.url,chunk.size]);
        await sleep(1100);
        return;
      } catch (e) {
        errors.push([provider.url,a,b,e.message]);
        if (e.message === 'http_429' || /rate|limit.*request/i.test(e.message)) provider.blockedUntil=Date.now()+60000;
        else if (e.message==='http_403' || e.message==='http_401' || e.message==='rpc_-32601') provider.cap=0;
        else if (b-a+1>10) provider.cap=Math.min(provider.cap,Math.floor((b-a+1)/2));
      }
    }
    if (b-a+1<=10 || attempts>=240) { gaps.push([a,b]); return; }
    const m = Math.floor((a+b)/2);
    await read(a,m);
    await read(m+1,b);
  }
  for (let a=lo;a<=hi;a+=200) await read(a,Math.min(a+199,hi));
  return {logs,coverage,gaps,errors,attempts};
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
  const {seen,heads,lo,hi,info,logHeights} = await capture();
  function publish(label, value) {
    const packed = gzipSync(JSON.stringify(value)).toString('base64');
    for (let i=0; i<packed.length; i+=3000) console.log(label+' '+packed.slice(i,i+3000));
  }
  publish('CAPTURE_B64',{seen:[...seen],heads:[...heads],logHeights:[...logHeights],lo,hi,info});
  const {logs,coverage,gaps,errors,attempts} = await reference(lo,hi);
  publish('REFERENCE_B64',{logs:[...logs].map(([k,v])=>[k,Number(v.blockNumber)]),coverage,gaps,errors,attempts});
  const chainHashMismatches = new Set();
  for (const row of logs.values()) {
    const n=Number(row.blockNumber), head=heads.get(n);
    if (head && head.hash.toLowerCase() !== row.blockHash.toLowerCase()) chainHashMismatches.add(n);
  }
  for (const n of chainHashMismatches) heads.delete(n);
  const covered = new Set(coverage.flatMap(([a,b])=>Array.from({length:b-a+1},(_,i)=>a+i)));
  const websocketOnly = [...seen.keys()].filter(k=>covered.has(logHeights.get(k))&&!logs.has(k)).length;
  console.log('PROBE_RESULT '+JSON.stringify({websocket:wsURL,window:[lo,hi],...info,
    coveredBlocks:covered.size,websocketOnly,gaps,attempts,referenceProviders:[...new Set(coverage.map(x=>x[2]))],
    referenceErrors:errors,chainHashMismatches:[...chainHashMismatches],...grade(logs,seen,heads)}));
}
