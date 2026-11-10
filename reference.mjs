import { readFileSync } from 'node:fs';
import { setTimeout as sleep } from 'node:timers/promises';

const {lo,hi,seen} = JSON.parse(readFileSync(new URL('./window.json',import.meta.url)));
const url = 'https://public.1rpc.io/bnb';
const address = '0xe2ce6ab80874fa9fa2aae65d277dd6b8e65c9de0';
const topic = '0xa800a2038683844fac66747f771bfdfae862eb28b16bcfa387afa9fbacce8ff7';
const hex = n => '0x' + n.toString(16);
let id=0;
async function rpc(method,params) {
  const sent = ++id;
  const res=await fetch(url,{method:'POST',headers:{'content-type':'application/json'},
    body:JSON.stringify({jsonrpc:'2.0',id:sent,method,params}),signal:AbortSignal.timeout(10000)});
  if (!res.ok) throw new Error('http_'+res.status);
  const reply=await res.json();
  if (reply.error || reply.id!==sent || reply.result===undefined) throw new Error('rpc_'+(reply.error?.code ?? 'bad_reply'));
  return reply.result;
}
if (Number(await rpc('eth_chainId',[]))!==56) throw new Error('wrong_chain');
const reference = new Set(), coverage = [], failed = [];
for(let a=lo;a<=hi;a+=50) {
  const b=Math.min(hi,a+49);
  try {
    const rows=await rpc('eth_getLogs',[{address,topics:[topic],fromBlock:hex(a),toBlock:hex(b)}]);
    if (!Array.isArray(rows) || rows.length>=10000) throw new Error('reference_shape');
    const chunk = [];
    for(const row of rows) {
      const n=Number(row.blockNumber);
      if (!Number.isSafeInteger(n) || n<a || n>b || row.address?.toLowerCase()!==address ||
          row.topics?.length!==1 || row.topics[0].toLowerCase()!==topic || !row.blockHash ||
          !row.transactionHash || !Number.isSafeInteger(Number(row.logIndex)) || row.removed) throw new Error('bad_row');
      chunk.push([row.blockHash,row.transactionHash,Number(row.logIndex)].join(':').toLowerCase());
    }
    for (const k of chunk) reference.add(k);
    coverage.push([a,b]);
  } catch(e) { failed.push([a,b,e.message]); if(e.message==='http_429') break; }
  await sleep(1600);
}
const live = new Set(seen);
console.log('CROSSCHECK_RESULT '+JSON.stringify({url,lo,hi,coverage,failed,
  referenceBuys:reference.size,matched:[...reference].filter(k=>live.has(k)).length,
  referenceOnly:[...reference].filter(k=>!live.has(k)).length,
  websocketOnly:[...live].filter(k=>!reference.has(k)).length}));
