// TS-side (msgpackr) decode microbench for the Go→TS RowChange wire format.
// Reproducibility artifact for the positional-encoding proposal: quantifies the
// decode cost of the CURRENT map-keyed rows vs a positional frame + rebuild into
// the column-keyed objects downstream needs. Matches go-ivm-client.ts codec
// config (useRecords:false, mapsAsObjects:true). The Go-side counterpart lives in
// go-ivm/engine/format_profile_test.go (run with GO_IVM_BENCH=1).
//
// Run from the mono repo root:  node packages/zero-cache/src/services/view-syncer/go-sidecar/wire-format.bench.cjs
//
// Latest run (msgpackr, 15-col conversations row): positional decode+rebuild
// ~3.2-3.6x faster than the map baseline, ~3.1x smaller on the wire.
const {Packr, Unpackr} = require('msgpackr');

const packr = new Packr({useRecords: false, encodeUndefinedAsNil: true, mapsAsObjects: true, useBigIntExtension: false});
const unpackr = new Unpackr({useRecords: false, mapsAsObjects: true, useBigIntExtension: false});

const cols = ['conversationId','channelId','workspaceId','createdBy','title','createdAt','updatedAt','lastMessageAt','participantCount','unreadCount','messageCount','isPinned','isArchived','parentMessageId','conversationSeenCutoffAt'];

function rowVals(i) {
  return ['conv_' + String(i).padStart(12,'0'), 'chan_' + String(i%5000).padStart(8,'0'), 'ws_000007',
    'user_' + String(i%900).padStart(6,'0'), 'Re: incremental view maintenance throughput thread',
    1779813865070+i*1000, 1779813999070+i*1000, 1779814100070+i*1000, i%40, i%12, 100+i%4000,
    i%7===0, i%11===0, null, null];
}
function makeBaseline(n) {
  const out = new Array(n);
  for (let i=0;i<n;i++){ const v=rowVals(i); const row={}; for(let j=0;j<cols.length;j++) row[cols[j]]=v[j];
    out[i]={type:i%3, queryID:'q_channelConversationsPaginatedV3_abc123', table:'conversations', rowKey:{conversationId:v[0]}, row}; }
  return out;
}
function makePositional(n) {
  const r=new Array(n);
  for (let i=0;i<n;i++){ const v=rowVals(i); r[i]=[i%3, ...v]; }
  return {q:'q_channelConversationsPaginatedV3_abc123', t:'conversations', c:cols, r};
}

function timeit(fn, iters){ const t=process.hrtime.bigint(); for(let i=0;i<iters;i++) fn(); return Number(process.hrtime.bigint()-t)/iters/1000; }

const ITERS=200;
console.log('\n%s', ['N','baseline_dec_us','pos_dec_us','pos_dec+rebuild_us','base_bytes','pos_bytes'].join('  '));
for (const n of [100,1000,10000]) {
  const base=makeBaseline(n), pos=makePositional(n);
  const baseEnc=packr.pack(base), posEnc=packr.pack(pos);
  for(let i=0;i<20;i++){ unpackr.unpack(baseEnc); unpackr.unpack(posEnc); } // warm
  const baseDec=timeit(()=>unpackr.unpack(baseEnc), ITERS);
  const posDec=timeit(()=>unpackr.unpack(posEnc), ITERS);
  const posDecRebuild=timeit(()=>{
    const g=unpackr.unpack(posEnc); const C=g.c; const out=new Array(g.r.length);
    for(let i=0;i<g.r.length;i++){ const arr=g.r[i]; const row={}; for(let j=0;j<C.length;j++) row[C[j]]=arr[1+j];
      out[i]={type:arr[0], queryID:g.q, table:g.t, rowKey:{conversationId:arr[1]}, row}; }
    return out;
  }, ITERS);
  console.log([n, baseDec.toFixed(1), posDec.toFixed(1), posDecRebuild.toFixed(1), baseEnc.length, posEnc.length,
    `(dec ${(baseDec/posDec).toFixed(2)}x, dec+rebuild ${(baseDec/posDecRebuild).toFixed(2)}x, ${(baseEnc.length/posEnc.length).toFixed(2)}x bytes)`].join('  '));
}
console.log();
