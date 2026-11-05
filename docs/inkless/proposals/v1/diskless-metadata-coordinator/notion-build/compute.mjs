// Precomputes the explainer widgets' states for the Notion version.
function murmur2(b){
  const len=b.length, m=0x5bd1e995, r=24;
  let h=(0x9747b28c|0)^len;
  const l4=len>>2;
  for(let i=0;i<l4;i++){
    const i4=i<<2;
    let k=(b[i4]&0xff)|((b[i4+1]&0xff)<<8)|((b[i4+2]&0xff)<<16)|((b[i4+3]&0xff)<<24);
    k=Math.imul(k,m); k^=k>>>r; k=Math.imul(k,m);
    h=Math.imul(h,m); h^=k;
  }
  const idx=l4<<2;
  switch(len-idx){
    case 3: h^=(b[idx+2]&0xff)<<16;
    case 2: h^=(b[idx+1]&0xff)<<8;
    case 1: h^=b[idx]&0xff; h=Math.imul(h,m);
  }
  h^=h>>>13; h=Math.imul(h,m); h^=h>>>15;
  return h|0;
}
const toPositive=n=>n&0x7fffffff;
const S=4096;
const B64='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
function b64urlEncode(bytes){let bits=0,val=0,out='';for(const by of bytes){val=((val<<8)|by)&0xfffff;bits+=8;while(bits>=6){bits-=6;out+=B64[(val>>bits)&63];}}if(bits>0)out+=B64[(val<<(6-bits))&63];return out;}
function hexToBytes(h){const o=[];for(let i=0;i<h.length;i+=2)o.push(parseInt(h.substr(i,2),16));return o;}
const utf8=s=>Array.from(new TextEncoder().encode(s));
function logKey(tid,p){return tid.concat([(p>>>24)&255,(p>>>16)&255,(p>>>8)&255,p&255]);}
function mapLog(tid,p,N){const key=logKey(tid,p);const hash=murmur2(key);const pos=toPositive(hash);const slot=pos%S;return {key,hash,pos,slot,cp:slot%N};}
const v=[["21",-973932308],["foobar",-790332482],["abc",479470107]];
console.log('selftest', v.every(([s,x])=>murmur2(utf8(s))===x));

const hx=b=>b.toString(16).padStart(2,'0');
console.log('\n== mapping calculator ==');
const tid=hexToBytes('5f3a9c2e7b1d4e8fa0c6b3d29e4f7a18');
console.log('topic id b64', b64urlEncode(tid));
const m0=mapLog(tid,0,8);
console.log('key', m0.key.map(hx).join(' '));
console.log('hash',m0.hash,'pos',m0.pos,'slot',m0.slot,'cp',m0.cp,'pos%8',m0.pos%8);
for(let q=0;q<12;q++){const r=[4,8,16].map(N=>mapLog(tid,q,N).cp);console.log('p'+q,'slot',mapLog(tid,q,8).slot,'N4',r[0],'N8',r[1],'N16',r[2]);}
const c8=new Array(8).fill(0);for(let q=0;q<12;q++)c8[mapLog(tid,q,8).cp]++;console.log('counts N8',c8.join(','));

console.log('\n== slots ==');
for(const [F,T] of [[8,16],[4,8],[8,32],[16,8],[8,12],[8,6],[8,10],[8,24],[8,3]]){
  let moved=0,toOld=0;for(let s=0;s<S;s++){const a=s%F,b=s%T;if(a!==b){moved++;if(b<F)toOld++;}}
  console.log(F+'->'+T,'moved',moved,(moved/S*100).toFixed(1)+'%','toOld',toOld);
}
console.log('first 24 slots');
for(const N of [8,16,12]){const row=[];for(let s=0;s<24;s++)row.push(s%N);console.log('N'+N,row.join(','));}

console.log('\n== WAL object builder ==');
const TOPICS=[{name:'orders',hex:'8c1f4b2a9d3e4f6081a7c5e2b9d04f13',n:6},{name:'clicks',hex:'2e7d9a4c1b8f4e3a96c0d5f1a2b3c4d5',n:4},{name:'payments',hex:'f04a6b1c3d2e4f5a8b9c0d1e2f3a4b5c',n:3}];
TOPICS.forEach(t=>t.bytes=hexToBytes(t.hex));
const ups=[];TOPICS.forEach(t=>{for(let p=0;p<t.n;p++)ups.push({name:t.name+'-'+p,t,p});});
for(const u of ups){console.log(u.name,[1,2,4,8,16].map(N=>'N'+N+'='+mapLog(u.t.bytes,u.p,N).cp+'(s'+mapLog(u.t.bytes,u.p,N).slot+')').join(' '));}
const key='inkless/0193f2c4-7a1e-7c3b-9d2a-5e8f1b6c4a20';
const sel=['orders-0','orders-3','clicks-1','clicks-3','payments-2'];
for(const N of [4,8,16]){
  const chosen=ups.filter(u=>sel.includes(u.name)).map(u=>Object.assign({},u,mapLog(u.t.bytes,u.p,N)));
  const parts=Array.from(new Set(chosen.map(c=>c.cp))).sort((a,b)=>a-b);
  const kh=murmur2(utf8(key)),kp=toPositive(kh),idx=kp%parts.length;
  const order=parts.map((_,j)=>parts[(idx+j)%parts.length]);
  console.log('N',N,'parts',parts.join(','),'kh',kh,'kp',kp,'idx',idx,'preferred',parts[idx],'order',order.join('>'));
  parts.forEach(c=>console.log('  part CP'+c,chosen.filter(x=>x.cp===c).map(b=>b.name+'(slot '+b.slot+')').join(', ')));
}
// all partitions selected at N=8
{
  const N=8;const chosen=ups.map(u=>Object.assign({},u,mapLog(u.t.bytes,u.p,N)));
  const parts=Array.from(new Set(chosen.map(c=>c.cp))).sort((a,b)=>a-b);
  const kp=toPositive(murmur2(utf8(key)));console.log('all N8 parts',parts.join(','),'idx',kp%parts.length,'pref',parts[kp%parts.length]);
}

console.log('\n== findBatches ==');
const req=[{up:'orders-0',cp:1,b:[2,1,2]},{up:'clicks-1',cp:3,b:[1,1]},{up:'orders-3',cp:1,b:[3,2]},{up:'payments-2',cp:6,b:[1,2,1]}];
for(const B of [2,5,8,12]){
  const parts={};req.forEach((r,k)=>{(parts[r.cp]=parts[r.cp]||[]).push(k);});
  const ret={};Object.keys(parts).forEach(cp=>{let used=0;parts[cp].forEach(k=>{ret[k]=[];req[k].b.forEach(sz=>{if(used+sz<=B){ret[k].push(sz);used+=sz;}});});});
  let line='B='+B+' parts: ';Object.keys(parts).forEach(cp=>{line+='CP'+cp+'['+parts[cp].map(k=>req[k].up+':'+(ret[k].join('+')||'none')).join('; ')+'] ';});
  let tot=0;const merged=req.map((r,k)=>{const kept=[],cut=[];ret[k].forEach(sz=>{if(tot+sz<=B){kept.push(sz);tot+=sz;}else cut.push(sz);});return r.up+':'+(kept.join('+')||'-')+(cut.length?' cut '+cut.join('+'):'');});
  console.log(line);console.log('   merged',merged.join(' | '));
}

console.log('\n== log start ==');
function lsCompute(reps,rf,min,cur){
  const L=reps[0];const others=reps.slice(1).filter(r=>rf>1&&r.isr);const need=Math.min(min,rf)-1;let cand,why;
  if(rf===1){cand=L.older;why='single';}else if(need===0){cand=L.older;why='min1';}
  else{const rep=others.map(r=>r.newest).sort((a,b)=>b-a);if(rep.length<need){cand=null;why='not enough reports';}else{const kth=rep[need-1];cand=Math.min(L.older,kth);why='older '+L.older+' kth '+kth;}}
  const target=cand===null?cur:Math.max(cur,Math.min(cand,1000));return {cand,target,why};
}
const base=()=>[{id:'B1',older:600,newest:900,isr:true},{id:'B2',older:500,newest:850,isr:true},{id:'B3',older:200,newest:400,isr:true}];
for(const [label,mut,rf,min] of [
  ['default',r=>r,3,2],['min3',r=>r,3,3],['min1',r=>r,3,1],['B2 out of ISR',r=>{r[1].isr=false;return r;},3,2],['B2,B3 out',r=>{r[1].isr=false;r[2].isr=false;return r;},3,2],['rf1',r=>r,1,2],['leader rotated',r=>{r[0].older=900;r[0].newest=1000;return r;},3,2],['B2 newest 500',r=>{r[1].newest=500;return r;},3,2]]){
  console.log(label,JSON.stringify(lsCompute(mut(base()),rf,min,0)));
}
