// Generates Notion markdown fragments by running the explainer's own simulator code.
import fs from 'node:fs';
const html=fs.readFileSync(new URL('../explainer.html',import.meta.url),'utf8');
const between=(a,b)=>{const i=html.indexOf(a);const j=html.indexOf(b,i);if(i<0||j<0)throw new Error('marker '+a);return html.slice(i,j);};
const md=s=>String(s).replace(/<code>(.*?)<\/code>/g,'`$1`').replace(/<\/?i>/g,'*').replace(/<\/?em>/g,'*').replace(/<\/?b>/g,'**').replace(/<span class="t" data-k="[^"]*">(.*?)<\/span>/g,'$1').replace(/\u2019/g,'\u2019');
const out={};

/* ---------- replication playground ---------- */
{
  const code=between('const COUNTS=','function render(){');
  const factory=new Function('snap',code+`
    function render(){snap(S,{appliedOf,nextOffset,L,sum,BASE});}
    return {reset,commit,flush,fetch,crash,revive,retry,get S(){return S;}};`);
  const steps=[];let cur=null;
  const api=factory((S,h)=>{cur={S:JSON.parse(JSON.stringify(S)),h};});
  const fol=k=>{const S=api.S;return S.reps.map((_,i)=>i).filter(i=>i!==S.leader)[k];};
  const script=[
    ['Start','reset',()=>api.reset()],
    ['Commit for orders-0 (A)','commit',()=>api.commit()],
    ['Commit for orders-0 (B)','commit',()=>api.commit()],
    ['End linger: append batch','flush',()=>api.flush()],
    ['Follower fetch: B2','fetch',()=>api.fetch(fol(0))],
    ['Follower fetch: B3','fetch',()=>api.fetch(fol(1))],
    ['Commit for orders-0 (C), then end linger','commit',()=>{api.commit();api.flush();}],
    ['Follower fetch: B2','fetch',()=>api.fetch(fol(0))],
    ['Commit for orders-0 (D), then end linger','commit',()=>{api.commit();api.flush();}],
    ['Crash the leader','crash',()=>api.crash()],
    ['Broker retries failed commits','retry',()=>api.retry()],
    ['Follower fetch: B3','fetch',()=>api.fetch(fol(1))],
    ['Broker retries failed commits','retry',()=>api.retry()],
    ['Restart the old leader','revive',()=>api.revive()],
    ['End linger: append batch','flush',()=>api.flush()],
    ['Follower fetch: B3','fetch',()=>api.fetch(fol(1))],
    ['Follower fetch: B1','fetch',()=>api.fetch(fol(0))],
  ];
  let seen=0;
  for(const [title,,fn] of script){
    fn();const {S,h}=cur;const ev=S.events.slice(0,S.events.length-seen).reverse();seen=S.events.length;
    steps.push({title,S,ev});
  }
  const sum=a=>a.reduce((x,e)=>x+e.count,0);
  let o='';
  steps.forEach((st,n)=>{
    const S=st.S;const L=S.reps[S.leader];
    const appliedOf=i=>{const r=S.reps[i];return i===S.leader?Math.min(S.hw,r.log.length):Math.min(r.hwKnown,r.log.length);};
    const maxLen=Math.max(4,...S.reps.map(r=>r.log.length));
    let t='<table header-row="true" fit-page-width="true">\n<tr>\n<th>Replica</th>\n'+Array.from({length:maxLen},(_,k)=>'<th>#'+k+'</th>\n').join('')+'<th>Current batch</th>\n</tr>\n';
    S.reps.forEach((r,i)=>{
      const isL=i===S.leader,ap=appliedOf(i),hwPos=isL?S.hw:r.hwKnown;
      const role=!r.up?'down':isL?'leader, epoch '+S.epoch:(r.isr?'follower, in ISR':'follower, out of ISR');
      t+='<tr>\n<td>**'+r.id+'**<br>'+role+(r.up?'<br>HW '+hwPos:'')+'</td>\n';
      for(let k=0;k<maxLen;k++){const e=r.log[k];if(!e){t+='<td></td>\n';continue;}
        const color=!r.up?'gray_bg':(k<ap?'green_bg':'yellow_bg');
        t+='<td color="'+color+'">'+e.key+':'+e.start+'<br>e'+e.epoch+'</td>\n';}
      t+='<td'+(isL&&r.up&&S.batch.length?' color="gray_bg"':'')+'>'+(isL&&r.up?S.batch.map(e=>e.key+':'+e.start).join(', '):'')+'</td>\n</tr>\n';
    });
    t+='</table>';
    const ap=appliedOf(S.leader);const rng=e=>e.start+'\u2013'+(e.start+e.count-1);
    const above=L.log.slice(S.hw).map((e,k)=>({e,k:S.hw+k}));
    const ov=above.filter(x=>x.e.epoch===S.epoch).map(x=>'#'+x.k+' '+x.e.key+' '+rng(x.e)).concat(S.batch.map(e=>'reserved '+e.key+' '+rng(e)));
    const inh=above.filter(x=>x.e.epoch!==S.epoch).map(x=>'#'+x.k+' '+x.e.key+' '+rng(x.e));
    const next=100+sum(L.log)+sum(S.batch);
    const rep=S.replies.map(r=>r.status==='ok'?r.key+' \u2713 '+r.text:r.status==='parked'?r.key+' parked':r.key+' failed (retriable)').join(', ')||'none yet';
    let s='- **Leader SQLite:** orders-0 next offset **'+(100+sum(L.log.slice(0,ap)))+'**, applied through '+(ap?'#'+(ap-1):'nothing')+'.\n';
    s+='- **Overlay:** '+(ov.length?ov.join(', '):'empty')+'.\n';
    if(inh.length)s+='- **Inherited, above the HW:** '+inh.join(', ')+'. These come from an earlier epoch and apply once the HW passes them.\n';
    s+='- **Next commit gets:** **'+next+'** (SQLite plus the overlay).\n';
    s+='- **HW gate:** '+(S.gate.open?'open':'closed until the HW reaches '+S.gate.leo)+'.\n';
    S.reps.forEach((r,i)=>{if(i!==S.leader&&r.up){const a=appliedOf(i);s+='- **'+r.id+' SQLite:** next offset '+(100+sum(r.log.slice(0,a)))+', with no overlay.\n';}});
    s+='- **Replies:** '+rep+'.\n';
    const evs=st.ev.map(e=>'> '+(e[0]==='e-bad'?'\u274c ':e[0]==='e-ok'?'\u2705 ':'')+e[1]).join('\n>\n');
    o+='<toggle>\n**Step '+(n+1)+': '+st.title+'**\n\n'+evs+'\n\n'+t+'\n\n'+s.trimEnd()+'\n</toggle>\n\n';
  });
  out.repl=o;
}

/* ---------- owner protocol simulator ---------- */
{
  const code=between('const NODES=','let sc=0,i=0;');
  const SC=new Function(code+'return SC;')();
  const P=['CP1','CP3','CP6'];
  const FIELDS=[['part','commit part'],['batches','batches'],['mark','local marker'],['entry','owner entry'],['view','owner\u2019s view']];
  const nm={B:'Broker',ST:'Storage'};
  let o='<tabs>\n';
  SC.forEach(s=>{
    o+='<tab>\n'+s.name+'\n\n';
    o+='Owner: **'+s.owner+'**'+(s.stale?'. Stale owner: **'+s.stale+'**':'')+'.\n\n';
    let mm='```mermaid\nsequenceDiagram\n  participant B as Broker\n  participant ST as Storage\n  participant CP1\n  participant CP3\n  participant CP6\n';
    s.steps.forEach((st,k)=>{
      const tt=st.state.t.toFixed(st.state.t<0.1?2:1);
      mm+='  Note over B,CP6: '+(k+1)+'. t = '+tt+' T. '+st.title.replace(/[;#]/g,'')+'\n';
      st.state.arrows.forEach(a=>{const lost=a[3]==='lost',bad=a[3]==='bad';mm+='  '+a[0]+(lost?'-x':bad?'-x':'->>')+a[1]+': '+a[2]+(lost?' (lost)':bad?' (rejected)':'')+'\n';});
    });
    mm+='```\n\n';
    o+=mm;
    let prev=null;
    s.steps.forEach((st,k)=>{
      const S=st.state;const tt=S.t.toFixed(S.t<0.1?2:1);
      const ch=[];
      if(prev){if(prev.storage!==S.storage)ch.push('storage: '+S.storage);if(prev.broker!==S.broker)ch.push('broker: '+S.broker);
        P.forEach(p=>FIELDS.forEach(([f,lab])=>{if(prev.p[p][f]!==S.p[p][f])ch.push(p+' '+lab+': '+S.p[p][f]);}));}
      else{if(S.storage!=='empty')ch.push('storage: '+S.storage);P.forEach(p=>FIELDS.forEach(([f,lab])=>{if(S.p[p][f]!=='\u2014'&&S.p[p][f]!=='not sent')ch.push(p+' '+lab+': '+S.p[p][f]);}));}
      o+=(k+1)+'. **'+st.title+'** (t = '+tt+' T). '+md(st.desc)+(ch.length?'\\\n<span color="gray">Changes: '+ch.join(' \u00b7 ')+'</span>':'')+'\n';
      prev=S;
    });
    const F=s.steps[s.steps.length-1].state;
    o+='\n<table header-row="true">\n<tr>\n<th>Final state</th>\n<th>Commit part</th>\n<th>Batches</th>\n<th>Local marker</th>\n<th>Owner entry</th>\n<th>Owner\u2019s view</th>\n</tr>\n';
    P.forEach(p=>{o+='<tr>\n<td>**'+p+'**'+(p===s.owner?' (owner)':'')+(p===s.stale?' (stale)':'')+'</td>\n'+FIELDS.map(([f])=>'<td>'+F.p[p][f]+'</td>\n').join('')+'</tr>\n';});
    o+='</table>\n\nStorage: **'+F.storage+'**.\n</tab>\n';
  });
  o+='</tabs>\n';
  out.owner=o;
}

/* ---------- sequence widgets: retry and snapshot fetch ---------- */
function seqToMd(scenarios){
  let o=scenarios.length>1?'<tabs>\n':'';
  scenarios.forEach(s=>{
    const ids=s.actors.map((a,k)=>'A'+k);const id=a=>ids[s.actors.indexOf(a)];
    if(scenarios.length>1)o+='<tab>\n'+s.name+'\n\n';
    let mm='```mermaid\nsequenceDiagram\n'+s.actors.map((a,k)=>'  participant '+ids[k]+' as '+a).join('\n')+'\n';
    s.steps.forEach((st,k)=>{
      const label=(k+1)+'. '+st.label.replace(/[;#]/g,'');
      if(st.kind==='note'||st.from===st.to)mm+='  Note over '+id(st.from)+': '+label+'\n';
      else mm+='  '+id(st.from)+(st.kind==='lost'?'-x':st.kind==='reply'?'-->>':st.kind==='err'?'--x':'->>')+id(st.to)+': '+label+'\n';
    });
    mm+='```\n\n';o+=mm;
    s.steps.forEach((st,k)=>{o+=(k+1)+'. **'+(st.title||st.label)+'.** '+md(st.desc||'')+'\n';});
    if(scenarios.length>1)o+='</tab>\n';
  });
  if(scenarios.length>1)o+='</tabs>\n';
  return o;
}
{
  const code=between("const m=(from,to,label,desc,title,kind)=>({from,to,label,desc,title,kind:kind||'msg'});\n  const n=(who,label,desc,title)=>({from:who,to:who,label,desc,title,kind:'note'});\n  SeqWidget($('#w-retry'),",'/* ---------- Figure: participant state machine');
  const arr=code.slice(code.indexOf("SeqWidget($('#w-retry'),")+"SeqWidget($('#w-retry'),".length,code.lastIndexOf(']);')+1);
  const m=(from,to,label,desc,title,kind)=>({from,to,label,desc,title,kind:kind||'msg'});
  const n=(who,label,desc,title)=>({from:who,to:who,label,desc,title,kind:'note'});
  out.retry=seqToMd(new Function('m','n','return '+arr)(m,n));
}
{
  const code=between("safe('snapfetch'",'/* ---------- Interactive: findBatches');
  const pre=code.slice(code.indexOf('const R='),code.indexOf('const m='));
  const arr=code.slice(code.indexOf("SeqWidget($('#w-snapfetch'),")+"SeqWidget($('#w-snapfetch'),".length,code.lastIndexOf(']);')+1);
  const m=(from,to,label,desc,title,kind)=>({from,to,label,desc,title,kind:kind||'msg'});
  const n=(who,label,desc,title)=>({from:who,to:who,label,desc,title,kind:'note'});
  out.snapfetch=seqToMd(new Function('m','n',pre+'return '+arr)(m,n));
}
/* ---------- failure modes and glossary ---------- */
{
  const fm=new Function(between('const FM=','const cats=')+'return FM;')();
  const cats=Array.from(new Set(fm.map(f=>f[0])));
  let o='<tabs>\n';
  cats.forEach(c=>{o+='<tab>\n'+c+'\n\n| Failure | Behavior |\n| --- | --- |\n'+fm.filter(f=>f[0]===c).map(f=>'| '+f[1].replace(/\|/g,'\\|')+' | '+f[2].replace(/\|/g,'\\|')+' |').join('\n')+'\n</tab>\n';});
  o+='</tabs>\n';out.fm=o;
  const gl=new Function(between('const GL=','/* ---------- Tooltips')+'return GL;')();
  const sec=new Function(between('const SEC=','const GL=')+'return SEC;')();
  const keys=Object.keys(gl).filter(k=>!gl[k][3]).sort((a,b)=>gl[a][0].toLowerCase().localeCompare(gl[b][0].toLowerCase()));
  out.glossary='| Term | Definition | Introduced in |\n| --- | --- | --- |\n'+keys.map(k=>'| **'+gl[k][0]+'** | '+gl[k][1].replace(/\|/g,'\\|')+' | '+(sec[gl[k][2]]||gl[k][2])+' |').join('\n')+'\n';
}
fs.mkdirSync(new URL('./out/',import.meta.url),{recursive:true});
for(const [k,v] of Object.entries(out))fs.writeFileSync(new URL('./out/'+k+'.md',import.meta.url),v);
console.log(Object.entries(out).map(([k,v])=>k+':'+v.length).join(' '));
