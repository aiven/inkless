'use strict';

/* ---------------------------------------------------------------- helpers */
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

function h(tag, props, ...kids) {
  const el = document.createElement(tag);
  if (props) {
    for (const [k, v] of Object.entries(props)) {
      if (v == null || v === false) continue;
      if (k === 'class') el.className = v;
      else if (k === 'html') el.innerHTML = v;
      else if (k === 'style' && typeof v === 'object') Object.assign(el.style, v);
      else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v);
      else el.setAttribute(k, v);
    }
  }
  for (const kid of kids.flat()) {
    if (kid == null || kid === false) continue;
    el.append(kid.nodeType ? kid : document.createTextNode(String(kid)));
  }
  return el;
}

const fmt = n => Number(n).toLocaleString('en-US');
const cpColor = i => (i < 8 ? `var(--cp${i})` : `hsl(${(i * 47) % 360} 62% 46%)`);
const cpChip = i => `<span class="chip cp" style="background:${cpColor(i)}">cp${i}</span>`;
const chip = (cls, text) => `<span class="chip ${cls}">${text}</span>`;
const pair = p => (p ? `(${p[0]}, ${p[1]})` : 'none');
const cmpPair = (a, b) => (a[0] !== b[0] ? a[0] - b[0] : a[1] - b[1]);
const maxPair = (a, b) => (!a ? b : !b ? a : cmpPair(a, b) >= 0 ? a : b);

function setOn(group, active) { group.forEach(b => b.classList.toggle('on', b === active)); }
function show(el, cls, html) {
  el.className = 'readout' + (cls ? ' ' + cls : '');
  el.innerHTML = html;
}

/* Kafka's Utils.murmur2, ported with 32-bit integer math. */
function murmur2(bytes) {
  const m = 0x5bd1e995;
  const r = 24;
  const len = bytes.length;
  let hash = (0x9747b28c ^ len) | 0;
  const len4 = len >> 2;
  for (let i = 0; i < len4; i++) {
    const i4 = i * 4;
    let k = (bytes[i4] & 0xff) | ((bytes[i4 + 1] & 0xff) << 8) | ((bytes[i4 + 2] & 0xff) << 16) | ((bytes[i4 + 3] & 0xff) << 24);
    k = Math.imul(k, m);
    k ^= k >>> r;
    k = Math.imul(k, m);
    hash = Math.imul(hash, m);
    hash ^= k;
  }
  const base = len & ~3;
  switch (len % 4) {
    case 3: hash ^= (bytes[base + 2] & 0xff) << 16; // falls through
    case 2: hash ^= (bytes[base + 1] & 0xff) << 8; // falls through
    case 1: hash ^= bytes[base] & 0xff; hash = Math.imul(hash, m);
  }
  hash ^= hash >>> 13;
  hash = Math.imul(hash, m);
  hash ^= hash >>> 15;
  return hash | 0;
}
const toPositive = n => n & 0x7fffffff;

/* Draws a timeline. rows: [{label, segs: [{from, to, color, text, title}]}]. */
function drawGantt(host, rows, maxT, tick, unit) {
  host.innerHTML = '';
  for (const r of rows) {
    const lane = h('div', { class: 'lane' });
    for (const s of r.segs) {
      if (!(s.to > s.from)) continue;
      const left = (s.from / maxT) * 100;
      const width = Math.max(0.5, ((s.to - s.from) / maxT) * 100);
      lane.append(h('div', {
        class: 'seg',
        title: s.title || s.text || '',
        style: { left: left + '%', width: width + '%', background: s.color, color: s.ink || '#fff' },
      }, s.text || ''));
    }
    host.append(h('div', { class: 'row' }, h('div', { class: 'lbl' }, r.label), lane));
  }
  const axis = h('div', { class: 'axis' });
  for (let t = 0; t <= maxT + 1e-9; t += tick) {
    axis.append(h('span', { style: { left: (t / maxT) * 100 + '%' } }, fmt(t) + unit));
  }
  host.append(axis);
}

/* Runs a scripted stepper: steps are replayed from a fresh state on every move. */
function stepper(opts) {
  let idx = 0;
  const go = i => {
    const steps = opts.steps();
    idx = Math.max(0, Math.min(steps.length - 1, i));
    const state = opts.init();
    let prev = null;
    for (let k = 0; k <= idx; k++) {
      if (k === idx) prev = JSON.parse(JSON.stringify(state));
      if (steps[k].run) steps[k].run(state);
    }
    opts.render(state, prev, steps[idx], idx, steps.length);
    opts.prev.disabled = idx === 0;
    opts.next.disabled = idx === steps.length - 1;
  };
  opts.prev.addEventListener('click', () => go(idx - 1));
  opts.next.addEventListener('click', () => go(idx + 1));
  if (opts.reset) opts.reset.addEventListener('click', () => go(0));
  go(0);
  return { go, get index() { return idx; } };
}

/* ---------------------------------------------------------------- table of contents */
function buildToc() {
  const host = $('#toc-links');
  if (!host) return;
  const links = [];
  for (const el of $$('main .part-title[data-part], main section.chapter[data-title]')) {
    if (el.classList.contains('part-title')) {
      host.append(h('div', { class: 'toc-part' }, el.dataset.part));
    } else {
      const n = el.querySelector('h3 .n');
      const a = h('a', { href: '#' + el.id }, (n ? n.textContent + '. ' : '') + el.dataset.title);
      host.append(a);
      links.push([el, a]);
    }
  }
  const bar = $('#read-progress');
  let current = null;
  const onScroll = () => {
    let active = links[0];
    for (const pairEl of links) if (pairEl[0].getBoundingClientRect().top < 160) active = pairEl;
    if (active && active[1] !== current) {
      links.forEach(([, a]) => a.classList.toggle('active', a === active[1]));
      current = active[1];
    }
    const doc = document.documentElement;
    const pct = doc.scrollTop / Math.max(1, doc.scrollHeight - doc.clientHeight);
    if (bar) bar.style.width = (Math.min(1, Math.max(0, pct)) * 100).toFixed(1) + '%';
  };
  window.addEventListener('scroll', onScroll, { passive: true });
  onScroll();
}
