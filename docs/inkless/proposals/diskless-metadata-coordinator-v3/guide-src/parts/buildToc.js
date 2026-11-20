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
