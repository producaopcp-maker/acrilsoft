
(() => {
  const $ = (sel, el=document) => el.querySelector(sel);
  const $$ = (sel, el=document) => Array.from(el.querySelectorAll(sel));

  const html = document.documentElement;
  const body = document.body;

  // ---------------------------------------------------------------------------
  // Lucide icons
  // ---------------------------------------------------------------------------
  function refreshIcons(){
    try{ if (window.lucide) window.lucide.createIcons(); }catch(e){}
  }
  document.addEventListener('DOMContentLoaded', refreshIcons);

  // ---------------------------------------------------------------------------
  // Sidebar: colapsável (desktop) + tooltip titles
  // ---------------------------------------------------------------------------
  function applySidebarTitles(){
    $$('.sb-link').forEach(a => {
      if (!a.dataset.title) {
        const txt = (a.querySelector('.sb-text')?.textContent || a.textContent || '').trim();
        if (txt) a.dataset.title = txt;
      }
    });
  }
  document.addEventListener('DOMContentLoaded', applySidebarTitles);

  // ---------------------------------------------------------------------------
  // Sidebar: marcar item atual + desabilitar clique no item ativo
  // ---------------------------------------------------------------------------
  function markActiveSidebar(){
    try{
      const path = (window.location && window.location.pathname) ? window.location.pathname : '';
      if (!path) return;

      // Escolhe o link mais específico (maior href que case com o path)
      let best = null;
      let bestLen = -1;

      $$('.sb-link').forEach(a => {
        const href = a.getAttribute('href') || '';
        if (!href || href === '#') return;

        const h = (href.length > 1) ? href.replace(/\/$/, '') : href;
        const p = (path.length > 1) ? path.replace(/\/$/, '') : path;

        const isExact = (p === h);
        const isPrefix = (h !== '/' && p.startsWith(h + '/'));
        if (!isExact && !isPrefix) return;

        if (h.length > bestLen){
          best = a;
          bestLen = h.length;
        }
      });

      if (!best) return;

      $$('.sb-link').forEach(a => {
        const active = (a === best);
        a.classList.toggle('active', active);
        if (active) a.setAttribute('aria-current', 'page');
        else a.removeAttribute('aria-current');
      });
    }catch(e){}
  }
  document.addEventListener('DOMContentLoaded', markActiveSidebar);

  const SB_KEY = 'acrilsoft_sb_collapsed';
  function setCollapsed(v){
    body.classList.toggle('sb-collapsed', !!v);
    try{ localStorage.setItem(SB_KEY, v ? '1' : '0'); }catch(e){}
  }
  function loadCollapsed(){
    try{ return localStorage.getItem(SB_KEY) === '1'; }catch(e){ return false; }
  }
  document.addEventListener('DOMContentLoaded', () => {
    setCollapsed(loadCollapsed());
    const btn = $('.sb-collapse');
    if (btn) btn.addEventListener('click', () => setCollapsed(!body.classList.contains('sb-collapsed')));
  });

  // ---------------------------------------------------------------------------
  // Tema + Accent (configurável)
  // ---------------------------------------------------------------------------
  const THEME_KEY = 'acrilsoft_theme';
  const ACCENT_KEY = 'acrilsoft_accent';

  const themes = new Set(['dark','light']);
  const accents = new Set(['blue','purple','green','orange','pink','slate']);

  function setTheme(theme){
    const t = themes.has(theme) ? theme : 'dark';
    html.dataset.theme = t;
    try{ localStorage.setItem(THEME_KEY, t); }catch(e){}
    refreshIcons();
  }
  function setAccent(accent){
    const a = accents.has(accent) ? accent : 'blue';
    html.dataset.accent = a;
    try{ localStorage.setItem(ACCENT_KEY, a); }catch(e){}
  }

  function loadTheme(){
    try{ return localStorage.getItem(THEME_KEY) || html.dataset.theme || 'dark'; }catch(e){ return html.dataset.theme || 'dark'; }
  }
  function loadAccent(){
    try{ return localStorage.getItem(ACCENT_KEY) || html.dataset.accent || 'blue'; }catch(e){ return html.dataset.accent || 'blue'; }
  }

  document.addEventListener('DOMContentLoaded', () => {
    setTheme(loadTheme());
    setAccent(loadAccent()); 

    $$('[data-theme-set]').forEach(btn => {
      btn.addEventListener('click', () => setTheme(btn.dataset.themeSet));
    });
    $$('[data-accent-set]').forEach(btn => {
      btn.addEventListener('click', () => setAccent(btn.dataset.accentSet));
    });
  });

  // ---------------------------------------------------------------------------
  // Dropdown padrão
  // ---------------------------------------------------------------------------
  function closeAllDropdowns(except=null){
    $$('[data-dropdown].open').forEach(dd => { if (dd !== except) dd.classList.remove('open'); });
  }

  document.addEventListener('click', (e) => {
    const dd = e.target.closest('[data-dropdown]');
    if (!dd) return closeAllDropdowns();
    const btn = e.target.closest('.dd-btn');
    if (btn){
      const isOpen = dd.classList.contains('open');
      closeAllDropdowns(dd);
      dd.classList.toggle('open', !isOpen);
      btn.setAttribute('aria-expanded', (!isOpen).toString());
      e.preventDefault();
    }
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeAllDropdowns();
  });

  // ---------------------------------------------------------------------------
  // Modal padrão
  // ---------------------------------------------------------------------------
  const modal = $('#appModal');
  function closeModal(){
    if (!modal) return;
    modal.classList.remove('open');
    modal.setAttribute('aria-hidden','true');
    document.body.style.overflow = '';
  }
  function openModal({title='Modal', bodyHtml='', footHtml=''} = {}){
    if (!modal) return;
    $('#appModalTitle').textContent = title;
    $('#appModalBody').innerHTML = bodyHtml;
    if (footHtml) $('#appModalFoot').innerHTML = footHtml;
    modal.classList.add('open');
    modal.setAttribute('aria-hidden','false');
    document.body.style.overflow = 'hidden';
    refreshIcons();
  }
  window.AcrilsoftUI = { openModal, closeModal, setTheme, setAccent };

  document.addEventListener('click', (e) => {
    if (e.target.closest('[data-modal-close]')) closeModal();
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeModal();
  });

  // Demo buttons (mantém apenas Toast)
  document.addEventListener('click', (e) => {
    if (e.target.closest('[data-demo-toast]')){
      // tenta usar showToast do layout se existir
      if (typeof window.showToast === 'function'){
        window.showToast('success','Toast','Componente padrão funcionando.');
      } else {
        alert('Toast: funcionando.');
      }
      closeAllDropdowns();
    }
  });

  // ---------------------------------------------------------------------------
  // Tabs padrão (data-tabs)
  // ---------------------------------------------------------------------------
  document.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-tab]');
    if (!btn) return;
    const wrap = btn.closest('[data-tabs]');
    if (!wrap) return;
    const name = btn.dataset.tab;
    $$('.tab-btn', wrap).forEach(b => b.classList.toggle('active', b === btn));
    $$('.tab-panel', wrap).forEach(p => p.classList.toggle('active', p.dataset.panel === name));
  });
  // ---------------------------------------------------------------------------
  // Command Palette (Ctrl/Cmd + K)
  // ---------------------------------------------------------------------------
  function buildCommands(){
    const cmds = [];

    // Sidebar links
    $$('.sb-link').forEach(a => {
      const href = a.getAttribute('href');
      if (!href || href === '#') return;
      const title = (a.dataset.title || a.textContent || '').trim();
      if (!title) return;
      const icoEl = a.querySelector('[data-lucide]');
      const ico = icoEl ? icoEl.getAttribute('data-lucide') : 'arrow-right';
      cmds.push({ type:'nav', title, href, icon: ico, meta:'Tela' });
    });

    // Ações rápidas (se existirem no menu)
    const quick = [
      { title:'Novo Pedido', href:'/pedidos/novo', icon:'shopping-cart', meta:'Ação' },
      { title:'Novo Produto', href:'/cadastro-produtos/novo', icon:'box', meta:'Ação' },
      { title:'Financeiro: Receber', href:'/financeiro/receber', icon:'wallet', meta:'Atalho' },
      { title:'Fluxo de Caixa', href:'/financeiro/fluxo', icon:'line-chart', meta:'Atalho' },
    ];
    quick.forEach(q => cmds.push(q));

    // de-dup by href+title
    const seen = new Set();
    return cmds.filter(c => {
      const k = (c.href||'')+'|'+c.title;
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  }

  function initCmdk(){
    const wrap = $('#cmdPalette');
    const input = $('#cmdkInput');
    const list = $('#cmdkList');
    if (!wrap || !input || !list) return;

    const commands = buildCommands();
    let filtered = commands.slice();
    let active = 0;

    const render = () => {
      list.innerHTML = '';
      filtered.forEach((c, idx) => {
        const div = document.createElement('div');
        div.className = 'cmdk-item';
        div.setAttribute('role','option');
        div.setAttribute('aria-selected', idx === active ? 'true' : 'false');
        div.innerHTML = `
          <span class="cmdk-ico"><i data-lucide="${c.icon}"></i></span>
          <span class="cmdk-title">${escapeHtml(c.title)}</span>
          <span class="cmdk-meta">${escapeHtml(c.meta||'')}</span>
        `;
        div.addEventListener('click', () => {
          close();
          window.location.href = c.href;
        });
        list.appendChild(div);
      });
      refreshIcons();
    };

    const open = () => {
      wrap.classList.add('is-open');
      wrap.setAttribute('aria-hidden','false');
      input.value = '';
      filtered = commands.slice();
      active = 0;
      render();
      setTimeout(() => input.focus(), 0);
    };

    const close = () => {
      wrap.classList.remove('is-open');
      wrap.setAttribute('aria-hidden','true');
    };

    const updateFilter = () => {
      const q = input.value.trim().toLowerCase();
      filtered = !q ? commands.slice() : commands.filter(c => c.title.toLowerCase().includes(q));
      active = 0;
      render();
    };

    input.addEventListener('input', updateFilter);

    const move = (dir) => {
      if (!filtered.length) return;
      active = (active + dir + filtered.length) % filtered.length;
      render();
      // scroll into view
      const el = list.children[active];
      if (el) el.scrollIntoView({ block: 'nearest' });
    };

    input.addEventListener('keydown', (e) => {
      if (e.key === 'ArrowDown') { e.preventDefault(); move(1); }
      if (e.key === 'ArrowUp') { e.preventDefault(); move(-1); }
      if (e.key === 'Enter') {
        e.preventDefault();
        const c = filtered[active];
        if (c) { close(); window.location.href = c.href; }
      }
      if (e.key === 'Escape') { e.preventDefault(); close(); }
    });

    wrap.addEventListener('click', (e) => {
      if (e.target && e.target.matches('[data-cmdk-close]')) close();
    });

    document.addEventListener('keydown', (e) => {
      const isMac = navigator.platform.toLowerCase().includes('mac');
      const combo = isMac ? (e.metaKey && e.key.toLowerCase() === 'k') : (e.ctrlKey && e.key.toLowerCase() === 'k');
      if (combo) { e.preventDefault(); open(); }
      if (e.key === 'Escape' && wrap.classList.contains('is-open')) close();
    });
  }

  function escapeHtml(s){
    return String(s).replace(/[&<>"']/g, (m) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
  }

})();
