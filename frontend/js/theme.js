/**
 * Theme manager — 4-colour palette for partner-test.
 * Applies a data-theme attribute to <html> so CSS vars follow.
 * Persists to localStorage.
 */

var Theme = (function() {
  var STORAGE_KEY = 'partner-test-theme';
  var themes = [
    { id: 'rose',   label: '玫瑰粉', icon: '🌸' },
    { id: 'ocean',  label: '海洋蓝', icon: '🌊' },
    { id: 'forest', label: '森林绿', icon: '🌿' },
    { id: 'sunset', label: '日落橙', icon: '🌅' }
  ];

  function get() {
    return document.documentElement.getAttribute('data-theme') || 'rose';
  }

  function set(name) {
    if (!themes.some(function(t) { return t.id === name; })) name = 'rose';
    document.documentElement.setAttribute('data-theme', name);
    try { localStorage.setItem(STORAGE_KEY, name); } catch(e) {}
  }

  function apply() {
    var saved;
    try { saved = localStorage.getItem(STORAGE_KEY); } catch(e) {}
    set(saved || 'rose');
  }

  function list() {
    return themes;
  }

  /**
   * Render a row of clickable theme dots into the given container element.
   * Call from any page that wants the palette.
   */
  function renderPalette(container) {
    if (!container) return;
    var current = get();
    var html = '';
    for (var i = 0; i < themes.length; i++) {
      var t = themes[i];
      var active = current === t.id;
      html += '<button title="' + t.label + '" onclick="Theme.set(\'' + t.id + '\')" style="'
        + 'width:28px;height:28px;border-radius:50%;border:2px solid ' + (active ? 'var(--primary-dark)' : 'transparent') + ';'
        + 'background:var(--primary);cursor:pointer;margin:0 4px;padding:0;'
        + 'transition:transform 0.2s,box-shadow 0.2s;'
        + 'display:inline-flex;align-items:center;justify-content:center;'
        + 'font-size:12px;'
        + '" onmouseover="this.style.transform=\'scale(1.2)\'" onmouseout="this.style.transform=\'scale(1)\'">'
        + t.icon
        + '</button>';
    }
    container.innerHTML = html;
  }

  // Auto-apply on script load
  apply();

  return {
    get: get,
    set: set,
    apply: apply,
    list: list,
    renderPalette: renderPalette
  };
})();
