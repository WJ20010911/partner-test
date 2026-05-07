/**
 * Shared utility functions for Partner Test frontend.
 */

/**
 * Escape HTML special characters to prevent XSS.
 */
function escapeHtml(str) {
  if (!str) return '';
  const div = document.createElement('div');
  div.appendChild(document.createTextNode(str));
  return div.innerHTML.replace(/"/g, '&quot;');
}

/**
 * Format a ISO datetime string to Beijing time (UTC+8).
 */
function formatBeijingTime(isoStr) {
  if (!isoStr) return '-';
  try {
    var d = new Date(isoStr);
    var beijing = new Date(d.getTime() + 8 * 60 * 60 * 1000);
    var pad = function(n) { return (n < 10 ? '0' : '') + n; };
    return beijing.getUTCFullYear() + '-' +
      pad(beijing.getUTCMonth() + 1) + '-' +
      pad(beijing.getUTCDate()) + ' ' +
      pad(beijing.getUTCHours()) + ':' +
      pad(beijing.getUTCMinutes()) + ':' +
      pad(beijing.getUTCSeconds());
  } catch {
    return isoStr;
  }
}

/**
 * Format a ISO datetime string to a human-readable format.
 * @deprecated Use formatBeijingTime instead for consistent UTC+8 display.
 */
function formatDateTime(isoStr) {
  if (!isoStr) return '-';
  try {
    const d = new Date(isoStr);
    return d.toLocaleString('zh-CN', {
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit'
    });
  } catch {
    return isoStr;
  }
}

/**
 * Copy text to clipboard, with fallback for older browsers.
 */
function copyToClipboard(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    return navigator.clipboard.writeText(text);
  }
  // Fallback
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.style.position = 'fixed';
  ta.style.left = '-9999px';
  document.body.appendChild(ta);
  ta.select();
  document.execCommand('copy');
  document.body.removeChild(ta);
  return Promise.resolve();
}

/**
 * Simple localStorage wrapper with JSON serialization.
 */
const storage = {
  get(key, fallback) {
    try {
      const raw = localStorage.getItem(key);
      if (raw === null) return fallback;
      return JSON.parse(raw);
    } catch {
      return fallback;
    }
  },
  set(key, value) {
    localStorage.setItem(key, JSON.stringify(value));
  },
  remove(key) {
    localStorage.removeItem(key);
  }
};

/**
 * Debounce a function call.
 */
function debounce(fn, ms) {
  let timer = null;
  return function (...args) {
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => {
      timer = null;
      fn.apply(this, args);
    }, ms);
  };
}

/**
 * Throttle a function call.
 */
function throttle(fn, ms) {
  let last = 0;
  return function (...args) {
    const now = Date.now();
    if (now - last >= ms) {
      last = now;
      fn.apply(this, args);
    }
  };
}