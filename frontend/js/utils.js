// Utility functions

function getQueryParam(name) {
  const params = new URLSearchParams(window.location.search);
  return params.get(name);
}

function showFlash(message, type = 'info') {
  const container = document.getElementById('flash-container');
  if (!container) return;
  // Clear all existing flash messages to prevent stacking
  container.innerHTML = '';
  const div = document.createElement('div');
  div.className = `flash flash-${type}`;
  div.textContent = message;
  container.appendChild(div);
  setTimeout(() => { if (div.parentNode) div.remove(); }, 5000);
}

function clearFlash() {
  const container = document.getElementById('flash-container');
  if (container) container.innerHTML = '';
}
