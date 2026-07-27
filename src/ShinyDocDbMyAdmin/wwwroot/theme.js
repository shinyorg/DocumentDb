// Theme is applied by an inline script in the document head before first paint; this module only
// handles the toggle so a click never has to round-trip to the server.
export function toggle() {
    const root = document.documentElement;
    const current = root.getAttribute('data-theme')
        || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
    const next = current === 'dark' ? 'light' : 'dark';

    root.setAttribute('data-theme', next);
    try { localStorage.setItem('shinydocdbmyadmin-theme', next); } catch { /* private mode */ }
    return next;
}

export function current() {
    return document.documentElement.getAttribute('data-theme')
        || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
}

/** Triggers a browser download for a URL without navigating the Blazor circuit away. */
export function download(url) {
    const a = document.createElement('a');
    a.href = url;
    a.download = '';
    document.body.appendChild(a);
    a.click();
    a.remove();
}

/** Copies text to the clipboard, returning false when the browser refuses. */
export async function copy(text) {
    try {
        await navigator.clipboard.writeText(text);
        return true;
    } catch {
        return false;
    }
}
