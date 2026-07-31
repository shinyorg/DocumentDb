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

/**
 * Saves text the server produced as a file, without an endpoint to fetch it from.
 *
 * Document exports go through /export/... because they can be gigabytes and must never land in the
 * circuit's memory. A connection bundle is kilobytes, and routing it through a URL would mean
 * either putting an export passphrase in a query string or inventing a server-side handoff token
 * for something that fits in a chat message.
 */
export function downloadText(fileName, contentType, text) {
    const url = URL.createObjectURL(new Blob([text], { type: contentType }));
    const a = document.createElement('a');
    a.href = url;
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
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
