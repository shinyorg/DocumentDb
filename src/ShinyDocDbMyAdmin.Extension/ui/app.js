// The extension frontend. No framework and no build step on purpose: everything this tab does is
// shell out to the Docker CLI and render a list, and a React toolchain in a .NET repo would be more
// infrastructure than the feature is worth.
//
// Everything is inside this IIFE, and that is not stylistic. Docker Desktop injects the extension API
// into the page as a global *declaration* named `ddClient`, so a classic script that says
// `const ddClient = ...` at top level is a redeclaration - "Identifier 'ddClient' has already been
// declared" - and the whole file fails to parse before a line of it runs. The page still renders,
// styled, with every button inert, which is a memorably confusing way to find out. The same trap is
// waiting for any other name the host happens to use, so nothing here is declared globally.
//
// `window.ddClient` is the same object @docker/extension-api-client hands back - that package is a
// one-line wrapper around this global.
//
// Everything that turns container output into connections lives in discovery.js, loaded before this.
(function () {
    'use strict';

    const dd = window.ddClient;
    const { toConnections, redact } = window.ShinyDocDbDiscovery;

    // Canonical registry. The Docker Hub copy is a byte-identical mirror (see .github/workflows/
    // admin-image.yml), so either would work; this matches what AddDocumentDbAdmin pulls and what the
    // docs quote, which is what someone comparing the two would expect to see.
    const IMAGE = 'ghcr.io/shinyorg/shiny-docdb-myadmin';
    const CONTAINER = 'shiny-docdb-myadmin';

    // Marks the container as ours so a rename or a port change cannot orphan it - the name is the handle
    // but the label is how we recognise what we started.
    const LABEL = 'com.shiny.documentdb.admin';

    // Set on the container we create so the app will tolerate being embedded. Blazor's interactive server
    // render mode blocks framing by default; without this the iframe below renders nothing at all.
    const FRAME_ANCESTORS_ENV = 'ShinyDocDbMyAdmin__FrameAncestors';

    const el = id => document.getElementById(id);
    const ui = {
        statusDot: el('status-dot'),
        statusText: el('status-text'),
        port: el('port'),
        tag: el('tag'),
        volume: el('volume'),
        start: el('start'),
        open: el('open'),
        stop: el('stop'),
        remove: el('remove'),
        imageHint: el('image-hint'),
        discovered: el('discovered'),
        setup: el('setup'),
        appFrame: el('app-frame'),
        frame: el('frame'),
        noEmbed: el('no-embed'),
        logCard: el('log-card'),
        log: el('log')
    };

    let discovered = [];
    let selected = new Set();
    let busy = false;

    // ---------------------------------------------------------------------------- docker plumbing

    // The SDK does not spawn the CLI with an argv. It joins what you give it into one shell command
    // line and refuses the result if `shell-quote` finds an operator in it - so an argument containing
    // `;`, `*`, `|`, `&`, a redirect or a space is either rejected outright or silently split. Every
    // relational connection string contains `;`; a bare `*` is a glob. Both had to go.
    //
    // Checking here rather than trusting each call site: the failure arrives as an opaque "shell
    // operators are not allowed when executing commands through SDK APIs" naming the whole command
    // line, which is a poor way to learn which argument was at fault.
    const SHELL_OPERATORS = /[;&|<>*?()$`\\"'\s]/;

    async function docker(command, args) {
        const offender = args.find(a => SHELL_OPERATORS.test(a));
        if (offender !== undefined)
            throw new Error(`refusing to run "docker ${command}": the argument "${offender}" contains a character the extension SDK will not pass through.`);

        const result = await dd.docker.cli.exec(command, args);
        if (result.stderr && result.stderr.trim())
            log(result.stderr.trim());

        return result;
    }

    function log(message) {
        ui.logCard.hidden = false;
        ui.log.textContent += (ui.log.textContent ? '\n' : '') + message;
        ui.log.scrollTop = ui.log.scrollHeight;
    }

    function describe(error) {
        return String(error?.stderr || error?.message || error);
    }

    // btoa works on bytes, not text, so a non-ASCII character in a password would throw. Encode to
    // UTF-8 first - the admin side decodes with Encoding.UTF8 to match.
    function base64(value) {
        return btoa(String.fromCharCode(...new TextEncoder().encode(value)));
    }

    // `docker inspect` exits non-zero when nothing matches, which is an answer rather than a failure -
    // the callers here are asking "does this exist". But *only* that one failure is an answer: swallowing
    // the rest turns a broken Docker connection into "no container yet", which renders as a perfectly
    // calm setup form that does nothing when you press Start. Anything else is surfaced.
    //
    // `--type container` is not optional. Bare `docker inspect` resolves a name against images, volumes
    // and networks too, and the admin container shares its name with the image it is built from - so
    // without this, a machine that has pulled the image but has no container reports one that exists,
    // is not running, and cannot be started.
    async function inspect(refs) {
        if (refs.length === 0)
            return [];

        try {
            const result = await dd.docker.cli.exec('inspect', ['--type', 'container', ...refs]);
            return result.parseJsonObject();
        }
        catch (error) {
            const message = describe(error);
            if (/no such (object|container)/i.test(message))
                return [];

            throw new Error(`docker inspect failed: ${message}`);
        }
    }

    // ---------------------------------------------------------------------------- admin container

    async function refresh() {
        const [container] = await inspect([CONTAINER]);
        const exists = container !== undefined;
        const running = container?.State?.Running === true;
        const port = exists ? hostPortOf(container) : null;

        // Only a container started with the flag can be framed. Checking what it was actually created
        // with beats trying to detect the block from the iframe: a refused embed just renders blank, with
        // nothing the parent page is allowed to observe.
        const embeddable = (container?.Config?.Env || []).some(e => e.startsWith(FRAME_ANCESTORS_ENV + '='));

        setStatus(
            exists ? (running ? 'running' : 'stopped') : 'missing',
            exists ? (running ? 'Running' : 'Stopped') : 'Not created'
        );

        ui.start.disabled = busy || running;
        ui.open.disabled = busy || !running;
        ui.stop.disabled = busy || !running;
        ui.remove.disabled = busy || !exists;

        // The settings baked into `docker run` cannot be changed on a container that already exists, so
        // rather than silently ignoring an edit, the inputs go read-only and say what to do instead.
        for (const input of [ui.port, ui.tag, ui.volume])
            input.disabled = busy || exists;

        if (port)
            ui.port.value = port;

        if (exists) {
            ui.imageHint.textContent =
                `Container "${CONTAINER}" exists (${container.Config?.Image || IMAGE}). Remove it to change the port, tag, volume or the connections it was started with.`;
        }
        else {
            ui.imageHint.textContent =
                `Starting pulls ${IMAGE}:${ui.tag.value || 'latest'} if it is not already here - roughly 1 GB the first time.`;
        }

        showApp(running && embeddable && port, running, port);

        return { exists, running };
    }

    // Three states: the app embedded, the app running but refusing to be framed, or the setup form.
    //
    // Embedded means the tab *is* the app - no header, no padding, edge to edge, the way every other
    // extension that fronts a web app looks. The controls that bar carried are not lost: Docker
    // Desktop's own Containers tab stops and removes containers, and this one is an ordinary
    // container precisely so that works. Remove it there and this tab returns to the setup form.
    function showApp(embedded, running, port) {
        document.body.classList.toggle('embedded', embedded);
        ui.appFrame.hidden = !embedded;
        ui.noEmbed.hidden = !(running && !embedded);
        ui.setup.hidden = running;

        const src = embedded ? `http://localhost:${port}/` : '';

        // Assigning the same src again reloads the app and throws away whatever the user was looking at,
        // and refresh() runs after every action - so only touch it when it actually changes.
        if (ui.frame.getAttribute('src') !== src) {
            if (src)
                ui.frame.setAttribute('src', src);
            else
                ui.frame.removeAttribute('src');
        }
    }

    function hostPortOf(container) {
        const binding = container.NetworkSettings?.Ports?.['8080/tcp'];
        return binding && binding.length > 0 ? binding[0].HostPort : null;
    }

    function setStatus(state, text) {
        ui.statusDot.className = 'dot ' + state;
        ui.statusText.textContent = text;
    }

    // ---------------------------------------------------------------------------- discovery

    async function scan() {
        ui.discovered.innerHTML = '<p class="empty">Scanning…</p>';

        // `-q` rather than `--format '{{json .}}'` deliberately. That format string contains a space,
        // and the extension host does not hand arguments to the Docker CLI as cleanly as a browser
        // harness does - the SDK's own examples quote it defensively for the same reason. Ids need no
        // quoting and cannot be split, and the pre-filter it used to do is redundant anyway: the
        // provider match below reads `Config.Image` from the inspect output, not from `ps`.
        const ids = (await docker('ps', ['-q'])).lines().map(id => id.trim()).filter(Boolean);

        discovered = toConnections(await inspect(ids));

        // Anything that vanished between scans should not stay ticked - the start command would then
        // reference a container that is no longer there.
        const present = new Set(discovered.map(d => d.name));
        selected = new Set([...selected].filter(n => present.has(n)));

        renderDiscovered();
    }

    function renderDiscovered() {
        if (discovered.length === 0) {
            ui.discovered.innerHTML =
                '<p class="empty">No PostgreSQL, MySQL, MariaDB, SQL Server, Oracle or CockroachDB containers are running. Start one and rescan &mdash; or add the connection yourself once the UI is open.</p>';
            return;
        }

        ui.discovered.innerHTML = '';
        for (const db of discovered) {
            const row = document.createElement('label');
            row.className = 'db';

            const check = document.createElement('input');
            check.type = 'checkbox';
            check.checked = selected.has(db.name);
            check.addEventListener('change', () => {
                if (check.checked)
                    selected.add(db.name);
                else
                    selected.delete(db.name);
            });

            const main = document.createElement('div');
            main.className = 'db-main';
            main.innerHTML = '<span class="db-name"></span><span class="badge"></span><code class="db-conn"></code>';
            main.querySelector('.db-name').textContent = db.name;
            main.querySelector('.badge').textContent = db.provider.label;
            main.querySelector('.db-conn').textContent = redact(db.connectionString);

            row.append(check, main);
            ui.discovered.append(row);
        }
    }

    // ---------------------------------------------------------------------------- actions

    // Every button goes through here, so nothing it calls may reject: an unhandled rejection out of a
    // click handler is invisible, and the button just appears not to work.
    async function withBusy(action) {
        busy = true;
        try {
            await refresh();
            await action();
        }
        catch (error) {
            log(describe(error));
            dd.desktopUI?.toast?.error('DocumentDb Admin: ' + describe(error));
        }
        finally {
            busy = false;
            try {
                await refresh();
            }
            catch (error) {
                log(describe(error));
            }
        }
    }

    async function start() {
        await withBusy(async () => {
            const { exists } = await refresh();

            if (exists)
                await docker('start', [CONTAINER]);
            else
                await create();

            await waitUntilListening();
            dd.desktopUI.toast.success('DocumentDb Admin is running.');
        });
    }

    async function create() {
        const port = String(parseInt(ui.port.value, 10) || 8085);
        const tag = ui.tag.value.trim() || 'latest';
        const volume = ui.volume.value.trim() || 'shiny-docdb-myadmin';
        const chosen = discovered.filter(d => selected.has(d.name));

        const args = [
            '-d',
            '--name', CONTAINER,
            '--label', `${LABEL}=true`,

            // What the extensions that declare a `vm` service get for free from their compose file:
            // the container comes back after a Docker Desktop restart, so the tab keeps opening
            // straight onto the app instead of onto a setup form once per reboot.
            '--restart', 'unless-stopped',

            '-p', `${port}:8080`,
            '-v', `${volume}:/data`,

            // What lets the tab show the app instead of just launching it. Scoped to this container, and
            // only ever set here - a container someone starts by hand keeps Blazor's default refusal.
            // "any" rather than the CSP-idiomatic "*", which the SDK reads as a glob and rejects.
            '-e', `${FRAME_ANCESTORS_ENV}=any`
        ];

        // What ProvidedConnections looks for. Almost the pair the Aspire integration emits - a seeded
        // connection is indistinguishable from one an AppHost referenced in, and just as un-editable
        // in the UI, which is right: this tab owns it.
        //
        // The one difference is that the connection string goes over base64. It cannot go literally:
        // every relational connection string is full of `;`, which the SDK reads as a shell operator
        // and refuses. Base64's alphabet has no operator, glob or space in it.
        for (const db of chosen) {
            args.push('-e', `Shiny__DocumentDb__${db.name}__ConnectionStringBase64=${base64(db.connectionString)}`);
            args.push('-e', `Shiny__DocumentDb__${db.name}__Provider=${db.provider.kind}`);
        }

        args.push(`${IMAGE}:${tag}`);

        log(`Starting ${CONTAINER} on port ${port}…`);
        await docker('run', args);

        // Joining after the fact rather than with `--network`, which takes one network only. A database
        // that turns out to be unreachable is worth a warning but not a failed start: the other
        // connections still work, and the UI can be pointed anywhere by hand.
        const networks = [...new Set(chosen.map(d => d.network).filter(Boolean))];
        for (const network of networks) {
            try {
                await docker('network', ['connect', network, CONTAINER]);
                log(`Joined network ${network}.`);
            }
            catch (error) {
                log(`Could not join network ${network}: ${describe(error)}`);
            }
        }
    }

    // The app is ready when Kestrel says so. Polling the container's own log beats polling the port: the
    // extension frontend cannot make an ordinary request to the host, and "the container is running"
    // arrives well before the first page will render.
    async function waitUntilListening() {
        for (let attempt = 0; attempt < 60; attempt++) {
            const [container] = await inspect([CONTAINER]);
            if (container?.State?.Running !== true)
                throw new Error('The container stopped while starting up. Check its logs in the Containers tab.');

            const logs = await docker('logs', ['--tail', '50', CONTAINER]);
            if ((logs.stdout + logs.stderr).includes('Now listening on'))
                return;

            await new Promise(resolve => setTimeout(resolve, 1000));
        }

        log('Timed out waiting for the app to report it was listening; it may still come up.');
    }

    function open() {
        dd.host.openExternal(`http://localhost:${parseInt(ui.port.value, 10) || 8085}`);
    }

    async function stop() {
        await withBusy(async () => {
            await docker('stop', [CONTAINER]);
            log('Stopped.');
        });
    }

    async function remove() {
        await withBusy(async () => {
            const volume = ui.volume.value.trim() || 'shiny-docdb-myadmin';
            await docker('rm', ['-f', CONTAINER]);
            log(`Removed ${CONTAINER}. The ${volume} volume is untouched - your saved connections and queries are still there.`);
        });
    }

    // ---------------------------------------------------------------------------- wiring

    // Runs the handful of commands everything else is built on and prints exactly what came back -
    // stdout, stderr, or the thrown value. When the tab is behaving unexpectedly this says why in one
    // click, instead of leaving someone to infer it from a form that renders but does nothing.
    async function diagnose() {
        ui.log.textContent = '';
        log(`dd: ${dd ? 'present' : 'MISSING'}`);
        log(`docker.cli.exec: ${typeof dd?.docker?.cli?.exec}`);
        log(`host.openExternal: ${typeof dd?.host?.openExternal}`);
        log(`location: ${window.location.href}`);

        const probes = [
            ['version', ['--format', '{{.Server.Version}}']],
            ['ps', ['-q']],
            ['ps', ['--format', '{{json .}}']],
            ['inspect', ['--type', 'container', CONTAINER]]
        ];

        for (const [command, args] of probes) {
            log(`\n$ docker ${command} ${args.join(' ')}`);
            try {
                const result = await dd.docker.cli.exec(command, args);
                const out = (result.stdout || '').trim();
                log(`  stdout (${out.length} chars): ${out.slice(0, 400) || '(empty)'}`);
                if ((result.stderr || '').trim())
                    log(`  stderr: ${result.stderr.trim().slice(0, 400)}`);
            }
            catch (error) {
                log(`  THREW: ${describe(error)}`);
            }
        }
    }

    el('diagnose').addEventListener('click', () => diagnose().catch(e => log(describe(e))));
    ui.start.addEventListener('click', start);
    ui.open.addEventListener('click', open);
    ui.stop.addEventListener('click', stop);
    ui.remove.addEventListener('click', remove);
    el('rescan').addEventListener('click', () => scan().catch(e => log(describe(e))));
    el('clear-log').addEventListener('click', () => {
        ui.log.textContent = '';
        ui.logCard.hidden = true;
    });

    (async () => {
        // The extension host injects this. Without it nothing below can work, and every symptom would
        // otherwise read as "no container yet" - so say so plainly rather than showing a form that
        // silently refuses to do anything.
        if (!dd || !dd.docker || !dd.docker.cli) {
            setStatus('error', 'Extension API unavailable');
            ui.setup.hidden = true;
            log('window.ddClient was not injected by Docker Desktop, so this tab cannot run Docker commands. Update Docker Desktop, or reinstall the extension.');
            return;
        }

        // Two failures, two meanings. Not knowing the container's state leaves the tab with nothing to
        // show, and the status has to say so. Failing to enumerate databases does not: the app may be
        // running and embedded right above, and overwriting a perfectly accurate "Running" with
        // "Docker is not responding" would be its own small lie.
        try {
            await refresh();
        }
        catch (error) {
            setStatus('error', 'Docker is not responding');
            log(describe(error));
            return;
        }

        try {
            await scan();
        }
        catch (error) {
            ui.discovered.innerHTML = '<p class="empty">Could not list containers &mdash; see Output below.</p>';
            log(`Scanning for database containers failed: ${describe(error)}`);
        }
    })();
})();
