// Turning `docker ps` + `docker inspect` output into DocumentDb connections. Split out from app.js
// because none of it touches the DOM or the Docker CLI: it is a pure function of what inspect
// returned, which is what makes it checkable against real containers without a browser.
//
// Wrapped, and publishing exactly one name, for the reason app.js spells out at length: the extension
// page already has globals of its own, and a classic script that declares a colliding one fails to
// parse in its entirety.
(function (root) {
    'use strict';

    // The backends this tool can administer that can also exist as a container. SQLite, SQLCipher and
    // DuckDB are files, so they are absent by design - there is nothing running to discover.
    const PROVIDERS = [
        {
            kind: 'PostgreSql',
            label: 'PostgreSQL',
            matches: /(^|\/)(postgres|postgis|pgvector|timescaledb)([-\w]*)(:|$)/i,
            build: (host, env) => {
                const user = env.POSTGRES_USER || 'postgres';
                return `Host=${host};Port=5432;Database=${env.POSTGRES_DB || user};Username=${user};Password=${env.POSTGRES_PASSWORD || ''}`;
            }
        },
        {
            kind: 'CockroachDb',
            label: 'CockroachDB',
            matches: /cockroachdb\/cockroach/i,
            build: host => `Host=${host};Port=26257;Database=defaultdb;Username=root;SSL Mode=Disable`
        },
        {
            kind: 'MariaDb',
            label: 'MariaDB',
            matches: /(^|\/)mariadb(:|$)/i,
            build: (host, env) => {
                const password = env.MARIADB_ROOT_PASSWORD || env.MYSQL_ROOT_PASSWORD || '';
                const database = env.MARIADB_DATABASE || env.MYSQL_DATABASE || 'mysql';
                return `Server=${host};Port=3306;Database=${database};User Id=root;Password=${password}`;
            }
        },
        {
            kind: 'MySql',
            label: 'MySQL',
            matches: /(^|\/)mysql(\/mysql-server)?(:|$)/i,
            build: (host, env) =>
                `Server=${host};Port=3306;Database=${env.MYSQL_DATABASE || 'mysql'};User Id=root;Password=${env.MYSQL_ROOT_PASSWORD || ''}`
        },
        {
            kind: 'SqlServer',
            label: 'SQL Server',
            matches: /mssql\/server|azure-sql-edge/i,
            build: (host, env) => {
                const password = env.MSSQL_SA_PASSWORD || env.SA_PASSWORD || '';
                return `Server=${host},1433;Database=master;User Id=sa;Password=${password};TrustServerCertificate=true`;
            }
        },
        {
            kind: 'Oracle',
            label: 'Oracle',
            matches: /oracle-(free|xe)|database\/free/i,
            // ORACLE_DATABASE names an extra pluggable database the image creates on first boot; without
            // it the only service is the default one.
            build: (host, env) =>
                `User Id=system;Password=${env.ORACLE_PASSWORD || ''};Data Source=${host}:1521/${env.ORACLE_DATABASE || 'FREEPDB1'}`
        }
    ];

    // Networks that carry no usable DNS or no reachable address at all. `bridge` is the odd one: it is a
    // real network the admin container is already on, but the default bridge has no name resolution, so
    // containers there have to be addressed by IP.
    const DEFAULT_BRIDGE = 'bridge';
    const UNROUTABLE = ['host', 'none'];

    function providerFor(image) {
        return PROVIDERS.find(p => p.matches.test(image || ''));
    }

    // A connection name becomes a configuration key on the admin side, so it has to survive the trip
    // through an environment variable - `ConnectionStrings__<name>`. Anything outside this set would
    // either be rejected by the shell or split the key in two.
    function safeName(name) {
        const cleaned = (name || '').replace(/^\//, '').replace(/[^A-Za-z0-9-]/g, '-');
        return cleaned.length > 0 ? cleaned : 'db';
    }

    function envOf(container) {
        const entries = {};
        for (const item of (container.Config?.Env || [])) {
            const eq = item.indexOf('=');
            if (eq > 0)
                entries[item.slice(0, eq)] = item.slice(eq + 1);
        }
        return entries;
    }

    // Where the *admin container* should look for this database. Once the two share a network the
    // container name resolves, which beats a published port: it works whether or not the database ever
    // published one. The default bridge is the exception - no DNS there, so it is the IP or nothing.
    function addressOf(container) {
        const networks = container.NetworkSettings?.Networks || {};
        const names = Object.keys(networks).filter(n => !UNROUTABLE.includes(n));

        const shared = names.find(n => n !== DEFAULT_BRIDGE);
        if (shared)
            return { host: safeName(container.Name), network: shared };

        if (names.includes(DEFAULT_BRIDGE)) {
            const ip = networks[DEFAULT_BRIDGE].IPAddress;
            return ip ? { host: ip, network: null } : null;
        }

        return null;
    }

    /// Everything `docker inspect` returned, reduced to the connections worth offering. A container with
    /// no reachable address is dropped rather than listed-and-broken: there is nothing the user could do
    /// about it from here.
    function toConnections(inspected) {
        const connections = [];
        for (const container of inspected) {
            const provider = providerFor(container.Config?.Image);
            const address = provider ? addressOf(container) : null;

            if (provider && address) {
                connections.push({
                    name: safeName(container.Name),
                    provider,
                    network: address.network,
                    image: container.Config.Image,
                    connectionString: provider.build(address.host, envOf(container))
                });
            }
        }
        return connections;
    }

    // The password is read out of the database container's own environment, so it is hardly a secret
    // from anyone who can run `docker inspect` - but it does not need to be sitting on screen either.
    function redact(connectionString) {
        return connectionString.replace(/(Password=)([^;]*)/i, (_, key, value) => key + '•'.repeat(Math.min(value.length, 8)));
    }

    const api = { PROVIDERS, providerFor, safeName, envOf, addressOf, toConnections, redact };

    // One global for the browser, CommonJS for the checks that exercise this against real `docker
    // inspect` output. Neither the extension nor the repo has a bundler, so this is how one file
    // serves both.
    if (typeof module !== 'undefined' && module.exports)
        module.exports = api;
    else
        root.ShinyDocDbDiscovery = api;
})(typeof globalThis !== 'undefined' ? globalThis : window);
