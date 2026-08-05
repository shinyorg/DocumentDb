# The Docker Desktop extension. A handful of static files - metadata, an icon and the tab's HTML/CSS/JS -
# and nothing else: there is no `vm` section in metadata.json, so no backend container comes with it.
#
# That is deliberate. The tab drives the Docker CLI and starts the admin UI as an ordinary container,
# then frames its published port. A `vm` service would instead hand the container's lifecycle to a
# hidden Compose project that is torn down on uninstall - taking the data volume with it - and leave
# nothing published to frame. As an ordinary container it shows up in the Containers list, it is the
# same `docker run` the readme documents, and uninstalling the extension leaves it and its saved
# connections alone.
#
# Build from this directory - unlike the admin image, nothing here needs the repository:
#   docker build -t shinyorg/shiny-docdb-myadmin-extension src/ShinyDocDbMyAdmin.Extension
#   docker extension validate shinyorg/shiny-docdb-myadmin-extension
#   docker extension install shinyorg/shiny-docdb-myadmin-extension

FROM alpine:3.22

LABEL org.opencontainers.image.title="Shiny DocumentDb Admin" \
      org.opencontainers.image.description="Browse, query and edit any relational Shiny.DocumentDb store, with the databases already running on your machine wired up for you." \
      org.opencontainers.image.vendor="Shiny" \
      org.opencontainers.image.source="https://github.com/shinyorg/DocumentDb" \
      org.opencontainers.image.licenses="MIT" \
      com.docker.desktop.extension.api.version=">= 0.3.0" \
      com.docker.desktop.extension.icon="https://raw.githubusercontent.com/shinyorg/DocumentDb/v13/src/ShinyDocDbMyAdmin.Extension/docdb.svg" \
      com.docker.extension.publisher-url="https://shinylib.net/documentdb/admin" \
      com.docker.extension.categories="database" \
      com.docker.extension.screenshots='[ \
        {"alt":"The document explorer, with a type selected and its documents in a paged grid","url":"https://raw.githubusercontent.com/shinyorg/DocumentDb/v13/src/ShinyDocDbMyAdmin.Extension/screenshots/admin-overview.jpg"}, \
        {"alt":"Browsing documents with columns inferred from nested JSON paths","url":"https://raw.githubusercontent.com/shinyorg/DocumentDb/v13/src/ShinyDocDbMyAdmin.Extension/screenshots/admin-browse.jpg"}, \
        {"alt":"The SQL console running a query in the target dialect with bound parameters","url":"https://raw.githubusercontent.com/shinyorg/DocumentDb/v13/src/ShinyDocDbMyAdmin.Extension/screenshots/admin-sql.jpg"}, \
        {"alt":"The connections screen listing saved and host-provided stores","url":"https://raw.githubusercontent.com/shinyorg/DocumentDb/v13/src/ShinyDocDbMyAdmin.Extension/screenshots/admin-connections.jpg"} \
      ]' \
      com.docker.extension.detailed-description="<h2>Shiny DocumentDb Admin</h2> \
        <p>A phpMyAdmin-style front end for <a href=\"https://shinylib.net/documentdb\">Shiny.DocumentDb</a> stores. Browse and edit documents in a paged, filterable grid, run SQL in the target dialect, read query plans, manage JSON property indexes, search full text, inspect geometry, blobs, history and vectors, import and export JSON/NDJSON/CSV, and ask a read-only AI assistant about the data.</p> \
        <p>Works against every relational DocumentDb backend: SQLite, SQLCipher, DuckDB, PostgreSQL, SQL Server, MySQL, MariaDB, Oracle 23ai+ and CockroachDB.</p> \
        <p>This tab starts the admin UI as an ordinary container and shows it right here. Any PostgreSQL, MySQL, MariaDB, SQL Server, Oracle or CockroachDB container already running is discovered and can be handed over already connected, with credentials read from the container's own environment.</p>" \
      com.docker.extension.changelog="<p>First release. Runs the admin UI in this tab, and seeds it with the database containers running on your machine.</p>" \
      com.docker.extension.additional-urls='[ \
        {"title":"Documentation","url":"https://shinylib.net/documentdb/admin"}, \
        {"title":"Support","url":"https://github.com/shinyorg/DocumentDb/issues"}, \
        {"title":"License","url":"https://github.com/shinyorg/DocumentDb/blob/v13/LICENSE"} \
      ]'

COPY metadata.json .
COPY docdb.svg .
COPY ui ./ui
