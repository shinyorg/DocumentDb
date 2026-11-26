# Combined PostGIS + pgvector image for spatial + vector integration tests (multi-arch incl. arm64).
FROM pgvector/pgvector:pg17
# postgresql-17 is upgraded alongside PostGIS on purpose: the base image's server can trail PGDG by a
# minor release, and PGDG's newest PostGIS may need symbols only that newer server exports (PostGIS
# 3.7.0 on a 17.11 server fails with "undefined symbol: heap_datum_check").
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-17 postgresql-17-postgis-3 \
    && rm -rf /var/lib/apt/lists/*
