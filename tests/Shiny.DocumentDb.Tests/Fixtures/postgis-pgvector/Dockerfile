# Combined PostGIS + pgvector image for spatial + vector integration tests (multi-arch incl. arm64).
FROM pgvector/pgvector:pg17
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-17-postgis-3 \
    && rm -rf /var/lib/apt/lists/*
