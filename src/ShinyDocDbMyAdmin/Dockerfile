# ShinyDocDbMyAdmin - the admin UI. This image is the only way the app ships; it is what
# Shiny.DocumentDb.Aspire.Hosting's AddDocumentDbAdmin runs, both locally and in a deployment.
#
# Build from the repository root, since the app project references the provider projects:
#   docker build -f src/ShinyDocDbMyAdmin/Dockerfile -t shinyorg/shiny-docdb-myadmin .

# The SDK must satisfy global.json (11.x) because the referenced library projects also build net11.0;
# the app itself still targets net10.0, so the runtime image below stays on 10.0.
FROM mcr.microsoft.com/dotnet/sdk:11.0 AS build
WORKDIR /src

# release.yml passes the v* tag's version so the app reports the tag the image is pushed under,
# which is also what the NuGets ship as. Local builds fall back to Directory.Build.props' 0.0.0-dev.
ARG VERSION=

COPY . .
RUN dotnet publish src/ShinyDocDbMyAdmin/ShinyDocDbMyAdmin.csproj -c Release -o /app \
    ${VERSION:+-p:Version=${VERSION}}

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .

# Links the GHCR package to this repository - the package page picks up the repo, readme and
# license from these, and can inherit the repo's access settings.
LABEL org.opencontainers.image.source="https://github.com/shinyorg/DocumentDb" \
      org.opencontainers.image.description="Shiny DocumentDb Admin UI - browse and query any DocumentDb provider." \
      org.opencontainers.image.licenses="MIT"

# Matches DocumentDbAdminResourceBuilderExtensions.ContainerPort - change one, change the other.
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080

# Saved connections, saved queries and uploaded database files. Mount something here to keep them.
ENV ShinyDocDbMyAdmin__DataDirectory=/data
VOLUME /data

ENTRYPOINT ["dotnet", "ShinyDocDbMyAdmin.dll"]
