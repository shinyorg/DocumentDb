# ShinyDocDbMyAdmin - the admin UI. This image is the only way the app ships; it is what
# Shiny.DocumentDb.Aspire.Hosting's AddDocumentDbAdmin runs, both locally and in a deployment.
#
# Build from the repository root, since the app project references the provider projects:
#   docker build -f src/ShinyDocDbMyAdmin/Dockerfile -t shinyorg/shiny-docdb-myadmin .

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Nerdbank.GitVersioning stamps the build from git history in here. CI passes true so the version
# matches the tag the image is pushed under, which is also what the NuGets ship as.
ARG PUBLIC_RELEASE=false

COPY . .
RUN dotnet publish src/ShinyDocDbMyAdmin/ShinyDocDbMyAdmin.csproj -c Release -o /app \
    -p:PublicRelease=${PUBLIC_RELEASE}

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
