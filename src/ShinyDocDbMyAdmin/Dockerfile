# ShinyDocDbMyAdmin - the admin UI. This image is the only way the app ships; it is what
# Shiny.DocumentDb.Aspire.Hosting's AddDocumentDbAdmin runs, both locally and in a deployment.
#
# Build from the repository root, since the app project references the provider projects:
#   docker build -f src/ShinyDocDbMyAdmin/Dockerfile -t shinyorg/shiny-docdb-myadmin .

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY . .
RUN dotnet publish src/ShinyDocDbMyAdmin/ShinyDocDbMyAdmin.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .

# Matches DocumentDbAdminResourceBuilderExtensions.ContainerPort - change one, change the other.
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080

# Saved connections, saved queries and uploaded database files. Mount something here to keep them.
ENV ShinyDocDbMyAdmin__DataDirectory=/data
VOLUME /data

ENTRYPOINT ["dotnet", "ShinyDocDbMyAdmin.dll"]
