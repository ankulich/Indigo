FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Aggregator.Core/Aggregator.Core.csproj", "Aggregator.Core/"]
RUN dotnet restore "Aggregator.Core/Aggregator.Core.csproj"
COPY . .
WORKDIR "/src/Aggregator.Core"
RUN dotnet build "Aggregator.Core.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "Aggregator.Core.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Aggregator.Core.dll"]

