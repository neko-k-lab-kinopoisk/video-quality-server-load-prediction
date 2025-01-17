FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /app

COPY RedisKafkaQualityManager.csproj .
RUN dotnet restore

COPY . .
RUN dotnet publish -c Release -o out

FROM mcr.microsoft.com/dotnet/runtime:7.0 AS runtime
WORKDIR /app
COPY --from=build /app/out ./

ENTRYPOINT ["dotnet", "RedisKafkaQualityManager.dll"]
