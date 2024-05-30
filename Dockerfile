#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
USER app
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Ajeevi.IOT.RabbitMQListenerStateless.csproj", "."]
RUN dotnet restore "./Ajeevi.IOT.RabbitMQListenerStateless.csproj"
COPY . .
WORKDIR "/src/."
RUN dotnet build "./Ajeevi.IOT.RabbitMQListenerStateless.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./Ajeevi.IOT.RabbitMQListenerStateless.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Ajeevi.IOT.RabbitMQListenerStateless.dll"]