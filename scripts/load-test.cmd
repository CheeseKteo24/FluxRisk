@echo off
setlocal
cd /d "%~dp0.."
if not exist ".appdata" mkdir ".appdata"
if not exist ".nuget-packages" mkdir ".nuget-packages"
set "APPDATA=%CD%\.appdata"
set "NUGET_PACKAGES=%CD%\.nuget-packages"
set "TARGET=%~1"
if "%TARGET%"=="" set "TARGET=http://127.0.0.1:8080"
set "REQUESTS=%~2"
if "%REQUESTS%"=="" set "REQUESTS=1000"
set "CONCURRENCY=%~3"
if "%CONCURRENCY%"=="" set "CONCURRENCY=32"
if exist ".nuget-local\npgsql.10.0.3.nupkg" (
  dotnet restore tools\FluxRisk.Load\FluxRisk.Load.csproj --source .nuget-local --ignore-failed-sources || exit /b 1
) else (
  dotnet restore tools\FluxRisk.Load\FluxRisk.Load.csproj --configfile NuGet.Config || exit /b 1
)
dotnet run --project tools\FluxRisk.Load --configuration Release --no-restore -- "%TARGET%" "%REQUESTS%" "%CONCURRENCY%"
