@echo off
setlocal
cd /d "%~dp0.."
if not exist ".appdata" mkdir ".appdata"
if not exist ".nuget-packages" mkdir ".nuget-packages"
set "APPDATA=%CD%\.appdata"
set "NUGET_PACKAGES=%CD%\.nuget-packages"

if exist ".nuget-local\npgsql.10.0.3.nupkg" (
  dotnet restore FluxRisk.slnx --source .nuget-local --ignore-failed-sources || exit /b 1
) else (
  dotnet restore FluxRisk.slnx --configfile NuGet.Config || exit /b 1
)
dotnet build FluxRisk.slnx --configuration Release --no-restore || exit /b 1
dotnet run --project tests\FluxRisk.Specs --configuration Release --no-build --no-restore || exit /b 1
dotnet run --project tests\FluxRisk.PostgresSpecs --configuration Release --no-build --no-restore || exit /b 1

echo FluxRisk verification completed successfully.
