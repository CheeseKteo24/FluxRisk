@echo off
setlocal
cd /d "%~dp0.."
if not exist ".appdata" mkdir ".appdata"
if not exist ".nuget-packages" mkdir ".nuget-packages"
set "APPDATA=%CD%\.appdata"
set "NUGET_PACKAGES=%CD%\.nuget-packages"
dotnet run --project src\FluxRisk.Api --no-restore --urls http://127.0.0.1:8080
