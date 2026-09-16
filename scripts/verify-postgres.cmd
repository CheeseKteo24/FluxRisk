@echo off
setlocal
cd /d "%~dp0.."
docker compose up -d --wait postgres || exit /b 1
set "FLUXRISK_POSTGRES=Host=127.0.0.1;Port=5432;Database=fluxrisk;Username=fluxrisk;Password=fluxrisk"
call scripts\verify.cmd
