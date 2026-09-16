@echo off
setlocal
set "BASE_URL=http://127.0.0.1:8080"

echo [1/3] Health
curl.exe -fsS "%BASE_URL%/health" || exit /b 1
echo.

echo [2/3] Submit decision
curl.exe -fsS -X POST "%BASE_URL%/v1/decisions" -H "Content-Type: application/json" -d "{\"eventId\":\"cmd-smoke-001\",\"accountId\":\"account-001\",\"deviceId\":\"device-a\",\"amount\":15000,\"currency\":\"CNY\",\"country\":\"CN\",\"occurredAt\":\"2026-09-16T00:00:00Z\"}" || exit /b 1
echo.

echo [3/3] Read decision
curl.exe -fsS "%BASE_URL%/v1/decisions/cmd-smoke-001" || exit /b 1
echo.
echo Smoke test completed successfully.
