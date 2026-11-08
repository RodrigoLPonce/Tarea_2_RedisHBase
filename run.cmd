@echo off
REM Doble clic en Windows: ejecuta run.ps1 (todo de punta a punta) y deja la ventana abierta.
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run.ps1" %*
echo.
echo Terminado (codigo %ERRORLEVEL%). Log completo en resultados\run_log.txt
pause
