@echo off
REM Script para iniciar todos los componentes del pipeline de datos

echo Iniciando el pipeline de datos de criptomonedas...
echo.

REM Verificar que el entorno virtual exista
if not exist "venv\Scripts\activate.bat" (
    echo El entorno virtual no existe! Por favor, ejecuta setup.bat primero.
    echo.
    echo Presiona cualquier tecla para salir...
    pause > nul
    exit /b 1
)

REM Comprobar si ya hay ventanas de cmd ejecutándose para nuestro pipeline
tasklist /FI "WINDOWTITLE eq Crypto Producer*" 2>nul | find "cmd.exe" >nul
if not errorlevel 1 (
    echo El productor ya parece estar en ejecución.
) else (
    echo Iniciando el productor en una nueva ventana...
    start "Crypto Producer" cmd /k "call venv\Scripts\activate.bat && python producer.py"
)

timeout /t 2 /nobreak >nul

tasklist /FI "WINDOWTITLE eq Crypto Consumer*" 2>nul | find "cmd.exe" >nul
if not errorlevel 1 (
    echo El consumidor ya parece estar en ejecución.
) else (
    echo Iniciando el consumidor en una nueva ventana...
    start "Crypto Consumer" cmd /k "call venv\Scripts\activate.bat && python consumer.py"
)

timeout /t 2 /nobreak >nul

tasklist /FI "WINDOWTITLE eq Crypto Dashboard*" 2>nul | find "cmd.exe" >nul
if not errorlevel 1 (
    echo El dashboard ya parece estar en ejecución.
) else (
    echo Iniciando el dashboard en una nueva ventana...
    start "Crypto Dashboard" cmd /k "call venv\Scripts\activate.bat && python visualization.py"
)

echo.
echo Todos los componentes del pipeline han sido iniciados!
echo.
echo Puedes acceder al dashboard en: http://127.0.0.1:8050
echo.
echo Presiona cualquier tecla para salir...
pause > nul