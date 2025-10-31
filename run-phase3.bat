@echo off
echo Running Phase 3 (Download)...
echo.

REM Build and copy dependencies
call mvn clean compile dependency:copy-dependencies -q

if %ERRORLEVEL% NEQ 0 (
    echo Build failed!
    exit /b %ERRORLEVEL%
)

REM Run Phase 3
java -cp "target/classes;target/dependency/*" org.standict.codelist.shared.PhaseRunner 3

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo Execution failed with error code: %ERRORLEVEL%
)

echo.
echo Done! Check:
echo - src\main\resources\phase3\downloads-*.csv for timestamped CSV files
echo - src\main\resources\phase3\downloads-latest.csv for latest CSV file

