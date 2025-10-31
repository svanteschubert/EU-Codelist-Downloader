@echo off
echo Running Phase 2 (Compare)...
echo.

REM Build and copy dependencies
call mvn clean compile dependency:copy-dependencies -q

if %ERRORLEVEL% NEQ 0 (
    echo Build failed!
    exit /b %ERRORLEVEL%
)

REM Run Phase 2
java -cp "target/classes;target/dependency/*" org.standict.codelist.shared.PhaseRunner 2

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo Execution failed with error code: %ERRORLEVEL%
)

echo.
echo Done! Check:
echo - src\main\resources\phase2\diff-*.csv for timestamped CSV files
echo - src\main\resources\phase2\diff-latest.csv for latest CSV file

