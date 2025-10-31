@echo off
echo Running Phase 1 (Analysis)...
echo.

REM Build and copy dependencies
call mvn clean compile dependency:copy-dependencies -q

if %ERRORLEVEL% NEQ 0 (
    echo Build failed!
    exit /b %ERRORLEVEL%
)

REM Run the test
java -cp "target/classes;target/dependency/*" org.standict.codelist.shared.TestPhases

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo Execution failed with error code: %ERRORLEVEL%
)

echo.
echo Done! Check:
echo - Logs above for detected files
echo - src\main\resources\phase1\inventory-*.csv for timestamped CSV files
echo - src\main\resources\phase1\inventory-latest.csv for latest CSV file

