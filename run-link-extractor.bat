@echo off
echo Running Registry Link Extractor...
echo.

REM First compile and copy dependencies
call mvn compile dependency:copy-dependencies -q

if %ERRORLEVEL% NEQ 0 (
    echo Build failed!
    exit /b %ERRORLEVEL%
)

REM Then run using java
java -cp "target/classes;target/dependency/*" org.standict.codelist.shared.RegistryLinkExtractor

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo Execution failed with error code: %ERRORLEVEL%
)

echo.
echo Done! Check src\main\resources\registry-links-analysis.txt

