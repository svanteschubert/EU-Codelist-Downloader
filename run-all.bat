@echo off
REM Run the Code List Downloader application

echo Building the project...
call mvn clean package

if %ERRORLEVEL% NEQ 0 (
    echo Build failed!
    exit /b %ERRORLEVEL%
)

set JAR_FILE=target\eu-codelist-downloader-1.0.0-jar-with-dependencies.jar

if not exist "%JAR_FILE%" (
    echo Error: JAR file not found: %JAR_FILE%
    echo Build may have failed or produced a different filename.
    exit /b 1
)

echo.
echo Starting Code List Downloader (one-shot)...
echo.

java -jar "%JAR_FILE%" --once

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo Application exited with error code: %ERRORLEVEL%
)

