@ECHO OFF
SETLOCAL ENABLEDELAYEDEXPANSION

SET ABSOLUTE_BATCH_DIRECTORY=%~DPS0

REM Set CLASSPATH to include all the jars in the lib directory. Also add
REM the conf directory, so that application.conf can be found for
REM permanent overrides.

for %%i in (%ABSOLUTE_BATCH_DIRECTORY%..\lib\*.jar) do call :appendClassPath %%i
SET CLASSPATH=%ABSOLUTE_BATCH_DIRECTORY%..\conf;!CLASSPATH!

REM Run java from JAVA_HOME, if present, otherwise PATH.
IF DEFINED JAVA_HOME (
  SET JAVA=%JAVA_HOME%\bin\java
) ELSE (
  SET JAVA=java
)

REM Attempt to find the window width, to make help output look nicer.
for /F "usebackq tokens=2* delims=: " %%W in (`mode con ^| findstr Columns`) do set COLUMNS=%%W

REM Run the Java tool.
"%JAVA%" %DSBULK_JAVA_OPTS% DataStaxBulkLoader %*
GOTO :eof

REM Helper for adding a particular item to CLASSPATH.
:appendClassPath
  SET CLASSPATH=!CLASSPATH!;%1
  GOTO :eof

ENDLOCAL
