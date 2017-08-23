@ECHO OFF
SETLOCAL ENABLEDELAYEDEXPANSION

SET ABSOLUTE_BATCH_DIRECTORY=%~DPS0
SET LOADER_ARGS=

REM First parse command-line args into an option string that the Java tool
REM can handle.
REM If the user provided the -h or --help option, emit help and exit.
REM If parsing fails, exit.
SET STATE=GETOPT
FOR %%A IN (%*) DO (
  call :process_arg %%A
  IF !ERRORLEVEL! == 2 (
    REM This means the user asked for usage info.
    call :usage
    EXIT /B 0
  )
  IF !ERRORLEVEL! == 1 (
    REM Something went wrong; the callee emitted relevant info. Just exit.
    EXIT /B 1
  )
)

REM LOADER_ARGS starts with a ',' that must be removed.
SET LOADER_ARGS=!LOADER_ARGS:~1!

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

REM Run the Java tool.
"%JAVA%" com.datastax.loader.engine.Main "!LOADER_ARGS!"
GOTO :eof

REM Helper for adding a particular item to CLASSPATH.
:appendClassPath
  SET CLASSPATH=!CLASSPATH!;%1
  GOTO :eof

REM Simple subroutine to emit usage text.
:usage
  SET BATCH_FILENAME=%~N0%
  ECHO Usage: !BATCH_FILENAME! ^<options^>
  ECHO Options:
  ECHO  -c, --connector.name ^<name^>       Name of connector; only the built-in csv connector
  ECHO                                    is supported at this time, so this option must have
  ECHO                                    value csv.
  ECHO  -k, --schema.keyspace ^<keyspace^>  Keyspace into which to load data.
  ECHO  -t, --schema.table ^<table^>        Table into which to load data.
  ECHO  -m, --schema.mapping ^<mapping^>    Mapping of fields in data to columns in the database.
  ECHO.
  ECHO All arguments except connector.name are optional in that values fall back to defaults or
  ECHO are inferred from the input data. However, some connectors have required settings of
  ECHO their own and those must be set as well. For example, the csv connector requires the
  ECHO connector.csv.url setting to specify the source path/url of the csv data to load.
  ECHO.
  ECHO.
  ECHO CONFIG FILES, SETTINGS SEARCH ORDER, AND OVERRIDES:
  ECHO.
  ECHO Available settings along with defaults are recorded in conf/reference.conf. This file
  ECHO also contains detailed descriptions of settings and is a great source of information.
  ECHO When the loader starts up, settings are first loaded from conf/reference.conf.
  ECHO.
  ECHO The conf directory also contains an application.conf where a user may specify permanent
  ECHO overrides of settings. These may be in nested-structure form like this:
  ECHO.
  ECHO datastax-loader {
  ECHO   connector {
  ECHO     name="csv"
  ECHO   }
  ECHO }
  ECHO.
  ECHO or dotted form: datastax-loader.connector.name="csv"
  ECHO.
  ECHO Finally, a user may specify impromptu overrides via options on the command line.
  ECHO See examples for details.
  ECHO.
  ECHO.
  ECHO EXAMPLES:
  ECHO * Load CSV data from stdin to the ks1.table1 table in a cluster with
  ECHO   a localhost contact point. Field names in the data match column names in the
  ECHO   table. Field names are obtained from a "header row" in the data:
  ECHO     generate_data ^| !BATCH_FILENAME! -c csv --connector.csv.url stdin:/ -k ks1 -t table1 --connector.csv.header=true
  ECHO.
  ECHO * Same as last example, but load from a local file:
  ECHO     !BATCH_FILENAME! -c csv --connector.csv.url C:\data\export.csv -k ks1 -t table1 --connector.csv.header=true
  ECHO.
  ECHO * Same as last example, but load data from a url:
  ECHO     !BATCH_FILENAME! -c csv --connector.csv.url https://svr/data/export.csv -k ks1 -t table1 --connector.csv.header=true
  ECHO.
  ECHO * Same as last example, but there is no header row and we specify an explicit field mapping based
  ECHO   on field indices in the input:
  ECHO     !BATCH_FILENAME! -c csv --connector.csv.url https://svr/data/export.csv -k ks1 -t table1 -m "{0=col1,1=col3}"
  ECHO.
  ECHO * Same as last example, but specify a few contact points at the default port:
  ECHO     !BATCH_FILENAME! -c csv --connector.csv.url https://svr/data/export.csv -k ks1 -t table1 -m "{0=col1,1=col3}" --driver.contactPoints "[10.200.1.3, 10.200.1.4]"
  ECHO.
  ECHO * Same as last example, but specify port 9876 for the contact points:
  ECHO     !BATCH_FILENAME! -c csv --connector.csv.url https://svr/data/export.csv -k ks1 -t table1 -m "{0=col1,1=col3}" --driver.contactPoints "[10.200.1.3, 10.200.1.4]" --driver.port 9876
  ECHO.
  ECHO * Same as last example, but with default port for contact points, and connector-name, keyspace, table, and mapping set in
  ECHO   conf/application.conf:
  ECHO     !BATCH_FILENAME! --connector.csv.url https://svr/data/export.csv --driver.contactPoints "[10.200.1.3, 10.200.1.4]"

  GOTO :eof

ENDLOCAL
