@REM streamliner launcher script
@REM
@REM Environment:
@REM JAVA_HOME - location of a JDK home dir (optional if java on path)
@REM CFG_OPTS  - JVM options (optional)
@REM Configuration:
@REM STREAMLINER_config.txt found in the STREAMLINER_HOME.
@setlocal enabledelayedexpansion

@echo off


if "%STREAMLINER_HOME%"=="" (
  set "APP_HOME=%~dp0\\.."

  rem Also set the old env name for backwards compatibility
  set "STREAMLINER_HOME=%~dp0\\.."
) else (
  set "APP_HOME=%STREAMLINER_HOME%"
)

set "APP_LIB_DIR=%APP_HOME%\lib\"

rem Detect if we were double clicked, although theoretically A user could
rem manually run cmd /c
for %%x in (!cmdcmdline!) do if %%~x==/c set DOUBLECLICKED=1

rem FIRST we load the config file of extra options.
set "CFG_FILE=%APP_HOME%\STREAMLINER_config.txt"
set CFG_OPTS=
call :parse_config "%CFG_FILE%" CFG_OPTS

rem We use the value of the JAVACMD environment variable if defined
set _JAVACMD=%JAVACMD%

if "%_JAVACMD%"=="" (
  if not "%JAVA_HOME%"=="" (
    if exist "%JAVA_HOME%\bin\java.exe" set "_JAVACMD=%JAVA_HOME%\bin\java.exe"
  )
)

if "%_JAVACMD%"=="" set _JAVACMD=java

rem Detect if this java is ok to use.
for /F %%j in ('"%_JAVACMD%" -version  2^>^&1') do (
  if %%~j==java set JAVAINSTALLED=1
  if %%~j==openjdk set JAVAINSTALLED=1
)

rem BAT has no logical or, so we do it OLD SCHOOL! Oppan Redmond Style
set JAVAOK=true
if not defined JAVAINSTALLED set JAVAOK=false

if "%JAVAOK%"=="false" (
  echo.
  echo A Java JDK is not installed or can't be found.
  if not "%JAVA_HOME%"=="" (
    echo JAVA_HOME = "%JAVA_HOME%"
  )
  echo.
  echo Please go to
  echo   http://www.oracle.com/technetwork/java/javase/downloads/index.html
  echo and download a valid Java JDK and install before running streamliner.
  echo.
  echo If you think this message is in error, please check
  echo your environment variables to see if "java.exe" and "javac.exe" are
  echo available via JAVA_HOME or PATH.
  echo.
  if defined DOUBLECLICKED pause
  exit /B 1
)


rem We use the value of the JAVA_OPTS environment variable if defined, rather than the config.
set _JAVA_OPTS=%JAVA_OPTS%
if "!_JAVA_OPTS!"=="" set _JAVA_OPTS=!CFG_OPTS!

rem We keep in _JAVA_PARAMS all -J-prefixed and -D-prefixed arguments
rem "-J" is stripped, "-D" is left as is, and everything is appended to JAVA_OPTS
set _JAVA_PARAMS=
set _APP_ARGS=

set "APP_CLASSPATH=%APP_LIB_DIR%\io.phdata.streamliner.streamliner-3.0.jar;%APP_LIB_DIR%\ojdbc8.jar;%APP_LIB_DIR%\mysql-connector-java-5.1.6.jar;%APP_LIB_DIR%\org.scala-lang.scala-library-2.12.12.jar;%APP_LIB_DIR%\org.slf4j.slf4j-api-1.7.30.jar;%APP_LIB_DIR%\org.slf4j.slf4j-log4j12-1.7.30.jar;%APP_LIB_DIR%\log4j.log4j-1.2.17.jar;%APP_LIB_DIR%\org.rogach.scallop_2.12-3.1.1.jar;%APP_LIB_DIR%\io.circe.circe-yaml_2.12-0.13.1.jar;%APP_LIB_DIR%\io.circe.circe-core_2.12-0.13.0.jar;%APP_LIB_DIR%\io.circe.circe-numbers_2.12-0.13.0.jar;%APP_LIB_DIR%\org.typelevel.cats-core_2.12-2.1.0.jar;%APP_LIB_DIR%\org.typelevel.cats-macros_2.12-2.1.0.jar;%APP_LIB_DIR%\org.typelevel.cats-kernel_2.12-2.1.0.jar;%APP_LIB_DIR%\org.yaml.snakeyaml-1.26.jar;%APP_LIB_DIR%\io.circe.circe-generic_2.12-0.13.0.jar;%APP_LIB_DIR%\com.chuusai.shapeless_2.12-2.3.3.jar;%APP_LIB_DIR%\org.typelevel.macro-compat_2.12-1.1.1.jar;%APP_LIB_DIR%\us.fatehi.schemacrawler-mysql-16.2.5.jar;%APP_LIB_DIR%\us.fatehi.schemacrawler-16.2.5.jar;%APP_LIB_DIR%\us.fatehi.schemacrawler-commandline-16.2.5.jar;%APP_LIB_DIR%\info.picocli.picocli-4.0.4.jar;%APP_LIB_DIR%\info.picocli.picocli-shell-jline3-4.0.4.jar;%APP_LIB_DIR%\org.jline.jline-3.9.0.jar;%APP_LIB_DIR%\mysql.mysql-connector-java-8.0.18.jar;%APP_LIB_DIR%\com.google.protobuf.protobuf-java-3.6.1.jar;%APP_LIB_DIR%\us.fatehi.schemacrawler-postgresql-16.2.5.jar;%APP_LIB_DIR%\org.postgresql.postgresql-42.2.8.jar;%APP_LIB_DIR%\us.fatehi.schemacrawler-oracle-16.2.5.jar;%APP_LIB_DIR%\us.fatehi.schemacrawler-db2-16.2.5.jar;%APP_LIB_DIR%\com.ibm.db2.jcc-11.5.0.0.jar;%APP_LIB_DIR%\us.fatehi.schemacrawler-sqlserver-16.2.5.jar;%APP_LIB_DIR%\com.microsoft.sqlserver.mssql-jdbc-7.4.1.jre8.jar;%APP_LIB_DIR%\guru.nidi.graphviz-java-0.8.1.jar;%APP_LIB_DIR%\org.webjars.npm.viz.js-2.0.0.jar;%APP_LIB_DIR%\commons-io.commons-io-2.6.jar;%APP_LIB_DIR%\guru.nidi.com.kitfox.svgSalamander-1.1.2.jar;%APP_LIB_DIR%\net.arnx.nashorn-promise-0.1.1.jar;%APP_LIB_DIR%\com.eclipsesource.j2v8.j2v8_macosx_x86_64-4.6.0.jar;%APP_LIB_DIR%\org.apache.commons.commons-exec-1.3.jar;%APP_LIB_DIR%\com.google.code.findbugs.jsr305-3.0.2.jar;%APP_LIB_DIR%\org.slf4j.jcl-over-slf4j-1.7.25.jar;%APP_LIB_DIR%\org.slf4j.jul-to-slf4j-1.7.25.jar;%APP_LIB_DIR%\org.apache.xmlgraphics.xmlgraphics-commons-2.3.jar;%APP_LIB_DIR%\com.eclipsesource.j2v8.j2v8_linux_x86_64-4.6.0.jar;%APP_LIB_DIR%\com.eclipsesource.j2v8.j2v8_win32_x86_64-4.6.0.jar;%APP_LIB_DIR%\com.eclipsesource.j2v8.j2v8_win32_x86-4.6.0.jar;%APP_LIB_DIR%\org.scalatra.scalate.scalate-core_2.12-1.9.0.jar;%APP_LIB_DIR%\org.scala-lang.scala-compiler-2.12.12.jar;%APP_LIB_DIR%\org.scala-lang.scala-reflect-2.12.12.jar;%APP_LIB_DIR%\org.scalatra.scalate.scalate-util_2.12-1.9.0.jar;%APP_LIB_DIR%\org.scala-lang.modules.scala-xml_2.12-1.1.0.jar;%APP_LIB_DIR%\org.scala-lang.modules.scala-parser-combinators_2.12-1.1.1.jar;%APP_LIB_DIR%\com.amazonaws.aws-java-sdk-glue-1.11.774.jar;%APP_LIB_DIR%\com.amazonaws.aws-java-sdk-core-1.11.774.jar;%APP_LIB_DIR%\commons-logging.commons-logging-1.1.3.jar;%APP_LIB_DIR%\org.apache.httpcomponents.httpclient-4.5.9.jar;%APP_LIB_DIR%\org.apache.httpcomponents.httpcore-4.4.11.jar;%APP_LIB_DIR%\commons-codec.commons-codec-1.11.jar;%APP_LIB_DIR%\software.amazon.ion.ion-java-1.0.2.jar;%APP_LIB_DIR%\com.fasterxml.jackson.core.jackson-databind-2.6.7.3.jar;%APP_LIB_DIR%\com.fasterxml.jackson.core.jackson-annotations-2.6.0.jar;%APP_LIB_DIR%\com.fasterxml.jackson.core.jackson-core-2.6.7.jar;%APP_LIB_DIR%\com.fasterxml.jackson.dataformat.jackson-dataformat-cbor-2.6.7.jar;%APP_LIB_DIR%\joda-time.joda-time-2.8.1.jar;%APP_LIB_DIR%\com.amazonaws.jmespath-java-1.11.774.jar"
set "APP_MAIN_CLASS=io.phdata.streamliner.App"
set "SCRIPT_CONF_FILE=%APP_HOME%\conf\application.ini"

rem if configuration files exist, prepend their contents to the script arguments so it can be processed by this runner
call :parse_config "%SCRIPT_CONF_FILE%" SCRIPT_CONF_ARGS

call :process_args %SCRIPT_CONF_ARGS% %%*

set _JAVA_OPTS=!_JAVA_OPTS! !_JAVA_PARAMS!

if defined CUSTOM_MAIN_CLASS (
    set MAIN_CLASS=!CUSTOM_MAIN_CLASS!
) else (
    set MAIN_CLASS=!APP_MAIN_CLASS!
)

rem Call the application and pass all arguments unchanged.
"%_JAVACMD%" !_JAVA_OPTS! !STREAMLINER_OPTS! -cp "%APP_CLASSPATH%" %MAIN_CLASS% !_APP_ARGS!

@endlocal

exit /B %ERRORLEVEL%


rem Loads a configuration file full of default command line options for this script.
rem First argument is the path to the config file.
rem Second argument is the name of the environment variable to write to.
:parse_config
  set _PARSE_FILE=%~1
  set _PARSE_OUT=
  if exist "%_PARSE_FILE%" (
    FOR /F "tokens=* eol=# usebackq delims=" %%i IN ("%_PARSE_FILE%") DO (
      set _PARSE_OUT=!_PARSE_OUT! %%i
    )
  )
  set %2=!_PARSE_OUT!
exit /B 0


:add_java
  set _JAVA_PARAMS=!_JAVA_PARAMS! %*
exit /B 0


:add_app
  set _APP_ARGS=!_APP_ARGS! %*
exit /B 0


rem Processes incoming arguments and places them in appropriate global variables
:process_args
  :param_loop
  call set _PARAM1=%%1
  set "_TEST_PARAM=%~1"

  if ["!_PARAM1!"]==[""] goto param_afterloop


  rem ignore arguments that do not start with '-'
  if "%_TEST_PARAM:~0,1%"=="-" goto param_java_check
  set _APP_ARGS=!_APP_ARGS! !_PARAM1!
  shift
  goto param_loop

  :param_java_check
  if "!_TEST_PARAM:~0,2!"=="-J" (
    rem strip -J prefix
    set _JAVA_PARAMS=!_JAVA_PARAMS! !_TEST_PARAM:~2!
    shift
    goto param_loop
  )

  if "!_TEST_PARAM:~0,2!"=="-D" (
    rem test if this was double-quoted property "-Dprop=42"
    for /F "delims== tokens=1,*" %%G in ("!_TEST_PARAM!") DO (
      if not ["%%H"] == [""] (
        set _JAVA_PARAMS=!_JAVA_PARAMS! !_PARAM1!
      ) else if [%2] neq [] (
        rem it was a normal property: -Dprop=42 or -Drop="42"
        call set _PARAM1=%%1=%%2
        set _JAVA_PARAMS=!_JAVA_PARAMS! !_PARAM1!
        shift
      )
    )
  ) else (
    if "!_TEST_PARAM!"=="-main" (
      call set CUSTOM_MAIN_CLASS=%%2
      shift
    ) else (
      set _APP_ARGS=!_APP_ARGS! !_PARAM1!
    )
  )
  shift
  goto param_loop
  :param_afterloop

exit /B 0
