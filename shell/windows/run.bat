@echo off
SETLOCAL

set JAVA_HOME="C:\Program Files\Java\jdk1.8"
set BASEPATH="C:\eclipse\workspace\COM_SimpleExportSam_20221101"
set CLASSPATH=%BASEPATH%\lib\ojdbc8.jar
set CLASSPATH=%CLASSPATH%;%BASEPATH%\lib\logback-core-1.3.4.jar
set CLASSPATH=%CLASSPATH%;%BASEPATH%\lib\logback-classic-1.3.4.jar
set CLASSPATH=%CLASSPATH%;%BASEPATH%\lib\slf4j-api-2.0.1.jar
set CLASSPATH=%CLASSPATH%;%BASEPATH%\bin

%JAVA_HOME%\bin\java.exe -cp %CLASSPATH% App %BASEPATH%

ENDLOCAL

pause