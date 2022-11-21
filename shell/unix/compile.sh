#!/bin.sh

JAVA_HOME=/usr/bin
BASEPATH=/app/xtorm/test/COM_SimpleExportSam_20221101
CLASSPATH=$BASEPATH/lib/ojdbc8.jar
CLASSPATH=$CLASSPATH:/lib/logback-core-1.3.4.jar
CLASSPATH=$CLASSPATH:/lib/logback-classic-1.3.4.jar
CLASSPATH=$CLASSPATH:/lib/slf4j-api-2.0.1.jar
CLASSPATH=$CLASSPATH:$BASEPATH/bin

$JAVA_HOME/javac -encoding utf-8 -cp $CLASSPATH -d $BASEPATH/bin `find $BASEPATH/src -type f -name *.java`
