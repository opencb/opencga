#!/bin/bash

OPTIONS=" -Xms128m -Xmx2048m -Djava.net.preferIPv4Stack=true "

CLASSPATH=""


shell_path=`dirname "$0"`;
absolute=`echo $shell_path | grep "^/"`;

if [ -z $absolute ]
then
        OPENCGA_HOME="`pwd`/$shell_path"
else
        OPENCGA_HOME="$shell_path"
fi


if [ -z "$OPENCGA_HOME" ]
then
	echo "You must define the enviroment variable: OPENCGA_HOME"
	exit 1
fi

#if [ $# -eq 0 ]
#then
#	echo "Arguments expected."
#	exit 1
#fi

for i in $OPENCGA_HOME/libs/*; do
	CLASSPATH=$CLASSPATH:$i
done;

echo "****************************"
echo "****************************"
echo "*** OpenCGA Local server ***"
echo "****************************"
echo "****************************"
echo "A example of URL address to access the server is: http://localhost:{PORT}/opencga/rest/storage/fetch?filepath={ABSOLUTE FILE PATH}&region={REGION}"

java $OPTIONS -classpath $CLASSPATH org.bioinfo.opencga.lib.cli.OpenCGAMain $OPENCGA_HOME $@ 2> /dev/null

