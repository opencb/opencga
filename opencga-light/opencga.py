#!/usr/bin/python

import os,sys, commands

OPENCGA_HOME = "./"
slash = "/"

if os.name == 'nt':
	OPENCGA_HOME=""
	slash = "\\"

shell_path = os.path.dirname(sys.argv[0])
if os.path.isabs(shell_path):
	OPENCGA_HOME=shell_path

CLASSPATH = ""
basepath = os.getcwd()
fileList = os.listdir(OPENCGA_HOME+""+slash+"libs"+slash)
for fileItem in fileList:
	CLASSPATH += basepath+""+slash+"libs"+slash+""+fileItem+":"

CLASSPATH = CLASSPATH[:-1]

print "****************************"
print "****************************"
print "*** OpenCGA Local server ***"
print "****************************"
print "****************************"
print "A example of URL address to access the server is: http://localhost:{PORT}/opencga/rest/storage/fetch?filepath={ABSOLUTE FILE PATH}&region={REGION}"

JAVA_OPTIONS=" -Xms128m -Xmx2048m -Djava.net.preferIPv4Stack=true "
command = "java"+JAVA_OPTIONS+" -classpath "+CLASSPATH+" org.bioinfo.opencga.lib.cli.OpenCGAMain "+OPENCGA_HOME
status, output = commands.getstatusoutput(command)
