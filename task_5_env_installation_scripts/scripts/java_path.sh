#!/usr/bin/env bash

#set environment variables
echo 'export J2SDKDIR=$HOME/Software/jdk' >> $1/.bashrc 2>> $1/Software/log.txt
echo 'export J2REDIR=$HOME/Software/jre' >> $1/.bashrc 2>> $1/Software/log.txt
echo 'export JAVA_HOME=$HOME/Software/jdk' >> $1/.bashrc 2>> $1/Software/log.txt
echo 'export DERBY_HOME=$HOME/Software/jdk/db' >> $1/.bashrc 2>> $1/Software/log.txt

update-alternatives --install "/usr/bin/java" "java" "$1/Software/jdk/bin/java" 1 2>> $1/Software/log.txt
update-alternatives --install "/usr/bin/java" "java" "$1/Software/jdk/bin/java" 1 2>> $1/Software/log.txt
update-alternatives --install "/usr/bin/javac" "javac" "$1/Software/jdk/bin/javac" 1 2>> $1/Software/log.txt
update-alternatives --install "/usr/bin/javac" "javac" "$1/Software/jdk/bin/javac" 1 2>> $1/Software/log.txt

