#!/usr/bin/env bash

#untar and install JDK
find jdk*gz -print0 | xargs tar xzfv 2>> $HOME/Software/log.txt 
find ./jdk* -maxdepth 0 -type d -print0 | xargs --null -I{} mv {} jdk 2>> $HOME/Software/log.txt
mv ./jdk ~/Software 2>> $HOME/Software/log.txt
rm -rf ./jdk-* 2>> $HOME/Software/log.txt
