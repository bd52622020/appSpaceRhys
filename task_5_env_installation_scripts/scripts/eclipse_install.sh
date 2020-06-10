#!/usr/bin/env bash

#Install Eclipse
wget -nv http://mirror.dkm.cz/eclipse/technology/epp/downloads/release/2020-03/R/eclipse-java-2020-03-R-linux-gtk-x86_64.tar.gz 2>> $HOME/Software/log.txt
su $2 -c "tar xfz eclipse*gz 2>> $HOME/Software/log.txt"
su $2 -c "mv eclipse $HOME/Software/ 2>> $HOME/Software/log.txt"

#Set Environment Variables
update-alternatives --install "/usr/local/bin/eclipse" "eclipse" "$1/Software/eclipse/eclipse" 1 2>> $1/Software/log.txt

#cleanup
rm -rf ./eclipse-* 2>> $HOME/Software/log.txt
