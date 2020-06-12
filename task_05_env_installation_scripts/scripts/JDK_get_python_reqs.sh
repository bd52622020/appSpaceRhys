#!/usr/bin/env bash

#Install Selenium
pip install selenium 2>> $HOME/Software/log.txt

#Call JDK Downloader 
python $3/JDK_get.py $1 $2 2>> $HOME/Software/log.txt
bash $3/java_install.sh 2>> $HOME/Software/log.txt

#cleanup
rm -rf ./geckodriver.log
