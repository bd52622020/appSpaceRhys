#!/usr/bin/env bash

#Fix for pip ssl bug.
python2 -m pip install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host pypi.org --upgrade pip

#Install Selenium
python2 -m pip install selenium 2>> $HOME/Software/log.txt

#Call JDK Downloader 
python2 $3/JDK_get.py $1 $2 2>> $HOME/Software/log.txt
bash $3/java_install.sh 2>> $HOME/Software/log.txt

#cleanup
rm -rf ./geckodriver.log
