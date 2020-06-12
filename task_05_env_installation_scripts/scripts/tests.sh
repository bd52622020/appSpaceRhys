#!/usr/bin/env bash

echo -e "\n\n\n\#\#\#\#TEST OUTPUT\#\#\#\#"

#python test
if python --version
then
	echo -e "\nPython exists." >> $HOME/Software/log.txt
	echo $(python --version) >> $HOME/Software/log.txt
else
	echo -e "\nPython failed to install correctly." >> $HOME/Software/log.txt
fi
#pip test
if pip --version
then
	echo -e "\nPip exists" >> $HOME/Software/log.txt
	echo $(pip --version) >> $HOME/Software/log.txt
else
	echo -e "\nPip failed to install correctly." >> $HOME/Software/log.txt
fi
#java test
if java -version
then
	echo -e "\nJava exists." >> $HOME/Software/log.txt
	java -version 2>> $HOME/Software/log.txt
else
	echo -e "\nJava failed to install correctly." >> $HOME/Software/log.txt
fi

#eclipse test
if eclipse
then
	echo -e "\nEclipse installed." >> $HOME/Software/log.txt 
else
	echo -e "\nEclipse failed to install correctly." >> $HOME/Software/log.txt
fi
