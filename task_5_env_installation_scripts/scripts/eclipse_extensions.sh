#!/usr/bin/env bash

#Install Extensions
$HOME/Software/eclipse/eclipse -application org.eclipse.equinox.p2.director -repository http://pydev.org/updates -installIU org.python.pydev.feature.feature.group 2>> $HOME/Software/log.txt
$HOME/Software/eclipse/eclipse -application org.eclipse.equinox.p2.director -repository http://pydev.org/updates -installIU org.python.pydev.mylyn.feature.feature.group 2>> $HOME/Software/log.txt


