#!/usr/bin/env bash


mkdir $HOME/Software
rm -f -- $HOME/Software/log.txt
touch $HOME/Software/log.txt
echo 'export PATH=$HOME/Software:$PATH' >> $HOME/.bashrc 2>> $HOME/Software/log.txt
