#!/usr/bin/env bash
#This script installs Python3 and selenium

#Download, make and install Python3
wget -nv https://www.python.org/ftp/python/3.8.2/Python-3.8.2.tgz 2>> $1/Software/log.txt
tar xzvf Python-3.8.2.tgz 2>> $1/Software/log.txt
cd ./Python-3.8.2 2>> $1/Software/log.txt
su $2 -c "./configure --enable-optimizations --prefix=$1/Software/python3.8 --with-ensurepip=install"  2>> $1/Software/log.txt
su $2 -c "make -j$(nproc)" 2>> $1/Software/log.txt
su $2 -c "make install" 2>> $1/Software/log.txt

update-alternatives --install "/usr/local/bin/python" "python" "$1/Software/python3.8/bin/python3.8" 1 2>> $1/Software/log.txt
update-alternatives --install "/usr/local/bin/pip" "pip" "$1/Software/python3.8/bin/pip3" 1 2>> $1/Software/log.txt

#Workaround for pip wrapper bug
sed -i 's/_main()/main()/g' "/usr/local/bin/pip"

#Clean Up
cd ../ 2>> $1/Software/log.txt
rm -rf ./Python-3.8.2/ 2>> $1/Software/log.txt 
rm ./Python-3.8.2.tgz 2>> $1/Software/log.txt


