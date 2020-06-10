#!/usr/bin/env bash

read -p "Enter Oracle Username: " oracleuser
read -s -p "Enter Oracle Password: " oraclepassword
echo ""
cd ./scripts
cwd=$(pwd)
installdir=$(su $SUDO_USER -c "realpath ~/")
install_user=$SUDO_USER

su $SUDO_USER -c "./path.sh" &
./apt_installs.sh &
wait
 
./python_install.sh $installdir $install_user &
su $SUDO_USER -c "./JDK_get_python_reqs.sh $oracleuser $oraclepassword $cwd" &
./eclipse_install.sh $installdir $install_user &
wait

./java_path.sh $installdir
su $SUDO_USER -c "./eclipse_extensions.sh"

echo "all scripts completed"

su $SUDO_USER -c "./tests.sh"


