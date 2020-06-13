#####################################################
#####################################################
############ DEV ENVIRONMENT SETUP SCRIPTS ##########
#####################################################
#####################################################

#This script installs Java 8 SDK, Python 3.8, eclipse and the pydev eclipse extension.
#
#The script also install Python 2.7 and runs a script with it in order to meet requirements for certain projects. This will no longer be viable in January 2021 when python2 pip support ends.
#
#Be aware that changes to oracle.com site structure will break this script.
#If this happens, you can manually download the Java SDK to the ./scripts folder as a workaround.
#
#Before install please run sudo apt update && sudo apt upgrade.
#If this results in a DPKG lock error, wait for a while then try again until this no longer occurs.
#
#Then run "chmod +x -R ./" in the directory containing deploy.sh
#
#Run "sudo ./deploy.sh" in the same directory to install
#
#You will need to enter your oracle.com credentials.
#Make sure they are correct or the java installation will fail.
#
#If anything fails to install correctly, please check ~/Software/log.txt

