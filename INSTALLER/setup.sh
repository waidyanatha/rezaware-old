#!/usr/bin/env bash

# *** run this bash script to install 'rezaware' with the
#     required apps
echo
echo "*****************************"
echo "Welcome to the rezaware setup"
echo "*****************************"

# *** if not exists, create a rezaware user and password with sudo previledges
REPO="https://github.com/waidyanatha/rezaware.git"
USER="rezaware"   # change if you wish to work with an existing user
GROUP="rezaware"  # change if you wish to work with an existing user
HOMEDIR="/home/$USER"
APPLIST="wrangler"

echo "If this your fist installation answer n/N"
echo "Instead, if you already have the code and wish to re-instantiate, answer y/Y"
read -p "Is this a new instantiation of $USER [y/n]?" -n 1 -r
echo    # move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cd $HOMEDIR
    echo "If you alread have the code and wish to fetch the latest, answer y/Y"
    read -p "Do you want to fetch $USER from the code repo [y/n]?" -n 1 -r
    if [[ $REPLY =~ ^[Yy]$ ]]
    then
        echo -n "fetching binaries from $REPO"
        git fetch origin master -v
    fi
    echo "Reconfiguring app.ini modules and packages"
    if [ -f "rezaware.py" ]
    then
PYCMD=$(cat <<EOF

import json
import sys
sys.path.insert(1,"$HOMEDIR")
import rezaware as rez
_rezApp = rez.App(app_name="$APPLIST")

app_list, ini_file_list = _rezApp.make_ini_files()

#import pdb; pdb.set_trace()

app_list, ini_file_list = _rezApp.make_ini_files()
if app_list or ini_file_list:
#     print("%s modules " % (_rezApp.appName))
#     print(json.dumps(app_list,indent=2))
#     print(json.dumps(ini_file_list,indent=2))
EOF
)
        TEMP_SCRIPT=$(mktemp)
        echo "$PYCMD" > "$TEMP_SCRIPT"
        python3 "$TEMP_SCRIPT"
        echo "Configuration complet, please proceed with using $USER"
        echo    # move to a new line
        exit 1
    else 
        echo "rezaware.py does not exist."
        echo "proceeding with cloning $USER"
    fi
else
    echo "Proceeding with new setup ..."
    echo    # move to a new line
fi

### *** Begin new installation
### check if user and group needs to be created
echo "Search user:"
if [ $(id -u) -eq 0 ]
then
    egrep "^$USER" /etc/passwd >/dev/null
    if [ $? -eq 0 ]
    then
        echo "$USER exists! continuing with installation ... "
    else
        adduser --system --disabled-password $USER
        [ $? -eq 0 ] && echo -n "$USER has been added to system!" || echo -n "Failed to add a user!"
#        usermod -aG sudo $USER
#        [ $? -eq 0 ] && echo "$USER added to sudoers!" || echo "Failed to add a user to sudoers!"
    fi
else
    echo "Only root may add a user to the system. Please run setup.sh as sudo -su"
    exit 2
    echo    # move to a new line
fi

### add user to rezaware group
egrep $USER -g $GROUP >/dev/null 2>&1 
if [ $? -eq 0 ]
then
    echo "$USER already in $GROUP. continuing with installation ..."
else
    groupadd $GROUP
    useradd -g $GROUP -d $HOMEDIR -m $USER
fi

### *** clone rezaware from github
echo    # move to a new line
read -p "Do you want to clone $REPO [y/n]?" -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cd /home
    echo -n "Cloning from $REPO. This may take a while ..."
    env GIT_SSL_NO_VERIFY=true git clone --recursive $REPO
    echo "Cloning complete, proceeding with installation ..."
else
    cd $HOMEDIR
    # check if file exists
    echo "Checking if rezaware.py exists"
    echo "if not then echo rezaware.py does not exists"
fi

### *** run rezaware.py script to initialize apps
echo    # move to a new line
cd $HOMEDIR
chmod +x rezaware.py
echo "Configuring app.ini modules and packages"
PYCMD=$(cat <<EOF

import json
import sys
sys.path.insert(1,"$HOMEDIR")
import rezaware as rez
_rezApp = rez.App(app_name="$APPLIST")

app_list, ini_file_list = _rezApp.make_ini_files()

#import pdb; pdb.set_trace()

app_list, ini_file_list = _rezApp.make_ini_files()
# if app_list or ini_file_list:
#     print("%s modules " % (_rezApp.appName))
#     print(json.dumps(app_list,indent=2))
#     print(json.dumps(ini_file_list,indent=2))
EOF
)
TEMP_SCRIPT=$(mktemp)
echo "$PYCMD" > "$TEMP_SCRIPT"
python3 "$TEMP_SCRIPT"

echo "Setup complet, please proceed with using $USER now"
echo