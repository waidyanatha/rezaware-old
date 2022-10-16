#!/bin/bash

# *** run this script with >>> sudo su
#
# --- AVOID USING apt install; instead use conda install -c conda-forge <package_name> in the env
#
# Script to install rezAWARE Wrangler and dependencies (Utils) packages
# with Cherokee & PostgreSQL
# - tunes PostgreSQL for 512Mb RAM (e.g. Amazon Micro (free tier))
# - run pg1024 to tune for 16Gb RAM (e.g. Amazon Small or greater)
# - run .... to tune apache airflow for Amazon EC2

# *** Insatall Anaconda3 and setup an environment ***
# cheatsheet: https://docs.conda.io/projects/conda/en/4.6.0/_downloads/52a95608c49671267e40c689e0bc00ca/conda-cheatsheet.pdf

# TODO -- fix this
sudo su


# Which OS are we running?
# TODO -- fix this
read -d . UBUNTU < /etc/os-release | grep "VERSION_ID"

# *** Python and PiP installation
# TODO -- fix this
if [ $UBUNTU == '20' ]; then
    update-alternatives --install /usr/bin/python python /usr/bin/python3.8 1
    apt remove python3-jinja2 python3-yaml -qy
    apt install python3-pip python3-dev -qy
    python3 -m pip install --upgrade pip
else
    apt-get install python-pip python-dev -qy
fi

# *** Update system ***
apt update
apt -y upgrade
apt clean

# *** Postgres database
# https://gist.github.com/gwangjinkim/f13bf596fefa7db7d31c22efd1627c7a
#
conda install -c conda-forge postgresql
# -- postgresql-contrib not available in anacoda
## conda install -c conda-forge postgresql-contrib
initdb -D airflow
pg_ctl -D airflow -l logfile start
createuser --encrypted --pwprompt airflow
createdb --owner=airflow airflow

# >>> sudo apt install mlocate to find pg_hba.conf and postgresql.conf
# should be in workingdir/airflow/ in my case it is in /workspace/airflow/
locate pg_hba.conf
locate postgresql.conf
sudo nano /workspace/airflow/pg_hba.conf
# IPv4 local connections:
host    all             all             0.0.0.0/0            trust
sudo nano /workspace/airflow/postgresql.conf
listen_addresses = ‘*’ # what IP address(es) to listen on
pg_ctl -D airflow -l logfile restart

# *** Apache Airflow with Postgres ***
# Borrow from https://medium.com/@abraham.pabbathi/airflow-on-aws-ec2-instance-with-ubuntu-aff8d3206171
# But use pip for all installations; conda would have been preferred but has no repo
pip install apache-airflow[postgres,aws,s3]
sudo python3 -m pip install apache-airflow[all]

# Create a reza env
conda activate reza (or env)

export AIRFLOW_HOME=~/airflow
source ~/.bashrc
# Add Custom TCP HTTPS Port 8989 to EC2 security group
airflow webserver --port=8989
# python3 -m pip install apache-airflow==2.3.4
# python3 -m pip install apache-airflow-providers-celery==3.0.0
# python3 -m pip install apache-airflow-providers-common-sql==1.1.0
# python3 -m pip install apache-airflow-providers-ftp==3.1.0
# python3 -m pip install apache-airflow-providers-http==4.0.0
# python3 -m pip install apache-airflow-providers-imap==3.0.0
# python3 -m pip install apache-airflow-providers-sqlite==3.2.0

conda install -c anaconda psycopg2 -qy
sudo adduser airflow
sudo usermod -aG sudo airflow
su - airflow
conda install -c conda-forge apache-airflow[all]

# *** Apache Spark ***
conda install -c conda-forge findspark configparser logging

# *** S3 Boto *** 
### conda installs 
### run with >>> conda install -c conda-forge boto3

# install git

# git clone rezAWARE
git clones http://waidyanatha.github.com/rezaware

# steup all the .ini & __init__.py using rezaware.py
EOF>>python3
import sys
sys.path.insert(1,__CWD__)
import rezaware as rez
_rezApp = rez.App(container="wrangler")
app_list, ini_file_list = _rezApp.make_ini_files()
<<<EOF