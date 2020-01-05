#!/bin/sh

mkdir -p /home/cdsw/airlines/flights/
wget https://mlamairesse.s3-eu-west-1.amazonaws.com/Airlines_Dataset/1988.csv.bz2 -O /home/cdsw/airlines/flights/1988.csv.bz2

mkdir -p /home/cdsw/airlines/airports/
wget http://stat-computing.org/dataexpo/2009/airports.csv -O /home/cdsw/airlines/airports/airports.csv

mkdir -p /home/cdsw/airlines/carriers/
wget http://stat-computing.org/dataexpo/2009/carriers.csv -O /home/cdsw/airlines/carriers/carriers.csv

hdfs dfs -mkdir -p airlines/
hdfs dfs -copyFromLocal -f /home/cdsw/airlines/

hdfs dfs -ls airlines/flights
hdfs dfs -ls airlines/airports
hdfs dfs -ls airlines/carriers

python3 /home/cdsw/setup/create_tables.py

pip3 install -r /home/cdsw/requirements.txt

