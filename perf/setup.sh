#!/usr/bin/env bash
#setup ctool http://docsreview.datastax.lan/en/dse/doc/ctool/ctool/ctoolGettingStarted.html#ctoolGettingStarted
pyenv activate ctool-env
export LC_ALL="en_US.UTF-8"
export LC_CTYPE="en_US.UTF-8"

ctool destroy dsbulk-dse
ctool destroy dsbulk-client
#ctool --provider=ironic launch -p devtools-ironic dsbulk-dse 3
#ctool --provider=ironic launch -p devtools-ironic dsbulk-client 1

ctool launch -p xenial dsbulk-dse 3
ctool launch -p xenial dsbulk-client 1

#setup dse and opscenter
ctool install dsbulk-dse -i tar -v 6.0.4 enterprise
ctool run --sudo dsbulk-dse "mkdir /mnt/data; mkdir /mnt/data/data; mkdir /mnt/data/saved_caches; mkdir /mnt/commitlogs; chmod 777 /mnt/data; chmod 777 /mnt/data/data; chmod 777 /mnt/data/saved_caches; chmod 777 /mnt/commitlogs"
ctool yaml -f cassandra.yaml -o set -k data_file_directories -v '["/mnt/data/data"]' dsbulk-dse all
ctool yaml -f cassandra.yaml -o set -k commitlog_directory -v '["/mnt/commitlogs"]' dsbulk-dse all
ctool yaml -f cassandra.yaml -o set -k saved_caches_directory -v '["/mnt/data/saved_caches"]' dsbulk-dse all
ctool start dsbulk-dse enterprise
ctool install -a public -i package -v 6.5.4 dsbulk-dse opscenter
ctool start dsbulk-dse opscenter

dse_ip=`ctool info --public-ips dsbulk-dse`
curl "http://${dse_ip}:8888/opscenter/index.html" #verifying that ops-center started

ctool install_agents dsbulk-dse dsbulk-dse

#setup data-set
ctool run --sudo dsbulk-client "mkdir /mnt/data; chmod 777 /mnt/data"
ctool run --sudo dsbulk-client "cd /mnt/data; sudo su automaton; git clone https://github.com/brianmhess/DSEBulkLoadTest; cd DSEBulkLoadTest; make compile; make dirs; make data"

#setup DSE keyspaces/tables
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'};\""
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE TABLE IF NOT EXISTS test.test100b(pkey TEXT, ccol BIGINT, data TEXT, PRIMARY KEY ((pkey), ccol));\""
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE TABLE IF NOT EXISTS test.test1kb(pkey TEXT, ccol BIGINT, data TEXT, PRIMARY KEY ((pkey), ccol));\""
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE TABLE IF NOT EXISTS test.test10kb(pkey TEXT, ccol BIGINT, data TEXT, PRIMARY KEY ((pkey), ccol));\""
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE TABLE IF NOT EXISTS test.test1mb(pkey TEXT, ccol BIGINT, data TEXT, PRIMARY KEY ((pkey), ccol));\""
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE TABLE IF NOT EXISTS test.test100(pkey BIGINT, ccol BIGINT, col0 BIGINT, col1 BIGINT, col2 BIGINT, col3 BIGINT, col4 BIGINT, col5 BIGINT, col6 BIGINT, col7 BIGINT, col8 BIGINT, col9 BIGINT, col10 BIGINT, col11 BIGINT, col12 BIGINT, col13 BIGINT, col14 BIGINT, col15 BIGINT, col16 BIGINT, col17 BIGINT, col18 BIGINT, col19 BIGINT, col20 BIGINT, col21 BIGINT, col22 BIGINT, col23 BIGINT, col24 BIGINT, col25 BIGINT, col26 BIGINT, col27 BIGINT, col28 BIGINT, col29 BIGINT, col30 BIGINT, col31 BIGINT, col32 BIGINT, col33 BIGINT, col34 BIGINT, col35 BIGINT, col36 BIGINT, col37 BIGINT, col38 BIGINT, col39 BIGINT, col40 BIGINT, col41 BIGINT, col42 BIGINT, col43 BIGINT, col44 BIGINT, col45 BIGINT, col46 BIGINT, col47 BIGINT, col48 BIGINT, col49 BIGINT, col50 BIGINT, col51 BIGINT, col52 BIGINT, col53 BIGINT, col54 BIGINT, col55 BIGINT, col56 BIGINT, col57 BIGINT, col58 BIGINT, col59 BIGINT, col60 BIGINT, col61 BIGINT, col62 BIGINT, col63 BIGINT, col64 BIGINT, col65 BIGINT, col66 BIGINT, col67 BIGINT, col68 BIGINT, col69 BIGINT, col70 BIGINT, col71 BIGINT, col72 BIGINT, col73 BIGINT, col74 BIGINT, col75 BIGINT, col76 BIGINT, col77 BIGINT, col78 BIGINT, col79 BIGINT, col80 BIGINT, col81 BIGINT, col82 BIGINT, col83 BIGINT, col84 BIGINT, col85 BIGINT, col86 BIGINT, col87 BIGINT, col88 BIGINT, col89 BIGINT, col90 BIGINT, col91 BIGINT, col92 BIGINT, col93 BIGINT, col94 BIGINT, col95 BIGINT, col96 BIGINT, col97 BIGINT, PRIMARY KEY ((pkey), ccol));\""
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE TABLE IF NOT EXISTS test.test10(pkey BIGINT, ccol BIGINT, col0 BIGINT, col1 BIGINT, col2 BIGINT, col3 BIGINT, col4 BIGINT, col5 BIGINT, col6 BIGINT, col7 BIGINT, PRIMARY KEY ((pkey), ccol));\""

#install maven && java
ctool run --sudo dsbulk-client "sudo apt update --assume-yes; sudo apt install maven --assume-yes; sudo apt-get install unzip --assume-yes"

github_username="username"
github_password="password"
#to build
ctool run --sudo dsbulk-client "cd /mnt/data; git clone https://${github_username}:${github_password}@github.com/riptano/dsbulk.git"
ctool run --sudo dsbulk-client "cd /mnt/data/dsbulk; sudo mvn clean package -DskipTests -P release"

#to download release
#ctool run --sudo dsbulk-client "cd /mnt/data; curl -L https://api.github.com/repos/riptano/dsbulk/zipball/${release} -u ${github_username}:${github_password}> dsbulk.zip; unzip dsbulk.zip"
