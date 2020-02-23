#!/usr/bin/env bash
#setup ctool http://docsreview.datastax.lan/en/dse/doc/ctool/ctool/ctoolGettingStarted.html#ctoolGettingStarted
pyenv activate ctool-env
export LC_ALL="en_US.UTF-8"
export LC_CTYPE="en_US.UTF-8"

ctool destroy dsbulk-dse
ctool destroy dsbulk-client

#to launch virtual machines on openstack:
#ctool launch -p xenial dsbulk-dse 3
#ctool launch -p xenial dsbulk-client 1

#to launch virtual machines on nebula:
#http://docsreview.datastax.lan/en/dse/6.7/ctoolnebula/ctool/ctoolNebulaConfigureNebula
ctool launch dsbulk-dse 3
ctool launch dsbulk-client 1

#setup dse
ctool install dsbulk-dse -i tar -v 6.0.4 enterprise
ctool run --sudo dsbulk-dse "mkdir /mnt/data; mkdir /mnt/data/data; mkdir /mnt/data/saved_caches; mkdir /mnt/commitlogs; chmod 777 /mnt/data; chmod 777 /mnt/data/data; chmod 777 /mnt/data/saved_caches; chmod 777 /mnt/commitlogs"
ctool yaml -f cassandra.yaml -o set -k data_file_directories -v '["/mnt/data/data"]' dsbulk-dse all
ctool yaml -f cassandra.yaml -o set -k commitlog_directory -v '"/mnt/commitlogs"' dsbulk-dse all
ctool yaml -f cassandra.yaml -o set -k saved_caches_directory -v '"/mnt/data/saved_caches"' dsbulk-dse all
ctool start dsbulk-dse enterprise
#to see logs tail -f /var/log/cassandra/system.log

#setup ops-center
ctool install -a public -i package -v 6.5.4 dsbulk-dse opscenter
ctool start dsbulk-dse opscenter

ctool install_agents dsbulk-dse dsbulk-dse

dse_ip=`ctool info --public-ips dsbulk-dse -n 0`
curl "http://${dse_ip}:8888/opscenter/index.html" #verifying that ops-center started

#setup data-set (random Partition Key)
ctool run --sudo dsbulk-client "mkdir /mnt/data; chmod 777 /mnt/data"
ctool run --sudo dsbulk-client "cd /mnt/data; sudo su automaton; git clone https://github.com/brianmhess/DSEBulkLoadTest; cd DSEBulkLoadTest; make compile; make dirs; make data"

#prepare data for parallel LOAD
ctool run --sudo dsbulk-client "mkdir /mnt/data/DSEBulkLoadTest/in/data100B_one_file"
ctool run --sudo dsbulk-client "cd /mnt/data/DSEBulkLoadTest/in/data100B; cat data100B_0.csv data100B_1.csv data100B_2.csv data100B_3.csv data100B_4.csv data100B_5.csv data100B_6.csv data100B_7.csv data100B_8.csv data100B_9.csv data100B_10.csv data100B_11.csv data100B_12.csv data100B_13.csv data100B_14.csv data100B_15.csv data100B_16.csv data100B_17.csv data100B_18.csv data100B_19.csv > ../data100B_one_file/data100B.csv"

#install maven && java
ctool run --sudo dsbulk-client "sudo apt update --assume-yes; sudo apt install maven --assume-yes; sudo apt-get install unzip --assume-yes"

#setup data-set (multiple records per Partition Key)
github_username="username"; github_password="password";
ctool run --sudo dsbulk-client "cd /mnt/data; sudo su automaton; git clone https://${github_username}:${github_password}@github.com/riptano/data_faker.git; cd data_faker; mvn clean package"
#generate 1 million PKs. Every PK has >= 50 && <= 100 records.
ctool run --sudo dsbulk-client "cd /mnt/data/data_faker; java -jar target/fake-data-generator-1.0.jar 32 1000000 50 100 false"

#prepare data for parallel LOAD ordered
ctool run --sudo dsbulk-client "mkdir /mnt/data/data_faker/generated_one_file"
ctool run --sudo dsbulk-client "cd /mnt/data/data_faker/generated; cat purchases_1.csv purchases_2.csv purchases_3.csv purchases_4.csv purchases_5.csv purchases_6.csv purchases_7.csv purchases_8.csv purchases_9.csv purchases_10.csv purchases_11.csv purchases_12.csv purchases_13.csv purchases_14.csv purchases_15.csv purchases_16.csv purchases_17.csv purchases_18.csv purchases_19.csv purchases_20.csv purchases_21.csv purchases_22.csv purchases_23.csv purchases_24.csv purchases_25.csv purchases_26.csv purchases_27.csv purchases_28.csv purchases_29.csv purchases_30.csv purchases_31.csv purchases_32.csv > ../generated_one_file/purchases.csv"


#setup DSE keyspaces/tables
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'};\""
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE TABLE IF NOT EXISTS test.test100b(pkey TEXT, ccol BIGINT, data TEXT, PRIMARY KEY ((pkey), ccol));\""
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE TABLE IF NOT EXISTS test.test1kb(pkey TEXT, ccol BIGINT, data TEXT, PRIMARY KEY ((pkey), ccol));\""
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE TABLE IF NOT EXISTS test.test10kb(pkey TEXT, ccol BIGINT, data TEXT, PRIMARY KEY ((pkey), ccol));\""
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE TABLE IF NOT EXISTS test.test1mb(pkey TEXT, ccol BIGINT, data TEXT, PRIMARY KEY ((pkey), ccol));\""
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE TABLE IF NOT EXISTS test.test100(pkey BIGINT, ccol BIGINT, col0 BIGINT, col1 BIGINT, col2 BIGINT, col3 BIGINT, col4 BIGINT, col5 BIGINT, col6 BIGINT, col7 BIGINT, col8 BIGINT, col9 BIGINT, col10 BIGINT, col11 BIGINT, col12 BIGINT, col13 BIGINT, col14 BIGINT, col15 BIGINT, col16 BIGINT, col17 BIGINT, col18 BIGINT, col19 BIGINT, col20 BIGINT, col21 BIGINT, col22 BIGINT, col23 BIGINT, col24 BIGINT, col25 BIGINT, col26 BIGINT, col27 BIGINT, col28 BIGINT, col29 BIGINT, col30 BIGINT, col31 BIGINT, col32 BIGINT, col33 BIGINT, col34 BIGINT, col35 BIGINT, col36 BIGINT, col37 BIGINT, col38 BIGINT, col39 BIGINT, col40 BIGINT, col41 BIGINT, col42 BIGINT, col43 BIGINT, col44 BIGINT, col45 BIGINT, col46 BIGINT, col47 BIGINT, col48 BIGINT, col49 BIGINT, col50 BIGINT, col51 BIGINT, col52 BIGINT, col53 BIGINT, col54 BIGINT, col55 BIGINT, col56 BIGINT, col57 BIGINT, col58 BIGINT, col59 BIGINT, col60 BIGINT, col61 BIGINT, col62 BIGINT, col63 BIGINT, col64 BIGINT, col65 BIGINT, col66 BIGINT, col67 BIGINT, col68 BIGINT, col69 BIGINT, col70 BIGINT, col71 BIGINT, col72 BIGINT, col73 BIGINT, col74 BIGINT, col75 BIGINT, col76 BIGINT, col77 BIGINT, col78 BIGINT, col79 BIGINT, col80 BIGINT, col81 BIGINT, col82 BIGINT, col83 BIGINT, col84 BIGINT, col85 BIGINT, col86 BIGINT, col87 BIGINT, col88 BIGINT, col89 BIGINT, col90 BIGINT, col91 BIGINT, col92 BIGINT, col93 BIGINT, col94 BIGINT, col95 BIGINT, col96 BIGINT, col97 BIGINT, PRIMARY KEY ((pkey), ccol));\""
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE TABLE IF NOT EXISTS test.test10(pkey BIGINT, ccol BIGINT, col0 BIGINT, col1 BIGINT, col2 BIGINT, col3 BIGINT, col4 BIGINT, col5 BIGINT, col6 BIGINT, col7 BIGINT, PRIMARY KEY ((pkey), ccol));\""

#ordered data-set table setup
ctool run dsbulk-dse 0 "cqlsh -e \"CREATE TABLE IF NOT EXISTS test.transactions(user_id TEXT, date timestamp, item TEXT, price float, quantity int, total decimal, currency TEXT, payment TEXT, contact list<text>, PRIMARY KEY ((user_id), date));\""


# TODO tweak settings.xml

#to build dsbulk on dsbulk-client (dsbulk should not have SNAPSHOT dependencies to build on ctool created instance)
#github_username="username"
#github_password="password"
#ctool run --sudo dsbulk-client "cd /mnt/data; git clone https://${github_username}:${github_password}@github.com/riptano/dsbulk.git"
#ctool run --sudo dsbulk-client "cd /mnt/data/dsbulk; sudo mvn clean package -DskipTests -P release"

#to build locally and scp to dsbulk-client, you can change --branch parameter to test against different branch
dsbulk_version=1.3.1-SNAPSHOT
rm -rf /tmp/dsbulk
mkdir /tmp/dsbulk
cd /tmp/dsbulk
`github_username="username"; github_password="password"; git clone --single-branch --branch 1.x https://${github_username}:${github_password}@github.com/riptano/dsbulk.git`
cd dsbulk
mvn clean package -DskipTests -P release
ctool scp -R dsbulk-client 0 /tmp/dsbulk/dsbulk/dist/target/*.zip /mnt/data/
ctool run --sudo dsbulk-client "cd /mnt/data/; unzip *.zip; mv dsbulk-${dsbulk_version} dsbulk"

# TODO single file vs multiple files (> # cores)

#LOAD - CSV-----------------------------------------------------------------------------------------------
ctool run dsbulk-dse 'nodetool -h localhost disableautocompaction test'

#run dsbulk step (random data-set) - LOAD
#100b
#tpc
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test100b -header false --batch.mode DISABLED --driver.basic.request.timeout '5 minutes' -url /mnt/data/DSEBulkLoadTest/in/data100B/ -h ${dse_ip} &> test100bLOAD_first_tpc"
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test100b;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test100b -header false --batch.mode DISABLED --driver.basic.request.timeout '5 minutes' -url /mnt/data/DSEBulkLoadTest/in/data100B/ -h ${dse_ip} &> test100bLOAD_second_tpc"

#parallel
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test100b;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test100b -header false --batch.mode DISABLED --driver.basic.request.timeout '5 minutes' -url /mnt/data/DSEBulkLoadTest/in/data100B_one_file/ -h ${dse_ip} &> test100bLOAD_parallel"

#1KB
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test1kb -header false --batch.mode REPLICA_SET -url /mnt/data/DSEBulkLoadTest/in/data1KB/ -h ${dse_ip} &> test1KBLOAD_first"
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test1kb;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test1kb -header false --batch.mode REPLICA_SET -url /mnt/data/DSEBulkLoadTest/in/data1KB/ -h ${dse_ip} &> test1KBLOAD_second"

#10KB
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test10kb -header false --batch.mode REPLICA_SET --connector.csv.maxCharsPerColumn 11000 -url /mnt/data/DSEBulkLoadTest/in/data10KB/ -h ${dse_ip} &> test10KBLOAD_first"
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test10kb;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test10kb -header false --batch.mode REPLICA_SET --connector.csv.maxCharsPerColumn 11000 -url /mnt/data/DSEBulkLoadTest/in/data10KB/ -h ${dse_ip} &> test10KBLOAD_second"

#1MB
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test1mb -header false --batch.mode DISABLED --connector.csv.maxCharsPerColumn 1100000 --executor.maxInFlight 64 -url /mnt/data/DSEBulkLoadTest/in/data1MB/ -h ${dse_ip} &> test1MBLOAD_first"
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test1mb;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test1mb -header false --batch.mode DISABLED --connector.csv.maxCharsPerColumn 1100000 --executor.maxInFlight 64 -url /mnt/data/DSEBulkLoadTest/in/data1MB/ -h ${dse_ip} &> test1MBLOAD_second"

#10 number of columns
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test10 -header false --batch.mode REPLICA_SET -url /mnt/data/DSEBulkLoadTest/in/data10/ -h ${dse_ip} &> test10LOAD_first"
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test10;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test10 -header false --batch.mode REPLICA_SET -url /mnt/data/DSEBulkLoadTest/in/data10/ -h ${dse_ip} &> test10LOAD_second"


#run dsbulk step (ordered data-set) - LOAD
#TPC
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t transactions -header false --batch.mode PARTITION_KEY -url /mnt/data/data_faker/generated -h ${dse_ip} -delim '|' -m '0=user_id,1=date,2=item,3=price,4=quantity,5=total,6=currency,7=payment,8=contact' --codec.timestamp ISO_ZONED_DATE_TIME &> transactionsLOAD_tpc"
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.transactions;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'

#parallel
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.transactions;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t transactions -header false --batch.mode PARTITION_KEY -url /mnt/data/data_faker/generated_one_file -h ${dse_ip} -delim '|' -m '0=user_id,1=date,2=item,3=price,4=quantity,5=total,6=currency,7=payment,8=contact' --codec.timestamp ISO_ZONED_DATE_TIME &> transactionsLOAD_parallel"

#run repair to make COUNT and LOAD yield proper results
ctool run dsbulk-dse 'nodetool -h localhost repair'


#UNLOAD as CSV-----------------------------------------------------------------------------------------------
ctool run dsbulk-dse 'nodetool -h localhost enableautocompaction test'
# blocks until finished - FIXME the command times out
ctool run dsbulk-dse 'nodetool -h localhost compact test'

#wait for compaction to finish -
#ctool run dsbulk-dse 'nodetool -h localhost compactionstats'

#run dsbulk step (random data-set) - UNLOAD

#100B TPC
ctool run --sudo dsbulk-client "rm -Rf /mnt/data/DSEBulkLoadTest/out/data100B/; /mnt/data/dsbulk/bin/dsbulk unload -k test -t test100b -header false -url /mnt/data/DSEBulkLoadTest/out/data100B/ -h ${dse_ip} &> 100BUNLOAD_tpc"

#100B parallel
ctool run --sudo dsbulk-client "rm -Rf /mnt/data/DSEBulkLoadTest/out/data100B/; /mnt/data/dsbulk/bin/dsbulk unload -header false -url /mnt/data/DSEBulkLoadTest/out/data100B/ -h ${dse_ip} -query 'SELECT * FROM test.test100b WHERE token(pkey) > -9223372036854775807 and token (pkey) <= 3074457345618258602' &> 100BUNLOAD_parallel"

ctool run --sudo dsbulk-client "rm -Rf /mnt/data/DSEBulkLoadTest/out/data1KB/; /mnt/data/dsbulk/bin/dsbulk unload -k test -t test1kb -header false -url /mnt/data/DSEBulkLoadTest/out/data1KB/ -h ${dse_ip} &> 1KBUNLOAD"

ctool run --sudo dsbulk-client "rm -Rf /mnt/data/DSEBulkLoadTest/out/data10KB/; /mnt/data/dsbulk/bin/dsbulk unload -k test -t test10kb -header false -url /mnt/data/DSEBulkLoadTest/out/data10KB/ -h ${dse_ip} &> 10kbUNLOAD"

ctool run --sudo dsbulk-client "rm -Rf /mnt/data/DSEBulkLoadTest/out/data1MB/; /mnt/data/dsbulk/bin/dsbulk unload -k test -t test1mb -header false -url /mnt/data/DSEBulkLoadTest/out/data1MB/ -h ${dse_ip} --executor.continuousPaging.pageSize 500000 --executor.continuousPaging.pageUnit BYTES &> 1mbUNLOAD"

ctool run --sudo dsbulk-client "rm -Rf /mnt/data/DSEBulkLoadTest/out/data10/; /mnt/data/dsbulk/bin/dsbulk unload -k test -t test10 -header false -url /mnt/data/DSEBulkLoadTest/out/data10/ -h ${dse_ip} &> 10UNLOAD"

#run dsbulk step (sorted data-set) - UNLOAD
#TPC
ctool run --sudo dsbulk-client "rm -Rf /mnt/data/data_faker/generated; /mnt/data/dsbulk/bin/dsbulk unload -k test -t transactions -header false -url /mnt/data/data_faker/generated -h ${dse_ip} -m '0=user_id,1=date,2=item,3=price,4=quantity,5=total,6=currency,7=payment,8=contact' &> transactions-UNLOAD_tpc"

#parallel
ctool run --sudo dsbulk-client "rm -Rf /mnt/data/data_faker/generated; /mnt/data/dsbulk/bin/dsbulk unload -header false -url /mnt/data/data_faker/generated -h ${dse_ip} -m '0=user_id,1=date,2=item,3=price,4=quantity,5=total,6=currency,7=payment,8=contact'  -query 'SELECT * FROM test.transactions WHERE token(user_id) > -9223372036854775807 and token (user_id) <= 3074457345618258602' &> transactions-UNLOAD_parallel"


#UNLOAD as JSON-----------------------------------------------------------------------------------------------

#run dsbulk step (random data-set) - UNLOAD
ctool run --sudo dsbulk-client "rm -Rf /mnt/data/DSEBulkLoadTest/out/data100B/; /mnt/data/dsbulk/bin/dsbulk unload -k test -t test100b -c json -url /mnt/data/DSEBulkLoadTest/out/data100B/ -h ${dse_ip} &> 100BUNLOADjson"

ctool run --sudo dsbulk-client "rm -Rf /mnt/data/DSEBulkLoadTest/out/data1KB/; /mnt/data/dsbulk/bin/dsbulk unload -k test -t test1kb -c json -url /mnt/data/DSEBulkLoadTest/out/data1KB/ -h ${dse_ip} &> 1KBUNLOADjson"

ctool run --sudo dsbulk-client "rm -Rf /mnt/data/DSEBulkLoadTest/out/data10KB/; /mnt/data/dsbulk/bin/dsbulk unload -k test -t test10kb -c json -url /mnt/data/DSEBulkLoadTest/out/data10KB/ -h ${dse_ip} &> 10kbUNLOADjson"

ctool run --sudo dsbulk-client "rm -Rf /mnt/data/DSEBulkLoadTest/out/data1MB/; /mnt/data/dsbulk/bin/dsbulk unload -k test -t test1mb -c json -url /mnt/data/DSEBulkLoadTest/out/data1MB/ -h ${dse_ip} --executor.continuousPaging.pageSize 500000 --executor.continuousPaging.pageUnit BYTES &> 1mbUNLOADjson"

ctool run --sudo dsbulk-client "rm -Rf /mnt/data/DSEBulkLoadTest/out/data10/; /mnt/data/dsbulk/bin/dsbulk unload -k test -t test10 -c json -url /mnt/data/DSEBulkLoadTest/out/data10/ -h ${dse_ip} &> 10UNLOADjson"

#run dsbulk step (sorted data-set) - UNLOAD
ctool run --sudo dsbulk-client "rm -Rf /mnt/data/data_faker/generated; /mnt/data/dsbulk/bin/dsbulk unload -k test -t transactions -c json -url /mnt/data/data_faker/generated -h ${dse_ip} &> transactionsUNLOADjson"


#COUNT-----------------------------------------------------------------------------------------------

#run dsbulk step (random data-set) - COUNT
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk count -k test -t test100b -h ${dse_ip} &> count100b"
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk count -k test -t test1kb -h ${dse_ip} &> count1kb"
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk count -k test -t test10kb -h ${dse_ip} &> count10kb"
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk count -k test -t test1mb -h ${dse_ip} &> count1mb"
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk count -k test -t test10 -h ${dse_ip} &> count10"

#run dsbulk step (ordered data-set) - COUNT
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk count -k test -t transactions -h ${dse_ip} &> countTransactions"

#LOAD - JSON-----------------------------------------------------------------------------------------------
ctool run dsbulk-dse 'nodetool -h localhost disableautocompaction test'

#run dsbulk step (random data-set)
#100b
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test100b;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test100b -c json --batch.mode REPLICA_SET -url /mnt/data/DSEBulkLoadTest/out/data100B/ -h ${dse_ip} &> test100bLOADjson_first"
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test100b;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test100b -c json --batch.mode REPLICA_SET -url /mnt/data/DSEBulkLoadTest/out/data100B/ -h ${dse_ip} &> test100bLOADjson_second"

#1KB
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test1kb;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test1kb -c json --batch.mode REPLICA_SET --driver.basic.request.timeout '5 minutes' -url /mnt/data/DSEBulkLoadTest/out/data1KB/ -h ${dse_ip} &> test1KBLOADjson_first"
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test1kb;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test1kb -c json --batch.mode REPLICA_SET --driver.basic.request.timeout '5 minutes' -url /mnt/data/DSEBulkLoadTest/out/data1KB/ -h ${dse_ip} &> test1KBLOADjson_second"

#10KB
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test10kb;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test10kb -c json --batch.mode DISABLED --driver.basic.request.timeout '5 minutes' -url /mnt/data/DSEBulkLoadTest/out/data10KB/ -h ${dse_ip} &> test10KBLOADjson_first"
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test10kb;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test10kb -c json --batch.mode DISABLED --driver.basic.request.timeout '5 minutes' -url /mnt/data/DSEBulkLoadTest/out/data10KB/ -h ${dse_ip} &> test10KBLOADjson_second"

#1MB
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test1mb;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test1mb -c json --batch.mode DISABLED --executor.maxInFlight 64 --driver.basic.request.timeout '5 minutes' -url /mnt/data/DSEBulkLoadTest/out/data1MB/ -h ${dse_ip} &> test1MBLOADjson_first"
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test1mb;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test1mb -c json --batch.mode DISABLED --executor.maxInFlight 64 --driver.basic.request.timeout '5 minutes' -url /mnt/data/DSEBulkLoadTest/out/data1MB/ -h ${dse_ip} &> test1MBLOADjson_second"

#10 number of columns
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test10;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test10 -c json --batch.mode REPLICA_SET --driver.basic.request.timeout '5 minutes' -url /mnt/data/DSEBulkLoadTest/out/data10/ -h ${dse_ip} &> test10LOADjson_first"
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.test10;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t test10 -c json --batch.mode REPLICA_SET --driver.basic.request.timeout '5 minutes' -url /mnt/data/DSEBulkLoadTest/out/data10/ -h ${dse_ip} &> test10LOADjson_second"

#run dsbulk step (ordered data-set)
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.transactions;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t transactions -c json --batch.mode PARTITION_KEY --driver.basic.request.timeout '5 minutes' -url /mnt/data/data_faker/generated -h ${dse_ip} --codec.timestamp ISO_ZONED_DATE_TIME &> transactionsLOAD_json_first"
ctool run dsbulk-dse 0 "cqlsh -e \"TRUNCATE test.transactions;\""
ctool run dsbulk-dse 'nodetool clearsnapshot --all'
ctool run --sudo dsbulk-client "/mnt/data/dsbulk/bin/dsbulk load -k test -t transactions -c json --batch.mode PARTITION_KEY --driver.basic.request.timeout '5 minutes' -url /mnt/data/data_faker/generated -h ${dse_ip} --codec.timestamp ISO_ZONED_DATE_TIME &> transactionsLOAD_json_second"
