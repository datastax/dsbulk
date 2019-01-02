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
curl "http://${dse_ip}:8888/opscenter/index.html"

ctool install_agents dsbulk-dse dsbulk-client
