#!/bin/bash

sudo sysctl vm.dirty_writeback_centisecs=30000
echo "sudo sysctl vm.dirty_writeback_centisecs=30000"
sudo sysctl vm.dirty_ratio=90
echo "sudo sysctl vm.dirty_ratio=90"
sudo sysctl vm.dirty_background_ratio=80
echo "sudo sysctl vm.dirty_background_ratio=80"
sudo sysctl vm.dirty_expire_centisecs=300000000
echo "sudo sysctl vm.dirty_expire_centisecs=300000000"
# This one is actually for fuseki. Otherwise, it fails on wikidata
sudo sysctl -w vm.max_map_count=1000000
echo "sudo sysctl -w vm.max_map_count=1000000"