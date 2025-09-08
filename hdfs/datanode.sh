#!/bin/bash

set -e

DATANODE_DIR="/opt/hadoop/data/dataNode"

if [ -d "\$DATANODE_DIR" ]; then
    rm -rf "\$DATANODE_DIR"/*
    echo "Clean data node directory"
else
    mkdir -p "\$DATANODE_DIR"
fi

chown -R hadoop:hadoop "\$DATANODE_DIR"
chmod 755 "\$DATANODE_DIR"

hdfs datanode