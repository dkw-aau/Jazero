#!/bin/bash

set -e

NAMENODE_DIR="/opt/hadoop/data/nameNode"

if [ ! -d "\$NAMENODE_DIR/current" ]; then
    echo "No metadata found"
    echo "Formatting name node"
    hdfs namenode -format -force -nonInteractive
else
    echo "NameNode already formatted"
    echo "Continuing"
fi

hdfs namenode