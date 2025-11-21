#!/bin/bash

thisdir=$(readlink -f "${BASH_SOURCE[0]%/*}")
source $thisdir/_common.sh

# Function to check if a directory exists and contains bin/java
check_jvm() {
    local jvm_dir="$1"

    if [[ ! -d "$jvm_dir" ]]; then
        echo "Error: Directory $jvm_dir does not exist. Run ./get-jvms.sh"
        exit 1
    fi

    if [[ ! -f "$jvm_dir/bin/java" ]]; then
        echo "Error: $jvm_dir/bin/java not found. Run ./get-jvms.sh"
        exit 1
    fi
}

check_jvm "$BASEDIR/jvm/jdk24"
check_jvm "$BASEDIR/jvm/graalvm24"
