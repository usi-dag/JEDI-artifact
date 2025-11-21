#!/bin/bash

thisdir=$(readlink -f "${BASH_SOURCE[0]%/*}")
source $thisdir/_common.sh


wget_or_exit() {
    local url="$1"
    wget "$url" || { echo "Error: Failed to download $url"; exit 1; }
}


extract_and_move() {
    local archive="$1"
    local destination="$2"
    if [ -d $destination ]; then
      rm -rf $destination
    fi
    tar xf "$archive" || { echo "
    }Error extracting $archive"; exit 1; }
    local folder_name=$(tar tf "$archive" | head -1 | cut -d/ -f1)
    mv $folder_name $destination
}


arch=$(dpkg --print-architecture)
if [ "$arch" = "amd64" ]; then
  jdk21file="jdk-21_linux-x64_bin.tar.gz"
  jdk24file="jdk-24_linux-x64_bin.tar.gz"
  graalvmfile="graalvm-jdk-24_linux-x64_bin.tar.gz"
elif [ "$arch" = "arm64" ]; then
  jdk21file="jdk-21_linux-aarch64_bin.tar.gz"
  jdk24file="jdk-24_linux-aarch64_bin.tar.gz"
  graalvmfile="graalvm-jdk-24_linux-aarch64_bin.tar.gz"
else
  echo "Unsupported architecture: $arch"
fi

mkdir -p "$JVM_DIR"

rm *.gz

wget_or_exit "https://download.oracle.com/java/24/latest/$jdk24file"
wget_or_exit "https://download.oracle.com/graalvm/24/latest/$graalvmfile"

extract_and_move $jdk24file $JDK_DIR
extract_and_move $graalvmfile $GRAALVM_DIR

