#!/bin/bash

thisdir=$(readlink -f "${BASH_SOURCE[0]%/*}")
source $thisdir/_common.sh

arch=$(dpkg --print-architecture)
if [ "$arch" = "arm64" ]; then
  cd $BASEDIR/TPCH-duckdb/tpch-dbgen
  echo "Compiling TPC-H DBGEN..."
  rm dbgen *.o
  make > /tmp/dbgen.out
  if [ $? -ne 0 ]; then
    echo "Error compiling TPC-H dbgen. See /tmp/dbgen.out"
    exit 1
  fi
  cd $BASEDIR
fi

gen_db() {
  sf=$1
  cd $BASEDIR/TPCH-duckdb/tpch-dbgen
  ./dbgen -v -f -s $sf
  if [ $? -ne 0 ]; then
    echo "Error invoking TPC-H dbgen."
    exit 1
  fi
  cd ..

  dbname="tpch.sf$sf.db"
  ./duckdb $dbname < duckdb-ddl.sql
  if [ $? -ne 0 ]; then
    echo "Error generating the DuckDB TPC-H database."
    exit 1
  fi

  echo "$dbname generated"
}


cd $BASEDIR/TPCH-duckdb

rm tpch*.db

# Generate databases with SF 0.01 (tests) and 1/10 (benchmarks)
gen_db "0.01"
gen_db "1"
gen_db "10"
