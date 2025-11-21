#!/bin/bash

thisdir=$(readlink -f "${BASH_SOURCE[0]%/*}")
source $thisdir/_common.sh


# TODO: test that TPC-H DuckDB test DB exists
if [ ! -f "$BASEDIR/TPCH-duckdb/tpch.sf0.01.db" ]; then
  echo "Test DB file does not exists"
  echo "First, run ./gen-tpch-data-duckdb.sh"
  exit 1
fi

export JAVA_HOME=$JDK_DIR

mvn -q clean package > /tmp/s2spackage
if [ $? -ne 0 ]; then
  echo "Error compiling S2S. See /tmp/s2spackage"
  exit 2
fi

generate() {
  mkdir -p $BENCH_DIR

  BENCH_DIR=`realpath $BENCH_DIR`
  echo "Generating the benchmark in: $BENCH_DIR"

  cd $BASEDIR

  rm -rf $BENCH_DIR/src
  mkdir -p $BENCH_DIR
  cp pom.benchmark.xml $BENCH_DIR/pom.xml

  # Ensure we test on TPC-H with SF 0.01
  cd $BASEDIR/TPCH-duckdb
  rm tpch.db
  ln -s tpch.sf0.01.db tpch.db
  cd $BASEDIR


  echo "Converting queries to Java sources"
  : ${CLS:="ConvertTPCH"}
  mvn -q exec:java \
    -Dexec.mainClass=s2s.experiments.$CLS \
    -Dexec.args="$BENCH_DIR" \
    $EXTRA_OPTIONS

  if [ $? -eq 0 ]; then
      echo "Query generation succeeded"
  else
      echo "Query generation failed"
      exit 3
  fi


  echo "Compiling and packaging the generated benchmark"
  cd $BENCH_DIR
  mkdir -p src/main/java/s2s/engine
  cp $BASEDIR/src/main/java/s2s/engine/*.java src/main/java/s2s/engine

  MAVEN_OPTS="-Xmx6g" mvn -q clean package -DskipTests > package.out

  if [ $? -eq 0 ]; then
    echo "Benchmark was successfully packaged"
  else
    echo "Error compiling the benchmark."
    echo "See $BENCH_DIR/package.out for more details"
    exit 4
  fi

  if [ "$SKIP_TESTS" == "true" ]; then
    echo "Tests should not be executed"
  else
    echo "Running tests..."
    cd $BENCH_DIR
    MAVEN_OPTS="-Xmx6g" mvn test > test.out
    if [ $? -eq 0 ]; then
      echo "All tests passed"
    else
      echo "There are failing tests."
      echo "See $BENCH_DIR/test.out for more details"
    fi
  fi

  cd $BASEDIR
}

export USE_ALL_QUERIES=true


# TPC-H Sequential
echo "Converting TPC-H sequential benchmark"
BENCH_DIR="$BENCH_SEQUENTIAL_FOLDER" \
generate


# TPC-H Parallel (Skip Q15 - see paper)
echo "Converting TPC-H parallel benchmark"
CLS="ConvertTPCHParallel" \
BENCH_DIR="$BENCH_PARALLEL_FOLDER" \
EXTRA_OPTIONS="-DskippedQueries=Q15" \
generate

# FilterIntensive query
echo "Generating FilterIntensive microbenchmark (RQ1 / O1)"
CLS="ConvertTPCHFilterIntensiveQuery" \
BENCH_DIR="$BENCH_MICRO_RQ1_FOLDER" \
generate

# VarGroupSize
echo "Compiling VaryGroupSize microbenchmark (RQ2)"
cd $BENCH_MICRO_RQ2_FOLDER
mvn -q clean package

# StatefulOperation Distinct
echo "Compiling StatefulOperation distinct (Revision)"
cd $BENCH_MICRO_RQ2_DISTINCT_FOLDER
mvn -q clean package