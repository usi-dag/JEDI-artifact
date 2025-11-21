#!/bin/bash

thisdir=$(readlink -f "${BASH_SOURCE[0]%/*}")
source $thisdir/_common.sh

readonly BENCH_SEQUENTIAL_JAR="$BENCH_SEQUENTIAL_FOLDER/target/benchmark.jar"
readonly BENCH_PARALLEL_JAR="$BENCH_PARALLEL_FOLDER/target/benchmark.jar"
readonly BENCH_MICRO_RQ1_JAR="$BENCH_MICRO_RQ1_FOLDER/target/benchmark.jar"
readonly BENCH_MICRO_RQ2_JAR="$BENCH_MICRO_RQ2_FOLDER/target/benchmark.jar"
readonly BENCH_MICRO_RQ2_DISTINCT_JAR="$BENCH_MICRO_RQ2_DISTINCT_FOLDER/target/benchmark.jar"

# Check that the jvms have been downloaded
./check-jvms.sh

# Check that all benchmarks have been compiled
check_bench_exist() {
  if [ ! -f $1 ]; then
    echo "Expected JAR does not exist: $1"
    echo "Run ./gen-benchmarks.sh as described in the README.md file"
    exit 1
  fi
}
check_bench_exist $BENCH_SEQUENTIAL_JAR
check_bench_exist $BENCH_PARALLEL_JAR
check_bench_exist $BENCH_MICRO_RQ1_JAR
check_bench_exist $BENCH_MICRO_RQ2_JAR
check_bench_exist $BENCH_MICRO_RQ2_DISTINCT_JAR


mkdir -p $BENCH_RESULTS

# Check this is a fast run (i.e., short harness, only two queries).
if [ "$1" = "veryfast" ]; then
  harness=" -f 1 -w 0 -wi 0 -i 1 -r 1 "
  tpch_filter="Q01|Q02"
  sf_seq="0.01"
  sf_par="0.01"
elif [ "$1" = "fast" ]; then
  harness=" -f 1 -w 0 -wi 0 -i 1 -r 1 "
  tpch_filter=""
  sf_seq="0.01"
  sf_par="0.01"
else
  harness=" -f 2 -w 10 -wi 5 -i 5 -r 10 "
  tpch_filter=""
  sf_seq="1"
  sf_par="10"
fi


run_tpch() {
  : ${SF:="1"}
  # Ensure we run TPC-H with the given SF

  cd $BASEDIR/TPCH-duckdb
  db="tpch.sf$SF.db"
  if [ ! -f "$db" ]; then
    echo "DB file does not exists: $db"
    echo "First, run ./gen-tpch-data-duckdb.sh"
    exit 1
  fi

  rm tpch.db
  ln -s $db tpch.db
  cd ..

  $JAVA_HOME/bin/java -jar $JAR $harness -bm avgt -tu ms -rf JSON -rff $FNAME $tpch_filter $@

  RESULT=$?
  if [ $RESULT -eq 0 ]; then
    echo "Out: $FNAME"
  else
    echo "Run Failed"
  fi
}

# Run sequential benchmarks
echo "Running sequential benchmarks (JDK)"
JAVA_HOME=$JDK_DIR \
  JAR=$BENCH_SEQUENTIAL_JAR \
  FNAME=$BENCH_RESULTS/sequential-jdk.json \
  SF=$sf_seq run_tpch \
  2>&1 | tee "$BENCH_RESULTS/sequential-jdk.out"

echo "Running sequential benchmarks (GraalVM)"
JAVA_HOME=$GRAALVM_DIR \
  JAR=$BENCH_SEQUENTIAL_JAR \
  FNAME=$BENCH_RESULTS/sequential-graalvm.json \
  SF=$sf_seq run_tpch \
  2>&1 | tee "$BENCH_RESULTS/sequential-graalvm.out"

# Run parallel benchmarks
echo "Running parallel benchmarks (JDK)"
JAVA_HOME=$JDK_DIR \
 JAR=$BENCH_PARALLEL_JAR \
 FNAME=$BENCH_RESULTS/parallel-jdk.json \
 SF=$sf_par run_tpch \
 --jvmArgsAppend="-XX:InitialRAMPercentage=90.0" \
 --jvmArgsAppend="-XX:MaxRAMPercentage=90.0" \
 2>&1 | tee "$BENCH_RESULTS/parallel-jdk.out"

# Run parallel benchmarks
echo "Running parallel benchmarks (GraalVM)"
JAVA_HOME=$GRAALVM_DIR \
 JAR=$BENCH_PARALLEL_JAR \
 FNAME=$BENCH_RESULTS/parallel-graalvm.json \
 SF=$sf_par run_tpch \
 --jvmArgsAppend="-XX:InitialRAMPercentage=90.0" \
 --jvmArgsAppend="-XX:MaxRAMPercentage=90.0" \
 2>&1 | tee "$BENCH_RESULTS/parallel-graalvm.out"

# Microbenchmarks run only on JDK
export JAVA_HOME=$JDK_DIR

# Run microbenchmark (RQ1)
echo "Running microbenchmark (RQ1)"
JAR=$BENCH_MICRO_RQ1_JAR \
  FNAME=$BENCH_RESULTS/microbenchmark-o1.json \
  SF=$sf_seq run_tpch \
  2>&1 | tee "$BENCH_RESULTS/microbenchmark-o1.out"

# Run microbenchmark (RQ2)
run_micro_rq2() {
  suffix=$1
  groupsizes=$2

  $JAVA_HOME/bin/java \
    -jar $BENCH_MICRO_RQ2_JAR \
    $harness -bm avgt -tu ms \
    -rf JSON -rff $BENCH_RESULTS/microbenchmark-o2-$suffix.json \
    -p numOrders=10000000 \
    -p mod="$groupsizes" \
    2>&1 | tee "$BENCH_RESULTS/microbenchmark-o2-$suffix.out"
}

echo "Running microbenchmark (RQ2 - small)"
run_micro_rq2 "small" "1,10,20,30,40,50,60,70,80,90,100,110,120,130,140,150,160,170,180,190,200,210,220,230,240,250,260,270,280,290,300,310,320,330,340,350,360,370,380,390,400,410,420,430,440,450,460,470,480,490"

echo "Running microbenchmark (RQ2 - large)"
run_micro_rq2 "large" "500,10500,20500,30500,40500,50500,60500,70500,80500,90500,100500,110500,120500,130500,140500,150500,160500,170500,180500,190500,200500,210500,220500,230500,240500,250500,260500,270500,280500,290500,300500,310500,320500,330500,340500,350500,360500,370500,380500,390500,400500,410500,420500,430500,440500,450500,460500,470500,480500,490500,500500"


# Run microbenchmark (RQ2 stateful)
echo "Running microbenchmark (RQ2 stateful)"

run_distinct() {
  suffix=$1
  ndistinct=$2

  $JDK_DIR/bin/java \
      -jar $BENCH_MICRO_RQ2_DISTINCT_JAR \
      $harness -bm avgt -tu ms \
      -rf JSON -rff "$BENCH_RESULTS/microbenchmark-distinct-$suffix.json" \
      "Distinct" \
      -p nDistinct="$ndistinct" \
      2>&1 | tee "$BENCH_RESULTS/microbenchmark-distinct-$suffix.out"
}

echo "Running microbenchmark (Distinct - small)"
run_distinct "small" "20,40,60,80,100,120,140,160,180,200,220,240,260,280,300,320,340,360,380,400,420,440,460,480,500,520,540,560,580,600,620,640,660,680,700,720,740,760,780,800,820,840,860,880,900,920,940,960,980"

echo "Running microbenchmark (Distinct - large)"
run_distinct "large" "1000,21000,41000,61000,81000,101000,121000,141000,161000,181000,201000,221000,241000,261000,281000,301000,321000,341000,361000,381000,401000,421000,441000,461000,481000,501000,521000,541000,561000,581000,601000,621000,641000,661000,681000,701000,721000,741000,761000,781000,801000,821000,841000,861000,881000,901000,921000,941000,961000,981000"
