#!/bin/bash

readonly BASEDIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")

# Benchmark folders
readonly BENCH_SEQUENTIAL_FOLDER="$BASEDIR/benchmarks/sequential"
readonly BENCH_PARALLEL_FOLDER="$BASEDIR/benchmarks/parallel"
readonly BENCH_MICRO_RQ1_FOLDER="$BASEDIR/microbenchmark-rq1/"
readonly BENCH_MICRO_RQ2_FOLDER="$BASEDIR/microbenchmark-rq2/"
readonly BENCH_MICRO_RQ2_DISTINCT_FOLDER="$BASEDIR/microbenchmark-rq2-distinct/"
readonly BENCH_RESULTS="$BASEDIR/analysis/results"

# JVMs
readonly JVM_DIR=$BASEDIR/jvm
readonly JDK21_DIR=$JVM_DIR/jdk21
readonly JDK_DIR=$JVM_DIR/jdk24
readonly GRAALVM_DIR=$JVM_DIR/graalvm24

