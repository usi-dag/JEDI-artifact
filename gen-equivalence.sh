#!/bin/bash

CLS=s2s.experiments.CodegenDuplicateFinder
mvn package -q && mvn -q exec:java -Dexec.mainClass=$CLS > analysis/equivalence.py
