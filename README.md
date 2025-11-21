# Artifact for the paper under review submitted at ICSE 2026: 
# JEDI: Java Evaluation of Declarative and Imperative queries - Benchmarking the Java Stream API

## Main steps to reproduce the experiments:
The artifact has been tested only on Linux, on x64 and aarch64.

The main dependencies are:
- `S2S` (extended version is provided)
- `duckdb` (linux-amd64 and linux-aarch64 provided)
- `tpch-dbgen` (source code provided, precompiled for Linux x64, `gcc` required for aarch64)
- `python3` (should be pre-installed in major Linux distributions)
  - pandas
  - numpy
  - seaborn
  - matplotlib

As described in the paper,  complexity metrics (RQ4) are not part on this artifact.
We have manually calculated these metrics using IntelliJ from the code generated in Step (2), below.


## Reproducibility 
Below the steps required to reproduce the experiments in the paper.

Note that the paper shows results on multiple machines (i.e., x64 and aarch64).
This artifact is intended for reproducing data on a single machine.
To get combined data, as presented in the paper, the artifact should be executed on two machines and the results should be manually combined.

### Download the required JVMs: OracleJDK and GraalVM
`get-jvms.sh`


### Generate the input TPC-H dataset 
The following script generate multiple TPC-H datasets of different sizes (SF-0.01 for test, SF-1 and SF-10 for performance analysis) and load them as DuckDB databases.

`gen-tpch-duckdb.sh`

### Run the extended S2S to generate all the benchmark variants used in the paper
The script should work out of the box for amd64.
`./gen-benchmarks.sh`

For aarch64, the script automatically tries to compile the dbgen. In this case, `gcc` is required.
In case of issues, the DBGEN can be compiled as follows:
`cd TPCH-duckdb/tpch-dbgen/ && make`


### Run the experiments
All the experiments can be executed with the script:
`./run-experiments.sh`

Output data is produced by JMH in the folder `analysis/results`.

The script accepts an optional parameter: `fast` or `veryfast`.
`veryfast` runs only two queries (expected execution time, 10 minutes).
`fast` runs all the queries (expected execution time, 2 hours).
Both `fast` and `veryfast` run a single iteration of the benchmark on a very small dataset.
Therefore, they are not suitable for actual performance measurements, and should be used only to test the overall environment.

Expected execution time of all the experiments is about one week, but it largely depends on the machine.

Note that we have executed our parallel experiments on a machine with 128GB of RAM, providing a heap size of 90% of the total RAM to the JVM.
This setting is hardcoded in the `./run-experiment.sh` script. 
On a machine with smaller RAM, performance results may be different.

### Generate tables and figures to analyze output data
Run the Python script in the `analysis` folder:
`python3 analysis/analyze-data.py`

Images and tables will be generated in the folder `analysis/out`.

We also provide a simple LaTeX document to generate a PDF with all figures and tables.
This is an optional step which requires `pdflatex`.
`./gen-pdf.sh`
 
The output PDF will be produced in the `analysis/out/data.pdf`.