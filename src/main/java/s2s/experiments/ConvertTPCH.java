package s2s.experiments;


import s2s.bench_generator.Generator;
import s2s.bench_generator.MaybePlannedQuery;
import s2s.planner.qp.Schema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import s2s.query_compiler.QueryCompiler;
import s2s.query_compiler.template.imperative.ImperativeCompilerOptions;
import s2s.query_compiler.template.imperative.SQLToImperativeJava;
import s2s.query_compiler.template.stream.SQLToStream;
import s2s.query_compiler.template.stream.StreamCompilerOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConvertTPCH {

    private static final Logger LOGGER = LogManager.getLogger(ConvertTPCH.class);
    private static final List<String> SKIPPED_QUERIES = Arrays.asList(
            System.getProperty("skippedQueries", "").split(","));


    public static void main(String[] args) throws Exception {
        try {
            String benchFolderName = args.length > 0 ? args[0] : "benchmarks";
            String packageFolder = "tpch";

            Generator generator = ConvertNewProject.generateTPCH(benchFolderName, packageFolder);
            Map<String, Schema> schema = generator.getSchema();
            Map<String, MaybePlannedQuery> queries = TPCHQueriesOptPlanned.getQueries(schema);
            SKIPPED_QUERIES.forEach(queries::remove);

            // Build all compiler options for sequential streams
            List<StreamCompilerOptions> sequentialStreamOption = List.of(StreamCompilerOptions.ALL_SEQUENTIAL);

            // create all the query compilers, i.e., all the stream ones and the imperative one
            List<QueryCompiler> queryCompilers = sequentialStreamOption.stream()
                    .map(SQLToStream::new)
                    .collect(Collectors.toList());
            queryCompilers.add(new SQLToImperativeJava(ImperativeCompilerOptions.DEFAULT));

            // generate the benchmark
            generator.generateMaybePlannedQueries(queries, queryCompilers);
        } catch (Exception e) {
            LOGGER.error(e);
            System.exit(1);
        }

    }
}