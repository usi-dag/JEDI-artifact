package s2s.experiments;


import s2s.bench_generator.Generator;
import s2s.bench_generator.MaybePlannedQuery;
import s2s.planner.qp.Schema;
import s2s.query_compiler.QueryCompiler;
import s2s.query_compiler.template.stream.SQLToStream;
import s2s.query_compiler.template.stream.StreamCompilerOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ConvertTPCHParallel {

    private static final Logger LOGGER = LogManager.getLogger(ConvertTPCHParallel.class);
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

            // Build all compiler options, i.e.,all sequential ones + all parallel based on the best sequential
            StreamCompilerOptions.Builder bestSequentialStreamOption = StreamCompilerOptions.newBuilder()
                    .withSequentialThread()
                    .joinWithMapMulti()
                    .withPredicateConjunctionCompression(true);

            List<QueryCompiler> queryCompilers = new ArrayList<>();
            for(StreamCompilerOptions.MultiThreading multiThreading : StreamCompilerOptions.MultiThreading.values()) {
                StreamCompilerOptions option = bestSequentialStreamOption.withMultiThreading(multiThreading).build();
                queryCompilers.add(new SQLToStream(option));
            }

            // generate the benchmark
            generator.generateMaybePlannedQueries(queries, queryCompilers);
        } catch (Exception e) {
            LOGGER.error(e);
            System.exit(1);
        }

    }
}