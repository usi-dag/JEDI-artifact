package s2s.experiments;


import s2s.bench_generator.Generator;
import s2s.query_compiler.QueryCompiler;
import s2s.query_compiler.template.imperative.ImperativeCompilerOptions;
import s2s.query_compiler.template.imperative.SQLToImperativeJava;
import s2s.query_compiler.template.stream.SQLToStream;
import s2s.query_compiler.template.stream.StreamCompilerOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ConvertTPCHFilterIntensiveQuery {

    private static final Logger LOGGER = LogManager.getLogger(ConvertTPCHFilterIntensiveQuery.class);

    public static final String QUERY = """
            SELECT COUNT(*) AS ctn
            FROM lineitem
            WHERE l_orderkey >= 0
            AND l_partkey >= 0
            AND l_suppkey >= 0
            AND l_linenumber >= 0
            AND l_quantity >= 0
            AND l_extendedprice >= 0
            AND l_tax >= 0
            """;

    public static void main(String[] args) throws Exception {
        try {
            String benchFolderName = args.length > 0 ? args[0] : "microbenchmark-o1";
            String packageFolder = "micro";
            Generator generator = ConvertNewProject.generateTPCH(benchFolderName, packageFolder);

            StreamCompilerOptions.Builder basaelineStreamOptionBuilder = StreamCompilerOptions
                    .newBuilder()
                    .withSequentialThread()
                    .joinWithFlatMap()
                    .withPredicateConjunctionCompression(false);

            List<QueryCompiler> compilers = new LinkedList<>();
            compilers.add(new SQLToImperativeJava(ImperativeCompilerOptions.DEFAULT));
            compilers.add(new SQLToStream(basaelineStreamOptionBuilder.build()));
            compilers.add(new SQLToStream(basaelineStreamOptionBuilder.withPredicateConjunctionCompression(true).build()));


            generator.generateQueries(Map.of("Q01", QUERY), compilers);
        } catch (Exception e) {
            LOGGER.error(e);
            System.exit(1);
        }

    }
}