package s2s.experiments;

import s2s.bench_generator.Generator;
import s2s.bench_generator.PlannedQuery;
import s2s.planner.qp.S2SPlan;
import s2s.query_compiler.QueryCompiler;
import s2s.query_compiler.template.GeneratedClassHolder;
import s2s.query_compiler.template.stream.SQLToStream;
import s2s.query_compiler.template.stream.StreamCompilerOptions;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;


public class CodegenDuplicateFinder {

    public static final String EOL = System.lineSeparator();
    public static final String EOL_TAB = EOL + "\t";

    public static String indent(String code) {
        return "\t" + code.replace(EOL, EOL_TAB);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("USE_ALL_QUERIES", "true");
        System.setProperty("USE_ALL_OPTIONS", "true");
        Map<String, PlannedQuery> queries = getQueries();

        List<QueryCompiler> compilers = Arrays.stream(StreamCompilerOptions.ALL_SEQUENTIAL)
                .map(SQLToStream::new)
                .collect(Collectors.toList());
        Map<String, List<List<String>>> partitionsMap = partitionQueryCompilers(queries, compilers);

        StringBuilder sb = new StringBuilder();
        sb.append("# Automatically generated code from S2S - CodegenDuplicateFinder").append(EOL);
        sb.append("duplicates={").append(EOL);

        String duplicates = partitionsMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> {
                    String query = e.getKey();
                    List<List<String>> partitions = e.getValue();
                    String s = "'%s': %s".formatted(query, partitionsToString(partitions));
                    return indent(s);
                })
                .collect(Collectors.joining(',' + EOL));

        sb.append(duplicates);
        sb.append("}");
        System.out.println(sb);
    }

    private static Map<String, List<List<String>>> partitionQueryCompilers(Map<String, PlannedQuery> queries, List<? extends QueryCompiler> compilers) {
        Map<String, List<List<String>>> partitions = new HashMap<>();
        for (Map.Entry<String, PlannedQuery> entry : queries.entrySet()) {
            String qName = entry.getKey();
            List<List<String>> partitionList = new ArrayList<>();
            partitions.put(qName, partitionList);

            S2SPlan plan = entry.getValue().plan();

            HashMap<String, List<String>> queryPartitions = new HashMap<>();

            for(QueryCompiler compiler : compilers) {
                GeneratedClassHolder generatedSqlStreamClass = compiler.compile(plan, "exec", "DB db");
                String genCode = generatedSqlStreamClass.asClass("CLS", "PACKAGE", new String[]{});
                List<String> partition = queryPartitions.get(genCode);
                if(partition == null) {
                    partition = new LinkedList<>();
                    queryPartitions.put(genCode, partition);
                    partitionList.add(partition);
                }
                partition.add(compiler.getVariantName());
            }
        }
        return partitions;
    }

    private static Map<String, PlannedQuery> getQueries() throws SQLException, IOException {
        String benchFolderName = "/tmp/tmp";
        String packageFolder = "tmp";

        File duckdbFile = new File("TPCH-duckdb/tpch.db");
        String duckdbPath = duckdbFile.getAbsolutePath();
        String jdbcUrl = "jdbc:duckdb:" + duckdbPath;
        String basedir = System.getProperty("basedir");
        File benchFolder = new File(basedir, benchFolderName);

        File outputDir = new File(benchFolder, "src/main/java/" + packageFolder);
        outputDir.mkdirs();

        File testOutputDir = new File(benchFolder, "src/test/java/" + packageFolder);
        testOutputDir.mkdirs();

        Generator generator = new Generator(packageFolder, outputDir.getAbsolutePath(), testOutputDir.getAbsolutePath());
        return generator.planMaybePlannedQueries(TPCHQueriesOptPlanned.getQueries(generator.generateSchema(jdbcUrl)));
    }

    // Utilities to generate a Python output file
    private static String partitionsToString(List<List<String>> partitionList) {
        String partitions = partitionList.stream()
                .map(CodegenDuplicateFinder::partitionToString)
                .collect(Collectors.joining(',' + EOL));
        return '[' + EOL + indent(partitions) + EOL + ']';
    }


    private static String partitionToString(List<String> partition) {
        String elements = partition.stream()
                .map(s -> '"' + s + '"')
                .collect(Collectors.joining(","));
        return '[' + elements + ']';
    }
}
