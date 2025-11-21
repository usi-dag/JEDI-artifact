package s2s.bench_generator;

import s2s.engine.Date;
import s2s.experiments.Utils;
import s2s.planner.calcite.CalcitePlanner;
import s2s.planner.qp.Field;
import s2s.planner.qp.PlanningException;
import s2s.planner.qp.S2SPlan;
import s2s.planner.qp.Schema;
import s2s.query_compiler.QueryCompiler;
import s2s.query_compiler.options.CommonCompilerOption;
import s2s.query_compiler.TypingUtils;
import s2s.query_compiler.template.GeneratedClassHolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static s2s.planner.calcite.SqlToCalciteRel.DEBUG_ENABLED;

public class Generator {

    private final String testFolderName;
    private final String queryPackageName;
    private final String queryFolderName;
    private final String dbPackageName;
    private final String dbFolderName;
    private final String benchPackageName;
    private final String benchFolderName;
    private final CalcitePlanner planner;
    private final Map<String, Schema> schemaMap = new HashMap<>();
    private String connectionUrl;

    private boolean isSchemaGenerated = false;

    public Generator(String packageName, String folderName, String testFolderName) {
        this.testFolderName = Paths.get(testFolderName, "queries").toAbsolutePath().toString();;

        queryPackageName = packageName + ".queries";
        queryFolderName = Paths.get(folderName, "queries").toAbsolutePath().toString();
        dbPackageName = packageName + ".db";
        dbFolderName = Paths.get(folderName, "db").toAbsolutePath().toString();
        benchPackageName = packageName + ".benchmarks";
        benchFolderName = Paths.get(folderName, "benchmarks").toAbsolutePath().toString();
        planner = new CalcitePlanner();
    }

    public Map<String, Schema> getSchema() {
        return schemaMap;
    }

    public Map<String, Schema> generateSchema(String connectionUrl) throws SQLException, IOException {
        mkFolders();

        Connection connection = DriverManager.getConnection(connectionUrl);

        this.connectionUrl = connectionUrl;

        // generate the schema Java files and populate the planner with DB tables
        return generateSchema(connection, getTableNames(connection));
    }

    public void generateQueries(Map<String, String> queries,
                                List<? extends QueryCompiler> compilers)
            throws IOException {
        generateMaybePlannedQueries(Utils.unplanned(queries), compilers);
    }


    public void generateMaybePlannedQueries(Map<String, MaybePlannedQuery> queries,
                                            List<? extends QueryCompiler> compilers)
            throws IOException {
        doGenerateQueries(planMaybePlannedQueries(queries), compilers);
    }

    public Map<String, PlannedQuery>  planMaybePlannedQueries(Map<String, MaybePlannedQuery> queries) {
        Map<String, PlannedQuery> plans = new HashMap<>();
        for(Map.Entry<String, MaybePlannedQuery> entry : queries.entrySet()) {
            if(DEBUG_ENABLED) {
                System.out.println("Planning " + entry.getKey());
            }
            try {
                PlannedQuery planned = entry.getValue().plan(planner);
                if(DEBUG_ENABLED) {
                    System.out.println("Plan " + planned.plan());
                }
                plans.put(entry.getKey(), planned);
            } catch (Exception e) {
                System.err.println("Exception planning " + entry.getKey() + ": " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
        return plans;
    }


    private void mkFolders() {
        for(String folder : new String[]{queryFolderName, dbFolderName, benchFolderName, testFolderName}) {
            new File(folder).mkdirs();
        }
    }

    private List<String> getTableNames(Connection connection) throws SQLException {
        List<String> tables = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tablesRS = metaData.getTables(null, null, "%", null);
        while (tablesRS.next()) {
            tables.add(tablesRS.getString("TABLE_NAME"));
        }
        return tables;
    }


    private void doGenerateQueries(Map<String, PlannedQuery> queries, List<? extends QueryCompiler> compilers)
            throws IOException {

        if(!isSchemaGenerated) {
            throw new IllegalStateException("Schema should be generated before queries.");
        }



        // generate a test base class
        String testBaseClassCode = TestGeneratorUtils.getTestBaseClassTemplateJdbc(queryPackageName, connectionUrl);
        // alternative using Calcite as engine:
//        String testBaseClassCode = TestGeneratorUtils.getTestBaseClassTemplateCalcite(
//                packageName, dbPackageName, ConnectionUtil.class.getCanonicalName());

        genClass(testFolderName, "TestQueryBaseClass", testBaseClassCode);

        // keep a set a of codegen-ned queries to avoid generating the same code for different options
        // TODO: think if this list should be of CommonCompilerOptions
        HashMap<String, List<QueryCompiler>> generatedQueryOptions = new HashMap<>();

        for (QueryCompiler compiler : compilers) {
            // generate the implementations
            ArrayList<Map.Entry<String, PlannedQuery>> entries = new ArrayList<>(queries.entrySet());
            entries.sort(Map.Entry.comparingByKey());

            String variantName = compiler.getVariantName();
            String queryPackageName = this.queryPackageName + "." + variantName;
            String benchPackageName = this.benchPackageName + "." + variantName;
            String queryFolderName = this.queryFolderName + "/" + variantName;
            String testFolderName = this.testFolderName + "/" + variantName;
            String benchFolderName = this.benchFolderName + "/" + variantName;
            String[] imports = new String[]{dbPackageName + ".*", queryPackageName + ".*"};

            CommonCompilerOption options = compiler.getOption();


            for (Map.Entry<String, PlannedQuery> entry : entries) {
                String qName = entry.getKey();
                String query = entry.getValue().query();
                S2SPlan plan = entry.getValue().plan();
                if(DEBUG_ENABLED) {
                    System.out.println("Generating implementation for " + qName + " with compiler: " + compiler.getVariantName());
                }
                try {
                    GeneratedClassHolder generatedSqlStreamClass = compiler.compile(plan, "exec", "DB db");

                    String className = "Query_" + qName;
                    String genCode = generatedSqlStreamClass.asClass(className, queryPackageName, imports);
                    String packageFreeCode = genCode.substring(genCode.indexOf("public class")+1);
                    List<QueryCompiler> generatedOptionsForQuery = generatedQueryOptions.get(packageFreeCode);
                    if(generatedOptionsForQuery != null) {
                        // TODO code generation should be skipped and the list of equivalent compiler should be stored
                        generatedOptionsForQuery.add(compiler);
                    } else {
                        generatedOptionsForQuery = new LinkedList<>();
                        generatedOptionsForQuery.add(compiler);
                        generatedQueryOptions.put(packageFreeCode, generatedOptionsForQuery);
                    }

                    genClass(queryFolderName, className, genCode);

                    String outputRowClassName = "";
                    Schema outputSchema = generatedSqlStreamClass.getOutputSchema();
                    // if not unwrapped or if the output class name is a return value not a long, double, etc
                    if (!options.getSingleFieldTuplesConverter().isUnwrappedSchema(outputSchema) &&
                            !schemaMap.containsKey(generatedSqlStreamClass.getOutputClassName())){
                            // TODO get rid of this second check in case we will always produce a Result type
                            // this check is there because at the moment we may return a list of elements of the same type as an input table
                        outputRowClassName = className + ".";
                    }
                    outputRowClassName += generatedSqlStreamClass.getOutputClassName();
                    String outputRowCollectionClass = "List<" + outputRowClassName + ">";

                    // generate tests
                    String testClassName = "Test" + className;
                    String rowAssertions = Arrays.stream(outputSchema.getFields())
                            .map(field -> {
                                String fieldName = "row";
                                if (!options.getSingleFieldTuplesConverter().isUnwrappedSchema(outputSchema)) {
                                    fieldName += "." + field.getName() + (field.isGetter() ? "()" : "");
                                }
                                String rsField = "rs." + getResultSetMethodForField(field) + "(\"" + field.getName() + "\")";
                                Class<?> fieldType = TypingUtils.boxed(field.getType());
                                if (fieldType == Date.class) {
                                    fieldName += ".toString()";
                                    rsField += ".toString()";
                                }
                                if (fieldType == Double.class || fieldType == Float.class) {
                                    return "assertSimilarDoubles(%s, %s);".formatted(rsField, fieldName);
                                }
                                return "assertEquals(%s, %s);".formatted(rsField, fieldName);
                            })
                            .collect(Collectors.joining("\n\t"));
                    String streamGetter = """
                            %s executor = new %s();
                            %s result = executor.exec(DB.create());
                            """.formatted(className, className, outputRowCollectionClass);

                    String testClassMethodCode = """
                            // Execute the query on the given connection
                            String query = "%s";
                            // Execute the query using the with the generated class
                            try(Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
                                %s
                                for(%s row : result) {
                                    assertTrue(rs.next(), "Result sets have different size (S2S result is larger)");
                                    %s
                                }
                                assertFalse(rs.next(), "Result sets have different size (S2S result one is smaller)");
                            }
                            """.formatted(
                            escapeForJava(query),
                            streamGetter,
                            outputRowClassName,
                            rowAssertions);

                testClassMethodCode = testClassMethodCode.replace("\n", "\n\t\t");

                String qualifiedQueryClsName = queryPackageName + "." + className;
                String testClassCode =
                        """
						package %s;
						
						import %s.*;
						import static %s.*;
						
						import static org.junit.jupiter.api.Assertions.*;
						import org.junit.jupiter.api.*;
						import java.sql.*;
						import java.util.*;
		
						public class %s extends %s.TestQueryBaseClass {
							@Test
							@Timeout(60) // 1 minute
							public void testQuery() throws Exception {
								%s
							}
						}
						""".formatted(
                                queryPackageName,
                                dbPackageName,
                                qualifiedQueryClsName,
                                testClassName,
                                this.queryPackageName,
                                testClassMethodCode);
                genClass(testFolderName, testClassName, testClassCode);

                    // generate benchmarks
                    String benchClassName = "Bench" + className;
                    String importStr = Arrays.stream(imports)
                            .map("import %s;"::formatted)
                            .collect(Collectors.joining("\n"));
                    String benchClassCode = """
                            package %s;
                            
                            %s
                            import org.openjdk.jmh.annotations.*;

                            import java.util.List;

                            @State(Scope.Thread)
                            public class %s {
                                DB db = DB.create();
                                %s executor = new %s();

                                @Benchmark
                                public %s run() {
                                    return executor.exec(db);
                                }
                            }
                            """.formatted(benchPackageName, importStr, benchClassName, className, className, outputRowCollectionClass);

                    genClass(benchFolderName, benchClassName, benchClassCode);
                } catch (PlanningException e) {
                    System.out.println("PlanningException converting a Calcite plan into an S2S plan:\n" + qName);
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    System.out.printf("Exception (%s): %s%n", qName, e.getMessage());
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private Map<String, Schema> generateSchema(Connection connection,
                                               List<String> tableNames) throws SQLException, IOException {
        // get schemas from the database and add tables to Calcite planner

        for (String t : tableNames) {
            Schema schema = asSchema(connection, t);
            schemaMap.put(t, schema);
            if(planner != null) {
                planner.addTable(t, schema);
            }
        }

        // generate class files for the tables
        for (Map.Entry<String, Schema> entry : schemaMap.entrySet()) {
            String clsName = entry.getKey();
            Schema schema = entry.getValue();

            // record class code
            String[] resultSetToParams = new String[schema.getFields().length];
            for (int i = 0; i < resultSetToParams.length; i++) {
                Field f = schema.getFields()[i];
                String method = getResultSetMethodForField(f);
                resultSetToParams[i] = "rs.%s(%s)".formatted(method, i + 1);
                if (f.getType() == Date.class) {
                    resultSetToParams[i] = "new %s((int)LocalDate.parse(rs.getObject(%s).toString()).toEpochDay())".formatted(
                            Date.class.getCanonicalName(), i + 1);
                }
            }
            String createRecordArray = """
                    // create from database connection
                    static %s[] create(Connection con) throws SQLException {
                        ResultSet rs;
                        Statement stm = con.createStatement();
                        rs = stm.executeQuery("select count(*) from %s");
                        rs.next();
                        int ctn = rs.getInt(1);
                        %s[] data = new %s[ctn];
                        rs = stm.executeQuery("select * from %s");
                        int i=0;
                        while(rs.next()) {
                            data[i++] = new %s(%s);
                        }
                        rs.close();
                        stm.close();
                        return data;
                    }
                    """.formatted(
                    clsName, clsName, clsName, clsName, clsName, clsName,
                    String.join(",\n\t\t\t\t", resultSetToParams));

            String classFieldsDeclaration = Stream.of(schema.getFields())
                    .map(f -> "public final %s %s;".formatted(f.getType().getCanonicalName(), f.getName()))
                    .collect(Collectors.joining("\n\t"));
            String classFieldsParams = Stream.of(schema.getFields())
                    .map(f -> "%s %s".formatted(f.getType().getCanonicalName(), f.getName()))
                    .collect(Collectors.joining(", "));
            String classFieldsInitialization = Stream.of(schema.getFields())
                    .map(f -> "this.%s = %s;".formatted(f.getName(), f.getName()))
                    .collect(Collectors.joining("\n\t\t"));

            String recordDef = """
                    package %s;

                    import java.sql.*;
                    import java.time.LocalDate;

                    public final class %s {
                        // fields
                    %s

                        public %s(%s) {
                        %s
                        }

                        %s
                    }
                    """.formatted(
                    dbPackageName,
                    clsName,
                    classFieldsDeclaration,
                    clsName,
                    classFieldsParams,
                    classFieldsInitialization,
                    createRecordArray.replace("\n", "\n\t"));

            genClass(dbFolderName, clsName, recordDef);
        }

        // generate a class file for the in-memory db, i.e., an array for each generated table
        String tableDeclarations = tableNames.stream()
                .map(t -> "public final %s[] %s;".formatted(t, t))
                .collect(Collectors.joining("\n\t"));
        String tableParams = tableNames.stream()
                .map(t -> "%s[] %s".formatted(t, t))
                .collect(Collectors.joining(", "));
        String tableInitializations = tableNames.stream()
                .map(t -> "this.%s = %s;".formatted(t, t))
                .collect(Collectors.joining("\n\t\t"));
        String tableCreations = tableNames.stream()
                .map(t -> "%s.%s.create(con)".formatted(dbPackageName, t))
                .collect(Collectors.joining(",\n\t\t\t\t\t"));
        String interfaceMethodsDefinitions = tableNames.stream()
                .map(t -> """
                        public abstract Stream<%s> %sStream();
                        public abstract %s[] %s_arr();
                        """.formatted(t, t, t, t))
                .collect(Collectors.joining("\n\t"));
        String dbGetters = tableNames.stream()
                .map(t -> """
                        @Override
                        public Stream<%s> %sStream() {
                            return Arrays.stream(%s);
                        }
                        
                        @Override
                        public %s[] %s_arr() {
                            return %s;
                        }
                        """.formatted(t, t, t, t, t, t))
                .collect(Collectors.joining("\n\t"));


        String dbConnector;
        if(connectionUrl.contains("duckdb")) {
            dbConnector = "org.duckdb.DuckDBDriver";
        } else if (connectionUrl.contains("sqlite")) {
            dbConnector = "org.sqlite.JDBC";
        } else {
            throw new RuntimeException("Unknown DB connector: " + connectionUrl);
        }
        String dbClassCode = """
                package %s;
                import java.sql.*;
                import java.util.Arrays;
                import java.util.stream.Stream;
                import java.util.Properties;
                
                
                public abstract class DB {
                
                    private static String JDBC_URL = System.getenv().getOrDefault("JDBC_URL", "%s");
                    private static DBImpl INSTANCE;
                
                    public static DB create() {
                        try {
                            if(INSTANCE == null) {
                                Class.forName("%s");
                                Properties readOnlyProperty = new Properties();
                                readOnlyProperty.setProperty("duckdb.read_only", "true");
                                try(Connection con = DriverManager.getConnection(JDBC_URL, readOnlyProperty)) {
                                    INSTANCE = new DBImpl(
                                        %s);
                                }
                            }
                            return INSTANCE;
                        } catch(Exception e) {
                            throw new RuntimeException(e);
                        }
                    }

                    // abstract methods
                    %s

                    static final class DBImpl extends DB {
                        // tables
                        %s

                        public DBImpl(%s) {
                            %s
                        }


                        // getters for streams
                        %s
                    }
                }
                """.formatted(
                dbPackageName,
                connectionUrl,
                dbConnector,
                tableCreations,
                interfaceMethodsDefinitions,
                tableDeclarations,
                tableParams,
                tableInitializations,
                dbGetters);

        genClass(dbFolderName, "DB", dbClassCode);

        isSchemaGenerated = true;

        return schemaMap;
    }

    private static void genClass(String folder, String name, String code) throws IOException {
        new File(folder).mkdirs();
        name = Path.of(folder, name).toFile().getAbsolutePath();
        FileWriter writer = new FileWriter(name + ".java");
        writer.write(code);
        writer.close();
    }

    private static Schema asSchema(Connection connection, String tableName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getColumns(null, null, tableName, "%");
        List<Field> fieldList = new ArrayList<>();
        while (rs.next()) {
            String fieldName = rs.getString("COLUMN_NAME");
            Class<?> fieldClass = switch (rs.getString("TYPE_NAME")) {
                case "INTEGER" -> int.class;
                case "DECIMAL", "DOUBLE PRECISION", "DOUBLE" -> double.class;
                case "BIGINT" -> long.class;
                case "DATE" -> Date.class;
                case "TEXT", "CHAR", "VARCHAR" -> String.class;
                default -> throw new IllegalStateException("Unexpected SQL field type: " +
                        rs.getString("TYPE_NAME"));
            };
            boolean nullable = !"NO".equals(rs.getString("IS_NULLABLE"));
            if (nullable)
                fieldClass = TypingUtils.boxed(fieldClass);
            fieldList.add(new Field(fieldName, fieldClass, nullable));
        }
        return Schema.byFields(fieldList.toArray(Field[]::new));
    }

    private static String getResultSetMethodForField(Field field) {
        return switch (field.getType().getSimpleName()) {
            case "int",     "Integer"   -> "getInt";
            case "double",  "Double"    -> "getDouble";
            case "long",    "Long"      -> "getLong";
            case "Date"                 -> "getDate";
            case "String"               -> "getString";

            default -> throw new IllegalStateException("Unexpected type: " + field.getType());
        };
    }

    public static String escapeForJava(String input) {
        return input
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }
}
