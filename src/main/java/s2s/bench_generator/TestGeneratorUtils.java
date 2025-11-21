package s2s.bench_generator;


public class TestGeneratorUtils {


	public static final String TEST_BASE_CLASS_TEMPLATE_JDBC = """
                package %s;

                import java.sql.*;
                import java.util.Properties;
                import static org.junit.jupiter.api.Assertions.*;

                public class TestQueryBaseClass {

                    public static final Connection connection;
                    static {
                        Properties readOnlyProperty = new Properties();
                        readOnlyProperty.setProperty("duckdb.read_only", "true");
                        try {
                            connection = DriverManager.getConnection("%s", readOnlyProperty);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    protected void assertSimilarDoubles(Double fst, Double snd) {
                        // null?
                        if(fst == null) assertTrue(snd == null);
                        else if(snd == null) assertTrue(fst == null);

                        // NaN? coerce into zero
                        else if(Double.isNaN(fst)) {
                            if(Double.isNaN(snd)) return;
                            assertEquals(0D, snd, 0.01);
                        } else if(Double.isNaN(snd)) {
                            if(Double.isNaN(fst)) return;
                            assertEquals(0D, fst, 0.01);
                        } else {
                            // Not null nor NaN, just compare
                            assertEquals(fst, snd, 0.01);
                        }
                    }
                }
               """;


	public static final String TEST_BASE_CLASS_TEMPLATE_CALCITE = """
                package %s;

                import %s.*;
                import %s;

                import org.apache.calcite.adapter.java.ReflectiveSchema;
                import org.apache.calcite.jdbc.CalciteConnection;
                import org.apache.calcite.rel.RelNode;
                import org.apache.calcite.rel.RelRoot;
                import org.apache.calcite.schema.SchemaPlus;
                import org.apache.calcite.sql.SqlNode;
                import org.apache.calcite.sql.parser.SqlParser;
                import org.apache.calcite.sql.type.SqlTypeName;
                import org.apache.calcite.tools.*;
                
                import java.sql.PreparedStatement;
                import java.sql.ResultSet;
                import java.util.TimeZone;
                                
                public class TestQueryBaseClass {
                    static final FrameworkConfig frameworkConfig;
                    static {
                        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
                        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
                        ReflectiveSchema schema = new ReflectiveSchema(DB.create());
                        SchemaPlus schemaPlus = rootSchema.add("root", schema);
                        
                        SqlParser.Config insensitiveParser = SqlParser.configBuilder()
                                .setCaseSensitive(false)
                        		.build();
                        frameworkConfig = Frameworks.newConfigBuilder()
                        		.parserConfig(insensitiveParser)
                        		.defaultSchema(schemaPlus)
                        		.build();
                    }
                    
                    public ResultSet run(String query) throws Exception {
                        Planner planner = Frameworks.getPlanner(frameworkConfig);
                        SqlNode sqlNode = planner.parse(query);
                        SqlNode sqlNodeValidated = planner.validate(sqlNode);
                        RelRoot relRoot = planner.rel(sqlNodeValidated);
                        RelNode relNode = relRoot.project();
                        PreparedStatement run = RelRunners.run(relNode);
                        return run.executeQuery();
                    }
                    
                    protected void assertSimilarDoubles(Double fst, Double snd) {
                    	// null?
                    	if(fst == null) assertTrue(snd == null);
                    	else if(snd == null) assertTrue(fst == null);
                    	
                    	// NaN? coerce into zero
                    	else if(Double.isNaN(fst)) {
                    		if(Double.isNaN(snd)) return;
                    		assertEquals(0D, snd, 0.01);
                    	} else if(Double.isNaN(snd)) {
                    		if(Double.isNaN(fst)) return;
                    		assertEquals(0D, fst, 0.01);
                    	} else {
                    		// Not null nor NaN, just compare
                    		assertEquals(fst, snd, 0.01);
                    	}
                    }
                }
                """;

	static String getTestBaseClassTemplateJdbc(String packageName, String jdbcConnector) {
		return TEST_BASE_CLASS_TEMPLATE_JDBC.formatted(packageName, jdbcConnector);
	}
	static String getTestBaseClassTemplateCalcite(String packageName,
												  String dbPackageName,
												  String connectionUtilClassName) {
		return TEST_BASE_CLASS_TEMPLATE_CALCITE.formatted(
				packageName, dbPackageName, connectionUtilClassName);
	}
}
