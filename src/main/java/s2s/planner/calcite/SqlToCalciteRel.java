package s2s.planner.calcite;


import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;

import java.util.*;

import static org.apache.calcite.tools.Programs.sequence;

public final class SqlToCalciteRel {

    private final SchemaPlus defaultSchema;

    static public final boolean DEBUG_ENABLED = "true".equals(
            System.getenv().getOrDefault("DEBUG_ENABLED", "false"));

    private final VolcanoPlanner planner;

    public SqlToCalciteRel(SchemaPlus schemaPlus) {
        this.defaultSchema = schemaPlus;
        planner = getPlanner();
    }
    public static RelNode expandSearch(RelNode relNode) {
        RexBuilder builder = new RexBuilder(new JavaTypeFactoryImpl());
        return relNode.accept(new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                if(call.getOperator() == SqlStdOperatorTable.SEARCH) {
                    return RexUtil.expandSearch(builder, null, call);
                } else if(call.getOperator() == SqlStdOperatorTable.AND) {
                    List<RexNode> children = new LinkedList<>();
                    for(RexNode child : call.operands) {
                        child = child.accept(this);
                        if(child instanceof RexCall && ((RexCall) child).getOperator() == SqlStdOperatorTable.AND) {
                            for(RexNode granChild : ((RexCall) child).getOperands()) {
                                children.add(granChild.accept(this));
                            }
                        } else {
                            children.add(child);
                        }
                    }
                    return builder.makeCall(SqlStdOperatorTable.AND, children);
                } else {
                    return super.visitCall(call);
                }
            }
        });
    }

    public RelNode convert(String sql) throws SqlParseException {

        RelRoot relRoot = parseSql(sql, planner);

        List<RelOptMaterialization> materializationList = new ArrayList<>();
        List<RelOptLattice> latticeList = new ArrayList<>();
        RelTraitSet desiredTraits = getDesiredRootTraitSet(relRoot);


        // TODO check, maybe relRoot.rel; ?
        RelNode rel = relRoot.project();

        Program program = getProgram();
        RelNode optimized = program.run(planner, rel, desiredTraits, materializationList, latticeList);

        optimized.childrenAccept(new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                node = expandSearch(node);
                parent.replaceInput(ordinal, node);
                super.visit(node, ordinal, parent);
            }
        });
        optimized = expandSearch(optimized);

        if(DEBUG_ENABLED) {
            System.out.println("With Calcite Planner");
            printPlan(optimized);
        }

        return optimized;
    }


    public VolcanoPlanner getPlanner() {
        VolcanoPlanner planner = new VolcanoPlanner();
        RelOptUtil.registerDefaultRules(planner, true, false);
        planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);

        // Enable Semi/Anti Joins
        planner.addRule(CoreRules.JOIN_TO_SEMI_JOIN);
        planner.addRule(CoreRules.PROJECT_TO_SEMI_JOIN);
        planner.addRule(AntiJoinRule.INSTANCE);

        // Use custom rule for aggregations
        planner.removeRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        planner.removeRule(CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
        planner.addRule(MyEnumerableAggregateRule.INSTANCE);
        planner.addRule(MyAggregateCaseToFilterRule.INSTANCE);

        // No CALC rules
        RelOptRules.CALC_RULES.forEach(planner::removeRule);

        // TODO check this one
        planner.removeRule(CoreRules.SORT_PROJECT_TRANSPOSE);

        // avoid join reordering
        planner.removeRule(CoreRules.JOIN_COMMUTE);
        planner.removeRule(JoinPushThroughJoinRule.LEFT);
        planner.removeRule(JoinPushThroughJoinRule.RIGHT);

        // but always put scalar subqueries on the left side
        planner.addRule(SwapNestedJoinRightAggregation.INSTANCE);

        planner.setExecutor(new RexExecutorImpl(DataContexts.EMPTY));

        return planner;
    }

    private Program getProgram() {
        DefaultRelMetadataProvider metadataProvider = DefaultRelMetadataProvider.INSTANCE;
        Program subQuery = Programs.subQuery(metadataProvider);
        Program decorrelate = new DecorrelateProgram();
        Program trimFields = new TrimFieldsProgram();
        Program rulesProgram = Programs.ofRules(planner.getRules());
		return sequence(subQuery, decorrelate, trimFields, rulesProgram);
    }

    private RelRoot parseSql(String sql, VolcanoPlanner planner) throws SqlParseException {
        // SQL Parser
        SqlParser parser = SqlParser.create(sql, SqlParser.config().withLex(Lex.JAVA));
        SqlNode parsed = parser.parseQuery();

        // Calcite Catalog reader
        SqlTypeFactoryImpl factory = getSqlTypeFactory();

        // SQL Validator
        SqlValidator validator = getSqlValidator();
        SqlNode validated = validator.validate(parsed);


        // SqlToRelConverter -- SqlNode -> RelNode
        SqlToRelConverter.Config config = getSqlToRelConverterConfig();
        FrameworkConfig frameworkConfig = getFrameworkConfig();

        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        RexBuilder rexBuilder = new RexBuilder(factory);
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        RelOptTable.ViewExpander expander = (rowType, queryString, schemaPath, viewPath) -> null;
        CalciteCatalogReader calciteCatalogReader = getCalciteCatalogReader(factory);
        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
                expander, validator, calciteCatalogReader, cluster, frameworkConfig.getConvertletTable(), config);
        RelRoot root = sqlToRelConverter.convertQuery(validated, false, true);

        return root.withRel(sqlToRelConverter.decorrelate(validated, root.rel));
    }

    public SqlValidator getSqlValidator() {
        SqlTypeFactoryImpl factory = getSqlTypeFactory();
        CalciteCatalogReader catalogReader = getCalciteCatalogReader(factory);
        SqlStdOperatorTable sqlStdOperatorTable = SqlStdOperatorTable.instance();
        return SqlValidatorUtil.newValidator(sqlStdOperatorTable, catalogReader, factory, SqlValidator.Config.DEFAULT);
    }

    public FrameworkConfig getFrameworkConfig() {
        return Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(defaultSchema)
                .build();
    }

    public SqlToRelConverter.Config getSqlToRelConverterConfig() {
        return SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);
    }

    public SqlTypeFactoryImpl getSqlTypeFactory() {
        return new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    }


    public CalciteCatalogReader getCalciteCatalogReader(SqlTypeFactoryImpl factory) {
        Properties properties = new Properties();
        properties.setProperty("caseSensitive", "false");
        SchemaPlus defaultSchema = this.defaultSchema;
        return new CalciteCatalogReader(
                CalciteSchema.from(defaultSchema),
                CalciteSchema.from(defaultSchema).path(null),
                factory,
                new CalciteConnectionConfigImpl(properties));
    }

    RelTraitSet getDesiredRootTraitSet(RelRoot root) {
        return root.rel.getTraitSet()
                .replace(EnumerableConvention.INSTANCE)
                .replace(root.collation)
                .simplify();
    }

    void printPlan(RelNode plan) {
        String str = RelOptUtil.toString(plan);
        System.out.println("plan>");
        System.out.println(str);
    }


    private static class TrimFieldsProgram implements Program {
        public RelNode run(RelOptPlanner planner, RelNode rel,
                           RelTraitSet requiredOutputTraits,
                           List<RelOptMaterialization> materializations,
                           List<RelOptLattice> lattices) {
            final RelBuilder relBuilder =
                    RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
            return new RelFieldTrimmer(null, relBuilder).trim(rel);
        }
    }

    private static class DecorrelateProgram implements Program {
        public RelNode run(RelOptPlanner planner, RelNode rel,
                           RelTraitSet requiredOutputTraits,
                           List<RelOptMaterialization> materializations,
                           List<RelOptLattice> lattices) {
            RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
            return RelDecorrelator.decorrelateQuery(rel, relBuilder);
        }
    }

}

