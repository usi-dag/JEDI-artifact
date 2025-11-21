package s2s.planner.calcite;

import s2s.planner.qp.S2SPlan;
import s2s.planner.qp.Schema;
import s2s.planner.qp.operators.Operator;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.parser.SqlParseException;

public class CalcitePlanner {
    private static final boolean DUMP_CALCITE_PLAN = "true".equals(System.getenv().get("DUMP_CALCITE_PLAN"));
    private static final boolean DUMP_S2S_PLAN = "true".equals(System.getenv().get("DUMP_S2S_PLAN"));
    private final CalciteConnection connection = ConnectionUtil.connection();
    private final SchemaPlus rootSchema = connection.getRootSchema();

    public void addTable(String name, Schema schema) {
        rootSchema.add(name, asTable(schema));
    }

    private static Table asTable(Schema schema) {
        return new SchemaTable(schema);
    }

    public S2SPlan plan(String query) throws SqlParseException {
        SqlToCalciteRel sqlToCalciteRel = new SqlToCalciteRel(rootSchema);
        RelNode relNode = sqlToCalciteRel.convert(query);
        if(DUMP_CALCITE_PLAN) {
            System.out.println("Calcite Plan>");
            System.out.println(RelOptUtil.toString(relNode));
        }
        Operator root = CalciteRelToInternalQP.convert(relNode);
        if(DUMP_S2S_PLAN) {
            System.out.println("S2S Plan>");
            // TODO
        }
        return new S2SPlan(root);
    }
}
