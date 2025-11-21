package s2s.planner.qp.operators;

import s2s.planner.qp.Field;
import s2s.planner.qp.Schema;
import s2s.planner.qp.expressions.Expression;

public class Project extends SingleChildOperator {

    final Expression[] projections;
    final Schema schema;

    public Project(Expression[] projections, Operator child, String[] projectionNames) {
        super(child);
        this.projections = projections;
        Field[] fields = new Field[projections.length];
        for (int i = 0; i < fields.length; i++) {
            Expression expr = projections[i];
            fields[i] = new Field(projectionNames[i], expr.type(), expr.isNullable(), true);
        }
        this.schema = Schema.byFields(fields);
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        visitor.visit(this);
    }

    public Expression[] getProjections() {
        return projections;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
