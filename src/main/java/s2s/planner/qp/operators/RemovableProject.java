package s2s.planner.qp.operators;

import s2s.planner.qp.Schema;

public class RemovableProject extends SingleChildOperator {
    final Schema schema;


    public RemovableProject(Operator child, Schema schema) {
        super(child);
        this.schema = schema;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        visitor.visit(this);
    }

}
