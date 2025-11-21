package s2s.planner.qp.operators;

import s2s.planner.qp.Schema;
import s2s.planner.qp.sources.ArrayDataSource;

public record ArrayTableScan(ArrayDataSource source) implements Operator {

    @Override
    public void accept(OperatorVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public Schema getSchema() {
        return source.schema();
    }
}
