package s2s.planner.qp.operators;

import s2s.planner.qp.Schema;

public interface Operator {

    void accept(OperatorVisitor visitor);

    Schema getSchema();
}
