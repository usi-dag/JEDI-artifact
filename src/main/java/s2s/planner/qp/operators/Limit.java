package s2s.planner.qp.operators;

public class Limit extends SingleChildOperator {

    final int limit;

    public Limit(int limit, Operator child) {
        super(child);
        this.limit = limit;
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        visitor.visit(this);
    }

    public int getLimit() {
        return limit;
    }
}
