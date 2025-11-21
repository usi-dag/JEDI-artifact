package s2s.planner.qp.operators;

public class Skip extends SingleChildOperator {

    final int skip;

    public Skip(int skip, Operator child) {
        super(child);
        this.skip = skip;
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        visitor.visit(this);
    }

    public int getSkip() {
        return skip;
    }
}
