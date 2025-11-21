package s2s.planner.qp.operators;

public class OffsetFetch extends SingleChildOperator {

    final int offset;
    final int fetch;

    public OffsetFetch(int offset, int fetch, Operator child) {
        super(child);
        this.offset = offset;
        this.fetch = fetch;
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        visitor.visit(this);
    }

    public int getFetch() {
        return fetch;
    }

    public int getOffset() {
        return offset;
    }

}
