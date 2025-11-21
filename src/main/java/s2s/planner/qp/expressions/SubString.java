package s2s.planner.qp.expressions;


public record SubString(int start, int len, Expression stringGetter) implements Expression {


    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Class<?> type() {
        return String.class;
    }
}
