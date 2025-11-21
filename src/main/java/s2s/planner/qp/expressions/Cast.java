package s2s.planner.qp.expressions;


public record Cast(Class<?> type, Expression expression) implements Expression {

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
