package s2s.planner.qp.expressions;

public class IsNull extends BooleanExpression {

    final Expression expression;

    public IsNull(Expression expression) {
        this.expression = expression;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public Expression getExpression() {
        return expression;
    }
}
