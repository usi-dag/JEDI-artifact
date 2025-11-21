package s2s.planner.qp.expressions;

public class And extends BooleanExpression {

    final Expression[] expressions;

    public And(Expression... expressions) {
        this.expressions = expressions;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public Expression[] getExpressions() {
        return expressions;
    }
}
