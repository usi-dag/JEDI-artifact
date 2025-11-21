package s2s.planner.qp.expressions;

public class Or extends BooleanExpression {

    final Expression[] expressions;

    public Or(Expression[] expressions) {
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
