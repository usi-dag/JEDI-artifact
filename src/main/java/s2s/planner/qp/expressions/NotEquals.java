package s2s.planner.qp.expressions;

public class NotEquals extends BinaryExpression {

    public NotEquals(Expression left, Expression right) {
        super(left, right, boolean.class);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

}
