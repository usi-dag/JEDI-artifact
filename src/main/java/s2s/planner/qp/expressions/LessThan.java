package s2s.planner.qp.expressions;

public class LessThan extends BinaryExpression {

    public LessThan(Expression left, Expression right) {
        super(left, right, boolean.class);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

}
