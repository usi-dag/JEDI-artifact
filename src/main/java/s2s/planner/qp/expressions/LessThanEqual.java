package s2s.planner.qp.expressions;

public class LessThanEqual extends BinaryExpression {

    public LessThanEqual(Expression left, Expression right) {
        super(left, right, boolean.class);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

}
