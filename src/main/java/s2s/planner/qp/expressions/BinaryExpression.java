package s2s.planner.qp.expressions;

public abstract class BinaryExpression implements Expression {
    final Expression left, right;
    final Class<?> type;

    public BinaryExpression(Expression left, Expression right, Class<?> type) {
        this.left = left;
        this.right = right;
        this.type = type;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }

    @Override
    final public Class<?> type() {
        return type;
    }

}
