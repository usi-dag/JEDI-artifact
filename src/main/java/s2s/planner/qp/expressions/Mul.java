package s2s.planner.qp.expressions;

import s2s.planner.qp.PlanningException;

public class Mul extends BinaryExpression {
    public Mul(Expression left, Expression right) throws PlanningException {
        super(left, right, Add.deduce(left.type(), right.type()));
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

}
