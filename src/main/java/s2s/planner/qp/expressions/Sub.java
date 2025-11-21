package s2s.planner.qp.expressions;

import s2s.planner.qp.PlanningException;

public class Sub extends BinaryExpression {
    public Sub(Expression left, Expression right) throws PlanningException {
        super(left, right, Add.deduce(left.type(), right.type()));
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

}
