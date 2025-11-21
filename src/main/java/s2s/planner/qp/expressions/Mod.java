package s2s.planner.qp.expressions;

import s2s.planner.qp.PlanningException;

public class Mod extends BinaryExpression {
    public Mod(Expression left, Expression right) throws PlanningException {
        super(left, right, int.class);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

}