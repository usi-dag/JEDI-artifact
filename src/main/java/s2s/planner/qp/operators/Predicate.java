package s2s.planner.qp.operators;

import s2s.planner.qp.expressions.Expression;

public class Predicate extends SingleChildOperator {

    final Expression expression;

    public Predicate(Expression expression, Operator child) {
        super(child);
        this.expression = expression;
        if(expression.type() != boolean.class) {
            throw new IllegalArgumentException("Predicate expression must be boolean");
        }
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        visitor.visit(this);
    }

    public Expression getExpression() {
        return expression;
    }
}
