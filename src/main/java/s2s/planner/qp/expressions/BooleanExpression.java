package s2s.planner.qp.expressions;

public abstract class BooleanExpression implements Expression {

    @Override
    final public Class<?> type() {
        return boolean.class;
    }

}
