package s2s.planner.qp.expressions;

public record Case(Expression condition, Expression ifTrue, Expression ifFalse, Class<?> type) implements Expression {


    private static Class<?> ensureSameType(Expression ifTrue, Expression ifFalse) {
        if(!ifTrue.type().equals(ifFalse.type())) {
            throw new RuntimeException("Expected same type in Case - Got: "
                    + ifTrue.type() + ", " + ifFalse.type());
        }
        return ifTrue.type();
    }
    public Case(Expression condition, Expression ifTrue, Expression ifFalse) {
        this(condition, ifTrue, ifFalse, ensureSameType(ifTrue, ifFalse));
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Class<?> type() {
        return type;
    }
}
