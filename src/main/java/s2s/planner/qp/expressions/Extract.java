package s2s.planner.qp.expressions;

public record Extract(TimeUnit timeUnit,
                      Expression dateGetter) implements Expression {

    public enum TimeUnit {
        YEAR, MONTH
    }


    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Class<?> type() {
        return int.class;
    }
}
