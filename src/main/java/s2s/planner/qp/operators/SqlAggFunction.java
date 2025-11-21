package s2s.planner.qp.operators;

import s2s.planner.qp.expressions.Expression;

import java.util.Optional;

public class SqlAggFunction {

    final Expression expression;
    final Expression filterExpression;
    final AggKind kind;

    public SqlAggFunction(Expression expression, Expression filterExpression, AggKind kind) {
        this.expression = expression;
        this.filterExpression = filterExpression;
        this.kind = kind;
    }

    public SqlAggFunction(Expression expression, AggKind kind) {
        this.expression = expression;
        this.filterExpression = null;
        this.kind = kind;
    }

    public static SqlAggFunction countStar() {
        return new SqlAggFunction(null, AggKind.COUNT);
    }

    public static SqlAggFunction sum(Expression expr) {
        return new SqlAggFunction(expr, AggKind.SUM);
    }
    public static SqlAggFunction min(Expression expr) {
        return new SqlAggFunction(expr, AggKind.MIN);
    }
    public static SqlAggFunction max(Expression expr) {
        return new SqlAggFunction(expr, AggKind.MAX);
    }
    public static SqlAggFunction avg(Expression expr) {
        return new SqlAggFunction(expr, AggKind.AVG);
    }

    public enum AggKind {
        COUNT,
        SUM,
        MIN,
        MAX,
        AVG;

        public boolean isAdditive() {
            return this != MIN && this != MAX;
        }
    }


    public Class<?> getType() {
        if(kind == AggKind.COUNT) return long.class;
        if(expression != null) return expression.type();
        throw new IllegalStateException("Unable to infer type for aggregation function: " + this);
    }

    public boolean isCountStar() {
        return expression == null && kind == AggKind.COUNT;
    }

    public Expression getExpression() {
        return expression;
    }

    public Optional<Expression> getFilterExpression() {
        return filterExpression != null ? Optional.of(filterExpression) : Optional.empty();
    }

    public AggKind getKind() {
        return kind;
    }

    @Override
    public String toString() {
        return "SqlAggFunction{expression=" + expression +", kind=" + kind + '}';
    }
}
