package s2s.planner.qp.expressions;

import s2s.planner.qp.Field;

public record InputRef(int index, Field field) implements Expression {

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Class<?> type() {
        return field.getType();
    }

    public String getIdentifier() {
        return field.getName();
    }
}
