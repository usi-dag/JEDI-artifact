package s2s.planner.qp.expressions;

import java.util.Objects;

public class Const implements Expression {

    final Class<?> type;
    final Object value;

    public Const(Object value) {
        this(value != null ? value.getClass() : Object.class, value);
    }

    private Const(Class<?> type, Object value) {
        this.value = value;
        this.type = maybePrimitive(type);
    }

    private static Class<?> maybePrimitive(Class<?> c) {
        if(c == Byte.class || c == Short.class || c == Integer.class)
            return int.class;
        if(c == Float.class || c == Double.class)
            return double.class;
        if(c == Long.class) return long.class;
        return c;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Class<?> type() {
        return type;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Const aConst = (Const) o;
        return value.equals(aConst.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
