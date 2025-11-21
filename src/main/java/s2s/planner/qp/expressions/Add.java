package s2s.planner.qp.expressions;

import s2s.planner.qp.PlanningException;
import s2s.query_compiler.TypingUtils;

public class Add extends BinaryExpression {
    private static final Class<?>[] primitives = new Class[] {
        int.class,
        long.class,
        double.class
    };

    public Add(Expression left, Expression right) throws PlanningException {
        super(left, right, deduce(left.type(), right.type()));
    }

    static Class<?> deduce(Class<?> left, Class<?> right) throws PlanningException {
        if(left.equals(right)) {
            return left;
        }
        if(left.isPrimitive() && right.isPrimitive()) {
            int leftOrder = primitiveOrderIdx(left), rightOrder = primitiveOrderIdx(right);
            return primitives[Math.max(leftOrder, rightOrder)];
        }
        if(left.isPrimitive() || right.isPrimitive()) {
            Class<?> onlyPrimitive = left.isPrimitive() ? left : right;
            Class<?> other = left.isPrimitive() ? right : left;
            if(TypingUtils.boxed(onlyPrimitive) == other) {
                // this is a valid expression among a primitive and
                // a (nullable?) boxed value
                return other;
            }
        }
        throw new PlanningException("Unexpected types (add): " + left + ", " + right);
    }

    private static int primitiveOrderIdx(Class<?> c) {
        switch (c.getName()) {
            case "int": return 0;
            case "long": return 1;
            case "double": return 2;
        }
        throw new IllegalArgumentException("not a supported primitive class: " + c);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

}
