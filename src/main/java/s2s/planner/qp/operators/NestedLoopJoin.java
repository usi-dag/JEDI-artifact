package s2s.planner.qp.operators;

import s2s.planner.qp.Field;
import s2s.planner.qp.Schema;
import s2s.planner.qp.expressions.Expression;
import s2s.planner.qp.expressions.InputRef;

import java.util.Arrays;

public record NestedLoopJoin(
        Operator left,
        Operator right,
        Expression[] mapper,
        Expression nonEquiCondition,
        boolean isLeftProjectMapper,
        boolean isRightProjectMapper,
        JoinType joinType,
        Schema outputSchema
) implements Join {

    public static NestedLoopJoin createInner(Operator left,
                                             Operator right,
                                             Expression[] mapper,
                                             Schema schema) {
        return new NestedLoopJoin(left, right, mapper, null, false, false, JoinType.INNER, schema);
    }

    public static NestedLoopJoin createInner(Operator left,
                                             Operator right,
                                             InputRef[] mapper) {
        return createInner(left, right, null, mapper);
    }
    public static NestedLoopJoin createInner(Operator left,
                                             Operator right,
                                             Expression nonEquiCondition,
                                             InputRef[] mapper) {
        return new NestedLoopJoin(
                left, right,
                mapper,
                nonEquiCondition, false, false,
                JoinType.INNER,
                Schema.byFields(Arrays.stream(mapper).map(InputRef::field).toArray(Field[]::new)));
    }

    public static NestedLoopJoin createWithRightProjectMapper(Operator left,
                                                              Operator right) {
        return new NestedLoopJoin(left, right, null, null, false, true, JoinType.INNER, right.getSchema());
    }
    public static NestedLoopJoin createWithRightProjectMapper(Operator left,
                                                              Operator right,
                                                              Expression nonEquiCondition) {
        return new NestedLoopJoin(left, right, null, nonEquiCondition, false, true, JoinType.INNER, right.getSchema());
    }

    public static NestedLoopJoin createWithLeftProjectMapper(Operator left,
                                                             Operator right) {
        return new NestedLoopJoin(left, right, null, null, true, false, JoinType.INNER, left.getSchema());
    }
    public static NestedLoopJoin createWithLeftProjectMapper(Operator left,
                                                             Operator right,
                                                             Expression nonEquiCondition) {
        return new NestedLoopJoin(left, right, null, nonEquiCondition, true, false, JoinType.INNER, left.getSchema());
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public Schema getSchema() {
        return outputSchema;
    }


}
