package s2s.query_compiler.options;

import s2s.planner.qp.Field;
import s2s.planner.qp.Schema;

public enum SingleFieldTuplesConverter {
    // keep tuples with single field as a tuple
    KEEP_TUPLE,

    // extract the field type (with boxing)
    TO_OBJECT,

    // extract the field type, if it is primitive and not nullable it gets unboxed to primitive stream
    TO_PRIMITIVE;

    public boolean isUnwrapped() {
        return this != KEEP_TUPLE;
    }

    public boolean isUnwrappedSchema(Schema schema) {
        return isUnwrapped() && schema.getFields().length == 1 && !schema.hasWrappedSchema();
    }

    public boolean isPrimitiveUnwrappedSchema(Schema schema) {
        return this == TO_PRIMITIVE && isUnwrappedSchema(schema);
    }

    public boolean isPrimitiveFieldInUnwrappedSchema(Schema schema, Field field) {
        return isPrimitiveUnwrappedSchema(schema) && !field.isNullable();
    }
}
