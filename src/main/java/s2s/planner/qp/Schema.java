package s2s.planner.qp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Schema {

    public static Schema reflective(Class<?> clazz) {
        ArrayList<Field> fields = new ArrayList<>();
        if(clazz.isRecord()) {
            for(java.lang.reflect.Method method : clazz.getDeclaredMethods()) {
                if(method.getParameterCount() == 0) {
                    // a getter
                    fields.add(new Field(
                            method.getName(),
                            method.getReturnType(),
                            !method.getReturnType().isPrimitive(),
                            true));
                }
            }
        } else {
            for(java.lang.reflect.Field field : clazz.getDeclaredFields()) {
                fields.add(new Field(
                        field.getName(),
                        field.getType(),
                        !field.getType().isPrimitive(),
                        false));
            }
        }
        return new Schema(fields.toArray(new Field[0]));
    }

    public static Schema byFields(Field... fields) {
        HashMap<String, Field> hm = new HashMap<>();
        Schema schema = new Schema(hm, fields);
        for (Field field : fields) {
            if(schema.hm.containsKey(field.name)) {
                throw new RuntimeException("duplicate name in schema fields: " + field.name);
            }
            schema.hm.put(field.name, field);
        }
        return schema;
    }

    public static Schema byFieldsForWrapperSchema(Schema original, Field... fields) {
        HashMap<String, Field> hm = new HashMap<>();
        Schema schema = new Schema(hm, fields, original);
        for (Field field : fields) {
            if(schema.hm.containsKey(field.name)) {
                throw new RuntimeException("duplicate name in schema fields: " + field.name);
            }
            schema.hm.put(field.name, field);
        }
        return schema;
    }

    final HashMap<String, Field> hm;
    final Field[] fields;
    final Schema wrapped;

    private Schema(Field[] fields, Schema wrapped) {
        this.fields = fields;
        this.hm = new HashMap<>();
        for (Field field : fields) {
            hm.put(field.getName(), field);
        }
        this.wrapped = wrapped;
    }

    public Schema(Field[] fields) {
        this(fields, null);
    }

    private Schema(Map<String, Field> hm, Field[] fields, Schema wrapped) {
        this.hm = new HashMap<>(hm);
        this.fields = fields;
        this.wrapped = wrapped;
    }
    public Schema(Map<String, Field> hm, Field[] fields) {
        this(hm, fields, null);
    }

    public Field get(String name) {
        Field field = hm.get(name);
        if(field == null) {
            String names = Arrays.stream(fields).map(Field::getName).collect(Collectors.joining(","));
            throw new RuntimeException("Unknown field: " + name + " for schema: " + names + "\n" + this);
        }
        return field;
    }

    public Field[] getFields() {
        return fields;
    }

    public Schema withFieldsAsGetters(boolean canForgetWrappedSchema) {
        Schema wrappedSchema = wrapped;
        if(canForgetWrappedSchema) {
            wrappedSchema = null;
        }
        return Schema.byFieldsForWrapperSchema(wrappedSchema, Arrays.stream(fields).map(Field::asGetter).toArray(Field[]::new));
    }

    public Schema wrappedSchema() {
        return wrapped;
    }

    public boolean hasWrappedSchema() {
        return wrapped != null;
    }

    @Override
    public String toString() {
        return "Schema" + hm;
    }
}
