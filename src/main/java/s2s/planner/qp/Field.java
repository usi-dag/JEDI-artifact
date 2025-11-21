package s2s.planner.qp;

public class Field {

    final String name;
    final Class<?> type;
    final boolean nullable;
    final boolean isGetter;

    public Field(String name, Class<?> type) {
        this(name, type, false, false);
    }

    public Field(String name, Class<?> type, boolean nullable) {
        this(name, type, nullable, false);
    }

    public Field(String name, Class<?> type, boolean nullable, boolean isGetter) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.isGetter = isGetter;
    }

    public static Field renamedWrapper(String name, Field wrapped) {
        return new Field(name, wrapped.type, wrapped.nullable, wrapped.isGetter) {
            @Override
            public Class<?> getType() {
                return wrapped.getType();
            }

            @Override
            public boolean isNullable() {
                return wrapped.isNullable();
            }

            @Override
            public boolean isGetter() {
                return wrapped.isGetter();
            }
        };
    }

    public String getName() {
        return name;
    }

    public Class<?> getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isGetter() {
        return isGetter;
    }

    public Field rename(String newName) {
        return new Field(newName, type, nullable, isGetter);
    }

    public Field asGetter() {
        return new Field(name, type, nullable, true);
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", type=" + type.getSimpleName() +
                ", nullable=" + nullable +
                ", isGetter=" + isGetter +
                '}';
    }
}
