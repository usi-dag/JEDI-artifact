package s2s.query_compiler;

import java.util.Optional;

public class TypingUtils {

    public static Class<?> boxed(Class<?> cls) {
        return switch (cls.getSimpleName()) {
            case "int" -> Integer.class;
            case "long" -> Long.class;
            case "double" -> Double.class;
            case "boolean" -> Boolean.class;
            default -> cls;
        };
    }

    public static String boxed(String className) {
        return switch (className) {
            case "int" -> "Integer";
            case "long" -> "Long";
            case "double" -> "Double";
            case "boolean" -> "Boolean";
            default -> className;
        };
    }

    public static boolean isPrimitive(String className) {
        return !boxed(className).equals(className);
    }

    public static String streamPrefix(Class<?> clz) {
        if (clz.equals(int.class) || clz.equals(Integer.class)) {
            return "Int";
        }
        return boxed(clz).getSimpleName();
    }

    public static Optional<Class<?>> maybePrimitiveOutputClass(String className) {
        return switch (className) {
            case "int" -> Optional.of(int.class);
            case "double" -> Optional.of(double.class);
            case "long" -> Optional.of(long.class);
            default -> Optional.empty();
        };
    }
}
