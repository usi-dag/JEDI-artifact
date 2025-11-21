package s2s.query_compiler.template;

public record Declaration(String name, String typeName, String initCode, String modifiers) {
    // manage a field declaration and initialization

    public Declaration(String name, String typeName, String initCode) {
        this(name, typeName, initCode, "");
    }

    public static Declaration unInitialized(String name, String typeName) {
        return new Declaration(name, typeName, null, "");
    }

    public String toCode() {
        String result = "";
        if(initCode != null) {
            result = "%s %s = %s;".formatted(typeName, name, initCode).replaceAll("\n\t*;", ";\n");
        } else {
            result = "%s %s;".formatted(typeName, name).replaceAll("\n\t*;", ";\n");
        }
        if(modifiers != null && !modifiers.isEmpty()) {
            return modifiers + " " + result;
        } else {
            return result;
        }
    }

}