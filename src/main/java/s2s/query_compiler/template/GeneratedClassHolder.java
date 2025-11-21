package s2s.query_compiler.template;

import s2s.planner.qp.Schema;

public class GeneratedClassHolder {

    private final String outputClassName;
    private final Schema outputSchema;
    private final CodegenContext codegenContext;

    public GeneratedClassHolder(String outputClassName, Schema outputSchema, CodegenContext codegenContext) {
        this.outputClassName = outputClassName;
        this.outputSchema = outputSchema;
        this.codegenContext = codegenContext;
    }

    public String asClass(String name, String packageName, String[] imports) {
        return codegenContext.asClass(name, packageName, imports, null, null);
    }

    public String getOutputClassName() {
        return outputClassName;
    }

    public Schema getOutputSchema() {
        return outputSchema;
    }
}
