package s2s.query_compiler.template.imperative;

import s2s.planner.qp.S2SPlan;
import s2s.planner.qp.operators.Operator;
import s2s.query_compiler.QueryCompiler;
import s2s.query_compiler.TypingUtils;
import s2s.query_compiler.options.CommonCompilerOption;
import s2s.query_compiler.template.CodeTemplateUtils;
import s2s.query_compiler.template.Declaration;
import s2s.query_compiler.template.GeneratedClassHolder;

import java.util.stream.Collectors;


public class SQLToImperativeJava implements QueryCompiler {


    private final ImperativeCompilerOptions options;

    public SQLToImperativeJava(ImperativeCompilerOptions options) {
        this.options = options;
    }


    @Override
    public GeneratedClassHolder compile(S2SPlan plan,
                                        String methodName,
                                        String methodParameters) {
        Operator operator = plan.getRoot();
        SqlOpToJavaImperativeVisitor visitor = SqlOpToJavaImperativeVisitor.visit(operator, options);

        String declarations = visitor.declarations.stream()
                .map(Declaration::toCode)
                .collect(Collectors.joining("\n\n"));

        String newOutputClassName = TypingUtils.boxed(visitor.outputClassName);
        String execReturnType = "List<" + newOutputClassName + ">";
        String methodBody = """
                // declaration initializations
                %s
                
                // main pipeline
                %s
                """.formatted(declarations, visitor.body());

        String methodCode = """
                    public %s %s(%s) {
                    %s
                    }
                    """.formatted(
                execReturnType,
                methodName,
                methodParameters,
                CodeTemplateUtils.indent(methodBody)
                );

        visitor.codegenContext.addMethodFixName(methodName, methodCode);
        return new GeneratedClassHolder(
                newOutputClassName, visitor.getOutputSchema(), visitor.codegenContext);
    }


    @Override
    public CommonCompilerOption getOption() {
        return options;
    }

    @Override
    public String getVariantName() {
        return "Imperative_" + options.toCompactString();
    }

}