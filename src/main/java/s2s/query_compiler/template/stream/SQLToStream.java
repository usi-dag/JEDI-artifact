package s2s.query_compiler.template.stream;

import s2s.planner.qp.S2SPlan;
import s2s.planner.qp.operators.*;
import s2s.planner.qp.operators.Operator;
import s2s.query_compiler.QueryCompiler;
import s2s.query_compiler.TypingUtils;
import s2s.query_compiler.options.CommonCompilerOption;
import s2s.query_compiler.template.Declaration;
import s2s.query_compiler.template.GeneratedClassHolder;

import java.util.stream.Collectors;


public class SQLToStream implements QueryCompiler {


    private final StreamCompilerOptions options;

    public SQLToStream(StreamCompilerOptions options) {
        this.options = options;
    }


    @Override
    public GeneratedClassHolder compile(S2SPlan plan,
                                        String methodName,
                                        String methodParameters) {
        Operator operator = plan.getRoot();
        SqlOpToJavaStreamVisitor visitor = SqlOpToJavaStreamVisitor.visit(operator, options);

        String declarations = visitor.declarations.stream()
                .map(Declaration::toCode)
                .collect(Collectors.joining("\n\n"));

        String newOutputClassName = TypingUtils.boxed(visitor.outputClassName);
        String execReturnType = "List<" + newOutputClassName + ">";


        String methodCode = """
                    public %s %s(%s) {
                    // declarations initialization
                    %s
                    
                    // stream
                    %s
                    }
                    """.formatted(
                execReturnType,
                methodName,
                methodParameters,
                declarations,
                visitor.body().replace("\n", "\n\t"));

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
        return "Stream_" + options.toCompactString();
    }

}