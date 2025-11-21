package s2s.query_compiler;

import s2s.planner.qp.S2SPlan;
import s2s.query_compiler.options.CommonCompilerOption;
import s2s.query_compiler.template.GeneratedClassHolder;


public interface QueryCompiler {

    GeneratedClassHolder compile(S2SPlan plan, String mainMethodName, String params);

    CommonCompilerOption getOption();

    String getVariantName();
}
