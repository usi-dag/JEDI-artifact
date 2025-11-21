package s2s.query_compiler.template;

public class CodeTemplateUtils {

    public static String genIf(String condition, String body) {
        return  """
                if(%s) {
                %s
                }
                """.formatted(condition, indent(body));
    }

    public static String genForEach(String type, String varName, String iterable, String body) {
        return  """
                for(%s %s : %s) {
                %s
                }
                """.formatted(type, varName, iterable, indent(body));
    }

    public static String indent(String code) {
        return "\t" + code.replace(CodeTemplateBasedVisitor.EOL, CodeTemplateBasedVisitor.EOL_TAB);
    }
}
