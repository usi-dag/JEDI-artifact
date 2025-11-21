package s2s.planner.qp.expressions;

import java.util.regex.Pattern;

public class Like extends BooleanExpression {

    private final Pattern pattern;
    private final Expression stringGetter;

    public Like(Pattern pattern, Expression stringGetter) {
        this.pattern = pattern;
        this.stringGetter = stringGetter;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public Expression getStringGetter() {
        return stringGetter;
    }

    public Pattern getPattern() {
        return pattern;
    }

    public static BooleanExpression create(Expression stringGetter, String reLike) {
        String regex = quotemeta(reLike);
        int starCount = (int) regex.chars().filter(ch -> ch == '%').count();
        if (!regex.contains("_") && starCount > 0) {
            // startsWith
            if (starCount == 1 && regex.endsWith("%")) {
                return new StrStartsWith(regex.substring(0, regex.length() - 1), stringGetter);
            }
            // endsWith
            if (starCount == 1 && regex.startsWith("%")) {
                return new StrEndsWith(regex.substring(1), stringGetter);
            }
            // contains
            if (starCount == 2 && regex.startsWith("%") && regex.endsWith("%")) {
                return new StrContains(regex.substring(1, regex.length() - 1), stringGetter);
            }
            if (regex.startsWith("%") && regex.endsWith("%")) {
                String[] patterns = regex.substring(1, regex.length() - 1).split("%");
                return new StrMultiContains(patterns, stringGetter);
            }
        }

        return new Like(like(regex), stringGetter);
    }


    public static Pattern like(String regex){
        regex = regex.replace("_", ".").replace("%", ".*?");
        return Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    }

    public static String quotemeta(String s){
        if (s == null) {
            throw new IllegalArgumentException("String cannot be null");
        }

        int len = s.length();
        if (len == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder(len * 2);
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if ("[](){}.*+?$^|#\\".indexOf(c) != -1) {
                sb.append("\\");
            }
            sb.append(c);
        }
        return sb.toString();
    }


}
