package s2s.planner.qp.expressions;

public class StrMultiContains extends BooleanExpression {
    // represents a LIKE operator in the form X LIKE %a1%a2%...%an%

    final String[] patterns;
    final Expression stringGetter;

    public StrMultiContains(String[] patterns, Expression stringGetter) {
        this.patterns = patterns;
        this.stringGetter = stringGetter;
        if(stringGetter.type() != String.class) {
            throw new IllegalArgumentException(this.getClass() + " requires a string returning subexpression");
        }
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public String[] getPatterns() {
        return patterns;
    }

    public Expression getStringGetter() {
        return stringGetter;
    }
}
