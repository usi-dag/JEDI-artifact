package s2s.planner.qp.expressions;


import java.util.Set;

public class InStringSet implements Expression  {

	private final Expression stringGetter;
	private final Set<String> strings;

	public InStringSet(Expression stringGetter, Set<String> strings) {
		this.stringGetter = stringGetter;
		this.strings = strings;
	}

	public Expression getStringGetter() {
		return stringGetter;
	}

	public Set<String> getStrings() {
		return strings;
	}

	@Override
	public <T> T accept(ExpressionVisitor<T> visitor) {
		return visitor.visit(this);
	}

	@Override
	public Class<?> type() {
		return Boolean.class;
	}
}
