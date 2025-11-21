package s2s.planner.qp.expressions;


import java.util.Set;

public class InNumSet implements Expression  {

	private final Expression getter;
	private final Set<Number> strings;

	public InNumSet(Expression stringGetter, Set<Number> strings) {
		this.getter = stringGetter;
		this.strings = strings;
	}

	public Expression getGetter() {
		return getter;
	}

	public Set<Number> getSet() {
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
