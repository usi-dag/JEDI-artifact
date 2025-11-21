package s2s.planner.qp.operators;

import s2s.planner.qp.Schema;
import s2s.planner.qp.expressions.Expression;

public class SingletonFilterJoin implements Join {

	final Operator left, right;
	final Expression leftFilterExpression;

	public SingletonFilterJoin(Operator left, Operator right, Expression leftFilterExpression) {
		this.left = left;
		this.right = right;
		this.leftFilterExpression = leftFilterExpression;
	}

	@Override
	public void accept(OperatorVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public Schema getSchema() {
		return right.getSchema();
	}

	public Expression getLeftFilterExpression() {
		return leftFilterExpression;
	}

	@Override
	public Operator left() {
		return left;
	}

	@Override
	public Operator right() {
		return right;
	}

	@Override
	public Expression[] mapper() {
		return null;
	}

	@Override
	public Expression nonEquiCondition() {
		return leftFilterExpression;
	}

	@Override
	public boolean isLeftProjectMapper() {
		return false;
	}

	@Override
	public boolean isRightProjectMapper() {
		return true;
	}

	@Override
	public JoinType joinType() {
		return JoinType.INNER;
	}
}
