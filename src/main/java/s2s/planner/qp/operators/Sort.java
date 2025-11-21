package s2s.planner.qp.operators;

import s2s.planner.qp.expressions.Expression;

import java.util.Arrays;

public class Sort extends SingleChildOperator {

    final Expression[] expressions;
    final Direction[] directions;

    public enum Direction  { ASC, DESC }

    public static Direction[] nASC(int size) {
        Direction[] directions = new Direction[size];
		Arrays.fill(directions, Direction.ASC);
        return directions;
    }

    public Sort(Expression[] expressions, Direction[] directions, Operator child) {
        super(child);
        if(expressions.length != directions.length) {
            throw new IllegalArgumentException("Different lengths (expr/dir): " + expressions.length + " " + directions.length);
        }
        this.expressions = expressions;
        this.directions = directions;
    }


    public Sort(Expression[] expressions, Operator child) {
        this(expressions, nASC(expressions.length), child);
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        visitor.visit(this);
    }

    public Expression[] getExpressions() {
        return expressions;
    }

    public Direction[] getDirections() {
        return directions;
    }

}
