package s2s.planner.qp.operators;

import s2s.planner.qp.expressions.Expression;

public interface Join extends Operator {
    enum JoinType {
        /**
         * Inner join.
         */
        INNER,

        /**
         * Left-outer join.
         */
        LEFT,

        /**
         * Right-outer join.
         */
        RIGHT,

        /**
         * Full-outer join.
         */
        FULL,

        /**
         * Semi-join.
         *
         * <p>For example, {@code EMP semi-join DEPT} finds all {@code EMP} records
         * that have a corresponding {@code DEPT} record:
         *
         * <blockquote><pre>
         * SELECT * FROM EMP
         * WHERE EXISTS (SELECT 1 FROM DEPT
         *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
         * </blockquote>
         */
        SEMI,

        /**
         * Anti-join (also known as Anti-semi-join).
         *
         * <p>For example, {@code EMP anti-join DEPT} finds all {@code EMP} records
         * that do not have a corresponding {@code DEPT} record:
         *
         * <blockquote><pre>
         * SELECT * FROM EMP
         * WHERE NOT EXISTS (SELECT 1 FROM DEPT
         *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
         * </blockquote>
         */
        ANTI,

        /**
		 * Note: Calcite only supports SEMI/ANTI on a single side
		 * and it puts on the right side the probing child.
		 * <p>
		 * Here we also use LEFT/RIGHT SEMI/ANTI joins in the same way they are used on Hyper.
		 * (see:
         * <a href="https://15721.courses.cs.cmu.edu/spring2018/papers/16-optimizer2/hyperjoins-btw2017.pdf">
         * The Complete Story of Joins (in HyPer)
         * </a>)
		 * Note that this means that LEFT_SEMI, RIGHT_SEMI, LEFT_ANTI, RIGHT_ANTI introduced here
		 * *always* build on the left side.
		 * <p>
		 * As an example: A RIGHT_SEMI B is equivalent to the B SEMI A produced by Calcite.
		 */
        LEFT_SEMI, RIGHT_SEMI, LEFT_ANTI, RIGHT_ANTI
    }
    Operator left();
    Operator right();

    Expression[] mapper();

    Expression nonEquiCondition();

    boolean isLeftProjectMapper();
    boolean isRightProjectMapper();

    JoinType joinType();

}
