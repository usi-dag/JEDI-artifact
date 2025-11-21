package s2s.planner.qp;

import s2s.planner.qp.operators.Operator;

public class S2SPlan {

    private final Operator root;

    public S2SPlan(Operator root) {
        this.root = root;
    }

    public Operator getRoot() {
        return root;
    }

}
