package org.apache.calcite.adapters.enumerable;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;


public class MyEnumerableAggregate extends Aggregate implements EnumerableRel {
    public MyEnumerableAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls)
            throws InvalidRelException {
        super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof EnumerableConvention;

        for (AggregateCall aggCall : aggCalls) {
            if (aggCall.isDistinct()) {
                throw new InvalidRelException("distinct aggregation not supported");
            }
            if(aggCall.getAggregation() != SqlStdOperatorTable.AVG) {
                AggImplementor implementor2 =
                        RexImpTable.INSTANCE.get(aggCall.getAggregation(), false);
                if (implementor2 == null) {
                    throw new InvalidRelException(
                            "aggregation " + aggCall.getAggregation() + " not supported");
                }
            }
        }
    }

    @Deprecated // to be removed before 2.0
    public MyEnumerableAggregate(RelOptCluster cluster, RelTraitSet traitSet,
                                 RelNode input, boolean indicator, ImmutableBitSet groupSet,
                                 List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls)
            throws InvalidRelException {
        this(cluster, traitSet, input, groupSet, groupSets, aggCalls);
        checkIndicator(indicator);
    }

    @Override
    public MyEnumerableAggregate copy(RelTraitSet traitSet, RelNode input,
                                      ImmutableBitSet groupSet,
                                      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        try {
            return new MyEnumerableAggregate(getCluster(), traitSet, input,
                    groupSet, groupSets, aggCalls);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw new AssertionError(e);
        }
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        return null;
    }
}
