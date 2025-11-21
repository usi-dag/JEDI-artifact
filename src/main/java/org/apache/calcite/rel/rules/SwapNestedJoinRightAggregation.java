package org.apache.calcite.rel.rules;


import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;


@Value.Enclosing
public class SwapNestedJoinRightAggregation extends RelRule<SwapNestedJoinRightAggregation.Config> implements SubstitutionRule {

    public static final SwapNestedJoinRightAggregation INSTANCE = Config.DEFAULT.toRule();

    protected SwapNestedJoinRightAggregation(Config config) {
        super(config);
    }

    @Override
    public boolean autoPruneOld() {
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join join = call.rel(0);
        final Aggregate aggregate = call.rel(2);
        if(!aggregate.getGroupSet().isEmpty()) {
            return;
        }
        RelBuilder builder = call.builder();
        RelNode swapped = JoinCommuteRule.swap(join, false, builder);
        if(swapped != null) {
            call.transformTo(swapped);
        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableSwapNestedJoinRightAggregation.Config.of()
                .withOperandFor(LogicalJoin.class, LogicalAggregate.class);

        @Override default SwapNestedJoinRightAggregation toRule() {
            return new SwapNestedJoinRightAggregation(this);
        }

        /** Defines an operand tree for the given classes. */
        default Config withOperandFor(Class<? extends Join> joinClass,
                                                                     Class<? extends Aggregate> aggregateClass) {

            return (Config)
                    withOperandSupplier(b0 -> b0.operand(joinClass).inputs(
                            b1 -> b1.operand(RelNode.class).anyInputs(),
                            b2 -> b2.operand(aggregateClass).anyInputs())
                    ).withDescription("SwapNestedJoinRightAggregation");
        }
    }
}
