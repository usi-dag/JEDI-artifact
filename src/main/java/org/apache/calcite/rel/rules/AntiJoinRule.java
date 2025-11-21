package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;


/**
 * Planner rule that creates a {@code AntiJoin} from a
 * {@link Join} on top of a
 * {@link org.apache.calcite.rel.logical.LogicalAggregate}.
 */
@SuppressWarnings("deprecation") // TODO use immutable instead of operand(...)
public class AntiJoinRule extends RelOptRule {
  // Note: this class has been copy pasted from a jira suggestion which was not accepted
  //  https://issues.apache.org/jira/browse/CALCITE-3367
  // TODO look for a better alternative or improve

  private static final Predicate<Join> IS_LEFT_JOIN =
          join -> join.getJoinType() == JoinRelType.LEFT;

  public static final AntiJoinRule INSTANCE = new AntiJoinRule();

  AntiJoinRule() {
    super(
            operand(Project.class,
                    operand(Filter.class,
                            operandJ(Join.class, null, IS_LEFT_JOIN,
                                    some(
                                            operand(RelNode.class, any()),
                                            operand(RelNode.class, operand(RelNode.class, any())))))),
            RelFactories.LOGICAL_BUILDER, "AntiJoinRule");
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Project project = call.rel(0);
    Filter filter = call.rel(1);
    Join join = call.rel(2);
    RelNode left = call.rel(3);
    RelNode right = call.rel(4);
    if(right instanceof  Aggregate) {
      onMatchWithAggregate(call, project, filter, join, left, (Aggregate) right);
    } else if(right instanceof Project && call.getRelList().size() > 5) {
      RelNode rel5 = call.rel(5);
      if (rel5 instanceof Aggregate) {
        onMatchWithProject(call, project, filter, join, left, (Project) right, (Aggregate) rel5);
      }
    }
  }


  private void onMatchWithAggregate(RelOptRuleCall call, Project project, Filter filter, Join join, RelNode left, Aggregate aggregate) {

    final RelOptCluster cluster = join.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();

    final ImmutableBitSet bits =
            RelOptUtil.InputFinder.bits(project.getProjects(), null);
    final ImmutableBitSet rightBits =
            ImmutableBitSet.range(left.getRowType().getFieldCount(),
                    join.getRowType().getFieldCount());
    if (aggregate.getGroupCount() == 0
            || aggregate.getAggCallList().isEmpty()
            || bits.intersects(rightBits)) {
      return;
    }

    List<RexNode> isNullConditionsForAnti = new ArrayList<>();
    List<RexNode> residualConditions = new ArrayList<>();

    for (RexNode cond: RelOptUtil.conjunctions(filter.getCondition())) {
      if (cond instanceof RexCall) {
        final RexCall rexCall = (RexCall) cond;
        if (rexCall.op == SqlStdOperatorTable.IS_NULL) {
          final RexNode operand = rexCall.operands.get(0);
          if (operand instanceof RexInputRef) {
            final RexInputRef inputRef = (RexInputRef) operand;
            final int idxOnAggregate =
                    inputRef.getIndex() - left.getRowType().getFieldCount();
            if (!aggregate.getRowType().getFieldList()
                    .get(idxOnAggregate).getType().isNullable()) {
              isNullConditionsForAnti.add(cond);
              continue;
            }
          }
        }
      }
      residualConditions.add(cond);
    }

    if (isNullConditionsForAnti.isEmpty()) {
      return;
    }

    final JoinInfo joinInfo = join.analyzeCondition();
    if (!joinInfo.rightSet().equals(
            ImmutableBitSet.range(aggregate.getGroupCount()))) {
      // Rule requires that aggregate key to be the same as the join key.
      // By the way, neither a super-set nor a sub-set would work.
      return;
    }

    if (!joinInfo.isEqui()) {
      return;
    }

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(left);
    final List<Integer> newRightKeyBuilder = new ArrayList<>();
    final List<Integer> aggregateKeys = aggregate.getGroupSet().asList();
    for (int key : joinInfo.rightKeys) {
      newRightKeyBuilder.add(aggregateKeys.get(key));
    }
    final ImmutableIntList newRightKeys = ImmutableIntList.copyOf(newRightKeyBuilder);
    relBuilder.push(aggregate.getInput());

    final RexNode newCondition =
            RelOptUtil.createEquiJoinCondition(relBuilder.peek(2, 0),
                    joinInfo.leftKeys, relBuilder.peek(2, 1), newRightKeys,
                    rexBuilder);
    relBuilder.antiJoin(newCondition);
    if (!residualConditions.isEmpty()) {
      relBuilder.filter(RexUtil.composeConjunction(rexBuilder, residualConditions));
    }
    relBuilder.project(project.getProjects(), project.getRowType().getFieldNames());
    final RelNode relNode = relBuilder.build();
    call.transformTo(relNode);
  }


  private void onMatchWithProject(RelOptRuleCall call, Project project, Filter filter, Join join, RelNode left, Project projectAboveAggregate, Aggregate aggregate) {
    final RelOptCluster cluster = join.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();

    final ImmutableBitSet bits =
            RelOptUtil.InputFinder.bits(project.getProjects(), null);
    final ImmutableBitSet rightBits =
            ImmutableBitSet.range(left.getRowType().getFieldCount(),
                    join.getRowType().getFieldCount());
    if (aggregate.getGroupCount() == 0 || bits.intersects(rightBits)) {
      return;
    }

    List<RexNode> isNullConditionsForAnti = new ArrayList<>();
    List<RexNode> residualConditions = new ArrayList<>();

    for (RexNode cond: RelOptUtil.conjunctions(filter.getCondition())) {
      if (cond instanceof RexCall) {
        final RexCall rexCall = (RexCall) cond;
        if (rexCall.op == SqlStdOperatorTable.IS_NULL) {
          final RexNode operand = rexCall.operands.get(0);
          if (operand instanceof RexInputRef) {
            final RexInputRef inputRef = (RexInputRef) operand;
            final int idxOnAggregate =
                    inputRef.getIndex() - left.getRowType().getFieldCount();
            if (projectAboveAggregate.getProjects().get(idxOnAggregate).isAlwaysTrue()) {
              isNullConditionsForAnti.add(cond);
              continue;
            }
          }
        }
      }
      residualConditions.add(cond);
    }

    if (isNullConditionsForAnti.isEmpty()) {
      return;
    }

    final JoinInfo joinInfo = join.analyzeCondition();
    if (!joinInfo.rightSet().equals(
            ImmutableBitSet.range(aggregate.getGroupCount()))) {
      // Rule requires that aggregate key to be the same as the join key.
      // By the way, neither a super-set nor a sub-set would work.
      return;
    }

    if (!joinInfo.isEqui()) {
      return;
    }

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(left);
    final List<Integer> newRightKeyBuilder = new ArrayList<>();
    final List<Integer> aggregateKeys = aggregate.getGroupSet().asList();
    for (int key : joinInfo.rightKeys) {
      newRightKeyBuilder.add(aggregateKeys.get(key));
    }
    final ImmutableIntList newRightKeys = ImmutableIntList.copyOf(newRightKeyBuilder);
    LinkedList<RexNode> newProj = new LinkedList<>(projectAboveAggregate.getProjects());
    newProj.removeLast();

    relBuilder.push(aggregate.getInput()).project(newProj);
    RexNode newCondition;
    try {
      newCondition = RelOptUtil.createEquiJoinCondition(relBuilder.peek(2, 0),
              joinInfo.leftKeys, relBuilder.peek(2, 1), newRightKeys,
              rexBuilder);
    }  catch (Exception e) {
      return;
    }

    relBuilder.antiJoin(newCondition);
    if (!residualConditions.isEmpty()) {
      relBuilder.filter(RexUtil.composeConjunction(rexBuilder, residualConditions));
    }
    relBuilder.project(project.getProjects(), project.getRowType().getFieldNames());
    final RelNode relNode = relBuilder.build();
    call.transformTo(relNode);
  }
}

// End AntiJoinRule.java