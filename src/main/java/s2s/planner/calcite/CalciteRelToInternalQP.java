package s2s.planner.calcite;

import s2s.engine.Date;
import s2s.planner.qp.Field;
import s2s.planner.qp.Schema;
import s2s.planner.qp.expressions.*;
import s2s.planner.qp.operators.*;
import s2s.planner.qp.expressions.*;
import s2s.planner.qp.operators.*;
import s2s.planner.qp.sources.ArrayDataSource;
import s2s.query_compiler.TypingUtils;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

/**
 *
 * Translates a Calcite Rel into an internal representation of a Query Plan
 * */
public class CalciteRelToInternalQP {

    public static Operator convert(RelNode relNode) {
        OpVisitor visitor = new OpVisitor();
        visitor.go(relNode);
        return visitor.operator;
    }

    public static Class<?> asJavaClass(RelDataType dataType) {
        if(dataType instanceof RelDataTypeFactoryImpl.JavaType javaType) {
            return javaType.getJavaClass();
        } else {
            Class<?> cls = switch (dataType.getSqlTypeName()) {
                case DATE -> Date.class;
                case BOOLEAN -> Boolean.class;
                case BIGINT -> long.class;
                case INTEGER -> int.class;
                case DECIMAL, FLOAT, DOUBLE -> double.class;
                case CHAR, VARCHAR -> String.class;
                default -> throw new RuntimeException("unknown type: " + dataType.getSqlTypeName());
            };
            return dataType.isNullable() ? TypingUtils.boxed(cls) : cls;
        }
    }
    private static ExprVisitor exprVisitor(RelNode relNode) {
        return new ExprVisitor(relNode);
    }


    private static Schema asSchema(RelDataType relDataType) {
        List<Field> fieldList = new LinkedList<>();
        for(RelDataTypeField relDataTypeField : relDataType.getFieldList()) {
            fieldList.add(new Field(
                    relDataTypeField.getName(),
                    asJavaClass(relDataTypeField.getType()),
                    relDataTypeField.getType().isNullable()));
        }
        return Schema.byFields(fieldList.toArray(Field[]::new));
    }

    private static class OpVisitor extends RelVisitor {
        Operator operator;
        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            super.visit(node, ordinal, parent);

            if(node instanceof TableScan scan) {
                String tableName = scan.getTable().getQualifiedName().get(scan.getTable().getQualifiedName().size()-1);
                operator = new ArrayTableScan(new ArrayDataSource(
                        tableName,
                        asSchema(scan.getRowType()),
                        tableName));
            } else if (node instanceof org.apache.calcite.rel.core.Project project ) {
                if(isRemovableProject(project)) {
                    // TODO
                    Schema inputSchema = operator.getSchema();
                    Field[] inputSchemaFields = inputSchema.getFields();
                    List<Pair<RexNode, String>> namedProjects = project.getNamedProjects();
                    Field[] fields = new Field[namedProjects.size()];
                    int outputIndex = 0;
                    for(Pair<RexNode, String> namedProject : namedProjects) {
                        RexInputRef inputRef = (RexInputRef) namedProject.left;
                        Field inputField = inputSchemaFields[inputRef.getIndex()];
                        Field field = Field.renamedWrapper(namedProject.right, inputField);
                        fields[outputIndex++] = field;
                    }
                    operator = new RemovableProject(operator, Schema.byFieldsForWrapperSchema(operator.getSchema(), fields));
                } else {
                    List<RexNode> rexNodes = project.getProjects();
                    Expression[] expressions = new Expression[rexNodes.size()];
                    String[] outSchemaNames = new String[rexNodes.size()];
                    for (int i = 0; i < expressions.length; i++) {
                        expressions[i] = rexNodes.get(i).accept(exprVisitor(project));
                        outSchemaNames[i] = project.getRowType().getFieldList().get(i).getName();
                    }
                    operator = new s2s.planner.qp.operators.Project(expressions, operator, outSchemaNames);
                }
            } else if(node instanceof Filter filter) {
                operator = new Predicate(filter.getCondition().accept(exprVisitor(filter)), operator);
            } else if(node instanceof org.apache.calcite.rel.core.Aggregate aggregate) {
                List<Integer> groupInputRefs = aggregate.getGroupSet().asList();
                boolean emptyGroupAndAllSingleValue =
                        groupInputRefs.isEmpty() &&
                                aggregate.getAggCallList().stream()
                                        .allMatch(agg -> agg.getAggregation().kind == SqlKind.SINGLE_VALUE);
                if(!emptyGroupAndAllSingleValue) {
                    List<RelDataTypeField> inputFields = aggregate.getInput().getRowType().getFieldList();
                    Expression[] groupKeys = new Expression[groupInputRefs.size()];
                    for (int i = 0; i < groupKeys.length; i++) {
                        groupKeys[i] = makeRef(i, inputFields.get(groupInputRefs.get(i)));
                    }
                    SqlAggFunction[] aggregations = makeAggregators(aggregate);

                    List<RelDataTypeField> outputFields = aggregate.getRowType().getFieldList();
                    String[] outputFieldNames = new String[outputFields.size()];
                    for (int i = 0; i < outputFieldNames.length; i++) {
                        outputFieldNames[i] = outputFields.get(i).getName();
                    }

                    operator = new s2s.planner.qp.operators.Aggregate(groupKeys, aggregations, outputFieldNames, operator);
                } else {
                    throw new RuntimeException("Unexpected Aggregate: emptyGroupAndAllSingleValue is not true");
                }
            } else if(node instanceof org.apache.calcite.rel.core.Sort sort) {
                List<RexNode> rexNodes = sort.getSortExps();
                Expression[] expressions = new Expression[rexNodes.size()];
                s2s.planner.qp.operators.Sort.Direction[] directions = new s2s.planner.qp.operators.Sort.Direction[rexNodes.size()];
                for (int i = 0; i < expressions.length; i++) {
                    expressions[i] = rexNodes.get(i).accept(exprVisitor(sort));
                    directions[i] = switch (sort.collation.getFieldCollations().get(i).direction.shortString) {
                        case "ASC" -> s2s.planner.qp.operators.Sort.Direction.ASC;
                        case "DESC" -> s2s.planner.qp.operators.Sort.Direction.DESC;
                        default -> throw new IllegalStateException("Unexpected sort direction: " + sort.collation.getFieldCollations().get(i).direction.shortString);
                    };
                }
                operator = new s2s.planner.qp.operators.Sort(expressions, directions, operator);
            } else if(node instanceof EnumerableLimit limit) {
                Integer offset = null, fetch = null;
                if(limit.offset != null) {
                    offset = RexLiteral.intValue(limit.offset);
                }
                if(limit.fetch != null) {
                    fetch = RexLiteral.intValue(limit.fetch);
                }
                if(offset != null && fetch != null) {
                    operator = new OffsetFetch(offset, fetch, operator);
                } else if(offset != null) {
                    operator = new Skip(offset, operator);
                } else if(fetch != null) {
                    operator = new Limit(fetch, operator);
                } else {
                    throw new RuntimeException("Unexpected EnumerableLimit: both fetch and offset cannot be null");
                }

            } else if(node instanceof Join join){
                OpVisitor leftVisitor = new OpVisitor();
                leftVisitor.visit(join.getLeft(), 0, join);
                Operator leftOp = leftVisitor.operator;
                visit(join.getRight(), 1, join);
                Operator rightOp = operator;

                RexBuilder builder = new RexBuilder(new JavaTypeFactoryImpl());
                Expression nonEquiCondition = null;
                JoinInfo info = join.analyzeCondition();
                if(!info.nonEquiConditions.isEmpty()) {
                    nonEquiCondition = RexUtil.composeConjunction(builder, info.nonEquiConditions)
                            .accept(exprVisitor(join));
                }
                int leftFieldsCount = join.getLeft().getRowType().getFieldCount();
                boolean projectOnlyLeft = true, projectOnlyRight = true;
                for (RelDataTypeField f : join.getRowType().getFieldList()) {
                    if(f.getIndex() < leftFieldsCount) {
                        projectOnlyRight = false;
                    } else {
                        projectOnlyLeft = false;
                    }
                }

                if(join instanceof EnumerableHashJoin) {

                    // Single key can be optimized (i.e., we do not need an array for keys)
                    // int nKeys = info.leftKeys.size();

                    Expression[] leftGetters = info.leftKeys.stream()
                            .map(i -> makeRef(i, join.getLeft().getRowType().getFieldList().get(i)))
                            .toArray(Expression[]::new);
                    Expression[] rightGetters = info.rightKeys.stream()
                            .map(i -> makeRef(i, join.getRight().getRowType().getFieldList().get(i)))
                            .toArray(Expression[]::new);

                    s2s.planner.qp.operators.Join.JoinType joinType ;
                    if(join.getJoinType() == JoinRelType.INNER) {
                        joinType = s2s.planner.qp.operators.Join.JoinType.INNER;
                        if(projectOnlyLeft) {
                            // TODO check, it looks like these optimizations are never triggered
                            operator = HashJoin.createInnerWithLeftProjectMapper(
                                    leftOp, rightOp,
                                    leftGetters, rightGetters,
                                    nonEquiCondition);
                            return;
                        } else if(projectOnlyRight) {
                            operator = HashJoin.createInnerWithRightProjectMapper(
                                    leftOp, rightOp,
                                    leftGetters, rightGetters,
                                    nonEquiCondition);
                            return;
                        }
                    } else if(join.getJoinType() == JoinRelType.LEFT) {
                        joinType = s2s.planner.qp.operators.Join.JoinType.LEFT;
                    } else if(join.getJoinType() == JoinRelType.SEMI) {
                        if(nonEquiCondition != null) {
                            throw new UnsupportedOperationException("SEMI with non equi condition");
                        }
                        operator = HashJoin.createSemi(leftOp, rightOp, leftGetters, rightGetters);
                        return;
                    } else if(join.getJoinType() == JoinRelType.ANTI) {
                        if(nonEquiCondition != null) {
                            throw new UnsupportedOperationException("SEMI with non equi condition");
                        }
                        operator = HashJoin.createAnti(leftOp, rightOp, leftGetters, rightGetters);
                        return;
                    } else {
                        throw new IllegalArgumentException("Unsupported join type: " + join.getJoinType());
                    }

                    List<RelDataTypeField> fields = join.getRowType().getFieldList();
                    InputRef[] mappers = IntStream.range(0, fields.size())
                            .mapToObj(i-> makeRef(i, fields.get(i)))
                            .toArray(InputRef[]::new);

                    operator = HashJoin.create(
                            leftOp, rightOp,
                            leftGetters, rightGetters,
                            nonEquiCondition, mappers,
                            joinType);

                } else if (join instanceof EnumerableNestedLoopJoin) {
                    operator = switch (join.getJoinType()) {
                        case INNER -> {
                            if(projectOnlyLeft) {
                                yield NestedLoopJoin.createWithLeftProjectMapper(leftOp, rightOp, nonEquiCondition);
                            } else if(projectOnlyRight) {
                                yield NestedLoopJoin.createWithRightProjectMapper(leftOp, rightOp, nonEquiCondition);
                            } else {
                                List<RelDataTypeField> fields = join.getRowType().getFieldList();
                                InputRef[] mappers = IntStream.range(0, fields.size())
                                        .mapToObj(i-> makeRef(i, fields.get(i))).toArray(InputRef[]::new);
                                yield NestedLoopJoin.createInner(leftOp, rightOp, nonEquiCondition, mappers);
                            }
                        }
                        // TODO other joins

                        default -> throw new IllegalStateException("Unexpected hashjoin type: " + join.getJoinType());
                    };
                } else throw new RuntimeException("Unknown join type: " + join);
            } else {
                throw new IllegalStateException("Unexpected relnode: " + node);
            }
        }

        private SqlAggFunction[] makeAggregators(org.apache.calcite.rel.core.Aggregate aggregate) {
            List<AggregateCall> aggregateCallList = aggregate.getAggCallList();
            SqlAggFunction[] aggregateFunctionNodes = new SqlAggFunction[aggregateCallList.size()];
            for (int j = 0; j < aggregateCallList.size(); j++) {
                // TODO now assuming the call has zero or one input (i.e., an input ref of the row returned by child)
                AggregateCall aggregateCall = aggregateCallList.get(j);
                aggregateFunctionNodes[j] = resolveAggregateFunctionByChild(aggregateCall, aggregate);
            }
            return aggregateFunctionNodes;
        }

        public SqlAggFunction resolveAggregateFunctionByChild(AggregateCall aggregateCall, org.apache.calcite.rel.core.Aggregate aggregate) {
            int nArgForCall = aggregateCall.getArgList().size();
            RelNode input = aggregate.getInput();
            Expression filterExpr = null;
            if(aggregateCall.filterArg >= 0) {
                filterExpr = makeRef(aggregateCall.filterArg, input.getRowType().getFieldList().get(aggregateCall.filterArg));
            }
            switch (nArgForCall) {
                case 0:
                    return resolveAggregateFunction(aggregateCall, null, filterExpr);
                case 1:
                    int inputRef = aggregateCall.getArgList().get(0);
                    Expression dataAccessor = makeRef(aggregateCall.filterArg, input.getRowType().getFieldList().get(inputRef));
                    return resolveAggregateFunction(aggregateCall, dataAccessor, filterExpr);
                default:
                    throw new RuntimeException("Unexpected number of arguments for aggregate call, currently supported only 0 or 1, got: " + nArgForCall);
            }
        }
    }





    private static InputRef makeRef(int idx, RelDataTypeField relDataTypeField) {
        Class<?> cls = asJavaClass(relDataTypeField.getType());
        return new InputRef(idx, new Field(
                relDataTypeField.getName(),
                cls,
                relDataTypeField.getType().isNullable()));
    }


    public static SqlAggFunction resolveAggregateFunction(AggregateCall aggregateCall,
                                                          Expression inputGetter,
                                                          Expression filterExpression) {
        switch (aggregateCall.getAggregation().kind) {
            case SUM:
            case SUM0: // TODO SUM0 should be implemented like a CASE (i.e., if sum=0 return null else sum)
                return new SqlAggFunction(inputGetter, filterExpression, SqlAggFunction.AggKind.SUM);
            case COUNT:
                return new SqlAggFunction(inputGetter, filterExpression, SqlAggFunction.AggKind.COUNT);
            case MIN:
                return new SqlAggFunction(inputGetter, filterExpression, SqlAggFunction.AggKind.MIN);
            case MAX:
                return new SqlAggFunction(inputGetter, filterExpression, SqlAggFunction.AggKind.MAX);
            case AVG:
                return new SqlAggFunction(inputGetter, filterExpression, SqlAggFunction.AggKind.AVG);
            case SINGLE_VALUE:
                // TODO
                throw new RuntimeException("TODO - SINGLE_VALUE AGG");

        }
        throw new RuntimeException("Unexpected aggregate call " + aggregateCall);
    }



    private static class ExprVisitor extends RexVisitorImpl<Expression> {
        final RelNode relNode;

        protected ExprVisitor(RelNode relNode) {
            super(true);
            this.relNode = relNode;
        }

        @Override
        public Expression visitInputRef(RexInputRef inputRef) {
            List<RelDataTypeField> fstFieldList = relNode.getInput(0).getRowType().getFieldList();
            int inputIdx = inputRef.getIndex();
            if(relNode instanceof SingleRel) {
                if(fstFieldList.size() <= inputIdx){
                    throw new IllegalArgumentException("input index: " + inputIdx +" exceed #fields: " + fstFieldList.size());
                }
                return makeRef(inputIdx, fstFieldList.get(inputIdx));
            }
            if(!(relNode instanceof Join join)) {
                throw new IllegalArgumentException("unknown relnode: " + relNode);
            }
            int leftFieldsCount = join.getLeft().getRowType().getFieldCount();
            if(inputIdx < leftFieldsCount) {
                return makeRef(inputIdx, fstFieldList.get(inputIdx));
            } else {
                return makeRef(inputIdx, relNode.getInput(1).getRowType().getFieldList().get(inputIdx - leftFieldsCount));
            }
        }

        @Override
        public Expression visitLiteral(RexLiteral literal) {
            Class<?> target = TypingUtils.boxed(asJavaClass(literal.getType()));
            if(target == Date.class) {
                Calendar calendar = literal.getValueAs(Calendar.class);
                LocalDate localDate = LocalDateTime.ofInstant(calendar.toInstant(), calendar.getTimeZone().toZoneId()).toLocalDate();
                return new Const(new Date((int)localDate.toEpochDay()));
            }
            return new Const(literal.getValueAs(target));
        }

        @Override
        public Expression visitCall(RexCall call) {
            List<Expression> children = new LinkedList<>();
            if(call.getKind() != SqlKind.EXTRACT) {
                for(var operand : call.operands) {
                    Expression expr = operand.accept(this);
                    if (expr == null) {
                        throw new RuntimeException("null expr from call: " + call);
                    }
                    children.add(expr);
                }
            }

            if(call.op.getName().equals("SUBSTRING")) {
                if(call.getOperands().size() != 3) {
                    throw new RuntimeException("Expecting exactly 3 operands for SUBSTRING operation");
                }
                Expression stringGetter = call.getOperands().get(0).accept(this);
                RexNode snd = call.getOperands().get(1);
                RexNode trd = call.getOperands().get(2);
                if(snd instanceof RexLiteral && trd instanceof RexLiteral) {
                    int from = RexLiteral.intValue(snd) - 1;
                    int len = RexLiteral.intValue(trd);
                    return new SubString(from, len, stringGetter);
                }
                throw new RuntimeException("Expecting both second and third operands for SUBSTRING to be literals (missing impl)");
            }

            return switch (call.getKind()) {
                case PLUS -> new Add(children.get(0), children.get(1));
                case MINUS -> new Sub(children.get(0), children.get(1));
                case TIMES -> new Mul(children.get(0), children.get(1));
                case DIVIDE -> new Div(children.get(0), children.get(1));
                case MOD -> new Mod(children.get(0), children.get(1));

                case GREATER_THAN -> new GreaterThan(children.get(0), children.get(1));
                case GREATER_THAN_OR_EQUAL -> new GreaterThanEqual(children.get(0), children.get(1));
                case LESS_THAN -> new LessThan(children.get(0), children.get(1));
                case LESS_THAN_OR_EQUAL -> new LessThanEqual(children.get(0), children.get(1));
                case EQUALS -> new Equals(children.get(0), children.get(1));
                case NOT_EQUALS -> new NotEquals(children.get(0), children.get(1));

                case LIKE -> Like.create(children.get(0),
                        ((RexLiteral) call.operands.get(1)).getValueAs(String.class));

                case AND -> new And(children.toArray(new Expression[0]));
                case OR -> new Or(children.toArray(new Expression[0]));
                case NOT -> new Not(children.get(0));
                case IS_NULL -> new IsNull(children.get(0));
                case IS_TRUE -> children.get(0); // TODO check

                case CASE -> new Case(children.get(0), children.get(1), children.get(2));

                case CAST -> {
                    // most of the casts are redundant here
                    RexNode operand = call.operands.get(0);
                    if(operand.getType().getSqlTypeName() == call.getType().getSqlTypeName()) {
                        yield children.get(0);
                    } else {
                        Class<?> castFrom = children.get(0).type();
                        Class<?> castTo = switch (call.getType().getSqlTypeName()) {
                            case INTEGER -> int.class;
                            case DOUBLE -> double.class;
                            case BIGINT -> long.class;
                            default ->
                                    throw new IllegalStateException("Unexpected CAST: " + call);
                        };
                        if(!castFrom.isPrimitive()) castTo = TypingUtils.boxed(castTo);
                        if(Number.class.isAssignableFrom(TypingUtils.boxed(castFrom)) &&
                                Number.class.isAssignableFrom(TypingUtils.boxed(castTo))) {
                            yield new Cast(castTo, children.get(0));
                        }
                        throw new RuntimeException("Unsupported CAST " + call);
                    }
                }

                case EXTRACT -> {
                    if(call.getOperands().size() != 2) {
                        throw new RuntimeException("Expecting exactly two operands for EXTRACT operation");
                    }
                    RexNode fst = call.getOperands().get(0);
                    if(!(fst instanceof RexLiteral fstLiteral)) {
                        throw new RuntimeException("Expecting first operand for EXTRACT operation to be a RexLiteral");
                    }
                    if(!(fstLiteral.getValue() instanceof TimeUnitRange what)) {
                        throw new RuntimeException("Expecting first operand for EXTRACT operation to be a TimeUnitRange");
                    }
                    RexNode snd = call.getOperands().get(1);
                    Expression from = snd.accept(this);
                    yield switch (what) {
                        case YEAR -> new Extract(Extract.TimeUnit.YEAR, from);
                        case MONTH -> new Extract(Extract.TimeUnit.MONTH, from);
                        default -> throw new RuntimeException("Unknown TimeUnitRange: " + what);
                    };
                }

                default -> throw new IllegalStateException("Unexpected call kind: " + call.getKind());
            };
        }

    }


    private static boolean isRemovableProject(Project project) {
        var outputFields = project.getRowType().getFieldList();
        var inputFields = project.getInput().getRowType().getFieldList();
        var exprs = project.getProjects();
        int i = 0;
        for (var expr : exprs) {
            if(!(expr instanceof RexInputRef ref)) {
                return false;
            }
            if(!outputFields.get(i).getName().equals(inputFields.get(ref.getIndex()).getName())) {
                return false;
            }
            i++;
        }
        return true;
    }
}
