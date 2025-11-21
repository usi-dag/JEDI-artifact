package s2s.query_compiler.template.imperative;

import s2s.planner.qp.Field;
import s2s.planner.qp.Schema;
import s2s.planner.qp.operators.*;
import s2s.planner.qp.expressions.Expression;
import s2s.planner.qp.expressions.InputRef;
import s2s.planner.qp.operators.*;
import s2s.query_compiler.options.SingleFieldTuplesConverter;
import s2s.query_compiler.TypingUtils;
import s2s.query_compiler.template.CodeTemplateBasedVisitor;
import s2s.query_compiler.template.CodegenContext;
import s2s.query_compiler.template.Declaration;
import s2s.query_compiler.template.stream.StreamCompilerOptions;

import static s2s.query_compiler.template.CodeTemplateUtils.*;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

// a visitor for a single pipeline - imperative code generation
class SqlOpToJavaImperativeVisitor extends CodeTemplateBasedVisitor {

    private record ConsumerOperator(Function<String, String> consumer) {}

    final Operator root;
    final ImperativeCompilerOptions options;

    String outputClassName = "";
    String code = "";
    String finalBody;
    Schema outputSchema;
    ConsumerOperator currentRowConsumer;


    private SqlOpToJavaImperativeVisitor(CodegenContext codegenContext, Operator root, ImperativeCompilerOptions options) {
        super(codegenContext);
        this.root = root;
        this.options = options;
    }

    SqlOpToJavaImperativeVisitor(Operator root, ImperativeCompilerOptions options) {
        this(new CodegenContext(), root, options);
    }


    static SqlOpToJavaImperativeVisitor visit(Operator operator, ImperativeCompilerOptions options) {
        SqlOpToJavaImperativeVisitor visitor = new SqlOpToJavaImperativeVisitor(operator, options);
        visitor.visitRoot();
        return visitor;
    }

    private void visitRoot() {
        // Optimizations for the last operators:
        // if last is sort or limit, result is ready as it is (either list or array)
        if(root instanceof Sort || root instanceof Limit) {
            // note that this opt is checked directly in the visit(Sort/Limit) method
            // by checking if this.consumer is null
            root.accept(this);
            finalBody = code;
        }
        else {
            // the general case simply puts elements in a list
            String finalListType = "ArrayList";
            String finalListName = codegenContext.freshName("result");
            currentRowConsumer = new ConsumerOperator(row -> finalListName+".add("+row+");");
            root.accept(this);
            String boxedOutputClassName = TypingUtils.boxed(outputClassName);
            declarations.add(new Declaration(
                    finalListName,
                    finalListType+"<" + boxedOutputClassName + ">",
                    "new " + finalListType + "<>()"));

            finalBody = code + EOL + "return " + finalListName + ";";
        }
    }

    String body() {
        return finalBody;
    }

    public Schema getOutputSchema() {
        return outputSchema;
    }

    @Override
    protected SingleFieldTuplesConverter getSingleFieldTuplesConverter() {
        return options.getSingleFieldTuplesConverter();
    }

    @Override
    protected StreamCompilerOptions.MultiThreading getMultithreadingOption() {
        return StreamCompilerOptions.MultiThreading.SEQUENTIAL;
    }

    @Override
    public void visit(ArrayTableScan scan) {
        outputClassName = scan.source().typeName();
        outputSchema =  scan.getSchema();

        // TODO generalize hardcoded db.
        String accessor = "db.%s_arr()".formatted(scan.source().name());
        String rowVarName = codegenContext.freshName("row");
        String consumerCode = indent(currentRowConsumer.consumer.apply(rowVarName));
        code = genForEach(outputClassName, rowVarName, accessor, consumerCode);
    }

    @Override
    public void visit(RemovableProject project) {
        project.getChild().accept(this);
        outputSchema = visitRemovableProject(project, outputSchema);
    }

    @Override
    public void visit(Project project) {
        final Schema projectSchema = project.getSchema();
        final Schema inputSchema = project.getChild().getSchema();
        final Expression[] projs = project.getProjections();

        final SingleFieldTuplesConverter converter = options.getSingleFieldTuplesConverter();
        final String projRowVarName = codegenContext.freshName("projRow");
        final ConsumerOperator myConsumer = currentRowConsumer;

        // to be defined in each branch below
        final String myOutputClassName;
        final Schema myOutputSchema;
        final Function<String, String> exprCodeFunc;
        if (converter.isUnwrappedSchema(projectSchema)) {
            // In this case, we have an unwrapped stream (primitive or object) -- no need to generate a record class
            Class<?> exprType = projectSchema.getFields()[0].getType();
            if (converter.isPrimitiveUnwrappedSchema(projectSchema)) {
                myOutputClassName = exprType.getSimpleName();
            } else {
                myOutputClassName = TypingUtils.boxed(exprType).getSimpleName();
            }
            myOutputSchema = project.getSchema();
            exprCodeFunc = row -> convertExpr(projs[0], inputSchema, row+".");
        } else {
            // project a tuple type: generate enclosing record
            // note: since it generates a record, schema fields are getters
            myOutputSchema = projectSchema.withFieldsAsGetters(true);
            String[] projNames = new String[projs.length];
            for (int i = 0; i < projs.length; i++) {
                projNames[i] = myOutputSchema.getFields()[i].getName();
            }
            String projClassName = getOrCreateRecordType(projs, "ProjClass", projNames);
            myOutputClassName = projClassName;
            exprCodeFunc = row -> getConstructorInvocationCode(projClassName, inputSchema, row+".", projs);
        }
        currentRowConsumer = new ConsumerOperator(row -> """
                   %s %s = %s;
                   %s
                   """.formatted(
                myOutputClassName, projRowVarName, exprCodeFunc.apply(row),
                myConsumer.consumer.apply(projRowVarName)));
        project.getChild().accept(this);
        outputClassName = myOutputClassName;
        outputSchema = myOutputSchema;

    }

    @Override
    public void visit(Predicate predicate) {
        ConsumerOperator myConsumer = currentRowConsumer;
        currentRowConsumer = new ConsumerOperator(row -> {
            String conditionCode = convertExpr(predicate.getExpression(), predicate.getChild().getSchema(), row + ".");
            String consumerCode = myConsumer.consumer.apply(row);
            return genIf(conditionCode, consumerCode);
        });
        predicate.getChild().accept(this);
    }

    @Override
    public void visit(Aggregate aggregate) {
        SqlAggFunction[] aggregations = aggregate.getAggregations();
        Expression[] groupKeyExpressions = aggregate.getGroupKeys();
        Schema inputSchema = aggregate.getChild().getSchema();
        boolean hasGroups = groupKeyExpressions.length > 0;

        // class for the collector
        // each output field in the schema should provide:
        //  - a sequence (usually of length one) of fields to be declared and initialized
        //  - a piece of code each generated method, i.e., accumulate, combine and finish

        Field[] schemaFields = aggregate.getSchema().getFields();
        List<AggregationFieldCodeGen> fieldsCodeGen = new LinkedList<>();

        // first codegen for key fields
        int schemaFieldsIdx = 0;
        for (int groupIndex : aggregate.getGroupSelector()) {
            Field schemaField = schemaFields[schemaFieldsIdx++];
            String varName = schemaField.getName();
            String varType = schemaField.getType().getSimpleName();
            Expression groupKeyExpression = groupKeyExpressions[groupIndex];
            String exprStrAccu = convertExpr(groupKeyExpression, inputSchema, "row.");

            // TODO manage concurrent accumulators?
            // TODO here I could use the optimized SQLGroupBy,
            //  let's do that iff I also apply it to the stream codegen
            fieldsCodeGen.add(new AggregationFieldCodeGen(
                    Declaration.unInitialized(varName, varType),
                    varName + " = " + exprStrAccu + ';',
                    "", ""
            ));
        }
        // then codegen aggregated fields
        for (SqlAggFunction aggFunction : aggregations) {
            Field schemaField = schemaFields[schemaFieldsIdx++];
            fieldsCodeGen.addAll(getOutputFieldCodeGenForAggregation(
                    schemaField.getName(),
                    schemaField.getType().getSimpleName(),
                    aggFunction,
                    inputSchema,
                    "row."));
        }

        BiFunction<Function<AggregationFieldCodeGen, String>, String, String> foldJoin = (f, sep) ->
                fieldsCodeGen
                        .stream()
                        .map(f)
                        .filter(s -> s != null && !s.isBlank())
                        .collect(Collectors.joining(sep));

        // Collector codegen
        String declarationsGenBody = foldJoin.apply(o -> o.declaration().toCode(), "\n\t");
        String accumulateGenBody = foldJoin.apply(AggregationFieldCodeGen::accumulate, "\n\t\t");
        String combineGenBody = foldJoin.apply(AggregationFieldCodeGen::combine, "\n\t\t");
        String finishGenBodyForAggFields = foldJoin.apply(AggregationFieldCodeGen::finish, "\n\t\t");

        String finishGenBodyForAggFinalizers = getFinishGenBodyForAggFinalizers(aggregate);

        final String collectorClassGenName = codegenContext.freshName("GroupByCollector");
        final String finishGenReturn;
        final String returnTypeName;
        final String finishGen;
        final String finisherGenBodyStr;
        final boolean needsFinisher;

        final boolean isUnwrappedSchema = getSingleFieldTuplesConverter().isUnwrappedSchema(aggregate.getSchema());
        final ConsumerOperator myConsumer = currentRowConsumer;

        if(isUnwrappedSchema) {
            needsFinisher = true;
            Field field = schemaFields[0];
            returnTypeName = TypingUtils.boxed(field.getType()).getSimpleName();
            finishGenReturn = "return this.%s;".formatted(field.getName());
            finisherGenBodyStr = finishGenBodyForAggFinalizers;
        } else {
            returnTypeName = collectorClassGenName;
            finisherGenBodyStr = finishGenBodyForAggFields + finishGenBodyForAggFinalizers;

            if (finisherGenBodyStr.isBlank()) {
                finishGenReturn = null;
                needsFinisher = false;
            } else {
                needsFinisher = true;
                finishGenReturn = "return this;";
            }
        }
        if(needsFinisher) {
            finishGen = """
                        %s finish() {
                            %s
                            %s
                        }
                        """.formatted(returnTypeName, finisherGenBodyStr, finishGenReturn);
        } else {
            finishGen = "";
        }



        // fields to be set in each branch below
        final boolean needsInnerClass;
        // TODO myOutputClassName is useless, returnTypeName above should be used instead of this
//        final String myOutputClassName;

        if (!hasGroups) {
            // a non grouped aggregation
            if (isUnwrappedSchema) {
                // TODO this condition is already checked above and this whole body could be moved above
                // TODO needsInnerClass will become useless if this body gets moved above
                // an aggregation which outputs a single value - generating a class is not needed
                needsInnerClass = false;

                // output type
//                Class<?> outputFieldType = aggregate.getSchema().getFields()[0].getType();
//                if(getSingleFieldTuplesConverter().isPrimitiveUnwrappedSchema(aggregate.getSchema())) {
//                    outputFieldType = TypingUtils.boxed(outputFieldType);
//                }
//                myOutputClassName = outputFieldType.getSimpleName();

                // generate the aggregation for `accumulate` and `finish` methods as above directly here
                SqlAggFunction aggFunction = aggregations[0];
                Field schemaField = schemaFields[0];
                String aggVarName = codegenContext.freshName(schemaField.getName());
                String aggVarType = schemaField.getType().getSimpleName();


                // get the codegen for the agg fields
                // note that we can have more than one fields in case of average and nullability

                List<AggregationFieldCodeGen> fieldsForAggOuter = new LinkedList<>();
                currentRowConsumer = new ConsumerOperator(row -> {
                    List<AggregationFieldCodeGen> fieldsForAgg = getOutputFieldCodeGenForAggregation(
                            aggVarName, aggVarType, aggFunction, inputSchema, row+'.');
                    fieldsForAggOuter.addAll(fieldsForAgg);

                    return fieldsForAgg.stream()
                            .map(AggregationFieldCodeGen::accumulate)
                            .collect(Collectors.joining(EOL));
                });
                aggregate.getChild().accept(this);
                // TODO check if for some reason I need to codegen the declarations below
                // instead of adding the into the declarations list of this visitor
                fieldsForAggOuter.forEach(f -> declarations.add(f.declaration()));
                String finisherCode = fieldsForAggOuter.stream()
                        .map(AggregationFieldCodeGen::finish)
                        .filter(s -> s != null && !s.isBlank())
                        .collect(Collectors.joining(EOL));
                if(!finisherCode.isBlank()) {
                    code += EOL + finisherCode;
                }
                String aggFinisherCode = finisherGenBodyStr.replace(schemaField.getName(), aggVarName);
                if(!aggFinisherCode.isBlank()) {
                    code += EOL + aggFinisherCode;
                }
                code += EOL + myConsumer.consumer().apply(aggVarName);

            } else {
                needsInnerClass = true;
                String collectorVarName = codegenContext.freshName("groupByCollector");
                String collectorInitCode = "new " + collectorClassGenName + "()";
                currentRowConsumer = new ConsumerOperator(row ->
                        "%s.accumulate(%s);".formatted(collectorVarName, row));
                aggregate.getChild().accept(this);
                declarations.add(new Declaration(collectorVarName, collectorClassGenName, collectorInitCode));
//                myOutputClassName = returnTypeName;
                String consumerCode = myConsumer.consumer().apply(collectorVarName);
                if(needsFinisher)
                    code += EOL + "%s.finish();".formatted(collectorVarName);
                code += EOL + consumerCode;
            }


        } else {
            // a group-by aggregation
            needsInnerClass = true;
//            myOutputClassName = collectorClassGenName;

            // generate record class code for the key and the lambda to create it in .computeIfAbsent
            final String keyClassName;
            final Function<String, String> keyGetterCodeFunction;
            if (groupKeyExpressions.length == 1) {
                keyClassName = TypingUtils.boxed(groupKeyExpressions[0].type().getSimpleName());
                keyGetterCodeFunction = row ->
                        convertExpr(groupKeyExpressions[0], inputSchema, row+'.');
            } else {
                keyClassName = getOrCreateRecordType(groupKeyExpressions, "AggKey");
                keyGetterCodeFunction =  row -> getConstructorInvocationCode(
                        keyClassName, inputSchema, row+'.', groupKeyExpressions);
            }

            final String hmName = codegenContext.freshName("groupByMap");
            final String mapValueName = codegenContext.freshName("group");
            currentRowConsumer = new ConsumerOperator(row -> {
                String keyGetterCode = keyGetterCodeFunction.apply(row);

                // TODO optimize here, I could use the SQLGroupByCollector that takes the key and
                // set the key params
                String groupGetter = "%s %s = %s.computeIfAbsent(%s, x -> new %s());".formatted(
                        collectorClassGenName, mapValueName, hmName, keyGetterCode, collectorClassGenName);
                String accumulatorCode = "%s.accumulate(%s);".formatted(mapValueName, row);
                return groupGetter + EOL + accumulatorCode;
            });
            aggregate.getChild().accept(this);

            final String hmTypeName = "FinalHashMap<%s, %s>".formatted(
                    keyClassName, collectorClassGenName);
            declarations.add(new Declaration(hmName, hmTypeName, "new FinalHashMap<>()"));

            final String hmFinalName;
            String methodName = generateStandalonePipelineMethodFromVisitor(
                    this, "Aggregation", "getGroupAgg", hmTypeName, hmName);
            hmFinalName = codegenContext.freshName("groupAgg");
            code = "%s %s = %s(db);".formatted(hmTypeName, hmFinalName, methodName);


            // codegen consumer code
            String loopVarName = codegenContext.freshName("group");
            final String loopBody;
            if(needsFinisher) {
                String finishVarName = codegenContext.freshName("finished");
                String consumerCode = myConsumer.consumer().apply(finishVarName);
                String finishedVarCode = "%s %s = %s.finish();".formatted(returnTypeName, finishVarName, loopVarName);
                loopBody = finishedVarCode + EOL + consumerCode;
            } else {
                loopBody = myConsumer.consumer().apply(loopVarName);
            }

            code += EOL + genForEach(collectorClassGenName, loopVarName, hmFinalName + ".values()", loopBody);
        }


        if(needsInnerClass) {
            String collectorClassGenCode = String.format("""
                            public static final class %s {
                                %s
                            
                                void accumulate(%s row) {
                                    %s
                                }
                            
                                %s combine(%s other) {
                                    %s
                                    return this;
                                }
                            
                                %s
                            }
                            """,
                    collectorClassGenName,
                    declarationsGenBody,
                    outputClassName,
                    accumulateGenBody,
                    collectorClassGenName,
                    collectorClassGenName,
                    combineGenBody,
                    finishGen);
            codegenContext.addInnerClass(collectorClassGenName, collectorClassGenCode);
        }
        outputClassName = returnTypeName;//myOutputClassName;
        outputSchema = aggregate.getSchema();
    }


    @Override
    public void visit(Sort sort) {
        String listVarName = codegenContext.freshName("sortList");
        ConsumerOperator myConsumer = currentRowConsumer;
        currentRowConsumer = new ConsumerOperator(row -> "%s.add(%s);".formatted(listVarName, row));
        sort.getChild().accept(this);

        String parametricTypeName ="<" + outputClassName + ">";
        String listVarInit = "new ArrayList<>()";
        declarations.add(new Declaration(listVarName, "ArrayList" + parametricTypeName, listVarInit));
        String rowVarName = codegenContext.freshName("row");
        code += EOL + "%s.sort(%s);".formatted(listVarName, generateSortComparators(sort, outputSchema, outputClassName));
        if(myConsumer != null) {
            // common case: this sort operator is not the root of the QP
            String consumerCode = myConsumer.consumer.apply(rowVarName);
            code += genForEach(outputClassName, rowVarName, listVarName, consumerCode);
        } else {
            // this is the root, just return the sorted list
            code += EOL + "return %s;".formatted(listVarName);
        }
    }


    @Override
    public void visit(Skip skip) {
        String countVarName = codegenContext.freshName("skipCounter");
        ConsumerOperator myConsumer = currentRowConsumer;
        currentRowConsumer = new ConsumerOperator(row -> """
                if(++%s > %s) {
                    %s
                }
                """.formatted(countVarName, skip.getSkip(), myConsumer.consumer.apply(row)));
        skip.getChild().accept(this);
    }

    @Override
    public void visit(Limit limit) {
        String listVarName = codegenContext.freshName("limitArrayList");
        String countVarName = codegenContext.freshName("limitCounter");

        ConsumerOperator myConsumer = currentRowConsumer;
        currentRowConsumer = new ConsumerOperator(row -> {
            String insertCode = "%s.add(%s);".formatted(listVarName, row);
            String checkExitCode = "if(++%s == %s) break;".formatted(countVarName, limit.getLimit());
            return insertCode + EOL + checkExitCode;
        });

        limit.getChild().accept(this);

        String arrayListType = "ArrayList<" + outputClassName + ">";
        String arrayVarInit = "new ArrayList<>(" + limit.getLimit() + ")";
        declarations.add(new Declaration(listVarName, arrayListType, arrayVarInit));
        declarations.add(new Declaration(countVarName, "int", "0"));
        String rowVarName = codegenContext.freshName("row");
        if(myConsumer != null) {
            // common case: this limit operator is not the root of the QP
            String consumerCode = indent(myConsumer.consumer.apply(rowVarName));
            code += EOL + genForEach(outputClassName, rowVarName, listVarName, consumerCode);
        } else {
            // optimization: this limit operator is the root of the QP
            code += EOL + "return %s;".formatted(listVarName);
        }
    }

    @Override
    public void visit(OffsetFetch offsetFetch) {
        String listVarName = codegenContext.freshName("limitArrayList");
        String countFetchVarName = codegenContext.freshName("fetchCounter");
        String countOffsetVarName = codegenContext.freshName("offsetCounter");

        int fetch = offsetFetch.getFetch();
        ConsumerOperator myConsumer = currentRowConsumer;
        currentRowConsumer = new ConsumerOperator(row -> {
            String insertCode = "%s.add(%s);".formatted(listVarName, row);
            String checkExitCode = "if(++%s == %s) break;".formatted(countFetchVarName, fetch);
            String body = insertCode + EOL + checkExitCode;
            return """
                    if(++%s > %s) {
                        %s
                    }
                    """.formatted(countOffsetVarName, offsetFetch.getOffset(), body);
        });

        offsetFetch.getChild().accept(this);

        String arrayListType = "ArrayList<" + outputClassName + ">";
        String arrayVarInit = "new ArrayList<>(" + fetch + ")";
        declarations.add(new Declaration(listVarName, arrayListType, arrayVarInit));
        declarations.add(new Declaration(countFetchVarName, "int", "0"));
        declarations.add(new Declaration(countOffsetVarName, "int", "0"));
        String rowVarName = codegenContext.freshName("row");
        String consumerCode = indent(myConsumer.consumer.apply(rowVarName));
        code += EOL + genForEach(outputClassName, rowVarName, listVarName, consumerCode);
    }


    @Override
    public void visit(HashJoin join) {
        Join.JoinType joinType = join.joinType();
        ConsumerOperator myConsumer = currentRowConsumer;

        VisitJoinInfo joinInfo = visitJoinCommonInfo(join);

        // join key getters
        final Schema leftSchema = join.left().getSchema();
        final Schema rightSchema = join.right().getSchema();
        final String keyTypeName;
        final Function<String, String> leftKeyGetterCodeFunc, rightKeyGetterCodeFunc;

        if (join.leftKeyGetters().length == 1 /*&& options.getSingleFieldTuplesConverter().isUnwrapped()*/) { // assuming #leftkeys == #rightkeys
            keyTypeName = TypingUtils.boxed(join.leftKeyGetters()[0].type().getSimpleName());
            leftKeyGetterCodeFunc = rowName -> convertExpr(join.leftKeyGetters()[0], leftSchema, rowName + ".");
            rightKeyGetterCodeFunc = rowName -> convertExpr(join.rightKeyGetters()[0], rightSchema, rowName + ".");
        } else {
            keyTypeName = getOrCreateRecordType(join.leftKeyGetters(), "JoinKey");
            leftKeyGetterCodeFunc = rowName -> getConstructorInvocationCode(
                    keyTypeName,
                    leftSchema,
                    rowName + ".",
                    join.leftKeyGetters());
            rightKeyGetterCodeFunc = rowName -> getConstructorInvocationCode(
                    keyTypeName,
                    rightSchema,
                    rowName + ".",
                    join.rightKeyGetters());
        }



        if(joinType == Join.JoinType.INNER) {
            String hmVarName = codegenContext.freshName("joinHashMap");

            // left side populates the map
            String listTypeErased = "ArrayList";

            SqlOpToJavaImperativeVisitor leftVisitor = new SqlOpToJavaImperativeVisitor(
                    codegenContext, join.left(), options);
            leftVisitor.currentRowConsumer = new ConsumerOperator(row ->
                    "%s.computeIfAbsent(%s, %s).add(%s);".formatted(
                            hmVarName,
                            leftKeyGetterCodeFunc.apply(row),
                            codegenContext.freshName("") + " -> new " + listTypeErased + "<>()",
                            row));
            join.left().accept(leftVisitor);

            final String leftOutputClassName = TypingUtils.boxed(leftVisitor.outputClassName);
            final String listType = "%s<%s>".formatted(listTypeErased, leftOutputClassName);
            final String hmTypeErased = "FinalHashMap";
            final String hmType = "%s<%s, %s>".formatted(hmTypeErased, keyTypeName, listType);
            leftVisitor.declarations.add(new Declaration(hmVarName, hmType, "new " + hmTypeErased + "<>()"));

            final String leftSideCode;
            String methodName = generateStandalonePipelineMethodFromVisitor(
                    leftVisitor, "Inner Join", "getInnerJoinMap", hmType, hmVarName);
            declarations.add(new Declaration(hmVarName, hmType, methodName + "(db)"));
            leftSideCode = generateAndClearDeclarations();


            // right side probe from the map
            String joinRowVarName = codegenContext.freshName("joinRow");
            String leftRowName = codegenContext.freshName("innerRow");
            String joinTypePlaceHolder = codegenContext.freshName("joinTypePlaceHolder");
            currentRowConsumer = new ConsumerOperator(rightRowName -> {
                String rowKeyGetterCode = rightKeyGetterCodeFunc.apply(rightRowName);
                String hmGetCode = "%s.getOrDefault(%s, EmptyArrayList.get())".formatted(hmVarName, rowKeyGetterCode);

                return generateInnerLoopForInnerJoin(
                        join, joinInfo,
                        leftRowName, rightRowName,
                        myConsumer,
                        joinTypePlaceHolder,
                        joinRowVarName,
                        leftOutputClassName,
                        hmGetCode
                );
            });

            join.right().accept(this);

            outputClassName = joinInfo.getOuputClassName(leftOutputClassName, outputClassName);
            code = leftSideCode + EOL + code.replace(joinTypePlaceHolder, outputClassName);
            outputSchema = joinInfo.getOutputSchema();

        } else if (joinType == Join.JoinType.LEFT) {
            String hmVarName = codegenContext.freshName("leftJoinHashMap");

            // left side populates the map
            String listTypeErased = "ArrayList";

            SqlOpToJavaImperativeVisitor rightVisitor = new SqlOpToJavaImperativeVisitor(
                    codegenContext, join.right(), options);
            rightVisitor.currentRowConsumer = new ConsumerOperator(row ->
                    "%s.computeIfAbsent(%s, %s).add(%s);".formatted(
                            hmVarName,
                            rightKeyGetterCodeFunc.apply(row),
                            codegenContext.freshName("") + " -> new " + listTypeErased + "<>()",
                            row));
            join.right().accept(rightVisitor);

            final String rightSideCode;
            final String rightOutputClassName = rightVisitor.outputClassName;
            final String listType = "%s<%s>".formatted(listTypeErased, rightOutputClassName);
            final String hmTypeErased = "FinalHashMap";
            final String hmType = "%s<%s, %s>".formatted(hmTypeErased, keyTypeName, listType);
            rightVisitor.declarations.add(new Declaration(hmVarName, hmType, "new " + hmTypeErased + "<>()"));
            String semiOrAnti = join.joinType() == Join.JoinType.SEMI ? "SemiJoin" : "AntiJoin";
            String methodName = generateStandalonePipelineMethodFromVisitor(
                    rightVisitor, semiOrAnti, "get"+semiOrAnti+"Map", hmType, hmVarName);
            declarations.add(new Declaration(hmVarName, hmType, methodName + "(db)"));
            rightSideCode = generateAndClearDeclarations();


            // left side probe from the map
            String joinRowVarName = codegenContext.freshName("joinRow");
            String rightRowName = codegenContext.freshName("rightSideRow");
            String typeNamePlaceHolder = codegenContext.freshName("typeNamePlaceHolder");
            currentRowConsumer = new ConsumerOperator(leftRowName -> {

                Function<InputRef, String> nameBinderRightNull = inputRef -> {
                    int idx = inputRef.index();
                    int nLeft = leftSchema.getFields().length;
                    if (idx < nLeft) {
                        if (this.options.getSingleFieldTuplesConverter().isUnwrappedSchema(leftSchema)) {
                            return leftRowName;
                        } else {
                            return leftRowName + "." + leftSchema.getFields()[idx].getName();
                        }
                    } else {
                        return "null";
                    }
                };

                String rowKeyGetterCode = leftKeyGetterCodeFunc.apply(leftRowName);

                Function<InputRef, String> nameBinder = joinInfo.getNameBinder(leftRowName, rightRowName);
                String consumerCode = myConsumer.consumer.apply(joinRowVarName);

                consumerCode = maybeWrapWithNonEquiCondition(join, consumerCode, nameBinder);

                String joinRowCode = joinInfo.getMapperCode(leftRowName, rightRowName);
                String joinRowCodeForRightNull = getConstructorInvocationCode(
                        joinInfo.getOuputClassName(null, rightOutputClassName),
                        join.getSchema(), nameBinderRightNull, join.mapper());
                String joinRowInit = "%s %s = %s;".formatted(
                        typeNamePlaceHolder, joinRowVarName, joinRowCode);

                // we need to generate two cases: 1) matching rows, 2) no matching
                // (1)
                String rightSideListVarName = codegenContext.freshName("rightSideList");
                String hmGetCode = "%s.get(%s)".formatted(hmVarName, rowKeyGetterCode);
                String rightSideListGetCode = listType + " " + rightSideListVarName + " = " + hmGetCode;
                String innerLoopBody = joinRowInit + EOL + consumerCode;
                String innerLoop = genForEach(rightOutputClassName, rightRowName, rightSideListVarName, innerLoopBody);

                // (2)
                String joinRowInitForRightNull = "%s %s = %s;".formatted(
                        typeNamePlaceHolder, joinRowVarName, joinRowCodeForRightNull);
                String nullMatch = joinRowInitForRightNull + EOL + consumerCode;

                return """
                        %s;
                        if(%s != null) {
                        %s
                        } else {
                        %s
                        }
                        """
                        .formatted(
                                rightSideListGetCode,
                                rightSideListVarName,
                                indent(innerLoop),
                                indent(nullMatch));
            });

            join.left().accept(this);

            outputClassName = joinInfo.getOuputClassName(outputClassName, rightOutputClassName);
            code = rightSideCode + EOL + code.replace(typeNamePlaceHolder, outputClassName);
            outputSchema = joinInfo.getOutputSchema();



        } else if (joinType == Join.JoinType.SEMI || joinType == Join.JoinType.ANTI) {
            // Note: here I am only implementing right-(semi|anti) joins
            // (i.e., building a set from the right side of the join).

            // codegen collect into hashset
            final String hsName = codegenContext.freshName("hashset");
            final String setTypeErased = "HashSet";

            SqlOpToJavaImperativeVisitor rightVisitor = new SqlOpToJavaImperativeVisitor(
                    codegenContext, join.right(), options);
            rightVisitor.currentRowConsumer = new ConsumerOperator(row ->
                    "%s.add(%s);".formatted(hsName, rightKeyGetterCodeFunc.apply(row)));
            join.right().accept(rightVisitor);
            final String setType = setTypeErased + "<" + keyTypeName + ">";

            final String rightSideCode;
            rightVisitor.declarations.add(new Declaration(hsName, setType, "new " + setTypeErased + "<>()"));
            String methodName = codegenContext.freshName("getMap");
            String methodCode =
                    """
                    // StandalonePipelineMethod - SEMI/ANTI Join
                    public static %s %s(DB db) {
                        %s
                        %s
                        return %s;
                    }
                    """.formatted(
                            setType,
                            methodName,
                            rightVisitor.generateAndClearDeclarations(),
                            rightVisitor.code,
                            hsName);

            codegenContext.addMethodFixName(methodName, methodCode);
            declarations.add(new Declaration(hsName, setType, methodName + "(db)"));
            rightSideCode = "";


            final String maybeNegate = joinType == Join.JoinType.SEMI ? "" : "!";

            currentRowConsumer = new ConsumerOperator(leftRowName -> {
                String rowKeyGetterCode = leftKeyGetterCodeFunc.apply(leftRowName);
                String conditionCode = "(%s%s.contains(%s))".formatted(maybeNegate, hsName, rowKeyGetterCode);
                Function<InputRef, String> nameBinder = joinInfo.getNameBinder(leftRowName, null);
                String consumerCode = myConsumer.consumer.apply(leftRowName);

                if(join.nonEquiCondition() != null) {
                    String nonEquiCode = convertExpr(join.nonEquiCondition(), join.getSchema(), nameBinder);
                    conditionCode += " && " + nonEquiCode;
                }

                return genIf(conditionCode, consumerCode);
            });

            join.left().accept(this);

            code = rightSideCode + EOL + code;
            // note: there is no need to set outputClassName/Schema.
            // they are already correctly set by visiting the left side of this join
        }
    }

    @Override
    public void visit(NestedLoopJoin join) {
        VisitJoinInfo joinInfo = visitJoinCommonInfo(join);
        ConsumerOperator myConsumer = currentRowConsumer;

        // build a list on the left side
        String leftListVarName = codegenContext.freshName("nlJoinList");
        String leftLististTypeErased = "ArrayList";
        SqlOpToJavaImperativeVisitor leftVisitor = new SqlOpToJavaImperativeVisitor(
                codegenContext, join.left(), options);

        leftVisitor.currentRowConsumer = new ConsumerOperator(row ->
                "%s.add(%s);".formatted(leftListVarName, row));
        join.left().accept(leftVisitor);
        final String leftOutputClassName = TypingUtils.boxed(leftVisitor.outputClassName);
        final String listType = "%s<%s>".formatted(leftLististTypeErased, leftOutputClassName);
        leftVisitor.declarations.add(new Declaration(leftListVarName, listType, "new " + leftLististTypeErased + "<>()"));

        final String leftSideCode;
        String methodName = codegenContext.freshName("getNLJoinList");
        String methodCode =
                """
                // StandalonePipelineMethod - Inner NL Join
                public static %s %s(DB db) {
                    %s
                    %s
                    return %s;
                }
                """.formatted(
                        listType,
                        methodName,
                        leftVisitor.generateAndClearDeclarations(),
                        leftVisitor.code,
                        leftListVarName);

        codegenContext.addMethodFixName(methodName, methodCode);
        declarations.add(new Declaration(leftListVarName, listType, methodName + "(db)"));
        leftSideCode = generateAndClearDeclarations();

        // probe the scan left-side list for each element on the right side
        String joinRowVarName = codegenContext.freshName("joinRow");
        String leftRowName = codegenContext.freshName("innerRow");
        String joinTypePlaceHolder = codegenContext.freshName("joinTypePlaceHolder");
        currentRowConsumer = new ConsumerOperator(rightRowName -> generateInnerLoopForInnerJoin(
                join, joinInfo,
                leftRowName, rightRowName,
                myConsumer,
                joinTypePlaceHolder,
                joinRowVarName,
                leftOutputClassName,
                leftListVarName
        ));

        join.right().accept(this);

        outputClassName = joinInfo.getOuputClassName(leftOutputClassName, outputClassName);
        code = leftSideCode + EOL + code.replace(joinTypePlaceHolder, outputClassName);
        outputSchema = joinInfo.getOutputSchema();
    }

    @Override
    public void visit(SingletonFilterJoin join) {
        if (join.joinType() != Join.JoinType.INNER) {
            throw new RuntimeException("TODO - Unsupported singleton join type: " + join.joinType());
        }
        if(! (join.left() instanceof Aggregate agg && agg.getGroupKeys().length == 0 && agg.getAggregations().length == 1)) {
            throw new RuntimeException("SingletonFilterJoin should have a scalar (non grouped) aggregation on the left side");
        }

        String leftScalarVarName = codegenContext.freshName("leftVal");
        SqlOpToJavaImperativeVisitor leftVisitor = new SqlOpToJavaImperativeVisitor(codegenContext, null, options);

        final String leftSideCode;
        leftVisitor.currentRowConsumer = new ConsumerOperator(row ->
                "return " + row + ";");
        leftVisitor.visit(agg);
        String leftSideType = leftVisitor.outputClassName;

        String methodName = generateStandalonePipelineMethod(
                "SingletonFilterJoin",
                "getScalarValueSingletonJoin",
                leftSideType,
                leftVisitor.generateAndClearDeclarations() + EOL + leftVisitor.code);
        declarations.add(new Declaration(leftScalarVarName, leftSideType, methodName + "(db)"));
        leftSideCode = generateAndClearDeclarations();


        ConsumerOperator myConsumer = currentRowConsumer;
        currentRowConsumer = new ConsumerOperator(row -> {
            // TODO check this name binder - likely wrong -
            //  indeed, refers to right schema not yet visited
            Function<InputRef, String> nameBinder = inputRef -> {
                Schema leftSchema = leftVisitor.outputSchema;
                // TODO check the following line, it may be wrong
                // it should be outputschema of join visit, but it not possible...
                // because the join visit cannot be done, yet
                Schema rightSchema = join.right().getSchema();
                int idx = inputRef.index();
                int nLeft = 1;
                if (idx < nLeft) {
                    if (this.options.getSingleFieldTuplesConverter().isUnwrappedSchema(leftSchema)) {
                        return leftScalarVarName;
                    } else {
                        return leftScalarVarName + "." + leftSchema.getFields()[idx].getName();
                    }
                } else {
                    if (this.options.getSingleFieldTuplesConverter().isUnwrappedSchema(rightSchema)) {
                        return row;
                    } else {
                        return row + "." + rightSchema.getFields()[idx - nLeft].getName();
                    }
                }
            };
            String conditionCode = convertExpr(join.getLeftFilterExpression(), join.getSchema(), nameBinder);
            String consumerCode = myConsumer.consumer.apply(row);
            return genIf(conditionCode, consumerCode);
        });

        join.right().accept(this);
        code = leftSideCode + EOL + code;
    }


    // Utility for splitting pipelines
    private String generateStandalonePipelineMethodFromVisitor(SqlOpToJavaImperativeVisitor visitor,
                                                               String comment,
                                                               String methodNamePrefix,
                                                               String returnType,
                                                               String returnVarName) {
        String methodBody = """
                        %s
                        %s
                        return %s;
                        """.formatted(visitor.generateAndClearDeclarations(), visitor.code, returnVarName);
        return generateStandalonePipelineMethod(comment, methodNamePrefix, returnType, methodBody);
    }


    // Utility for joins

    private String generateInnerLoopForInnerJoin(Join join,
                                                 VisitJoinInfo joinInfo,
                                                 String leftRowName,
                                                 String rightRowName,
                                                 ConsumerOperator myConsumer,
                                                 String joinTypePlaceHolder,
                                                 String joinRowVarName,
                                                 String leftOutputClassName,
                                                 String iterable) {

        Function<InputRef, String> nameBinder = joinInfo.getNameBinder(leftRowName, rightRowName);

        String consumerCode = myConsumer.consumer.apply(joinRowVarName);
        consumerCode = maybeWrapWithNonEquiCondition(join, consumerCode, nameBinder);

        String joinRowCode = joinInfo.getMapperCode(leftRowName, rightRowName);
        String joinRowInit = "%s %s = %s;".formatted(
                joinTypePlaceHolder, joinRowVarName, joinRowCode);
        String loopBody = joinRowInit + EOL + consumerCode;
        return genForEach(leftOutputClassName, leftRowName, iterable, loopBody);

    }

    private String maybeWrapWithNonEquiCondition(Join join, String consumerCode,
                                                 Function<InputRef, String> nameBinder) {
        if(join.nonEquiCondition() == null) return consumerCode;
        String nonEquiConditionCode = convertExpr(join.nonEquiCondition(), join.getSchema(), nameBinder);
        return genIf(nonEquiConditionCode, consumerCode);
    }


    private VisitJoinInfo visitJoinCommonInfo(Join join) {
        if (join.isLeftProjectMapper()) {
            return new InnerJoinProjectOnlyLeft(join, options);
        } else if (join.isRightProjectMapper()) {
            return new InnerJoinProjectOnlyRight(join, options);
        } else if (join.mapper().length == 1) {
            return new SingleElementJoinConverter(join, options);
        } else {
            return new DefaultJoinConverter(join, options);
        }
    }

    abstract static class VisitJoinInfo {

        final Join join;
        final Schema leftSchema, rightSchema;
        final int nLeft;
        final ImperativeCompilerOptions options;


        private VisitJoinInfo(Join join, ImperativeCompilerOptions options) {
            this.join = join;
            this.options = options;
            leftSchema = join.left().getSchema();
            rightSchema = join.right().getSchema();
            nLeft = leftSchema.getFields().length;
        }


        // TODO get rid of this, it is unfortunately unreliable.
        // the rationale is that the real output schema can change after visiting
        // a node, and it must be always checked using visitor.outputSchema after the visit
        // to a child node.
        // here, on the other hand, the output schema is derived from the children schemas
        // which are queried before the visit
        abstract Schema getOutputSchema();

        abstract String getOuputClassName(String leftClassName, String rightClassName);

        abstract String getMapperCode(String leftName, String rightName);

        Function<InputRef, String> getNameBinder(String leftName, String rightName) {
            return inputRef -> {
                int idx = inputRef.index();
                int nLeft = leftSchema.getFields().length;
                if (idx < nLeft) {
                    if (this.options.getSingleFieldTuplesConverter().isUnwrappedSchema(leftSchema)) {
                        return leftName;
                    } else {
                        return leftName + "." + leftSchema.getFields()[idx].getName();
                    }
                } else {
                    if (this.options.getSingleFieldTuplesConverter().isUnwrappedSchema(rightSchema)) {
                        return rightName;
                    } else {
                        return rightName + "." + rightSchema.getFields()[idx - nLeft].getName();
                    }
                }
            };
        }
    }

    // delegators

    private class InnerJoinProjectOnlyRight extends VisitJoinInfo {
        public InnerJoinProjectOnlyRight(Join join, ImperativeCompilerOptions options) {
            super(join, options);
        }

        @Override
        Schema getOutputSchema() {
            return rightSchema;
        }

        @Override
        String getOuputClassName(String leftClassName, String rightClassName) {
            return rightClassName;
        }

        @Override
        String getMapperCode(String leftName, String rightName) {
            return rightName;
        }
    }


    private class InnerJoinProjectOnlyLeft extends VisitJoinInfo {
        public InnerJoinProjectOnlyLeft(Join join, ImperativeCompilerOptions options) {
            super(join, options);
        }

        @Override
        Schema getOutputSchema() {
            return leftSchema;
        }

        @Override
        String getOuputClassName(String leftClassName, String rightClassName) {
            return leftClassName;
        }

        @Override
        String getMapperCode(String leftName, String rightName) {
            return leftName;
        }
    }


    private class SingleElementJoinConverter extends VisitJoinInfo {
        final Expression expr;
        public SingleElementJoinConverter(Join join, ImperativeCompilerOptions options) {
            super(join, options);
            expr = join.mapper()[0];

        }

        @Override
        Schema getOutputSchema() {
            return join.getSchema();
        }

        @Override
        String getOuputClassName(String leftClassName, String rightClassName) {
            return expr.type().getSimpleName();
        }

        @Override
        String getMapperCode(String leftName, String rightName) {
            return convertExpr(expr, join.getSchema(), getNameBinder(leftName, rightName));
        }
    }

    private class DefaultJoinConverter extends VisitJoinInfo {
        final Schema schema;
        String outputClassName;
        public DefaultJoinConverter(Join join, ImperativeCompilerOptions options) {
            super(join, options);
            schema = join.getSchema().withFieldsAsGetters(true);
        }

        @Override
        Schema getOutputSchema() {
            return schema;
        }

        @Override
        String getOuputClassName(String leftClassName, String rightClassName) {
            // TODO fix this, it should not be needed (both the field and the check:
            //  the getOrCreate... should create only if not yet generated
            //  i.e., two calls to this method should be idempotent but currently they are
            //  not, since they create two different classes, returning their names!
            if(outputClassName == null) {
                outputClassName = getOrCreateRecordType(join.mapper(), "JoinedRecord", schema);
            }
            return outputClassName;
        }

        @Override
        String getMapperCode(String leftName, String rightName) {
            return getConstructorInvocationCode(
                    getOuputClassName(null, null),
                    schema,
                    getNameBinder(leftName, rightName),
                    join.mapper());
        }
    }



}
