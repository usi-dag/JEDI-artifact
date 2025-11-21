package s2s.query_compiler.template.stream;

import s2s.planner.qp.Field;
import s2s.planner.qp.Schema;
import s2s.planner.qp.operators.*;
import s2s.planner.qp.expressions.And;
import s2s.planner.qp.expressions.Expression;
import s2s.planner.qp.expressions.InputRef;
import s2s.query_compiler.TypingUtils;
import s2s.query_compiler.options.SingleFieldTuplesConverter;
import s2s.query_compiler.template.CodeTemplateBasedVisitor;
import s2s.query_compiler.template.CodegenContext;
import s2s.query_compiler.template.Declaration;

import java.text.MessageFormat;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

// a visitor for a single pipeline
class SqlOpToJavaStreamVisitor extends CodeTemplateBasedVisitor {
    private record VisitJoinInfo(
            String leftClassName,
            String leftSideCode,
            Schema leftSchema,
            String leftExprBinderPrefix,
            List<Declaration> leftDeclarations,
            String rightClassName,
            String rightSideCode,
            Schema rightSchema,
            String rightExprBinderPrefix,
            String nonEquiConditionCode,
            String mapperCode,
            String mapperLeftNullCode,
            String mapperRightNullCode
    ) {
    }



    final Operator root;
    final StreamCompilerOptions options;

    String outputClassName = "";
    String code = "";
    String finalBody;
    Schema outputSchema;


    SqlOpToJavaStreamVisitor(CodegenContext codegenContext, Operator root, StreamCompilerOptions options) {
        super(codegenContext);
        this.root = root;
        this.options = options;
    }

    SqlOpToJavaStreamVisitor(Operator root, StreamCompilerOptions options) {
        this(new CodegenContext(), root, options);
    }


    static SqlOpToJavaStreamVisitor visit(Operator operator, StreamCompilerOptions options) {
        SqlOpToJavaStreamVisitor visitor = new SqlOpToJavaStreamVisitor(operator, options);
        operator.accept(visitor);
        visitor.generateBody(operator.getSchema());
        return visitor;
    }

    private String toListCodeGen() {
        return options.useStreamToList()
                ? ".toList()"
                : ".collect(Collectors.toList())";
    }

    private void generateBody(Schema schema) {
        if (options.getSingleFieldTuplesConverter().isPrimitiveUnwrappedSchema(schema)) {
            code += ".boxed()";
        }
        finalBody = "return " + code + toListCodeGen() + ';' + EOL;
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
        return options.getMultiThreading();
    }

    @Override
    public void visit(ArrayTableScan scan) {
        String accessor = "db.%s_arr()".formatted(scan.source().name());
        code = "Arrays.stream(%s)%n".formatted(accessor);
        if(!options.getMultiThreading().isSequential()) {
            code += ".parallel()";
            if(options.isUnordered()) {
                code += ".unordered()";
            }
            code += EOL;
        }
        outputClassName = scan.source().typeName();
        outputSchema =  scan.getSchema();
    }

    @Override
    public void visit(RemovableProject project) {
        project.getChild().accept(this);
        outputSchema = visitRemovableProject(project, outputSchema);
    }

    @Override
    public void visit(Project project) {
        project.getChild().accept(this);

        Schema inputSchema = outputSchema;
        Expression[] projs = project.getProjections();

        if (options.getSingleFieldTuplesConverter().isUnwrappedSchema(project.getSchema())) {
            // In this case, we have an unwrapped stream (primitive or object) -- no need to generate a record class
            String expr = convertExpr(projs[0], inputSchema, "row.");
            Class<?> exprType = project.getSchema().getFields()[0].getType();
            if (options.getSingleFieldTuplesConverter().isPrimitiveUnwrappedSchema(project.getSchema())) {
                Class<?> childExprType = outputSchema.getFields()[0].getType();
                if(options.getSingleFieldTuplesConverter().isPrimitiveUnwrappedSchema(outputSchema) &&
                        childExprType.equals(exprType)) {
                    // if the child schema is also mapped to the same primitive we can simply map
                    code += ".map(row -> %s)%n".formatted(expr);
                } else {
                    // otherwise, we simply map to a primitive
                    outputClassName = exprType.getSimpleName();
                    code += ".mapTo%s(row -> %s)%n".formatted(TypingUtils.streamPrefix(exprType), expr);
                }
            } else {
                outputClassName = TypingUtils.boxed(exprType).getSimpleName();
                code += ".map(row -> %s)%n".formatted(expr);
            }
            outputSchema = project.getSchema();
        } else {
            // project a tuple type: generate enclosing record
            // note: since it generates a record, schema fields are getters
            Schema schema = outputSchema = project.getSchema().withFieldsAsGetters(true);
            String[] projNames = new String[projs.length];
            for (int i = 0; i < projs.length; i++) {
                projNames[i] = schema.getFields()[i].getName();
            }
            String projClassName = getOrCreateRecordType(projs, "ProjClass", projNames);
            String expr = getConstructorInvocationCode(projClassName, inputSchema, "row.", projs);
            code += ".map(row -> %s)%n".formatted(expr);
            outputClassName = projClassName;
        }
    }

    @Override
    public void visit(Predicate predicate) {
        predicate.getChild().accept(this);
        // if expr is AND check if it should be expanded
        if (predicate.getExpression() instanceof And and && !options.shouldCompressPredicateConjunction()) {
            StringBuilder sb = new StringBuilder();
            for (Expression sub : and.getExpressions()) {
                String lambda = convertExpr(sub, outputSchema, "row.");
                sb.append(".filter(row -> %s)%n".formatted(lambda));
            }
            code += sb.toString();
        } else {
            String lambda = convertExpr(predicate.getExpression(), outputSchema, "row.");
            code += ".filter(row -> %s)%n".formatted(lambda);
        }
    }

    @Override
    public void visit(Aggregate aggregate) {
        visit(aggregate, true);
    }

    private void visit(Aggregate aggregate, boolean wrapResultIntoStream) {
        aggregate.getChild().accept(this);
        SqlAggFunction[] aggregations = aggregate.getAggregations();
        Expression[] groupKeyExpressions = aggregate.getGroupKeys();
        Schema inputSchema = outputSchema;
        boolean hasGroups = groupKeyExpressions.length > 0;

        final boolean isConcurrentAggregation = options.getMultiThreading().isConcurrentCollector();
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

            fieldsCodeGen.add(new AggregationFieldCodeGen(
                    Declaration.unInitialized(varName, varType),
                    varName + " = " + exprStrAccu + ';',
                    "",
                    ""
            ));
        }
        // then codegen aggregated fields
        if(isConcurrentAggregation) {
            for (SqlAggFunction aggFunction : aggregations) {
                Field schemaField = schemaFields[schemaFieldsIdx++];
                fieldsCodeGen.addAll(getOutputFieldCodeGenForConcurrentAggregation(
                        schemaField.getName(),
                        schemaField.getType().getSimpleName(),
                        aggFunction,
                        inputSchema,
                        "row."));
            }
        } else {
            for (SqlAggFunction aggFunction : aggregations) {
                Field schemaField = schemaFields[schemaFieldsIdx++];
                fieldsCodeGen.addAll(getOutputFieldCodeGenForAggregation(
                        schemaField.getName(),
                        schemaField.getType().getSimpleName(),
                        aggFunction,
                        inputSchema,
                        "row."));
            }
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
        StringBuilder finishGenBody = new StringBuilder(foldJoin.apply(AggregationFieldCodeGen::finish, "\n\t\t"));

        String finishGenBodyForAggFinalizers = getFinishGenBodyForAggFinalizers(aggregate);
        String finisherGenBodyStr = finishGenBody.append(finishGenBodyForAggFinalizers).toString();
        String collectorClassGenName = codegenContext.freshName("GroupByCollector");
        String finishGen, asCollectorGenBody, innerCollectorGenBody;

        final boolean isUnwrappedSchema = getSingleFieldTuplesConverter().isUnwrappedSchema(aggregate.getSchema());

        if (isUnwrappedSchema) {
            innerCollectorGenBody = MessageFormat.format("""
                            ,
                            c -> c.{0}""",
                    schemaFields[0].getName());
        } else {
            innerCollectorGenBody = "";
        }

        final String collectorGetters, finishClassName, finishReturnExpression, characteristics;
        final Schema finalSchema;

        boolean anyIsConcurrentField = fieldsCodeGen.stream()
                .anyMatch(f -> {
                    String name = f.declaration().typeName();
                    return name.endsWith("Adder")
                            || name.endsWith("Accumulator")
                            || name.startsWith("Atomic");
                });
        if(isConcurrentAggregation && anyIsConcurrentField) {
            finalSchema = aggregate.getSchema().withFieldsAsGetters(true);
            // field types are now concurrent ones, we need to finish creating a new class
//            finishClassName = getOrCreateRecordType("FinalizedConcurrentAgg", aggregate.getSchema());
            finishClassName = collectorClassGenName;
            HashMap<String, AggregationFieldCodeGen> aggFields = new HashMap<>();
            for(AggregationFieldCodeGen f : fieldsCodeGen) {
                aggFields.put(f.declaration().name(), f);
            }
            // generate instantiation code which extract values from *Adders
            finishReturnExpression = "this";
            collectorGetters = Arrays.stream(schemaFields)
                    .map(outputField -> {
                        AggregationFieldCodeGen aggField = aggFields.get(outputField.getName());
                        String aggFieldName = aggField.declaration().name();
                        String aggFieldType = aggField.declaration().typeName();
                        boolean isInt = outputField.getType().getSimpleName().toLowerCase().startsWith("int");
                        String valueGetter = switch (aggFieldType) {
                            case "LongAdder", "LongAccumulator" -> isInt ? ".intValue()" : ".longValue()";
                            case "DoubleAdder", "DoubleAccumulator" -> ".doubleValue()";
                            case "AtomicDouble", "AtomicLong" -> ".get()";
                            default -> "";
                        };

                        return """
                                %s %s() {
                                    return %s%s;
                                }
                                """.formatted(outputField.getType().getSimpleName(), aggFieldName, aggFieldName, valueGetter);
                    })
                    .collect(Collectors.joining(";" + EOL));
            if(finisherGenBodyStr.isBlank()) {
                // we only need that finisherGenBodyStr is not blank...
                //finisherGenBodyStr = "// convert into primitives";
            }

            // concurrent aggregations cannot be combined
            combineGenBody = "if(true) throw new IllegalStateException(\"concurrent aggregations cannot be combined\");";

            characteristics = """
                    ,
                    Collector.Characteristics.CONCURRENT""";
        } else {
            collectorGetters = "";
            finishClassName = collectorClassGenName;
            finishReturnExpression = "this";
            finalSchema = aggregate.getSchema();
            characteristics = "";
        }
        if (finisherGenBodyStr.isBlank()) {
            finishGen = "";
            asCollectorGenBody = MessageFormat.format("""
                            Collector.of(
                                    {0}::new,
                                    {0}::accumulate,
                                    {0}::combine{1}{2})""",
                    collectorClassGenName, innerCollectorGenBody, characteristics);
        } else {
            if (isUnwrappedSchema) {
                finishGen = """
                        %s finish() {
                            %s
                            return this.%s;
                        }
                        """.formatted(TypingUtils.boxed(schemaFields[0].getType()).getSimpleName(), finisherGenBodyStr, schemaFields[0].getName());
            } else {
                finishGen = """
                        %s finish() {
                            %s
                            return %s;
                        }
                        """.formatted(finishClassName, finisherGenBodyStr, finishReturnExpression);
            }
            asCollectorGenBody = MessageFormat.format("""
                            Collector.of(
                                    {0}::new,
                                    {0}::accumulate,
                                    {0}::combine,
                                    {0}::finish{1})""",
                    collectorClassGenName, characteristics);
        }
        String returnCollectorClassGenName = finishClassName;
        if (isUnwrappedSchema) {
            returnCollectorClassGenName = TypingUtils.boxed(schemaFields[0].getType()).getSimpleName();
        }
        String asCollectorGen = """
                static Collector<%s, %s, %s> collector(){
                    return %s;
                }
                """.formatted(
                TypingUtils.boxed(outputClassName),
                collectorClassGenName,
                returnCollectorClassGenName,
                asCollectorGenBody);
        asCollectorGen = asCollectorGen.replace("%n", "%n\t");
        asCollectorGen = asCollectorGen.replace("\n", "\n\t");

        String collectorClassGenCode = String.format("""
                        public static final class %s {
                            %s
                            
                            %s
                            void accumulate(%s row) {
                                %s
                            }
                            
                            %s combine(%s other) {
                                %s
                                return this;
                            }
                            
                            %s
                            
                            %s
                        }
                        """,
                collectorClassGenName,
                declarationsGenBody,
                collectorGetters,
                outputClassName,
                accumulateGenBody,
                collectorClassGenName,
                collectorClassGenName,
                combineGenBody,
                finishGen,
                asCollectorGen);
        String oldOutputClassName = outputClassName;
        Schema oldOutputSchema = outputSchema;
        outputClassName = returnCollectorClassGenName;
        outputSchema = finalSchema;

        String collectorInitialization = collectorClassGenName + ".collector()";

        if (!hasGroups) {
            // a non grouped aggregation

            // finishGenBodyForAggFinalizers.isBlank() &&
            if(getSingleFieldTuplesConverter().isPrimitiveUnwrappedSchema(outputSchema)) {
                outputClassName = options.getSingleFieldTuplesConverter().isPrimitiveUnwrappedSchema(aggregate.getSchema())
                        ? aggregate.getSchema().getFields()[0].getType().getSimpleName()
                        : returnCollectorClassGenName;
                // this is a scalar aggregation that can be converted into a special case of reduction (e.g., .sum)
                // note that finalizers are not supported in this case
                SqlAggFunction aggFunc = aggregations[0];
                if(aggFunc.isCountStar()) {
                    code += ".count()";
                } else {
                    String streamPrefix = TypingUtils.streamPrefix(aggFunc.getType());
                    if (TypingUtils.maybePrimitiveOutputClass(oldOutputClassName).isEmpty()) {
                        // but the previously generated stream is not a primitive one
                        // likely, the aggregation has a non-trivial expression (i.e. not just an InputRef)
                        code += ".mapTo" + streamPrefix;
                        code += "(row -> %s)".formatted(convertExpr(aggFunc.getExpression(), oldOutputSchema, "row."));
                    }
                    // now the stream is a primitive one
                    String aggKind = aggFunc.getKind().name().toLowerCase();
                    if (aggKind.equals("avg")) aggKind = "average";
                    code += ".%s()".formatted(aggKind);
                    if(Set.of("max", "min", "average").contains(aggKind)) {
                        code += ".getAs%s()".formatted(streamPrefix);
                    }
                    if(!finishGenBodyForAggFinalizers.isBlank()) {
                        String finalizedVarName = codegenContext.freshName("finalized");
                        // this is a hack, it would be nice to clean it
                        String fieldName = fieldsCodeGen.get(0).declaration().name();
                        String hackyFinish = finishGenBodyForAggFinalizers.replace(fieldName, finalizedVarName);
                        declarations.add(new Declaration(finalizedVarName, outputClassName, code + "; " + hackyFinish));
                        code = finalizedVarName;
                    }
                }
            } else {
                codegenContext.addInnerClass(collectorClassGenName, collectorClassGenCode);
                code += ".collect(" + collectorInitialization + ")";
            }


            if(wrapResultIntoStream) {
                if (options.getSingleFieldTuplesConverter().isPrimitiveUnwrappedSchema(aggregate.getSchema())) {
                    code = "java.util.stream." + TypingUtils.streamPrefix(aggregate.getSchema().getFields()[0].getType()) + "Stream.of(" + code + ")";
                } else {
                    code = "Stream.of(" + code + ")\n";
                }
                // no need to add .parallel here, a non-grouped aggregation has only one result row
            }
        } else {
            // a group-by aggregation

            codegenContext.addInnerClass(collectorClassGenName, collectorClassGenCode);

            // generate record class code for the key
            String keyClassName;
            String groupKeyConstructorInvocationCode;
            if (groupKeyExpressions.length == 1 /*&& options.getSingleFieldTuplesConverter().isUnwrapped()*/) {
                keyClassName = groupKeyExpressions[0].type().getSimpleName();
                keyClassName = TypingUtils.boxed(keyClassName);
                groupKeyConstructorInvocationCode = convertExpr(groupKeyExpressions[0], inputSchema, "row.");
            } else {
                keyClassName = getOrCreateRecordType(groupKeyExpressions, "AggKey");
                groupKeyConstructorInvocationCode = getConstructorInvocationCode(keyClassName, inputSchema, "row.", groupKeyExpressions);
            }

            final String collectorName, mapTypeName;

            if(options.getMultiThreading().isConcurrent()) {
                collectorName = "Collectors.groupingByConcurrent";
                mapTypeName = "FinalConcurrentHashMap";
            } else {
                collectorName = "Collectors.groupingBy";
                mapTypeName = "FinalHashMap";
            }


            String collectorCode =
                    """
                    .collect(%s(
                            row -> %s,
                            %s::new,
                            %s
                        ))
                    """.formatted(
                            collectorName,
                            groupKeyConstructorInvocationCode,
                            mapTypeName,
                            collectorInitialization);

            String declarationForNewMethods = declarations.stream()
                    .map(Declaration::toCode)
                    .collect(Collectors.joining("\n\n"));
            declarations.clear();

            String methodName = codegenContext.freshName("getGroupAgg");
            String methodCode =
                    """
                    // StandalonePipelineMethod - Aggregation
                    public static %s<%s, %s> %s(DB db) {
                        %s
                        return %s%s;
                    }
                    """.formatted(
                            mapTypeName,
                            keyClassName,
                            returnCollectorClassGenName,
                            methodName,
                            declarationForNewMethods,
                            code,
                            collectorCode);

            codegenContext.addMethodFixName(methodName, methodCode);
            String varName = codegenContext.freshName("groupAgg");
            declarations.add(new Declaration(varName, "%s<%s, %s>".formatted(mapTypeName, keyClassName, returnCollectorClassGenName), methodName + "(db)"));
            code = varName;

            if(wrapResultIntoStream) {
                code += "\n.values().stream()";
                if(!options.getMultiThreading().isSequential()) {
                    code += ".parallel()";
                    if(options.isUnordered()) {
                        code += ".unordered()";
                    }
                    code += EOL;
                }
            }
        }

    }

    @Override
    public void visit(Sort sort) {
        sort.getChild().accept(this);
        code += ".sorted(%s)%n".formatted(generateSortComparators(sort, outputSchema, outputClassName));
    }

    @Override
    public void visit(Limit limit) {
        limit.getChild().accept(this);
        code += ".limit(%s)%n".formatted(limit.getLimit());
    }

    @Override
    public void visit(Skip limit) {
        limit.getChild().accept(this);
        code += ".skip(%s)%n".formatted(limit.getSkip());
    }

    @Override
    public void visit(OffsetFetch offsetFetch) {
        offsetFetch.getChild().accept(this);
        code += ".skip(%s)%n".formatted(offsetFetch.getOffset());
        code += ".limit(%s)%n".formatted(offsetFetch.getFetch());
    }

    @Override
    public void visit(HashJoin join) {
        VisitJoinInfo visitJoinInfo = visitJoinCommonInfo(join);
        String leftExprBinderPrefixDot = visitJoinInfo.leftExprBinderPrefix + ".";
        String rightExprBinderPrefixDot = visitJoinInfo.rightExprBinderPrefix + ".";

        // join key getters
        String leftKeyGetterCodePrefix = visitJoinInfo.leftExprBinderPrefix + " -> ";
        String leftKeyGetterCode, rightKeyGetterCode, keyTypeName;
        if (join.leftKeyGetters().length == 1 /*&& options.getSingleFieldTuplesConverter().isUnwrapped()*/) { // assuming #leftkeys == #rightkeys
            leftKeyGetterCode = convertExpr(join.leftKeyGetters()[0], visitJoinInfo.leftSchema, leftExprBinderPrefixDot);
            rightKeyGetterCode = convertExpr(join.rightKeyGetters()[0], visitJoinInfo.rightSchema, rightExprBinderPrefixDot);
            keyTypeName = join.leftKeyGetters()[0].type().getSimpleName();
            keyTypeName = TypingUtils.boxed(keyTypeName);
        } else {
            keyTypeName = getOrCreateRecordType(join.leftKeyGetters(), "JoinKey");
            leftKeyGetterCode = getConstructorInvocationCode(
                    keyTypeName,
                    visitJoinInfo.leftSchema,
                    leftExprBinderPrefixDot,
                    join.leftKeyGetters());
            rightKeyGetterCode = getConstructorInvocationCode(
                    keyTypeName,
                    visitJoinInfo.rightSchema,
                    rightExprBinderPrefixDot,
                    join.rightKeyGetters());
        }

        Join.JoinType joinType = join.joinType();
        final String collectorName, mapTypeName, listCollector, joinBuildType, emptyListInitCode;

        if(options.getMultiThreading().isConcurrent()) {
            collectorName = "Collectors.groupingByConcurrent";
            mapTypeName = "FinalConcurrentHashMap";
        } else {
            collectorName = "Collectors.groupingBy";
            mapTypeName = "FinalHashMap";
        }


        if(options.getMultiThreading() == StreamCompilerOptions.MultiThreading.CONCURRENT_COLLECTOR) {
            listCollector = "S2SCollectors.toConcurrentQueue()";
            emptyListInitCode = "S2SCollectors.emptyConcurrentQueue()";
            joinBuildType = "ConcurrentLinkedQueue";

        } else {
            listCollector = "S2SCollectors.toArrayList()";
            emptyListInitCode = "S2SCollectors.emptyArrayList()";
            joinBuildType = "ArrayList";
        }


        if (joinType == Join.JoinType.INNER) {
            leftKeyGetterCode = leftKeyGetterCodePrefix + leftKeyGetterCode;
            // codegen collect into hashmap
            String hm = codegenContext.freshName("hashmap");
            String hmCode = "";
            final String returnType;
            if (options.getSingleFieldTuplesConverter().isPrimitiveUnwrappedSchema(visitJoinInfo.leftSchema)
                    && TypingUtils.maybePrimitiveOutputClass(visitJoinInfo.leftClassName).isPresent()) {

                hmCode += ".boxed()";
            }
            hmCode += ".collect(%s(%s, %s::new, %s))".formatted(
                    collectorName, leftKeyGetterCode, mapTypeName, listCollector);
            returnType = "%s<%s, %s<%s>>".formatted(
                    mapTypeName, keyTypeName, joinBuildType, visitJoinInfo.leftClassName);




            String declarationForNewMethods = visitJoinInfo.leftDeclarations.stream()
                    .map(Declaration::toCode)
                    .collect(Collectors.joining("\n\n"));

            String methodName = codegenContext.freshName("getMap");
            String methodCode =
                    """
                    // StandalonePipelineMethod - Inner Join
                    public static %s %s(DB db) {
                        %s
                        return %s;
                    }
                    """.formatted(
                            returnType,
                            methodName,
                            declarationForNewMethods,
                            visitJoinInfo.leftSideCode + hmCode);

            codegenContext.addMethodFixName(methodName, methodCode);
            declarations.add(new Declaration(hm, returnType, methodName + "(db)"));



            if (options.getJoinConverter() == StreamCompilerOptions.JoinConverter.FLATMAP) {
                String empty = codegenContext.freshName("empty");
                String emptyType = "%s<%s>".formatted(joinBuildType, visitJoinInfo.leftClassName);
                declarations.add(new Declaration(empty, emptyType, emptyListInitCode));

                String nonEquiCondition = "";
                if (visitJoinInfo.nonEquiConditionCode != null) {
                    nonEquiCondition = ".filter(%s -> %s)\n\t".formatted(visitJoinInfo.leftExprBinderPrefix, visitJoinInfo.nonEquiConditionCode);
                }

                if (options.getSingleFieldTuplesConverter().isPrimitiveUnwrappedSchema(visitJoinInfo.leftSchema)) {
                    String innerStream = visitJoinInfo.rightExprBinderPrefix + " -> java.util.stream.%sStream.of(%s.getOrDefault(%s, %s))\n\t"
                            .formatted(TypingUtils.streamPrefix(visitJoinInfo.leftSchema.getFields()[0].getType()), hm, rightKeyGetterCode, empty);
                    innerStream += nonEquiCondition;
                    innerStream += ".mapToObj(%s -> %s)\n".formatted(visitJoinInfo.leftExprBinderPrefix, visitJoinInfo.mapperCode);
                    this.code = visitJoinInfo.rightSideCode + ".flatMap(" + innerStream + ")\n";
                } else {
                    String innerStream = visitJoinInfo.rightExprBinderPrefix + " -> %s.getOrDefault(%s, %s)\n\t".formatted(hm, rightKeyGetterCode, empty);
                    innerStream += ".stream()\n\t";
                    innerStream += nonEquiCondition;
                    innerStream += ".map(%s -> %s)\n".formatted(visitJoinInfo.leftExprBinderPrefix, visitJoinInfo.mapperCode);
                    this.code = visitJoinInfo.rightSideCode + ".flatMap(" + innerStream + ")\n";
                }
            } else {
                String innerForEach = "";
                if (visitJoinInfo.nonEquiConditionCode != null) {
                    innerForEach += "if (!%s) return;\n\t".formatted(visitJoinInfo.nonEquiConditionCode);

                }
                innerForEach += """
                        consumer.accept(%s);
                        """.formatted(visitJoinInfo.mapperCode);
                String markedListName = codegenContext.freshName("list");
                String innerStream;
                String type = "%s<%s>".formatted(joinBuildType, visitJoinInfo.leftClassName);

                innerStream = """
                        (%s %s, java.util.function.Consumer<%s> consumer) -> {
                            %s %s = %s.get(%s);
                            if(%s != null) {
                                %s.forEach(%s -> {%s});
                            }
                        }
                        """.formatted(
                        TypingUtils.boxed(visitJoinInfo.rightClassName),
                        visitJoinInfo.rightExprBinderPrefix,
                        this.outputClassName,
                        type,
                        markedListName,
                        hm,
                        rightKeyGetterCode,
                        markedListName,
                        markedListName,
                        visitJoinInfo.leftExprBinderPrefix,
                        innerForEach

                );

                this.code = visitJoinInfo.rightSideCode + ".mapMulti(" + innerStream + ")\n";
            }
        } else if (joinType == Join.JoinType.LEFT) {

            // codegen collect into hashmap
            String hm = codegenContext.freshName("hashmap");
            String hmCode = "";
            if (TypingUtils.maybePrimitiveOutputClass(visitJoinInfo.rightClassName).isPresent()) {
                hmCode += ".boxed()";
            }
            hmCode += ".collect(%s(%s -> %s, %s::new, %s))".formatted(
                    collectorName, visitJoinInfo.rightExprBinderPrefix, rightKeyGetterCode, mapTypeName, listCollector);

            String returnType = "%s<%s, %s<%s>>".formatted(
                    mapTypeName, keyTypeName, joinBuildType, TypingUtils.boxed(visitJoinInfo.rightClassName));
            String declarationForNewMethods = declarations.stream()
                    .map(Declaration::toCode)
                    .collect(Collectors.joining("\n\n"));
            declarations.clear();

            String methodName = codegenContext.freshName("getMapMarkedArrayList");
            String methodCode =
                    """
                    // StandalonePipelineMethod
                    public static %s %s(DB db) {
                        %s
                        return %s;
                    }
                    """.formatted(
                            returnType,
                            methodName,
                            declarationForNewMethods,
                            visitJoinInfo.rightSideCode + hmCode);

            codegenContext.addMethodFixName(methodName, methodCode);
            declarations.add(new Declaration(hm, returnType, methodName + "(db)"));
            declarations.addAll(visitJoinInfo.leftDeclarations);



            final String leftJoinCode;

            String listName = codegenContext.freshName("list");

            if (options.getJoinConverter() == StreamCompilerOptions.JoinConverter.FLATMAP) {
                String innerStreamTail = "";
                if (visitJoinInfo.nonEquiConditionCode != null) {
                    innerStreamTail += ".filter(%s -> %s)\n\t".formatted(
                            visitJoinInfo.rightExprBinderPrefix, visitJoinInfo.nonEquiConditionCode);
                }
                innerStreamTail += ".map(%s -> %s)\n".formatted(
                        visitJoinInfo.rightExprBinderPrefix, visitJoinInfo.mapperCode);

                leftJoinCode = """
                        .flatMap(%s -> {
                            %s<%s> %s = %s.get(%s);
                            if(%s == null) {
                                return Stream.of(%s);
                            } else {
                                return %s.stream()%s;
                            }
                        })
                        """.formatted(
                        visitJoinInfo.leftExprBinderPrefix,
                        joinBuildType,
                        TypingUtils.boxed(visitJoinInfo.rightClassName),
                        listName,
                        hm,
                        leftKeyGetterCode,
                        listName,
                        visitJoinInfo.mapperRightNullCode,
                        listName,
                        innerStreamTail
                );

            } else {
                //using mapmulti
                leftJoinCode =
                        """
                        .mapMulti((%s %s, Consumer<%s> consumer) -> {
                            %s<%s> %s = %s.get(%s);
                            if(%s == null) {
                                consumer.accept(%s);
                            } else {
                                %s.forEach(%s -> consumer.accept(%s));
                            }
                        })
                        """.formatted(
                                visitJoinInfo.leftClassName,
                                visitJoinInfo.leftExprBinderPrefix,
                                outputClassName,
                                joinBuildType,
                                TypingUtils.boxed(visitJoinInfo.rightClassName),
                                listName,
                                hm,
                                leftKeyGetterCode,
                                listName,
                                visitJoinInfo.mapperRightNullCode,
                                listName,
                                visitJoinInfo.rightExprBinderPrefix,
                                visitJoinInfo.mapperCode
                        );
            }

            this.code = visitJoinInfo.leftSideCode + leftJoinCode;

        } else if (joinType == Join.JoinType.SEMI || joinType == Join.JoinType.ANTI) {
            // Note: here I am only implementing right-(semi|anti) joins
            // (i.e., building a set from the right side of the join).

            // codegen collect into hashset
            String hs = codegenContext.freshName("hashset");
            String boundedRightKeyGetterCode = visitJoinInfo.rightExprBinderPrefix + " -> " + rightKeyGetterCode;
            String hsCode = "";
            if (options.getSingleFieldTuplesConverter().isPrimitiveUnwrappedSchema(visitJoinInfo.rightSchema)) {
                if (TypingUtils.maybePrimitiveOutputClass(visitJoinInfo.rightClassName).isEmpty())
                    hsCode += ".mapTo%s(%s)".formatted(TypingUtils.streamPrefix(visitJoinInfo.rightSchema.getFields()[0].getType()), boundedRightKeyGetterCode);
                hsCode += ".boxed()%n\t.collect(Collectors.toSet())".formatted();
            } else {
                hsCode = ".map(%s)%n\t.collect(Collectors.toSet())".formatted(boundedRightKeyGetterCode);
            }
            final String hsInitCode;
            String methodName = codegenContext.freshName("getSet");
            String methodCode =
                    """
                    // StandalonePipelineMethod - Semi/Anti Join
                    public static java.util.Set<%s> %s(DB db) {
                        %s
                        return %s;
                    }
                    """.formatted(
                            keyTypeName,
                            methodName,
                            generateAndClearDeclarations(),
                            visitJoinInfo.rightSideCode + hsCode);

            codegenContext.addMethodFixName(methodName, methodCode);
            hsInitCode = methodName + "(db)";

            declarations.addAll(visitJoinInfo.leftDeclarations);
            declarations.add(new Declaration(hs, "java.util.Set<%s>".formatted(keyTypeName), hsInitCode));

            String maybeNegate = joinType == Join.JoinType.SEMI ? "" : "!";
            String filterFunc = visitJoinInfo.leftExprBinderPrefix +
                    " -> " + maybeNegate + hs +
                    ".contains(%s)".formatted(leftKeyGetterCode);
            this.code = visitJoinInfo.leftSideCode + ".filter(" + filterFunc + ")\n";
            this.outputClassName = visitJoinInfo.leftClassName;

        } else {
            throw new RuntimeException("TODO - Unsupported hash join type: " + join.joinType());
        }
    }

    @Override
    public void visit(NestedLoopJoin join) {
        VisitJoinInfo visitJoinInfo = visitJoinCommonInfo(join);
        String joinListType = "List";

        // note currently only inner joins
        if (join.joinType() == Join.JoinType.INNER) {
            // collect left rows into a list
            String list = codegenContext.freshName("list");
            String innerStream;
            if (options.getSingleFieldTuplesConverter().isPrimitiveUnwrappedSchema(visitJoinInfo.leftSchema)) {
                String emptyType = visitJoinInfo.leftClassName + "[]";
                declarations.add(new Declaration(list, emptyType, visitJoinInfo.leftSideCode + ".toArray()"));
                innerStream = visitJoinInfo.rightExprBinderPrefix + " -> java.util.stream." + TypingUtils.streamPrefix(visitJoinInfo.leftSchema.getFields()[0].getType()) + "Stream.of(%s)%n\t".formatted(list);
            } else {
                String emptyType = "%s<%s>".formatted(joinListType, visitJoinInfo.leftClassName);
                declarations.add(new Declaration(list, emptyType, visitJoinInfo.leftSideCode + toListCodeGen()));
                innerStream = visitJoinInfo.rightExprBinderPrefix + " -> %s.stream()%n\t".formatted(list);
            }

            if (visitJoinInfo.nonEquiConditionCode != null) {
                innerStream += ".filter(%s -> %s)%n\t".formatted(visitJoinInfo.leftExprBinderPrefix, visitJoinInfo.nonEquiConditionCode);
            }
            if (options.getSingleFieldTuplesConverter().isPrimitiveUnwrappedSchema(visitJoinInfo.leftSchema)) {
                innerStream += ".mapToObj(%s -> %s)%n".formatted(visitJoinInfo.leftExprBinderPrefix, visitJoinInfo.mapperCode);
            } else {
                innerStream += ".map(%s -> %s)%n".formatted(visitJoinInfo.leftExprBinderPrefix, visitJoinInfo.mapperCode);
            }

            prependDeclarations(visitJoinInfo.leftDeclarations);
            this.code = visitJoinInfo.rightSideCode + ".flatMap(" + innerStream + ")\n";

        } else {
            throw new RuntimeException("TODO - Unsupported nested-loop join type: " + join.joinType());
        }
    }

    @Override
    public void visit(SingletonFilterJoin join) {
        if (join.joinType() != Join.JoinType.INNER) {
            throw new RuntimeException("TODO - Unsupported singleton join type: " + join.joinType());
        }
        if(! (join.left() instanceof Aggregate agg && agg.getGroupKeys().length == 0 && agg.getAggregations().length == 1)) {
            throw new RuntimeException("SingletonFilterJoin should have a scalar (non grouped) aggregation on the left side");
        }

        String leftVal = codegenContext.freshName("leftVal");

        SqlOpToJavaStreamVisitor leftVisitor = new SqlOpToJavaStreamVisitor(codegenContext, null, options);
        leftVisitor.visit(agg, false);
        join.right().accept(this);

        // collect left rows into a list
        String leftValType = leftVisitor.outputClassName;
        String leftValInitCode;
        String declarationForNewMethods = leftVisitor.declarations.stream()
                .map(Declaration::toCode)
                .collect(Collectors.joining("\n\n"));

        String methodName = codegenContext.freshName("getSet");
        String methodCode =
                """
                // StandalonePipelineMethod - SingletonFilterJoin
                public static %s %s(DB db) {
                    %s
                    return %s;
                }
                """.formatted(
                        leftValType,
                        methodName,
                        declarationForNewMethods,
                        leftVisitor.code);
        codegenContext.addMethodFixName(methodName, methodCode);
        leftValInitCode = methodName + "(db)";

        declarations.add(new Declaration(leftVal, leftValType, leftValInitCode));

        String rightExprBinderPrefix = codegenContext.freshName("row");
        Function<InputRef, String> nameBinder = inputRef -> {
            Schema leftSchema = leftVisitor.outputSchema;
            Schema rightSchema = outputSchema;
            int idx = inputRef.index();
            int nLeft = 1;
            if (idx < nLeft) {
                if (this.options.getSingleFieldTuplesConverter().isUnwrappedSchema(leftSchema)) {
                    return leftVal;
                } else {
                    Field f = leftSchema.getFields()[idx];
                    String accessor = f.getName();
                    if(f.isGetter()) accessor += "()";
                    return leftVal + "." + accessor;
                }
            } else {
                if (this.options.getSingleFieldTuplesConverter().isUnwrappedSchema(rightSchema)) {
                    return rightExprBinderPrefix;
                } else {
                    Field f = rightSchema.getFields()[idx - nLeft];
                    String accessor = f.getName();
                    if(f.isGetter()) accessor += "()";
                    return rightExprBinderPrefix + "." + accessor;
                }
            }
        };

        String filterCode = convertExpr(join.getLeftFilterExpression(), join.getSchema(), nameBinder);
        String filter = ".filter(%s -> %s)%n\t".formatted(rightExprBinderPrefix, filterCode);
        this.code += filter + "\n";

    }

    private VisitJoinInfo visitJoinCommonInfo(Join join) {
        // first lets visit the children in execution order
        // execution order matters due to the caching of operator results (currently only for aggregations)
        SqlOpToJavaStreamVisitor leftVisitor = new SqlOpToJavaStreamVisitor(codegenContext, null, options);
        Join.JoinType joinType = join.joinType();
        // then get rid of this if/else, the order will not be important anymore
        if(joinType != Join.JoinType.SEMI && joinType != Join.JoinType.ANTI && joinType != Join.JoinType.LEFT) {
            // SEMI/ANTI/LEFT first execute (build on) the right side
            join.right().accept(this);
            join.left().accept(leftVisitor);
        } else {
            // all other joins (only INNER?) first execute (build on) the left side
            join.left().accept(leftVisitor);
            join.right().accept(this);
        }
        String leftSideCode = leftVisitor.code;
        Schema leftSchema = leftVisitor.outputSchema;//join.left().getSchema();

        String leftClassName = leftVisitor.outputClassName;
        String leftExprBinderPrefix = codegenContext.freshName("l");

        String rightClassName = this.outputClassName;
        String rightSideCode = this.code;
        Schema rightSchema = outputSchema;//join.right().getSchema();
        String rightExprBinderPrefix = codegenContext.freshName("r");
        // clean right visit
        this.code = this.outputClassName = "";

        Function<InputRef, String> nameBinder = inputRef -> {
            int idx = inputRef.index();
            int nLeft = leftSchema.getFields().length;
            if (idx < nLeft) {
                if (this.options.getSingleFieldTuplesConverter().isUnwrappedSchema(leftSchema)) {
                    return leftExprBinderPrefix;
                } else {
                    Field f = leftSchema.getFields()[idx];
                    String accessor = f.getName();
                    if(f.isGetter()) accessor += "()";
                    return leftExprBinderPrefix + "." + accessor;
                }
            } else {
                if (this.options.getSingleFieldTuplesConverter().isUnwrappedSchema(rightSchema)) {
                    return rightExprBinderPrefix;
                } else {
                    Field f = rightSchema.getFields()[idx - nLeft];
                    String accessor = f.getName();
                    if(f.isGetter()) accessor += "()";
                    return rightExprBinderPrefix + "." + accessor;
                }
            }
        };
        Function<InputRef, String> nameBinderLeftNull = inputRef -> {
            int idx = inputRef.index();
            int nLeft = leftSchema.getFields().length;
            if (idx < nLeft) {
                return "null";
            } else {
                if (this.options.getSingleFieldTuplesConverter().isUnwrappedSchema(rightSchema)) {
                    return rightExprBinderPrefix;
                } else {
                    Field f = rightSchema.getFields()[idx - nLeft];
                    String accessor = f.getName();
                    if(f.isGetter()) accessor += "()";
                    return rightExprBinderPrefix + "." + accessor;
                }
            }
        };
        Function<InputRef, String> nameBinderRightNull = inputRef -> {
            int idx = inputRef.index();
            int nLeft = leftSchema.getFields().length;
            if (idx < nLeft) {
                if (this.options.getSingleFieldTuplesConverter().isUnwrappedSchema(leftSchema)) {
                    return leftExprBinderPrefix;
                } else {
                    Field f = leftSchema.getFields()[idx];
                    String accessor = f.getName();
                    if(f.isGetter()) accessor += "()";
                    return leftExprBinderPrefix + "." + accessor;
                }
            } else {
                return "null";
            }
        };
        // non equi condition
        String nonEquiConditionCode = join.nonEquiCondition() == null
                ? null
                : // "(left, right) -> " +
                convertExpr(join.nonEquiCondition(), join.getSchema(), nameBinder);

        // gen mapped output class
        // opt candidate:
        //  generating an output class is not required if one of the following holds:
        //      - the join has no mapper, so it is marked to project only on one side
        //      - the join has a mapper which projects a single field
        //      left and right input prefixes map

        final String mapper;
        // mapper for left and right joins
        final String mapperLeftNull, mapperRightNull; //leftExprBinderPrefix + " -> ";
        if (join.isRightProjectMapper()) {
            mapper = rightExprBinderPrefix;
            mapperLeftNull = "null";
            mapperRightNull = "null";
            outputClassName = rightClassName;
            // outputSchema is already the correct one: it was set in rightNode.accept(this)
        } else if (join.isLeftProjectMapper()) {
            mapper = leftExprBinderPrefix;
            mapperLeftNull = "null";
            mapperRightNull = "null";
            outputClassName = leftClassName;
            outputSchema = leftVisitor.outputSchema;
        } else if (join.mapper().length == 1 /* && options.getSingleFieldTuplesConverter().isUnwrapped()*/) {
            Expression expr = join.mapper()[0];
            mapper = convertExpr(expr, join.getSchema(), nameBinder);
            mapperLeftNull = convertExpr(expr, join.getSchema(), nameBinderLeftNull);
            mapperRightNull = convertExpr(expr, join.getSchema(), nameBinderRightNull);
            outputClassName = expr.type().getSimpleName();
            outputSchema = join.getSchema();
        } else {
            // generating a class is required
            Schema schema = outputSchema = join.getSchema().withFieldsAsGetters(true);
            outputClassName = getOrCreateRecordType(join.mapper(), "JoinedRecord", schema);
            mapper = getConstructorInvocationCode(
                    outputClassName, schema, nameBinder, join.mapper());

            mapperLeftNull = getConstructorInvocationCode(
                    outputClassName, schema, nameBinderLeftNull, join.mapper());
            mapperRightNull = getConstructorInvocationCode(
                    outputClassName, schema, nameBinderRightNull, join.mapper());
        }


        return new VisitJoinInfo(
                leftClassName, leftSideCode, leftSchema, leftExprBinderPrefix, leftVisitor.declarations,
                rightClassName, rightSideCode, rightSchema, rightExprBinderPrefix,
                nonEquiConditionCode, mapper,
                mapperLeftNull, mapperRightNull);
    }


}
