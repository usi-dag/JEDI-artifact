package s2s.query_compiler.template;

import s2s.planner.qp.Field;
import s2s.planner.qp.Schema;
import s2s.planner.qp.expressions.Const;
import s2s.planner.qp.expressions.Expression;
import s2s.planner.qp.expressions.InputRef;
import s2s.planner.qp.operators.*;
import s2s.planner.qp.operators.*;
import s2s.query_compiler.TypingUtils;
import s2s.query_compiler.options.SingleFieldTuplesConverter;
import s2s.query_compiler.template.stream.StreamCompilerOptions;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static s2s.experiments.CodegenDuplicateFinder.indent;

public abstract class CodeTemplateBasedVisitor implements OperatorVisitor {

    public static final String EOL = System.lineSeparator();
    public static final String EOL_TAB = EOL + "\t";


    // TODO maybe a getter method?
    public final CodegenContext codegenContext;

    // list of in-method declarations - they must be before the code
    // TODO maybe a getter method?
    public final LinkedList<Declaration> declarations = new LinkedList<>();

    // storage for declaration of the constants - for in-class declarations
    final Map<Const, String> constantsDeclarationMap = new HashMap<>();

    protected CodeTemplateBasedVisitor(CodegenContext codegenContext) {
        this.codegenContext = codegenContext;
    }

    protected abstract SingleFieldTuplesConverter getSingleFieldTuplesConverter();

    protected abstract StreamCompilerOptions.MultiThreading getMultithreadingOption();



    public Schema visitRemovableProject(RemovableProject project, Schema inputSchema) {
        Field[] projectFields = project.getSchema().getFields();
        Field[] newFields = new Field[projectFields.length];
        for (int i = 0; i < projectFields.length; i++) {
            newFields[i] = inputSchema.get(projectFields[i].getName());
        }
        return Schema.byFieldsForWrapperSchema(project.getSchema().wrappedSchema(), newFields);
    }



    // Utilities to split pipelines
    protected String generateStandalonePipelineMethod(String comment,
                                                      String methodNamePrefix,
                                                      String returnType,
                                                      String body) {
        String methodName = codegenContext.freshName(methodNamePrefix);
        String methodCode =
                """
                // StandalonePipelineMethod - %s
                public static %s %s(DB db) {
                %s
                }
                """.formatted(comment, returnType, methodName, CodeTemplateUtils.indent(body));
        codegenContext.addMethodFixName(methodName, methodCode);
        return methodName;
    }

    // Utilities for declarations
    protected String generateAndClearDeclarations() {
        String code = declarations.stream()
                .map(Declaration::toCode)
                .collect(Collectors.joining(EOL));
        declarations.clear();
        return code;
    }

    protected void prependDeclarations(List<Declaration> otherDeclarations) {
        List<Declaration> newDeclarations = new ArrayList<>(otherDeclarations);
        newDeclarations.addAll(declarations);
        declarations.clear();
        declarations.addAll(newDeclarations);
    }

    // Utilities to deal with sort
    // TODO get rid of childOutputSchema once we get rid of isGetter
    protected String generateSortComparators(Sort sort, Schema childOutputSchema, String elementType) {
        Expression[] exprs = sort.getExpressions();
        Sort.Direction[] sortDirections = sort.getDirections();

        StringBuilder composedComparator = new StringBuilder();
        String comparatorClass = "Comparator<%s>".formatted(elementType);
        // TODO chain of comparators without creating a field for each one
        for (int i = 0; i < exprs.length; i++) {
            String exprCode = convertExpr(exprs[i], childOutputSchema, "row.");
            String comparatorCode = "Comparator.comparing((" + elementType + " row) -> " + exprCode + ")";
            if (sortDirections[i] == Sort.Direction.DESC) comparatorCode += ".reversed()";
            String name = codegenContext.addClassField("comp", comparatorClass, comparatorCode);
            if (i == 0) {
                composedComparator.append(name);
            } else {
                composedComparator.append(".thenComparing(").append(name).append(")");
            }
        }
        if(exprs.length < 1) {
            throw new RuntimeException("Should not reach this: Sort with no sorting expressions");
            // TODO why this code was here?
            //return codegenContext.addClassField("comp", comparatorClass, comparator)/**/
        }
        return composedComparator.toString();
    }


    // Utilities to deal with aggregations
    public record AggregationFieldCodeGen(Declaration declaration, String accumulate, String combine, String finish) {}

    protected List<AggregationFieldCodeGen> getOutputFieldCodeGenForConcurrentAggregation(String varName,
                                                                                          String varType,
                                                                                          SqlAggFunction aggFunction,
                                                                                          Schema inputSchema,
                                                                                          String inputRefPrexif) {

        List<AggregationFieldCodeGen> fieldsCodeGen = new LinkedList<>();

        SqlAggFunction.AggKind aggKind = aggFunction.getKind();
        final String exprStrInit;
        // no combiner with concurrent aggregation
        final String exprStrCombine = "";
        final String modifier;
        String exprStrAccu;
        String exprStrFinish = "";

        final String concurrentLongAccumulator,
                concurrentDoubleAccumulator,
                concurrentAddMethod,
                concurrentIncrementMethod,
                concurrentGetMethod,
                concurrentResetMethod;

        if(getMultithreadingOption() == StreamCompilerOptions.MultiThreading.CONCURRENT_COLLECTOR) {
            concurrentLongAccumulator = "LongAdder";
            concurrentDoubleAccumulator = "DoubleAdder";
            concurrentAddMethod = ".add";
            concurrentIncrementMethod = ".increment()";
            concurrentGetMethod = ".sum()";
            concurrentResetMethod = ".reset()";
        } else {
            throw new RuntimeException("Unsupported MultiThreading option for concurrent aggregation: " + getMultithreadingOption());
        }

        if (aggFunction.isCountStar()) {
            varType = concurrentLongAccumulator;
            exprStrInit = "new %s()".formatted(concurrentLongAccumulator);
            exprStrAccu = varName + concurrentIncrementMethod + ";";
            modifier = "final";

        } else if (aggKind == SqlAggFunction.AggKind.COUNT) {
            varType = concurrentLongAccumulator;
            exprStrInit = "new %s()".formatted(concurrentLongAccumulator);
            modifier = "final";

            Expression expression = aggFunction.getExpression();
            if (expression instanceof InputRef inputRef && !inputRef.field().isNullable()) {
                exprStrAccu = varName + concurrentIncrementMethod + ";";
            } else {
                String accessor = convertExpr(expression, inputSchema, inputRefPrexif);
                exprStrAccu = """
                            if(%s != null) {
                                %s%s;
                            }
                            """.formatted(accessor, varName, concurrentIncrementMethod);
            }
        } else {

            Expression expression = aggFunction.getExpression();
            exprStrAccu = convertExpr(expression, inputSchema, inputRefPrexif);

            final boolean isIntOrLong = varType.toLowerCase().startsWith("int") || varType.equalsIgnoreCase("long");
            if (aggKind.isAdditive()) {
                modifier = "final";
                if(varType.equalsIgnoreCase("double")) {
                    varType = concurrentDoubleAccumulator;
                    exprStrInit = "new %s()".formatted(concurrentDoubleAccumulator);
                } else if (isIntOrLong) {
                    varType = concurrentLongAccumulator;
                    exprStrInit = "new %s()".formatted(concurrentLongAccumulator);
                } else {
                    throw new IllegalStateException("Unsupported type for concurrent aggregation: " + varType);
                }

                exprStrAccu = varName + concurrentAddMethod + "(" + exprStrAccu + ");";
                if (aggKind == SqlAggFunction.AggKind.AVG) {
                    // AVG needs an extra counter field
                    String varCtnName = varName + "_counter";
                    fieldsCodeGen.add(new AggregationFieldCodeGen(
                            new Declaration(
                                    varCtnName,
                                    concurrentLongAccumulator, "new %s();".formatted(concurrentLongAccumulator),
                                    "final"),
                            varCtnName + concurrentIncrementMethod + ";",
                            "",
                            ""));
                    // AVG field
                    varType = concurrentDoubleAccumulator;
                    // average (currently only it) requires a finisher
                    // quite tricky: I create a new adder with the result
                    exprStrFinish = "double " + varName + "_finish = " +
                            varName + concurrentGetMethod + " / " + varCtnName + concurrentGetMethod + ";" +
                            varName + concurrentResetMethod + ";" + varName + concurrentAddMethod + "("+varName+"_finish);";
                }
            }
            // MIN/MAX are the only agg functions which requires
            //          checking if the accepted tuple is the first one
            //          this is currently implemented as in Stream API
            //          i.e., using an `empty` field, default=true and set to
            //          false at the first accepted element
            else {
                modifier = "";
                String originalVarType = varType;
                // TODO I am assuming a non-null field - deal with nullability
                // note: currently only min and max are not additive
                final String minMaxInit, comparison;
                if (aggKind == SqlAggFunction.AggKind.MIN) {
                    minMaxInit = "MAX_VALUE";
                    comparison = ">";
                } else if (aggKind == SqlAggFunction.AggKind.MAX) {
                    minMaxInit = "MIN_VALUE";
                    comparison = "<";
                } else {
                    throw new IllegalStateException("Unexpected agg kind: " + aggKind);
                }
                if (varType.equalsIgnoreCase("double")) {
                    varType = "AtomicDouble";
                    exprStrInit = "new AtomicDouble(Double.%s)".formatted(minMaxInit);
                } else if (isIntOrLong) {
                    varType = "AtomicLong";
                    exprStrInit = "new AtomicLong(Long.%s)".formatted(minMaxInit);
                } else {
                    throw new IllegalStateException("Unsupported type for concurrent aggregation: " + varType);
                }
                String emptyVarName = codegenContext.freshName("empty");
                fieldsCodeGen.add(new AggregationFieldCodeGen(
                        new Declaration(emptyVarName, "boolean", "true", "volatile"),
                        "", "", ""));

                String tmpVarName = codegenContext.freshName("tmp");
                String tmpVarInit = originalVarType + " " + tmpVarName + " = " + exprStrAccu + ";";
                String oldValueVarName = codegenContext.freshName("oldValue");
                exprStrAccu = """
                        %s = false;
                        %s
                        %s %s;
                        while((%s = %s.get()) %s %s && !%s.compareAndSet(%s, %s)) {}
                        """.formatted(
                        emptyVarName,
                        tmpVarInit,
                        originalVarType, oldValueVarName,
                        oldValueVarName, varName, comparison, tmpVarName, varName, oldValueVarName, tmpVarName);
            }

        }

        if (aggFunction.getFilterExpression().isPresent()) {
            Expression filter = aggFunction.getFilterExpression().get();
            String filterCode = convertExpr(filter, inputSchema, inputRefPrexif);
            exprStrAccu = """
                        if(%s) {
                            %s
                        }""".formatted(filterCode, exprStrAccu);
        }

        fieldsCodeGen.add(new AggregationFieldCodeGen(
                new Declaration(varName, varType, exprStrInit, modifier),
                exprStrAccu,
                exprStrCombine,
                exprStrFinish
        ));

        return fieldsCodeGen;
    }



    protected List<AggregationFieldCodeGen> getOutputFieldCodeGenForAggregation(String varName,
                                                                                String varType,
                                                                                SqlAggFunction aggFunction,
                                                                                Schema inputSchema,
                                                                                String inputRefPrexif) {

        List<AggregationFieldCodeGen> fieldsCodeGen = new LinkedList<>();

        SqlAggFunction.AggKind aggKind = aggFunction.getKind();
        final String exprStrInit;
        String exprStrAccu;
        String exprStrCombine;
        String exprStrFinish = "";


        if (aggFunction.isCountStar()) {
            exprStrInit = "0;";
            exprStrCombine = varName + " += other." + varName + ';';
            exprStrAccu = varName + "++;";

        } else if (aggKind == SqlAggFunction.AggKind.COUNT) {
            exprStrInit = "0;";

            Expression expression = aggFunction.getExpression();
            if (expression instanceof InputRef inputRef && !inputRef.field().isNullable()) {
                exprStrAccu = varName + "++;";
            } else {
                String accessor = convertExpr(expression, inputSchema, inputRefPrexif);
                exprStrAccu = """
                            if(%s != null) {
                                %s++;;
                            }
                            """.formatted(accessor, varName);
            }
            exprStrCombine = varName + " += other." + varName + ';';
        } else {
            Expression expression = aggFunction.getExpression();
            exprStrAccu = convertExpr(expression, inputSchema, inputRefPrexif);

            if (aggKind.isAdditive()) {
                exprStrInit = "0";
                exprStrAccu = varName + " += " + exprStrAccu + ';';
                exprStrCombine = varName + " += other." + varName + ';';
                if (aggKind == SqlAggFunction.AggKind.AVG) {
                    // AVG needs an extra counter field
                    String varCtnName = varName + "_counter";
                    fieldsCodeGen.add(new AggregationFieldCodeGen(
                            new Declaration(varCtnName, "long", "0"),
                            varCtnName + "++;",
                            varCtnName + " += other." + varCtnName + ';',
                            ""));
                    // AVG field
                    varType = "double";
                    // average (currently only it) requires a finisher
                    exprStrFinish = varName + " = " + varName + " / " + varCtnName + ';';
                }
            }
            // MIN/MAX are the only agg functions which requires
            //          checking if the accepted tuple is the first one
            //          this is currently implemented as in Stream API
            //          i.e., using an `empty` field, default=true and set to
            //          false at the first accepted element
            else {
                // TODO I am assuming a non-null field - deal with nullability
                // note: currently only min and max are not additive
                String fun = switch (aggKind) {
                    case MIN -> "min";
                    case MAX -> "max";
                    default -> throw new IllegalStateException("Unexpected agg kind: " + aggKind);
                };
                exprStrInit = "0";
                String emptyVarName = codegenContext.freshName("empty");
                fieldsCodeGen.add(new AggregationFieldCodeGen(
                        new Declaration(emptyVarName, "boolean", "true"),
                        "", "", ""));
                String exprStrAccuNotEmpty = "%s = Math.%s(%s, %s)".formatted(varName, fun, varName, exprStrAccu);
                exprStrAccu = """
                            if(!%s) {
                                %s;
                            } else {
                                %s = false;
                                %s = %s;
                            }
                            """.formatted(emptyVarName, exprStrAccuNotEmpty, emptyVarName, varName, exprStrAccu);


                // dealing with possible empty combiner
                String noneEmpty = "%s = Math.%s(%s, other.%s);".formatted(varName, fun, varName, varName);
                exprStrCombine = """
                            if(%s) {
                                // this is empty
                                %s = other.%s;
                            } else if(other.%s) {
                                // other is empty - nothing to do
                            } else { // none is empty
                                %s
                            }
                            """.formatted(
                        emptyVarName,
                        varName, varName, // this is empty
                        emptyVarName,
                        noneEmpty); // other is empty
            }
        }

        if (aggFunction.getFilterExpression().isPresent()) {
            Expression filter = aggFunction.getFilterExpression().get();
            String filterCode = convertExpr(filter, inputSchema, inputRefPrexif);
            exprStrAccu = """
                        if(%s) {
                            %s
                        }""".formatted(filterCode, exprStrAccu);
        }

        fieldsCodeGen.add(new AggregationFieldCodeGen(
                new Declaration(varName, varType, exprStrInit),
                exprStrAccu,
                exprStrCombine,
                exprStrFinish
        ));

        return fieldsCodeGen;
    }


    protected String getFinishGenBodyForAggFinalizers(Aggregate aggregate) {
        StringBuilder finishGenBodyForAggFinalizers = new StringBuilder();
        Expression[] finalizers = aggregate.getFinalizers();
        if(finalizers != null && finalizers.length > 0) {
            int nGroups = aggregate.getGroupKeys().length;
            int schemaIndex = 0;
            for (Expression finalizer : finalizers) {
                Field field = aggregate.getSchema().getFields()[nGroups+schemaIndex++];
                String varName = field.getName();
                String prefix = getSingleFieldTuplesConverter().isUnwrapped()
                        ? varName + "."
                        : "this."; // it works but it is super ugly. TODO replace condition with function
                String exprStr = convertExpr(finalizer, aggregate.getSchema(), prefix);
                finishGenBodyForAggFinalizers.append(varName).append(" = ").append(exprStr).append(';');
            }
        }
        return finishGenBodyForAggFinalizers.toString();
    }

    // Utilities to deal with generated types (records)

    protected String getOrCreateRecordType(Expression[] expressions, String prefix) {
        String[] names = IntStream.range(0, expressions.length).mapToObj(i -> "_" + i).toArray(String[]::new);
        return getOrCreateRecordType(expressions, prefix, names);
    }

    protected String getOrCreateRecordType(Expression[] expressions, String prefix, Schema schema) {
        String[] names = Arrays.stream(schema.getFields()).map(Field::getName).toArray(String[]::new);
        return getOrCreateRecordType(expressions, prefix, names);
    }

    protected String getOrCreateRecordType(String prefix, Schema schema) {
        String[] types = Arrays.stream(schema.getFields()).map(f -> f.getType().getSimpleName()).toArray(String[]::new);
        String[] names = Arrays.stream(schema.getFields()).map(Field::getName).toArray(String[]::new);
        return createRecordType(types, prefix, names);
    }

    protected String getOrCreateRecordType(Expression[] expressions, String prefix, String[] names) {
        // TODO this method could (and given its name, it should) cache
        //  created record types to return a previously created one in case of match.
        //  note: to be decided if the prefix should be part of the match, or only expr-types/names.
        String[] types = Arrays.stream(expressions)
                .map(expr -> expr.type().getSimpleName())
                .toArray(String[]::new);
        return createRecordType(types, prefix, names);
    }

    protected String createRecordType(String[] types, String prefix, String[] names) {
        if (types.length > 1 || getSingleFieldTuplesConverter() == SingleFieldTuplesConverter.KEEP_TUPLE) {
            return doCreateRecordType(types, prefix, names);
        } else if (getSingleFieldTuplesConverter() == SingleFieldTuplesConverter.TO_OBJECT) {
            // single field tuple
            return TypingUtils.boxed(types[0]);
        } else {
            // single field tuple
            return types[0];
        }
    }

    private String doCreateRecordType(String[] types, String prefix, String[] names) {
        // Note: we do not create an actual record anymore,
        // since we need backward compatibility with JDK8

        String className = codegenContext.freshName(prefix);

        String fields = IntStream.range(0, types.length)
                .mapToObj(i -> "final %s %s;".formatted(types[i], names[i]))
                .collect(Collectors.joining(EOL));

        String constructorParams = IntStream.range(0, types.length)
                .mapToObj(i -> "%s %s".formatted(types[i], names[i]))
                .collect(Collectors.joining(", "));

        String constructorBody = Arrays.stream(names)
                .map(name -> "this.%s = %s;".formatted(name, name))
                .collect(Collectors.joining(EOL));

        String getters = IntStream.range(0, types.length)
                .mapToObj(i -> """
                        public final %s %s() {
                            return %s;
                        }
                        """.formatted(types[i], names[i], names[i]))
                .collect(Collectors.joining(EOL + EOL));

        String constructor = """
                %s(%s) {
                %s
                }
                """.formatted(className, constructorParams, constructorBody);

        String equalsBody = IntStream.range(0, types.length)
                .mapToObj(i -> {
                    String name = names[i];
                    if(TypingUtils.isPrimitive(types[i])) {
                        return name + " == otherCast." + name;
                    } else {
                        return name + ".equals(otherCast." + name + ")";
                    }
                })
                .collect(Collectors.joining(" && "));

        String equals = """
                public boolean equals(Object other) {
                    if(other.getClass() == %s.class) {
                        %s otherCast = (%s) other;
                        return %s;
                    } else {
                        return false;
                    }
                }
                """.formatted(className, className, className, indent(equalsBody));

        String hashCode = """
                public int hashCode() {
                    return Objects.hash(%s);
                }
                """.formatted(String.join(",", names));

        String classCode = """
                public static final class %s {
                %s
                
                %s
                
                %s
                
                %s
                
                %s
                }
                """.formatted(
                className,
                indent(fields),
                indent(constructor),
                indent(getters),
                indent(equals),
                indent(hashCode));

        codegenContext.addInnerClass(className, classCode);
        return className;
    }



    // Utilities to deal with expressions

    protected String getConstructorInvocationCode(String className,
                                                  Schema inputSchema,
                                                  String inputPrefix,
                                                  Expression[] expressions) {
        Function<Expression, String> exprConverter =
                expression -> convertExpr(expression, inputSchema, inputPrefix);
        return getConstructorInvocationCode(className, exprConverter, expressions);
    }

    protected String getConstructorInvocationCode(String className,
                                                  Schema inputSchema,
                                                  Function<InputRef, String> inputRefConverter,
                                                  Expression[] expressions) {
        Function<Expression, String> exprConverter =
                expression -> convertExpr(expression, inputSchema, inputRefConverter);
        return getConstructorInvocationCode(className, exprConverter, expressions);
    }

    protected String getConstructorInvocationCode(String className,
                                                  Function<Expression, String> exprConverter,
                                                  Expression[] expressions) {
        return "new %s(%s)".formatted(className,
                Arrays.stream(expressions)
                        .map(exprConverter)
                        .collect(Collectors.joining(", ")));
    }

    protected String convertExpr(Expression expr, Schema inputSchema, Function<InputRef, String> inputRefConverter) {
        return SqlExprToJavaString.convert(expr, inputSchema, inputRefConverter, constantsDeclarationMap, codegenContext);
    }

    protected String convertExpr(Expression expr, Schema inputSchema, String inputRefPrefix) {
        if (getSingleFieldTuplesConverter().isUnwrappedSchema(inputSchema)) {
            if (!inputRefPrefix.endsWith(".")) {
                throw new IllegalStateException("inputRefPrefix is expected to end with a dot (.) but it does not: " + inputRefPrefix);
            }
            // This is quite hacky.
            // We are here iff we have an unwrapped row (see StreamCompilerOptions.isUnwrapped).
            // Unwrapped rows have always only one field, say F(name=x, ...).
            // We deal with this situation by:
            //  - getting rid of the given inputPrefix (which is "row." in most of the cases, maybe even all)
            //  - creating a fake schema S that has only one field, F(name=row, ...), such that:
            //      - S.get(row) -> return F  // as usual, but also:
            //      - S.get(F) -> return F
            Field originalField = inputSchema.getFields()[0];
            String nameForFakeField = inputRefPrefix.substring(0, inputRefPrefix.length() - 1);
            Field field = new Field(
                    nameForFakeField, originalField.getType(), originalField.isNullable(), false);
            HashMap<String, Field> schemaMap = new HashMap<>();
            schemaMap.put(nameForFakeField, field);
            schemaMap.put(originalField.getName(), field);
            inputSchema = new Schema(schemaMap, new Field[]{field});
            inputRefPrefix = "";
        }
        return SqlExprToJavaString.convert(expr, inputSchema, inputRefPrefix, constantsDeclarationMap, codegenContext);
    }


}
