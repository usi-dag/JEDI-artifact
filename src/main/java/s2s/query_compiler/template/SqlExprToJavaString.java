package s2s.query_compiler.template;

import s2s.engine.Date;
import s2s.engine.StringMultiContains;
import s2s.planner.qp.Field;
import s2s.planner.qp.Schema;
import s2s.planner.qp.expressions.*;
import s2s.planner.qp.expressions.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SqlExprToJavaString implements ExpressionVisitor<String> {
    final Map<String, String> inputRefPrefixesMap;
    final String inputRefPrefix;
    final Schema inputSchema;
    final Map<Const, String> constantsDeclarationMap;
    private final CodegenContext codegenContext;

    public SqlExprToJavaString(String inputRefPrefix, Schema inputSchema, Map<Const, String> constantsDeclarationMap, CodegenContext codegenContext) {
        this(inputRefPrefix, inputSchema, constantsDeclarationMap, new HashMap<>(), codegenContext);
    }

    public SqlExprToJavaString(String inputRefPrefix,
                               Schema inputSchema,
                               Map<Const, String> constantsDeclarationMap,
                               Map<String, String> inputRefPrefixesMap, CodegenContext codegenContext) {
        this.inputRefPrefix = inputRefPrefix;
        this.inputSchema = Objects.requireNonNull(inputSchema);
        this.constantsDeclarationMap = constantsDeclarationMap;
        this.inputRefPrefixesMap = inputRefPrefixesMap;
        this.codegenContext = codegenContext;
    }

    public static String convert(Expression expr,
                                 Schema inputSchema,
                                 String inputRefPrefix,
                                 Map<Const, String> constantsDeclarationMap,
                                 CodegenContext codegenContext) {
        return expr.accept(new SqlExprToJavaString(inputRefPrefix, inputSchema, constantsDeclarationMap, codegenContext));
    }

    public static String convert(Expression expr,
                                 Schema inputSchema,
                                 Function<InputRef, String> inputRefConverter,
                                 Map<Const, String> constantsDeclarationMap,
                                 CodegenContext codegenContext) {
        return expr.accept(new SqlExprToJavaString(null, inputSchema, constantsDeclarationMap, codegenContext) {
            @Override
            public String visit(InputRef expr) {
                return inputRefConverter.apply(expr);
            }
        });
    }


    @Override
    public String visit(InputRef expr) {
        Field field = inputSchema.get(expr.getIdentifier());
        String accessor = inputRefPrefixesMap.getOrDefault(field.getName(), inputRefPrefix) + field.getName();
        if (field.isGetter()) accessor += "()";
        return accessor;
    }

    @Override
    public String visit(Const expr) {
        if(expr.type() == Date.class && !constantsDeclarationMap.containsKey(expr)) {
            // dates are declared and initialized
            String initCode = "new Date(\"" + expr + "\");";
            String constName = codegenContext.addClassField("const", Date.class, initCode);
            constantsDeclarationMap.put(expr, constName);
        }

        String varName = constantsDeclarationMap.get(expr);
        if (varName != null) {
            return varName;
        }
        if(expr.type() == String.class)
            return "\"%s\"".formatted(expr);
        return expr.toString();
    }

    @Override
    public String visit(Cast expr) {
        return "(%s)".formatted(expr.type().getName()) + expr.expression().accept(this);
    }

    // arithmetic

    @Override
    public String visit(Add expr) {
        return primitiveBinary(expr, "+");
    }

    @Override
    public String visit(Sub expr) {
        return primitiveBinary(expr, "-");
    }

    @Override
    public String visit(Div expr) {
        return primitiveBinary(expr, "/");
    }

    @Override
    public String visit(Mul expr) {
        return primitiveBinary(expr, "*");
    }

    @Override
    public String visit(Mod expr) {
        return primitiveBinary(expr, "%");
    }

    private String primitiveBinary(BinaryExpression expr, String op) {
        return "(%s %s %s)".formatted(expr.getLeft().accept(this),
                op,
                expr.getRight().accept(this));
    }

    // logic

    @Override
    public String visit(And expr) {
        return booleanNary(expr.getExpressions(), "&&");
    }

    @Override
    public String visit(Or expr) {
        return booleanNary(expr.getExpressions(), "||");
    }

    @Override
    public String visit(Not expr) {
        return "!(%s)".formatted(expr.getExpression().accept(this));
    }

    @Override
    public String visit(IsNull expr) {
        return expr.getExpression().accept(this) + " == null";
    }

    private String booleanNary(Expression[] expressions, String op) {
        return "(" +
                Arrays.stream(expressions)
                .map(expression -> expression.accept(this))
                .collect(Collectors.joining(" " + op + " "))
                + ")";
        // note: parenthesis here preserves original evaluation order
    }

    // comparisons

    @Override
    public String visit(GreaterThanEqual expr) {
        return comparison(expr, ">= 0", ">=");
    }

    @Override
    public String visit(GreaterThan expr) {
        return comparison(expr, "> 0", ">");
    }

    @Override
    public String visit(LessThanEqual expr) {
        return comparison(expr, "<= 0", "<=");
    }

    @Override
    public String visit(LessThan expr) {
        return comparison(expr, "< 0", "<");
    }

    @Override
    public String visit(Equals expr) {
        Class<?> leftCls = expr.getLeft().type();
        if(leftCls.equals(String.class) || leftCls.equals(Date.class)) {
            return "(%s.equals(%s))".formatted(
                    expr.getLeft().accept(this),
                    expr.getRight().accept(this));
        }
        return comparison(expr, "== 0", "==");
    }

    @Override
    public String visit(NotEquals expr) {
        return comparison(expr, "!= 0", "!=");
    }


    private String comparison(BinaryExpression expr, String op, String opIfPrimitive) {
        if (expr.getLeft().type().isPrimitive()) {
            return primitiveBinary(expr, opIfPrimitive);
        }
        return "(%s.compareTo(%s) %s)".formatted(
                expr.getLeft().accept(this),
                expr.getRight().accept(this),
                op);
    }

    // string operations
    @Override
    public String visit(StrStartsWith expr) {
        return "%s.startsWith(\"%s\")".formatted(expr.getStringGetter().accept(this), expr.getPattern());
    }

    @Override
    public String visit(StrEndsWith expr) {
        return "%s.endsWith(\"%s\")".formatted(expr.getStringGetter().accept(this), expr.getPattern());
    }

    @Override
    public String visit(StrContains expr) {
        return "%s.contains(\"%s\")".formatted(expr.getStringGetter().accept(this), expr.getPattern());
    }

    @Override
    public String visit(StrMultiContains expr) {
        String params = Arrays.stream(expr.getPatterns())
                .map(pattern -> "\"" + pattern + "\"")
                .collect(Collectors.joining(","));
        String initCode = "new StringMultiContains(new String[] {" + params + "});";
        String constName = codegenContext.addClassField(
                "multiContainsPatterns", StringMultiContains.class, initCode);
        constantsDeclarationMap.put(new Const(expr.getPatterns()), constName);
        return "%s.match(%s)".formatted(constName, expr.getStringGetter().accept(this));
    }

    @Override
    public String visit(Like expr) {
        return null; // TODO
    }

    @Override
    public String visit(InStringSet inStringSet) {
        String params = inStringSet.getStrings().stream()
                .map(pattern -> "\"" + pattern + "\"")
                .collect(Collectors.joining(","));
        String initCode = "java.util.Set.of(" + params + ");";
        String constName = codegenContext.addClassField(
                "stringSet", Set.class, initCode);
        constantsDeclarationMap.put(new Const(inStringSet.getStrings()), constName);

        String input = inStringSet.getStringGetter().accept(this);
        return "%s.contains(%s)".formatted(constName, input);
    }

    @Override
    public String visit(InNumSet inNumSet) {
        String params = inNumSet.getSet().stream()
                .map(Objects::toString)
                .collect(Collectors.joining(","));
        String initCode = "java.util.Set.of(" + params + ");";
        String constName = codegenContext.addClassField(
                "stringSet", Set.class, initCode);
        constantsDeclarationMap.put(new Const(inNumSet.getSet()), constName);

        String input = inNumSet.getGetter().accept(this);
        return "%s.contains(%s)".formatted(constName, input);
    }

    @Override
    public String visit(SubString expr) {
        String sub = ".substring(%s, %s)".formatted(expr.start(), expr.start() + expr.len());
        return expr.stringGetter().accept(this) + sub;
    }

    @Override
    public String visit(Case expr) {
        return null;
    }

    @Override
    public String visit(Extract expr) {
        // TODO deal also with other date implementations
        if(expr.dateGetter().type() != Date.class) {
            throw new IllegalStateException("EXTRACT allowed only on dates");
        }
        String getter = switch (expr.timeUnit()) {
            case YEAR -> ".getYear()";
            case MONTH -> ".getMonth()";
        };
        return expr.dateGetter().accept(this) + getter;
    }
}
