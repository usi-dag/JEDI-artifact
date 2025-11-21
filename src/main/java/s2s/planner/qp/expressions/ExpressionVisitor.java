package s2s.planner.qp.expressions;

public interface ExpressionVisitor<T> {
    T visit(InputRef expr);

    T visit(Const expr);

    T visit(Cast expr);

    // arithmetic
    T visit(Add expr);
    T visit(Sub expr);
    T visit(Div expr);
    T visit(Mul expr);
    T visit(Mod expr);

    // logic
    T visit(And expr);
    T visit(Or expr);
    T visit(Not expr);
    T visit(IsNull expr);

    // comparisons
    T visit(GreaterThanEqual expr);
    T visit(GreaterThan expr);

    T visit(LessThanEqual expr);
    T visit(LessThan expr);
    T visit(Equals expr);
    T visit(NotEquals expr);

    // string operations
    T visit(StrStartsWith expr);
    T visit(StrEndsWith expr);
    T visit(StrContains expr);
    T visit(StrMultiContains expr);
    T visit(Like expr);
    T visit(InStringSet expr);
    T visit(InNumSet expr);
    T visit(SubString expr);

    T visit(Case expr);

    // date operations
    T visit(Extract expr);

}
