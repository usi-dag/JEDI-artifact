package s2s.experiments;


import s2s.bench_generator.MaybePlannedQuery;
import s2s.engine.Date;
import s2s.planner.qp.expressions.*;
import s2s.planner.qp.expressions.*;
import s2s.planner.qp.operators.*;
import s2s.planner.calcite.ConnectionUtil;
import s2s.planner.calcite.SchemaTable;
import s2s.planner.qp.Field;
import s2s.planner.qp.S2SPlan;
import s2s.planner.qp.Schema;
import s2s.planner.qp.operators.*;
import s2s.planner.qp.sources.ArrayDataSource;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TPCHQueriesOptPlanned {


    private final CalciteConnection connection = ConnectionUtil.connection();
    private final SchemaPlus rootSchema = connection.getRootSchema();
    private final Map<String, Schema> schemas;

    private TPCHQueriesOptPlanned(Map<String, Schema> schemas) {
        this.schemas = schemas;
    }

    static Map<String, MaybePlannedQuery> getQueries(Map<String, Schema> schemas) {
        Map<String, MaybePlannedQuery> queries = new HashMap<>();
        TPCHQueriesOpt.QUERIES.forEach((key, value) -> queries.put(key, planned(value, null)));

        TPCHQueriesOptPlanned tpch = new TPCHQueriesOptPlanned(schemas);
        schemas.forEach((s, schema) -> tpch.rootSchema.add(s, new SchemaTable(schema)));

        queries.put("Q01", planned(TPCHQueriesOpt.Q1, tpch.planQ1()));
        queries.put("Q03", planned(TPCHQueriesOpt.Q3, tpch.planQ3()));
        queries.put("Q04", planned(TPCHQueriesOpt.Q4, tpch.planQ4()));
        queries.put("Q06", planned(TPCHQueriesOpt.Q6, tpch.planQ6()));
        queries.put("Q09", planned(TPCHQueriesOpt.Q9, tpch.planQ9()));
        queries.put("Q11", planned(TPCHQueriesOpt.Q11, tpch.planQ11()));
        queries.put("Q15", planned(TPCHQueriesOpt.Q15, tpch.planQ15()));
        queries.put("Q16", planned(TPCHQueriesOpt.Q16, tpch.planQ16()));
        queries.put("Q18", planned(TPCHQueriesOpt.Q18, tpch.planQ18()));

        return queries;
    }

    private static MaybePlannedQuery planned(String query, Operator root) {
        if (root == null) {
            return new MaybePlannedQuery(query);
        }
        return new MaybePlannedQuery(query, new S2SPlan(root));
    }

    private ArrayTableScan scan(String table) {
        ArrayDataSource source = new ArrayDataSource(table, schemas.get(table), table);
        return new ArrayTableScan(source);
    }

    public static InputRef ref(String name, Schema inputSchema) {
        Field[] fields = inputSchema.getFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            if (field.getName().equals(name)) {
                return new InputRef(i, field);
            }
        }
        throw new RuntimeException("Field " + name + " not found in schema " + inputSchema);
    }

    public static <T> T[] array(T... arr) {
        return arr;
    }

    public static int find(String column, Field[] fields) {
        for (int i = 0; i < fields.length; i++) {
            if (column.equals(fields[i].getName())) return i;
        }
        throw new RuntimeException("Column: " + column + " not in fields: " + Arrays.toString(fields));
    }

    public static InputRef makeRef(Schema schema, String refName) {
        return makeRef(schema, refName, 0);
    }

    public static InputRef makeRef(Schema schema, String refName, int addendForLeftSideJoin) {
        Field[] fields = schema.getFields();
        int idx = find(refName, fields);
        return new InputRef(idx + addendForLeftSideJoin, fields[idx]);
    }

    public static InputRef[] makeJoinRefs(Schema leftSchema, Schema rightSchema, String[] leftCols, String[] rightCols) {
        int numColsOnLeft = leftSchema.getFields().length;
        InputRef[] refs = new InputRef[leftCols.length + rightCols.length];
        int refsIdx = 0;
        for (String leftCol : leftCols) {
            refs[refsIdx++] = makeRef(leftSchema, leftCol);
        }
        for (String rightCol : rightCols) {
            refs[refsIdx++] = makeRef(rightSchema, rightCol, numColsOnLeft);
        }
        return refs;
    }

    public static InputRef[] makeRefs(Schema schema, String... columns) {
        InputRef[] refs = new InputRef[columns.length];
        int refsIdx = 0;
        for (String column : columns) {
            refs[refsIdx++] = makeRef(schema, column);
        }
        return refs;
    }

    public static Schema refsToSchema(InputRef[] refs) {
        return Schema.byFields(Arrays.stream(refs).map(InputRef::field).toArray(Field[]::new));
    }

    public static Const aConst(Object val) {
        return new Const(val);
    }

    public static Const constDate(String val) {
        return new Const(new Date(val));
    }


    private Operator planQ1() {
        ArrayTableScan lineitem = scan("lineitem");
        Schema schema = lineitem.getSchema();
        // lineitem.l_shipdate <= date '1998-12-01' - interval '90' day
        Expression filter = new LessThanEqual(
                ref("l_shipdate", schema),
                constDate("1998-09-02")
        );
        Predicate predicate = new Predicate(filter, lineitem);

        // Aggregate
        Schema aggInputSchema = predicate.getSchema();

        // sum(lineitem.l_quantity) as sum_qty,
        SqlAggFunction sum1 = SqlAggFunction.sum(ref("l_quantity", aggInputSchema));

        // sum(lineitem.l_extendedprice) as sum_base_price,
        SqlAggFunction sum2 = SqlAggFunction.sum(ref("l_extendedprice", aggInputSchema));

        // sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as sum_disc_price
        Expression sum3Expr = new Mul(
                ref("l_extendedprice", aggInputSchema),
                new Sub(aConst(1), ref("l_discount", aggInputSchema)));

        SqlAggFunction sum3 = SqlAggFunction.sum(sum3Expr);

        // sum(lineitem.l_extendedprice * (1 - lineitem.l_discount) * (1 + l_tax)) as sum_charge,
        SqlAggFunction sum4 = SqlAggFunction.sum(new Mul(
                sum3Expr,
                new Add(aConst(1), ref("l_tax", schema))
        ));

        // avg(lineitem.l_quantity) as avg_qty,
        SqlAggFunction avg1 = SqlAggFunction.avg(ref("l_quantity", aggInputSchema));

        // avg(lineitem.l_extendedprice) as avg_price,
        SqlAggFunction avg2 = SqlAggFunction.avg(ref("l_extendedprice", aggInputSchema));

        // avg(lineitem.l_discount) as avg_disc,
        SqlAggFunction avg3 = SqlAggFunction.avg(ref("l_discount", aggInputSchema));

        // count(*) as count_order
        SqlAggFunction count_order = SqlAggFunction.countStar();

        // Group by keys
        //  lineitem.l_returnflag,
        //  lineitem.l_linestatus
        Expression[] groupKeys = makeRefs(aggInputSchema, "l_returnflag", "l_linestatus");

        Aggregate aggregate = new Aggregate(
                groupKeys,
                array(sum1, sum2, sum3, sum4, avg1, avg2, avg3, count_order),
                array(
                        "l_returnflag", "l_linestatus",
                        "sum_qty", "sum_base_price", "sum_disc_price", "sum_charge",
                        "avg_qty", "avg_price", "avg_disc",
                        "count_order"),
                predicate
        );

        // ORDER BY
        //  lineitem.l_returnflag,
        //  lineitem.l_linestatus
        Expression[] orderKeys = makeRefs(schema, "l_returnflag", "l_linestatus");
        return new Sort(orderKeys, aggregate);
    }

    private Operator planQ3() {
        ArrayTableScan orders = scan("orders");
        Schema ordersSchema = orders.getSchema();
        ArrayTableScan customer = scan("customer");
        Schema customerSchema = customer.getSchema();
        ArrayTableScan lineitem = scan("lineitem");
        Schema lineitemSchema = lineitem.getSchema();

        /*
        with cus_ord as (
            select *
            from orders, customer
            where customer.c_custkey = orders.o_custkey
            and customer.c_mktsegment = 'BUILDING'
            and orders.o_orderdate < date '1995-03-15'
        )
        */

        Predicate customerFilter = new Predicate(
                new Equals(ref("c_mktsegment", customerSchema), aConst("BUILDING")),
                customer);

        Predicate orderFilter = new Predicate(
                new LessThan(ref("o_orderdate", ordersSchema), constDate("1995-03-15")),
                orders);

        HashJoin cusordJoin = HashJoin.createInnerWithRightProjectMapper(
                customerFilter, orderFilter,
                makeRefs(customerSchema, "c_custkey"),
                makeRefs(ordersSchema, "o_custkey"));

        Expression lineitemFilter = new GreaterThan(
                ref("l_shipdate", lineitemSchema), constDate("1995-03-15"));

        Predicate lineitemPredicate = new Predicate(lineitemFilter, lineitem);

        Schema cusordJoinSchema = cusordJoin.outputSchema();

        InputRef[] join2OutputExpr = makeJoinRefs(
                cusordJoinSchema, lineitemSchema,
                array("o_orderdate", "o_shippriority"),
                array("l_orderkey", "l_extendedprice", "l_discount")
        );
        Schema join2Schema = refsToSchema(join2OutputExpr);

        HashJoin join2 = HashJoin.create(
                cusordJoin, lineitemPredicate,
                makeRefs(cusordJoinSchema, "o_orderkey"),
                makeRefs(lineitemSchema, "l_orderkey"),
                join2OutputExpr,
                Join.JoinType.INNER,
                join2Schema);

        Expression projectExpr = new Mul(
                ref("l_extendedprice", join2Schema),
                new Sub(aConst(1), ref("l_discount", lineitemSchema))
        );

        Expression[] groupKeys = makeRefs(join2Schema,
                "l_orderkey", "o_orderdate", "o_shippriority");

        Aggregate aggregate = new Aggregate(
                groupKeys,
                array(SqlAggFunction.sum(projectExpr)),
                array("l_orderkey", "o_orderdate", "o_shippriority", "revenue"),
                join2
        );

        Expression[] sortExpr = makeRefs(aggregate.getSchema(),
                "revenue", "o_orderdate");
        Sort sort = new Sort(sortExpr,
                array(Sort.Direction.DESC, Sort.Direction.ASC), aggregate);

        return new Limit(10, sort);
    }

    private Operator planQ4() {
        ArrayTableScan orders = scan("orders");
        Schema ordersSchema = orders.getSchema();
        ArrayTableScan lineitem = scan("lineitem");
        Schema lineitemSchema = lineitem.getSchema();

        // filter
        //  o_orderdate >= date '1993-07-01' AND o_orderdate > date '1993-04-01'
        Expression ordersFilterExpr = new And(
                new GreaterThanEqual(ref("o_orderdate", ordersSchema), constDate("1993-07-01")),
                new LessThan(ref("o_orderdate", ordersSchema), constDate("1993-10-01")));

        Predicate ordersFilter = new Predicate(ordersFilterExpr, orders);

        // and exists (
        //		select
        //			*
        //		from
        //			lineitem
        //		where
        //			lineitem.l_orderkey = orders.o_orderkey
        //			and lineitem.l_commitdate < lineitem.l_receiptdate
        Expression lineitemFilter = new LessThan(
                ref("l_commitdate", lineitemSchema),
                ref("l_receiptdate", lineitemSchema));

        Predicate lineitemPredicate = new Predicate(lineitemFilter, lineitem);
        HashJoin join1 = HashJoin.createSemi(
                ordersFilter, lineitemPredicate,
                array(ref("o_orderkey", ordersSchema)),
                array(ref("l_orderkey", lineitemSchema))
        );

        // Group by keys
        //  o_orderpriority
        Expression[] groupKeys1 = makeRefs(join1.getSchema(), "o_orderpriority");

        Aggregate aggregate1 = new Aggregate(
                groupKeys1,
                array(SqlAggFunction.countStar()),
                array("o_orderpriority", "order_count"),
                join1
        );

        // order by o_orderpriority
        Expression[] orderKeys1 = makeRefs(aggregate1.getSchema(), "o_orderpriority");

        return new Sort(orderKeys1, aggregate1);
    }

    private Operator planQ6() {
        ArrayTableScan lineitem = scan("lineitem");
        Schema schema = lineitem.getSchema();
        Expression filter = new And(
                // l_shipdate >= date '1994-01-01'
                new GreaterThanEqual(ref("l_shipdate", schema), constDate("1994-01-01")),

                // l_shipdate < date '1994-01-01' + interval '1' year
                new LessThan(ref("l_shipdate", schema), constDate("1995-01-01")),

                // l_discount between .05 and .07
                new GreaterThanEqual(ref("l_discount", schema), aConst(.05)),
                new LessThanEqual(ref("l_discount", schema), aConst(.07)),

                // l_quantity < 24
                new LessThan(ref("l_quantity", schema), aConst(24)));

        Predicate predicate = new Predicate(filter, lineitem);

        Expression projectExpr = new Mul(ref("l_extendedprice", schema), ref("l_discount", schema));

        return new Aggregate(
                array(),
                array(SqlAggFunction.sum(projectExpr)),
                new int[]{},
                array("revenue"),
                predicate
        );
    }

    private Operator planQ9() {
        ArrayTableScan nation = scan("nation");
        Schema nationSchema = nation.getSchema();
        ArrayTableScan supplier = scan("supplier");
        Schema supplierSchema = supplier.getSchema();
        ArrayTableScan part = scan("part");
        Schema partSchema = part.getSchema();
        ArrayTableScan partsupp = scan("partsupp");
        Schema partsuppSchema = partsupp.getSchema();
        ArrayTableScan lineitem = scan("lineitem");
        Schema lineitemSchema = lineitem.getSchema();
        ArrayTableScan orders = scan("orders");
        Schema ordersSchema = orders.getSchema();

        // sup_nat as (
        //    select * from nation, supplier
        //    where s_nationkey = n_nationkey),
        var sup_nat_refs = makeJoinRefs(nationSchema, supplierSchema, array("n_name"), array("s_suppkey"));

        HashJoin sup_nat = HashJoin.createInner(
                nation, supplier,
                makeRefs(nationSchema, "n_nationkey"),
                makeRefs(supplierSchema, "s_nationkey"),
                sup_nat_refs);

        // par_supp as(
        //    select * from part, partsupp
        //    where p_partkey = ps_partkey
        //    and p_name like '%green%'),
        Expression partFilterExpr = new StrContains("green", ref("p_name", partSchema));
        Predicate partPredicate = new Predicate(partFilterExpr, part);

        HashJoin par_supp = HashJoin.createInnerWithRightProjectMapper(
                partPredicate, partsupp,
                makeRefs(partSchema, "p_partkey"),
                makeRefs(partsuppSchema, "ps_partkey"));


        // sn_ps as (
        //    select * from sup_nat, par_supp
        //    where s_suppkey = ps_suppkey)
        var sn_ps_refs = makeJoinRefs(
                sup_nat.getSchema(), par_supp.getSchema(),
                array("n_name", "s_suppkey"), array("ps_suppkey", "ps_partkey", "ps_supplycost"));

        HashJoin sn_ps = HashJoin.createInner(
                sup_nat, par_supp,
                makeRefs(sup_nat.getSchema(), "s_suppkey"),
                makeRefs(par_supp.getSchema(), "ps_suppkey"),
                sn_ps_refs);


        // snps_line as (
        //    select * from sn_ps, lineitem
        //    where ps_partkey = l_partkey
        //    and ps_suppkey = l_suppkey),
        var snps_line_refs = makeJoinRefs(
                sn_ps.getSchema(), lineitemSchema,
                array("n_name", "ps_supplycost"),
                array("l_extendedprice", "l_discount", "l_quantity", "l_orderkey"));

        HashJoin snps_line = HashJoin.createInner(
                sn_ps, lineitem,
                makeRefs(sn_ps.getSchema(), "s_suppkey", "ps_partkey"),
                makeRefs(lineitemSchema, "l_suppkey", "l_partkey"),
                snps_line_refs);

        // profit as (
        //    select
        //        n_name as nation,
        //        year(o_orderdate) as o_year,
        //        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        //    from snps_line, orders
        //    where l_orderkey = o_orderkey)
        var snps_line_schema = snps_line.getSchema();

        var profit_refs = makeJoinRefs(snps_line_schema, ordersSchema,
                array("n_name", "ps_supplycost", "l_extendedprice", "l_discount", "l_quantity"),
                array("o_orderdate"));

        Expression amount = new Sub(
                new Mul(profit_refs[2], //  l_extendedprice
                        new Sub(aConst(1), profit_refs[3])), // 1-l_discount
                new Mul(profit_refs[1], profit_refs[4])); // ps_supplycost * l_quantity

        Expression o_year = new Extract(Extract.TimeUnit.YEAR,
                makeRef(ordersSchema, "o_orderdate", snps_line_schema.getFields().length));
        InputRef n_name = profit_refs[0];
        Expression[] profitExpressions = {n_name, o_year, amount};

        Schema profitSchema = Schema.byFields(
                new Field("nation", n_name.field().getType()),
                new Field("o_year", o_year.type()),
                new Field("amount", amount.type()));

        HashJoin profit = HashJoin.create(
                snps_line, orders,
                makeRefs(snps_line_schema, "l_orderkey"),
                makeRefs(ordersSchema, "o_orderkey"),
                profitExpressions,
                Join.JoinType.INNER,
                profitSchema);

        // select nation, o_year, sum(amount) as sum_profit from profit
        //group by nation, o_year order by nation, o_year DESC
        Expression[] nation_o_year_refs = makeRefs(profitSchema, "nation", "o_year");

        Aggregate aggregate = new Aggregate(
                nation_o_year_refs,
                array(SqlAggFunction.sum(ref("amount", profitSchema))),
                array("nation", "o_year", "sum_profit"),
                profit);

        return new Sort(nation_o_year_refs, array(Sort.Direction.ASC, Sort.Direction.DESC), aggregate);
    }

    private Operator planQ11() {
        ArrayTableScan nation = scan("nation");
        ArrayTableScan supplier = scan("supplier");
        ArrayTableScan partsupp = scan("partsupp");

        /*
        sup_nat as (
        select * from nation, supplier
        where n_nationkey = s_nationkey and n_name = 'GERMANY'),
        */
        Operator nationFilter = new Predicate(
                new Equals(ref("n_name", nation.getSchema()), aConst("GERMANY")),
                nation);

        Operator sup_nat = HashJoin.createInnerWithRightProjectMapper(
                nationFilter, supplier,
                array(ref("n_nationkey", nation.getSchema())),
                array(ref("s_nationkey", supplier.getSchema())));

        /*
        ps_sup as (
        select ps_partkey, 0+coalesce(sum(ps_supplycost * ps_availqty), 0) as value1
        from sup_nat, partsupp
        where s_suppkey = ps_suppkey
        group by ps_partkey),
         */

        Operator nationSuppPartSuppJoin = HashJoin.createInnerWithRightProjectMapper(
                sup_nat, partsupp,
                array(ref("s_suppkey", sup_nat.getSchema())),
                array(ref("ps_suppkey", partsupp.getSchema())));

        Schema psSchema = partsupp.getSchema();
        Expression projectExpr = new Mul(ref("ps_supplycost", psSchema), ref("ps_availqty", psSchema));
        Operator ps_sup = new Aggregate(
                array(ref("ps_partkey", psSchema)),
                array(SqlAggFunction.sum(projectExpr)),
                array("ps_partkey", "value1"),
                nationSuppPartSuppJoin);

        // value_sum as (select sum(value1)*0.0001 as v from ps_sup)
        // quite ugly - creating two aggs only to get the schema... TODO improve?
        SqlAggFunction sumValue = SqlAggFunction.sum(ref("value1", ps_sup.getSchema()));
        Operator value_sumAgg = new Aggregate(
                array(),
                array(sumValue),
                array("value1"),
                ps_sup
        );
        Schema value_sumAggSchema = value_sumAgg.getSchema();
        Operator value_sum = new Aggregate(
                array(),
                array(sumValue),
                array(new Mul(ref("value1", value_sumAggSchema), aConst(0.0001))),
                array("value1"),
                ps_sup
        );

        Operator nsJoin = new SingletonFilterJoin(
                value_sum, ps_sup,
                new GreaterThan(
                        makeRef(ps_sup.getSchema(), "value1", value_sum.getSchema().getFields().length),
                        makeRef(value_sum.getSchema() ,"value1"))
        );

        return new Sort(
                array(ref("value1", nsJoin.getSchema())),
                array(Sort.Direction.DESC),
                nsJoin);
    }

    private Operator planQ15() {
        ArrayTableScan lineitem = scan("lineitem");
        Schema lineitemSchema = lineitem.getSchema();
        ArrayTableScan supplier = scan("supplier");
        Schema supplierSchema = supplier.getSchema();

        /*
        with revenue as (
            select
                l_suppkey as supplier_no,
                sum(l_extendedprice * (1 - l_discount)) as total_revenue
            from
                lineitem
            where
                l_shipdate >= date '1996-01-01'
                and l_shipdate < date '1996-04-01'
            group by
                l_suppkey)
        */

        // where
        //		l_shipdate >= date '1996-01-01'
        //		and l_shipdate < date '1996-01-01' + interval '3' month
        Expression shipdate = ref("l_shipdate", lineitemSchema);
        Expression lineitemFilter = new And(
                new GreaterThanEqual(shipdate, constDate("1996-01-01")),
                new LessThan(shipdate, constDate("1996-04-01")));

        Predicate lineitemPredicate = new Predicate(lineitemFilter, lineitem);

        // sum(l_extendedprice * (1 - l_discount)) as total_revenue
        Expression revenueProject = new Mul(
                ref("l_extendedprice", lineitemSchema),
                new Sub(aConst(1), ref("l_discount", lineitemSchema)));

        // Aggregate - Group by l_suppkey
        Aggregate revenue = new Aggregate(
                makeRefs(lineitemSchema, "l_suppkey"),
                array(SqlAggFunction.sum(revenueProject)),
                array("l_suppkey", "total_revenue"),
                lineitemPredicate);
        Schema revenueSchema = revenue.getSchema();

        // max_rev as (select max(total_revenue) as max_total_rev from revenue0)
        Aggregate max_rev = new Aggregate(
                array(),
                array(SqlAggFunction.max(ref("total_revenue", revenueSchema))),
                array("total_revenue"),
                revenue);

        Expression nonEquiCondition = new Equals(
                makeRef(max_rev.getSchema(), "total_revenue"),
                makeRef(revenueSchema, "total_revenue", max_rev.getSchema().getFields().length));

        Join revenueSelfJoin = new SingletonFilterJoin(max_rev, revenue, nonEquiCondition);

        // select s_suppkey, s_name, s_address, s_phone, total_revenue
        // from max_rev, revenue0, supplier
        // where s_suppkey = l_suppkey and total_revenue = max_total_rev
        InputRef[] joinExprs = makeJoinRefs(
                revenueSchema, supplierSchema,
                array("total_revenue"),
                array("s_suppkey", "s_name", "s_address", "s_phone"));

        Schema joinSchema = refsToSchema(joinExprs);

        Operator join = HashJoin.createInner(
                revenueSelfJoin, supplier,
                makeRefs(revenueSchema, "l_suppkey"),
                array(makeRef(supplierSchema, "s_suppkey", revenueSchema.getFields().length)),
                joinExprs);

        // Order by s_suppkey
        return new Sort(makeRefs(joinSchema, "s_suppkey"), join);
    }

    private Operator planQ16() {
        ArrayTableScan partsupp = scan("partsupp");
        Schema partsuppSchema = partsupp.getSchema();
        ArrayTableScan supplier = scan("supplier");
        Schema supplierSchema = supplier.getSchema();
        ArrayTableScan part = scan("part");
        Schema partSchema = part.getSchema();

        Expression notEq = new Not(new Equals(ref("p_brand", partSchema), aConst("Brand#45")));
        Expression notStartsWith = new Not(new StrStartsWith("MEDIUM POLISHED", ref("p_type", partSchema)));
        InNumSet inNumSet = new InNumSet(ref("p_size", partSchema), Set.of(49, 14, 23, 45, 19, 3, 36, 9));

        Operator partFiltered = new Predicate(new And(notEq, notStartsWith, inNumSet), part);

        InputRef[] part_partsupp_refs = makeJoinRefs(
                partSchema, partsuppSchema,
                array("p_brand", "p_type", "p_size"),
                array("ps_suppkey"));
        Operator part_partsupp = HashJoin.createInner(partFiltered, partsupp,
                array(ref("p_partkey", partSchema)),
                array(ref("ps_partkey", partsuppSchema)),
                part_partsupp_refs);

        StrMultiContains multiContains = new StrMultiContains(array("Customer", "Complaints"),
                ref("s_comment", supplierSchema));
        Operator supplierFiltered = new Predicate(multiContains, supplier);
        Operator antiSupp = HashJoin.createAnti(part_partsupp, supplierFiltered,
                array(ref("ps_suppkey", part_partsupp.getSchema())),
                array(ref("s_suppkey", supplierSchema)));

        Aggregate aggregateForDistinct = new Aggregate(
                makeRefs(antiSupp.getSchema(), "p_brand", "p_type", "p_size", "ps_suppkey"),
                array(),
                array("p_brand", "p_type", "p_size", "ps_suppkey"),
                antiSupp);

        Aggregate aggregate = new Aggregate(
                makeRefs(aggregateForDistinct.getSchema(), "p_brand", "p_type", "p_size"),
                array(new SqlAggFunction(ref("ps_suppkey", aggregateForDistinct.getSchema()), SqlAggFunction.AggKind.COUNT)),
                array("p_brand", "p_type", "p_size", "supplier_cnt"),
                aggregateForDistinct);

        return new Sort(
                array(makeRefs(aggregate.getSchema(), "supplier_cnt", "p_brand", "p_type", "p_size")),
                array(Sort.Direction.DESC, Sort.Direction.ASC, Sort.Direction.ASC, Sort.Direction.ASC),
                aggregate);
    }

    private Operator planQ18() {
        ArrayTableScan lineitem = scan("lineitem");
        Schema lineitemSchema = lineitem.getSchema();
        ArrayTableScan customer = scan("customer");
        Schema customerSchema = customer.getSchema();
        ArrayTableScan orders = scan("orders");
        Schema ordersSchema = orders.getSchema();

        // all_sums as (
        //    select l_orderkey, sum(l_quantity) as q_sums
        //    from lineitem
        //    group by l_orderkey),
        Expression[] groupKeys1 = makeRefs(lineitemSchema, "l_orderkey");

        Aggregate aggregate1 = new Aggregate(
                groupKeys1,
                new SqlAggFunction[]{SqlAggFunction.sum(ref("l_quantity", lineitemSchema))},
                array("l_orderkey", "q_sums"),
                lineitem
        );

        // ord_cus as (
        //    select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        //    from customer, orders
        //    where c_custkey = o_custkey
        //    and o_orderkey in (select l_orderkey from all_sums where q_sums > 300))

        Expression filter1 = new GreaterThan(ref("q_sums", aggregate1.getSchema()), aConst(300));
        Predicate predicate1 = new Predicate(filter1, aggregate1);

        // SEMI join
        HashJoin join1 = HashJoin.createSemi(
                orders, predicate1,
                array(ref("o_orderkey", ordersSchema)),
                array(ref("l_orderkey", predicate1.getSchema()))
        );

        Schema join1Schema = join1.getSchema();
        InputRef[] join2Exprs = makeJoinRefs(
                customerSchema, join1Schema,
                array("c_name", "c_custkey"),
                array("o_orderkey", "o_orderdate", "o_totalprice")
        );
        Schema join2Schema = refsToSchema(join2Exprs);

        HashJoin join2 = HashJoin.create(
                customer, join1,
                array(ref("c_custkey", customerSchema)),
                array(ref("o_custkey", join1.getSchema())),
                join2Exprs,
                Join.JoinType.INNER,
                join2Schema);

        InputRef[] join3Exprs = makeJoinRefs(
                join2Schema, lineitemSchema,
                array("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"),
                array("l_quantity")
        );
        Schema join3Schema = refsToSchema(join3Exprs);

        HashJoin join3 = HashJoin.create(
                join2, lineitem,
                array(ref("o_orderkey", join2Schema)),
                array(ref("l_orderkey", lineitemSchema)),
                join3Exprs,
                Join.JoinType.INNER,
                join3Schema
        );

        Expression[] groupKeys2 = makeRefs(join3Schema,
                "c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice");

        Aggregate aggregate2 = new Aggregate(
                groupKeys2,
                array(SqlAggFunction.sum(ref("l_quantity", lineitemSchema))),
                array("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice", "sum_qty"),
                join3
        );

        Expression[] orderKeys = makeRefs(aggregate2.getSchema(), "o_totalprice", "o_orderdate");

        Sort sort = new Sort(orderKeys, array(Sort.Direction.DESC, Sort.Direction.ASC), aggregate2);
        return new Limit(100, sort);
    }
}
