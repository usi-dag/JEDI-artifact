package s2s.experiments;


import java.util.HashMap;
import java.util.Map;

public class TPCHQueriesOpt {

    public static final String Q1 = """
            select
            	lineitem.l_returnflag,
            	lineitem.l_linestatus,
            	sum(lineitem.l_quantity) as sum_qty,
            	sum(lineitem.l_extendedprice) as sum_base_price,
            	sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as sum_disc_price,
            	sum(lineitem.l_extendedprice * (1 - lineitem.l_discount) * (1 + l_tax)) as sum_charge,
            	avg(lineitem.l_quantity) as avg_qty,
            	avg(lineitem.l_extendedprice) as avg_price,
            	avg(lineitem.l_discount) as avg_disc,
            	count(*) as count_order
            from
            	lineitem
            where
            	lineitem.l_shipdate <= date '1998-12-01' - interval '90' day
            group by
            	lineitem.l_returnflag,
            	lineitem.l_linestatus
            order by
            	lineitem.l_returnflag,
            	lineitem.l_linestatus
            """;




    public static final String Q2 =
    """
    with
        reg_nat as (
            select n_nationkey, n_name
            from region, nation
            where r_regionkey = n_regionkey
              and r_name='EUROPE'),
        nat_sup as (
            select supplier.s_suppkey, supplier.s_acctbal, supplier.s_name,
                   reg_nat.n_name, supplier.s_address, supplier.s_phone, supplier.s_comment
            from reg_nat, supplier
            where supplier.s_nationkey = reg_nat.n_nationkey),
        sup_psup as (
            select ps_partkey, ps_supplycost,
                   nat_sup.s_acctbal, nat_sup.s_name, nat_sup.n_name,
                   nat_sup.s_address, nat_sup.s_phone, nat_sup.s_comment
            from nat_sup, partsupp
            where ps_suppkey = nat_sup.s_suppkey),
        brass as (
            select sup_psup.ps_partkey, sup_psup.ps_supplycost,
                   sup_psup.s_acctbal, sup_psup.s_name, sup_psup.n_name,
                   p_partkey, p_mfgr,
                   sup_psup.s_address, sup_psup.s_phone, sup_psup.s_comment
            from sup_psup, part
            where sup_psup.ps_partkey = p_partkey
              and p_size = 15
              and p_type like '%BRASS'),
        mincost as (
            select min(ps_supplycost) as min_ps_supplycost, ps_partkey
            from brass
            group by ps_partkey)
    
    select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
    from mincost, brass
    where min_ps_supplycost = ps_supplycost
      and mincost.ps_partkey = brass.ps_partkey -- don't get this, it should be already done
    order by
        s_acctbal desc,
        n_name,
        s_name,
        p_partkey
        limit 100
    """;



    public static final String Q3 = """
with cus_ord as (
    select *
    from orders, customer
    where customer.c_custkey = orders.o_custkey
    and customer.c_mktsegment = 'BUILDING'
    and orders.o_orderdate < date '1995-03-15'
)
select
	lineitem.l_orderkey,
	sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as revenue,
	cus_ord.o_orderdate,
	cus_ord.o_shippriority
from
	cus_ord,
	lineitem
where
	lineitem.l_orderkey = cus_ord.o_orderkey
	and lineitem.l_shipdate > date '1995-03-15'
group by
	lineitem.l_orderkey,
	cus_ord.o_orderdate,
	cus_ord.o_shippriority
order by
	revenue desc,
	cus_ord.o_orderdate
limit 10
    """;



    public static final String Q4 = """
select
	orders.o_orderpriority,
	count(*) as order_count
from
	orders
where
	orders.o_orderdate >= date '1993-07-01'
	and orders.o_orderdate < date '1993-07-01' + interval '3' month
	and exists (
		select
			*
		from
			lineitem
		where
			lineitem.l_orderkey = orders.o_orderkey
			and lineitem.l_commitdate < lineitem.l_receiptdate
	)
group by
	orders.o_orderpriority
order by
	orders.o_orderpriority
""";



    public static final String Q5 = """
with
reg_nat as (select * from region, nation where r_regionkey = n_regionkey and r_name = 'ASIA'),
reg_nat_cust as (select * from reg_nat, customer where n_nationkey = c_nationkey),
reg_nat_cust_ord as (
    select * from reg_nat_cust, orders
    where c_custkey = o_custkey
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1995-01-01'),
reg_nat_cust_ord_line as (select * from reg_nat_cust_ord, lineitem where o_orderkey = l_orderkey),
with_sup as (select * from reg_nat, supplier where n_nationkey = s_nationkey)


select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
from supplier, reg_nat_cust_ord_line
where s_suppkey = l_suppkey
and s_nationkey = n_nationkey
group by n_name
order by revenue desc
""";



    public static final String Q6 = """
select
	sum(lineitem.l_extendedprice * lineitem.l_discount) as revenue
from
	lineitem
where
	lineitem.l_shipdate >= date '1994-01-01'
	and lineitem.l_shipdate < date '1994-01-01' + interval '1' year
	and lineitem.l_discount between .06 - 0.01 and .06 + 0.01
	and lineitem.l_quantity < 24
            """;



    public static final String Q7 = """
with
cus_nat as (
    select c_custkey, n_name
    from nation, customer
    where n_nationkey = c_nationkey
    and (n_name = 'FRANCE' or n_name = 'GERMANY')),
ord_cus as (select * from cus_nat, orders where c_custkey = o_custkey),
sup_nat as (
    select s_suppkey, n_name
    from nation, supplier
    where s_nationkey = n_nationkey
    and (n_name = 'FRANCE' or n_name = 'GERMANY')),
sup_line as (
    select
        l_orderkey,
        year(l_shipdate) as l_year,
        l_extendedprice,
        l_discount,
        n_name
    from sup_nat, lineitem
    where l_suppkey = s_suppkey
    and l_shipdate between date '1995-01-01' and date '1996-12-31'
)

select
	sup_line.n_name as supp_nation,
	ord_cus.n_name as cust_nation,
	l_year,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from ord_cus, sup_line
where l_orderkey = o_orderkey
and (
    (sup_line.n_name = 'FRANCE' and ord_cus.n_name = 'GERMANY')
    or (sup_line.n_name = 'GERMANY' and ord_cus.n_name = 'FRANCE'))
group by
	sup_line.n_name,
	ord_cus.n_name,
	l_year
order by
	sup_line.n_name,
	ord_cus.n_name,
	l_year
""";

    public static final String Q7_hyper = """
with
nat_nat as (
    select 
        n1.n_name as supp_nation, 
        n2.n_name as cust_nation, 
        n1.n_nationkey as n1_nationkey, 
        n2.n_nationkey as n2_nationkey
    from nation n1, nation n2
    where 
        (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
        or 
        (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
),

cus_nat as (
    select supp_nation, cust_nation, n1_nationkey, c_custkey
    from nat_nat, customer
    where n2_nationkey = c_nationkey
),

cusnat_ord as (
    select supp_nation, cust_nation, n1_nationkey, o_orderkey
    from cus_nat, orders 
    where c_custkey = o_custkey
),

cusnatord_line as (
    select 
        supp_nation, cust_nation, n1_nationkey,
        year(l_shipdate) as l_year,  
        l_extendedprice * (1 - l_discount) as volume, 
        l_suppkey
    from cusnat_ord, lineitem
    where o_orderkey = l_orderkey
    and l_shipdate between date '1995-01-01' and date '1996-12-31'
),

sup_line as (
    select
        supp_nation,
        cust_nation,
        l_year,
        volume
    from supplier, cusnatord_line
    where s_suppkey = l_suppkey 
    and s_nationkey = n1_nationkey
)

select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from sup_line
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year
""";



    public static final String Q8 = """
with
nat_region as (
    select *
    from region, nation
    where r_regionkey = n_regionkey and r_name = 'AMERICA'),
cus_nat as (select * from nat_region, customer where n_nationkey = c_nationkey),
ord_cus as (
    select o_orderkey, year(o_orderdate) as o_year
    from cus_nat, orders
    where c_custkey = o_custkey
    and o_orderdate between date '1995-01-01' and date '1996-12-31'),
par_line as (
    select * from part, lineitem
    where p_partkey = l_partkey
    and p_type = 'ECONOMY ANODIZED STEEL'),
sup_nat as (
    select s_suppkey, n_name
    from nation, supplier
    where n_nationkey = s_nationkey),
par_line_sup as(
    select *
    from sup_nat, par_line
    where s_suppkey = l_suppkey)

select
	o_year,
	sum(case when n_name = 'BRAZIL' then l_extendedprice * (1 - l_discount)  else 0 end) / sum(l_extendedprice * (1 - l_discount) ) as mkt_share
from par_line_sup, ord_cus
where l_orderkey = o_orderkey
group by
	o_year
order by
	o_year
""";



    public static final String Q9 = """
with
sup_nat as (
    select * from nation, supplier
    where s_nationkey = n_nationkey),

par_supp as(
    select * from part, partsupp
    where p_partkey = ps_partkey
    and p_name like '%green%'),

sn_ps as (
    select * from sup_nat, par_supp
    where s_suppkey = ps_suppkey),

snps_line as (
    select * from sn_ps, lineitem
    where ps_partkey = l_partkey
    and ps_suppkey = l_suppkey),

profit as (
    select
        n_name as nation,
        year(o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from snps_line, orders
    where l_orderkey = o_orderkey)

select nation, o_year, sum(amount) as sum_profit from profit
group by nation, o_year order by nation, o_year DESC

""";



    public static final String Q10 = """
with
cus_nat as (select * from nation, customer where n_nationkey = c_nationkey),
ord_cus as (
    select * from cus_nat, orders
    where c_custkey = o_custkey
    and o_orderdate >= date '1993-10-01'
    and o_orderdate < date '1994-01-01'
)

select
    c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from ord_cus, lineitem
where o_orderkey = l_orderkey
and l_returnflag = 'R'
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
limit 20
            """;



    public static final String Q11 = """
with
sup_nat as (
    select * from nation, supplier
    where n_nationkey = s_nationkey and n_name = 'GERMANY'),
ps_sup as (
    select ps_partkey, 0+coalesce(sum(ps_supplycost * ps_availqty), 0) as value1
    from sup_nat, partsupp
    where s_suppkey = ps_suppkey
    group by ps_partkey),
value1_sum as (select sum(value1)*0.0001 as v from ps_sup)

select ps_partkey, value1
from value1_sum, ps_sup
where value1 > v
order by value1 desc
""";



    public static final String Q12 = """
select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	lineitem, orders
where
	o_orderkey = l_orderkey
	and l_shipmode in ('MAIL', 'SHIP')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '1994-01-01'
	and l_receiptdate < date '1994-01-01' + interval '1' year
group by
	l_shipmode
order by
	l_shipmode
""";



    public static final String Q13 = """
select
	c_count,
	count(*) as custdist
from
	(
		select
			customer.c_custkey,
			count(orders.o_orderkey) as c_count
		from
			customer left outer join orders on
				customer.c_custkey = orders.o_custkey
				and orders.o_comment not like '%special%requests%'
		group by
			customer.c_custkey
	) as c_orders
group by
	c_count
order by
	custdist desc,
	c_count desc
""";



    public static final String Q14 = """
    select
        100.00 * sum(case
            when part.p_type like 'PROMO%'
                then lineitem.l_extendedprice * (1 - lineitem.l_discount)
            else 0
        end) / sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as promo_revenue
    from
        lineitem,
        part
    where
        lineitem.l_partkey = part.p_partkey
        and lineitem.l_shipdate >= date '1995-09-01'
        and lineitem.l_shipdate < date '1995-09-01' + interval '1' month
""";



    public static final String Q15 = """
with
revenue0 as (
    select
		l_suppkey as supplier_no,
		sum(l_extendedprice * (1 - l_discount)) as total_revenue
	from
		lineitem
	where
		l_shipdate >= date '1996-01-01'
		and l_shipdate < date '1996-01-01' + interval '3' month
	group by
		l_suppkey),
max_rev as (select max(total_revenue) as max_total_rev from revenue0)

select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from max_rev, revenue0, supplier
where supplier_no = s_suppkey and total_revenue = max_total_rev
order by s_suppkey
""";



    public static final String Q16 = """
with ps_par as (
    select p_brand, p_type, p_size, ps_suppkey
    from part, partsupp
    where
        p_partkey = ps_partkey
        and p_brand <> 'Brand#45'
	    and p_type not like 'MEDIUM POLISHED%'
	    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
	    and not exists (
            select *
            from supplier
            where s_comment like '%Customer%Complaints%'
            and s_suppkey = ps_suppkey
        )
	group by p_brand, p_type, p_size, ps_suppkey
)

select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from ps_par
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size
""";



    public static final String Q17 = """
with
avg_q as (
    select l_partkey, avg(l_quantity) as avg_q
    from lineitem
    group by l_partkey),
part_scan as (select * from part where p_brand = 'Brand#23' and p_container = 'MED BOX'),
par_line_avg as (select p_partkey, avg_q from part_scan, avg_q where p_partkey = l_partkey)

select sum(l_extendedprice / 7.0) as avg_yearly
from par_line_avg, lineitem
where p_partkey = l_partkey and l_quantity < (0.2 * avg_q)
            """;



    public static final String Q18 =
            """
with
all_sums as (
    select l_orderkey, sum(l_quantity) as q_sums
    from lineitem
    group by l_orderkey),
ord_cus as (
    select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
    from customer, orders
    where c_custkey = o_custkey
    and o_orderkey in (select l_orderkey from all_sums where q_sums > 300))

select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity) as sum_qty
from ord_cus, lineitem
where o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
limit 100
            """;



    public static final String Q19 = """
select
	sum(lineitem.l_extendedprice* (1 - lineitem.l_discount)) as revenue
from
	lineitem,
	part
where
	part.p_partkey = lineitem.l_partkey
    and lineitem.l_shipmode in ('AIR', 'AIR REG')
	and lineitem.l_shipinstruct = 'DELIVER IN PERSON'
	and
	(
        (
            part.p_brand = 'Brand#12'
            and part.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and lineitem.l_quantity >= 1 and lineitem.l_quantity <= 1 + 10
            and part.p_size between 1 and 5
        )
        or
        (
            part.p_brand = 'Brand#23'
            and part.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and lineitem.l_quantity >= 10 and lineitem.l_quantity <= 10 + 10
            and part.p_size between 1 and 10
        )
        or
        (
            part.p_brand = 'Brand#34'
            and part.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and lineitem.l_quantity >= 20 and lineitem.l_quantity <= 20 + 10
            and part.p_size between 1 and 15
        )
    )
            """;



    public static final String Q20 = """
with
ps_part as (
    select ps_availqty, ps_partkey, ps_suppkey
	from part, partsupp
	where p_partkey = ps_partkey
	and p_name like 'forest%'),

group_lineitem as (
    select l_partkey, l_suppkey, sum(l_quantity) as sumq
    from lineitem
    where l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    group by l_partkey, l_suppkey
),
ps_line as (
    select *
    from ps_part, group_lineitem
    where ps_partkey = l_partkey
    and ps_suppkey = l_suppkey
    and ps_availqty > 0.5 * sumq
),

sup_nat as (
    select *
    from nation, supplier
    where n_nationkey = s_nationkey
    and n_name = 'CANADA')

select s_name, s_address
from sup_nat
where s_suppkey in (select ps_suppkey from ps_line)
order by s_name
""";



    public static final String Q21 = """
select
	supplier.s_name,
	count(*) as numwait
from
	nation,
	supplier,
	lineitem l1,
	orders
where
	supplier.s_suppkey = l1.l_suppkey
	and orders.o_orderkey = l1.l_orderkey
	and orders.o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and exists (
		select
			*
		from
			lineitem l2
		where
			l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey
	)
	and not exists (
		select
			*
		from
			lineitem l3
		where
			l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.l_receiptdate > l3.l_commitdate
	)
	and supplier.s_nationkey = nation.n_nationkey
	and nation.n_name = 'SAUDI ARABIA'
group by
	supplier.s_name
order by
	numwait desc,
	supplier.s_name
limit 100
            """;



    public static final String Q22 = """
with
min_bal as (
    select c_acctbal
    from customer
    where c_acctbal > 0.00
    and substring(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')),

cus_ord as (
    select substring(c_phone, 1, 2) as cntrycode, c_acctbal
    from customer
    where c_acctbal > (select avg(c_acctbal) from min_bal)
    and substring(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
    and not exists (select * from orders where o_custkey = c_custkey))


select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal
from cus_ord
group by cntrycode
order by cntrycode
""";

    public static final Map<String, String> QUERIES = new HashMap<>();
    public static final Map<String, String> IMPORTANT_QUERIES = new HashMap<>();

    static {
        QUERIES.put("Q01", Q1);
        QUERIES.put("Q02", Q2);
        QUERIES.put("Q03", Q3);
        QUERIES.put("Q04", Q4);
        QUERIES.put("Q05", Q5);
        QUERIES.put("Q06", Q6);
        QUERIES.put("Q07", Q7);
        QUERIES.put("Q08", Q8);
        QUERIES.put("Q09", Q9);
        QUERIES.put("Q10", Q10);
        QUERIES.put("Q11", Q11);
        QUERIES.put("Q12", Q12);
        QUERIES.put("Q13", Q13);
        QUERIES.put("Q14", Q14);
        QUERIES.put("Q15", Q15); // HashMap<Double, ...> issue with parallel streams
        QUERIES.put("Q16", Q16);
        QUERIES.put("Q17", Q17);
        QUERIES.put("Q18", Q18);
        QUERIES.put("Q19", Q19);
        QUERIES.put("Q20", Q20);
        QUERIES.put("Q21", Q21);
        QUERIES.put("Q22", Q22);
    }
    static {
        IMPORTANT_QUERIES.put("Q01", Q1);
        IMPORTANT_QUERIES.put("Q03", Q3);
        IMPORTANT_QUERIES.put("Q06", Q6);
        IMPORTANT_QUERIES.put("Q09", Q9);
        IMPORTANT_QUERIES.put("Q18", Q18);
    }


}
