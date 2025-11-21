package microbenchmark;

import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;


@State(Scope.Thread)
public class VarGroupSize {

    @Param(value = {"2", "4", "8", "16", "32", "64", "128", "256", "512", "1024"})
    int mod;

    @Param(value = "10000")
    int numOrders;

    Order[] orders;

    @Setup
    public void setup() {
        orders = new Order[numOrders];
        for (int i = 0; i < numOrders; i++) {
            orders[i] = new Order(i, i % 1000);
        }
    }

    static final class Order {
        final int id;
        final double price;

        public Order(int id, double price) {
            this.id = id;
            this.price = price;
        }
    }


    // PU/CG collectors
    public static final class OneFieldMergeCollector {
        double sumprice = 0;

        void accumulate(Order row) {
            sumprice += row.price;
        }

        OneFieldMergeCollector combine(OneFieldMergeCollector other) {
            sumprice += other.sumprice;
            return this;
        }

        static Collector<Order, OneFieldMergeCollector, OneFieldMergeCollector> collector(){
            return Collector.of(
                    OneFieldMergeCollector::new,
                    OneFieldMergeCollector::accumulate,
                    OneFieldMergeCollector::combine);
        }

    }


    public static final class ManyFieldsMergeCollector {
        long ctn = 0;;
        double sumprice = 0;
        long avgprice_counter = 0;
        double avgprice = 0;


        void accumulate(Order row) {
            ctn++;
            sumprice += row.price;
            avgprice_counter++;
            avgprice += row.price;

        }

        ManyFieldsMergeCollector combine(ManyFieldsMergeCollector other) {
            ctn += other.ctn;
            sumprice += other.sumprice;
            avgprice_counter += other.avgprice_counter;
            avgprice += other.avgprice;
            return this;
        }

        ManyFieldsMergeCollector finish() {
            return this;
        }


        static Collector<Order, ManyFieldsMergeCollector, ManyFieldsMergeCollector> collector(){
            return Collector.of(
                    ManyFieldsMergeCollector::new,
                    ManyFieldsMergeCollector::accumulate,
                    ManyFieldsMergeCollector::combine);
        }

    }



    // CGCC collectors
    public static final class OneFieldConcurrentCollector {
        final DoubleAdder sumprice = new DoubleAdder();

        double sumprice() {
            return sumprice.doubleValue();
        }

        void accumulate(Order row) {
            sumprice.add(row.price);
        }

        OneFieldConcurrentCollector combine(OneFieldConcurrentCollector other) {
            return this;
        }

        static Collector<Order, OneFieldConcurrentCollector, OneFieldConcurrentCollector> collector(){
            return Collector.of(
                    OneFieldConcurrentCollector::new,
                    OneFieldConcurrentCollector::accumulate,
                    OneFieldConcurrentCollector::combine,
                    Collector.Characteristics.CONCURRENT);
        }

    }



    public static final class ManyFieldsConcurrentCollector {
        final LongAdder ctn = new LongAdder();
        final DoubleAdder sumprice = new DoubleAdder();
        final LongAdder avgprice_counter = new LongAdder();
        final DoubleAdder avgprice = new DoubleAdder();


        void accumulate(Order row) {
            ctn.increment();
            sumprice.add(row.price);
            avgprice_counter.increment();
            avgprice.add(row.price);

        }

        ManyFieldsConcurrentCollector combine(ManyFieldsConcurrentCollector other) {
            return this;
        }



        static Collector<Order, ManyFieldsConcurrentCollector, ManyFieldsConcurrentCollector> collector() {
            return Collector.of(
                    ManyFieldsConcurrentCollector::new,
                    ManyFieldsConcurrentCollector::accumulate,
                    ManyFieldsConcurrentCollector::combine,
                    Collector.Characteristics.CONCURRENT);
        }

    }


    // P
    @Benchmark
    public Map<Integer, OneFieldMergeCollector> P_OneField() {
        return Arrays.stream(orders).parallel()
                .collect(Collectors.groupingBy(
                        row -> row.id % mod,
                        OneFieldMergeCollector.collector()
                ));
    }

    @Benchmark
    public Map<Integer, ManyFieldsMergeCollector> P_ManyFields() {
        return Arrays.stream(orders).parallel()
                .collect(Collectors.groupingBy(
                        row -> row.id % mod,
                        ManyFieldsMergeCollector.collector()
                ));
    }

    // PU
    @Benchmark
    public Map<Integer, OneFieldMergeCollector> PU_OneField() {
        return Arrays.stream(orders).parallel().unordered()
                .collect(Collectors.groupingBy(
                        row -> row.id % mod,
                        OneFieldMergeCollector.collector()
                ));
    }

    @Benchmark
    public Map<Integer, ManyFieldsMergeCollector> PU_ManyFields() {
        return Arrays.stream(orders).parallel().unordered()
                .collect(Collectors.groupingBy(
                        row -> row.id % mod,
                        ManyFieldsMergeCollector.collector()
                ));
    }


    // CG
    @Benchmark
    public Map<Integer, OneFieldMergeCollector>  CG_OneField() {
        return Arrays.stream(orders).parallel().unordered()
                .collect(Collectors.groupingByConcurrent(
                        row -> row.id % mod,
                        OneFieldMergeCollector.collector()
                ));
    }

    @Benchmark
    public Map<Integer, ManyFieldsMergeCollector>  CG_ManyFields() {
        return Arrays.stream(orders).parallel().unordered()
                .collect(Collectors.groupingByConcurrent(
                        row -> row.id % mod,
                        ManyFieldsMergeCollector.collector()
                ));
    }

    // CGCC
    @Benchmark
    public Map<Integer, OneFieldConcurrentCollector> CGCC_OneField() {
        return Arrays.stream(orders).parallel().unordered()
                .collect(Collectors.groupingByConcurrent(
                        row -> row.id % mod,
                        OneFieldConcurrentCollector.collector()
                ));
    }

    @Benchmark
    public Map<Integer, ManyFieldsConcurrentCollector> CGCC_ManyFields() {
        return Arrays.stream(orders).parallel().unordered()
                .collect(Collectors.groupingByConcurrent(
                        row -> row.id % mod,
                        ManyFieldsConcurrentCollector.collector()
                ));
    }



}
