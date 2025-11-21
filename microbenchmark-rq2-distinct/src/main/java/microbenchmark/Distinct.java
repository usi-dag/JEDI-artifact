package microbenchmark;

import org.openjdk.jmh.annotations.*;

import java.util.*;


@State(Scope.Thread)
public class Distinct {


    @Param(value = "10000000")
    int numOrders;

    @Param(value = {"100", "1000", "10000", "100000", "1000000"})
    int nDistinct;

    Order[] orders;

    @Setup
    public void setup() {
        orders = new Order[numOrders];
        for (int i = 0; i < numOrders; i++) {
            orders[i] = new Order(i% nDistinct, i);
        }
    }

    static final class Order {
        final int id;
        final double price;

        public Order(int id, double price) {
            this.id = id;
            this.price = price;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Order && this.id == ((Order)other).id;
        }

        @Override
        public int hashCode() {
            return id;
        }
    }


    // Seq
    @Benchmark
    public List<Order> SEQ() {
        return Arrays.stream(orders)
                .distinct()
                .toList();
    }

    // P
    @Benchmark
    public List<Order> P() {
        return Arrays.stream(orders)
                .parallel()
                .distinct()
                .toList();
    }

    // PU
    @Benchmark
    public List<Order> PU() {
        return Arrays.stream(orders)
                .parallel().unordered()
                .distinct()
                .toList();
    }

}
