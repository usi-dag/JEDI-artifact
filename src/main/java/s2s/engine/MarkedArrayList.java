package s2s.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;

public final class MarkedArrayList<T> extends ArrayList<T> {
    public static <T> Collector<T, ?, MarkedArrayList<T>> collector() {
        return Collector.of(
                MarkedArrayList::new,
                List::add,
                (left, right) -> { left.addAll(right); return left; },
                Collector.Characteristics.IDENTITY_FINISH);
    }


    private volatile boolean marked = false;

    public boolean isMarked() {
        return marked;
    }

    public void mark() {
        marked = true;
    }
}
