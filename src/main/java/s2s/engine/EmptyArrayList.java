package s2s.engine;


import java.util.*;
import java.util.function.Consumer;


public class EmptyArrayList<T> extends ArrayList<T> {

    private EmptyArrayList() {
        super(0);
    }

    // Note: this is basically a verbatim copy of Collections.emptyList, recasted to ArrayList
    private static final EmptyArrayList<?> INSTANCE = new EmptyArrayList<Object>() {
        @Override
        public Iterator<Object> iterator() {
            return Collections.emptyListIterator();
        }

        public int size() {return 0;}
        public boolean isEmpty() {return true;}
        public void clear() {}

        public boolean contains(Object obj) {return false;}
        public boolean containsAll(Collection<?> c) { return c.isEmpty(); }

        public Object[] toArray() { return new Object[0]; }

        public <T> T[] toArray(T[] a) {
            if (a.length > 0)
                a[0] = null;
            return a;
        }

        public Object get(int index) {
            throw new IndexOutOfBoundsException("Index: "+index);
        }

        public boolean equals(Object o) {
            return (o instanceof List) && ((List<?>)o).isEmpty();
        }

        public int hashCode() { return 1; }

        @Override
        public Spliterator<Object> spliterator() {
            return Spliterators.emptySpliterator();
        }

        @Override
        public void forEach(Consumer<? super Object> action) {
            Objects.requireNonNull(action);
        }
    };


    public static <T> EmptyArrayList<T> get() {
        return (EmptyArrayList<T>) INSTANCE;
    }
}
