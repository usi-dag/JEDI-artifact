package s2s.engine;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public final class Date implements Comparable<Date> {
    final int days;

    public Date(String s) {
        this.days = (int) LocalDate.parse(s, DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochDay();
    }

    /*
    Only used for testing
     */
    public Date(int days) {
        this.days = days;
    }


    /**
     * If the difference between the days of the two dates is positive, then the first date is greater than the second
     * date. If the difference is negative, then the first date is less than the second date. If the difference is zero,
     * then the two dates are equal
     *
     * @param o The date to be compared.
     * @return The difference between the days of the two dates.
     */
    @Override
    public int compareTo(Date o) {
        return this.days - o.days;
    }

    public boolean equals(Date obj) {
        return obj != null && this.days == obj.days;
    }

    @Override
    public boolean equals(Object o) {
        return o != null && o.getClass() == this.getClass() && this.days == ((Date) o).days;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(days);
    }

    @Override
    public String toString() {
        return LocalDate.ofEpochDay(days).toString();
    }

    public int getYear() {
        return LocalDate.ofEpochDay(days).getYear();
    }

    public int getMonth() {
        return LocalDate.ofEpochDay(days).getMonthValue();
    }

}
