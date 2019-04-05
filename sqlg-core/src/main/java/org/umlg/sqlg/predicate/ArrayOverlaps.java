package org.umlg.sqlg.predicate;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiPredicate;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Postgres specific array data type operator to check if two arrays overlap.
 * https://www.postgresql.org/docs/9.6/functions-array.html
 */
public class ArrayOverlaps<T> implements BiPredicate<T[], T[]> {
    private static Logger logger = LoggerFactory.getLogger(ArrayOverlaps.class);

    private final T[] values;

    public ArrayOverlaps(T[] values) {
        this.values = Arrays.copyOf(values, values.length);
    }

    public P<T[]> getPredicate() {
        return new P<>(this, values);
    }

    public T[] getValues() {
        return values;
    }

    @Override
    public boolean test(T[] lhs, T[] rhs) {
        logger.warn("Using Java implementation of && (array overlaps) instead of database");
        Set<T> lhsSet = new HashSet<>();
        for (T item : lhs) {
            lhsSet.add(item);
        }

        for (T item : rhs) {
            if (lhsSet.contains(item)) {
                return true;
            }
        }

        return false;
    }
}
