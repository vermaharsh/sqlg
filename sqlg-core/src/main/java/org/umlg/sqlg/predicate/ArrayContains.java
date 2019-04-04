package org.umlg.sqlg.predicate;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Postgres specific array data type operator to check if an array is fully contained in another.
 * https://www.postgresql.org/docs/9.6/functions-array.html
 */
public class ArrayContains<T> implements BiPredicate<List<T>, List<T>> {
    private static Logger logger = LoggerFactory.getLogger(ArrayContains.class);

    private final List<T> values;

    public ArrayContains(List<T> values) {
        this.values = new ArrayList<>(values);
    }

    public List<T> getValues() {
        return values;
    }

    @Override
    public boolean test(List<T> lhs, List<T> rhs) {
        logger.warn("Using Java implementation of @> (array contains) instead of database");
        return lhs.containsAll(rhs);
    }
}
