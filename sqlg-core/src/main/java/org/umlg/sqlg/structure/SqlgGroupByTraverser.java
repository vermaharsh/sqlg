package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_Traverser;

import java.util.Map;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/11/25
 */
public class SqlgGroupByTraverser<K, V> extends O_Traverser<Map<K,V>> {


    public SqlgGroupByTraverser(Map<K, V> result) {
        super(result);
    }

    public void put(K key, V value) {
        this.t.put(key, value);
    }

    @Override
    public long bulk() {
        return -1;
    }

    @Override
    public int hashCode() {
        return this.t.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return (object instanceof SqlgGroupByTraverser) && ((SqlgGroupByTraverser) object).t.equals(this.t);
    }
}
