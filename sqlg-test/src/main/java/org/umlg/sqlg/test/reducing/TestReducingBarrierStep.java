package org.umlg.sqlg.test.reducing;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/11/17
 */
public class TestReducingBarrierStep extends BaseTest {

    @SuppressWarnings("Duplicates")
    @Test
    public void testMax() {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 0);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V().hasLabel("Person").values("age").max();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.next(), 0);
    }

    @Test
    public void testGroupOverOneProperty() {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "age", 4);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<String, Integer>> traversal = sqlgGraph.traversal()
                .V().hasLabel("Person")
                .<String, Integer>group().by("name").by(__.values("age").max());
        printTraversalForm(traversal);
        Map<String, Integer> result = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertTrue(result.containsKey("A"));
        Assert.assertTrue(result.containsKey("B"));
        Assert.assertEquals(3, result.get("A"), 0);
        Assert.assertEquals(4, result.get("B"), 0);
    }

    @Test
    public void testGroupOverTwoPropertiesWithValues() {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "surname", "C", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "surname", "D", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "surname", "C", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "surname", "E", "age", 4);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "C", "surname", "E", "age", 5);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<List<String>, Integer>> traversal = this.sqlgGraph.traversal().V().hasLabel("Person")
                .<List<String>, Integer>group()
                .by(__.values("name", "surname").fold())
                .by(__.values("age").max());

        printTraversalForm(traversal);

        Map<List<String>, Integer> result = traversal.next();
        System.out.println(result);

        Assert.assertTrue(result.containsKey(Arrays.asList("A", "C")) || result.containsKey(Arrays.asList("C", "A")));
        Assert.assertTrue(result.containsKey(Arrays.asList("B", "D")) || result.containsKey(Arrays.asList("D", "B")));
        Assert.assertTrue(result.containsKey(Arrays.asList("B", "E")) || result.containsKey(Arrays.asList("E", "B")));
        Assert.assertTrue(result.containsKey(Arrays.asList("C", "E")) || result.containsKey(Arrays.asList("E", "C")));
        Assert.assertEquals(4, result.size());
        Assert.assertFalse(traversal.hasNext());

        if (result.containsKey(Arrays.asList("A", "C"))) {
            Assert.assertEquals(3, result.get(Arrays.asList("A", "C")), 0);
        } else {
            Assert.assertEquals(3, result.get(Arrays.asList("C", "A")), 0);
        }
        if (result.containsKey(Arrays.asList("B", "D"))) {
            Assert.assertEquals(2, result.get(Arrays.asList("B", "D")), 0);
        } else {
            Assert.assertEquals(2, result.get(Arrays.asList("D", "B")), 0);
        }
        if (result.containsKey(Arrays.asList("B", "E"))) {
            Assert.assertEquals(4, result.get(Arrays.asList("B", "E")), 0);
        } else {
            Assert.assertEquals(4, result.get(Arrays.asList("E", "B")), 0);
        }
        if (result.containsKey(Arrays.asList("C", "E"))) {
            Assert.assertEquals(5, result.get(Arrays.asList("C", "E")), 0);
        } else {
            Assert.assertEquals(5, result.get(Arrays.asList("E", "C")), 0);
        }
    }

    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    @Test
    public void testGroupOverTwoPropertiesWithValueMap() {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "surname", "C", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "surname", "D", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "surname", "C", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "surname", "E", "age", 4);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "C", "surname", "E", "age", 5);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<Map<String, List<String>>, Integer>> traversal = this.sqlgGraph.traversal().V().hasLabel("Person")
                .<Map<String, List<String>>, Integer>group()
                .by(__.valueMap("name", "surname"))
                .by(__.values("age").max());

        printTraversalForm(traversal);

        Map<Map<String, List<String>>, Integer> result = traversal.next();
        System.out.println(result);

        Assert.assertTrue(result.containsKey(new HashMap<String, List<String>>(){{
            put("surname", Arrays.asList("C"));
            put("name", Arrays.asList("A"));
        }}));
        Assert.assertTrue(result.containsKey(new HashMap<String, List<String>>(){{
            put("surname", Arrays.asList("D"));
            put("name", Arrays.asList("B"));
        }}));
        Assert.assertTrue(result.containsKey(new HashMap<String, List<String>>(){{
            put("surname", Arrays.asList("E"));
            put("name", Arrays.asList("B"));
        }}));
        Assert.assertTrue(result.containsKey(new HashMap<String, List<String>>(){{
            put("surname", Arrays.asList("E"));
            put("name", Arrays.asList("C"));
        }}));

        Assert.assertEquals(3, result.get(new HashMap<String, List<String>>(){{
            put("surname", Arrays.asList("C"));
            put("name", Arrays.asList("A"));
        }}), 0);
        Assert.assertEquals(2, result.get(new HashMap<String, List<String>>(){{
            put("surname", Arrays.asList("D"));
            put("name", Arrays.asList("B"));
        }}), 0);
        Assert.assertEquals(4, result.get(new HashMap<String, List<String>>(){{
            put("surname", Arrays.asList("E"));
            put("name", Arrays.asList("B"));
        }}), 0);
        Assert.assertEquals(5, result.get(new HashMap<String, List<String>>(){{
            put("surname", Arrays.asList("E"));
            put("name", Arrays.asList("C"));
        }}), 0);
    }
}
