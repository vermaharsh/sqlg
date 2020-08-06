package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.*;

import java.sql.*;
import java.util.*;

import static org.umlg.sqlg.structure.topology.Topology.EDGE_PREFIX;
import static org.umlg.sqlg.structure.topology.Topology.VERTEX_PREFIX;

/**
 * Date: 2016/11/26
 * Time: 7:35 PM
 */
public class Index implements TopologyInf {

    private static Logger logger = LoggerFactory.getLogger(Index.class);
    private String name;
    private boolean committed = true;
    private AbstractLabel abstractLabel;
    private IndexType indexType;
    private List<PropertyColumn> properties = new ArrayList<>();
    private IndexType uncommittedIndexType;
    private List<PropertyColumn> uncommittedProperties = new ArrayList<>();
    private PartialIndexClause partialIndexClause = null;

    /**
     * PartialIndexClause allows to add any conditional clause for a property column while adding an index to a
     * Vertex or Edge. This is used with ensureIndexExists method which is used to add user defined additional index to
     * a Vertex or an Edge.
     *
     * For implicit index created during Edge creation itself, use the boolean argument
     * usePartialIndexForForeignKeyNotNull with EdgeLabel#createEdgeLabel.
     */
    public static class PartialIndexClause {
        final PropertyColumn property;
        final String condition;

        public PartialIndexClause(PropertyColumn property, String condition) {
            this.property = property;
            this.condition = condition;
        }

        ObjectNode toNotifyJson() {
            ObjectNode propertyJsonNode = property.toNotifyJson();
            propertyJsonNode.put("condition", condition);
            return propertyJsonNode;
        }

        static PartialIndexClause fromNotifyJson(AbstractLabel abstractLabel, JsonNode jsonNode) {
            if (jsonNode == null) {
                return null;
            }

            PropertyColumn property = PropertyColumn.fromNotifyJson(abstractLabel, jsonNode);
            return new PartialIndexClause(property, jsonNode.get("condition").asText());
        }
    }

    /**
     * create uncommitted index
     *
     * @param name
     * @param indexType
     * @param abstractLabel
     * @param properties
     */
    Index(
            String name,
            IndexType indexType,
            AbstractLabel abstractLabel,
            List<PropertyColumn> properties,
            PartialIndexClause partialIndexClause) {
        this.name = name;
        this.indexType = indexType;
        this.uncommittedIndexType = indexType;
        this.abstractLabel = abstractLabel;
        this.uncommittedProperties.addAll(properties);
        this.partialIndexClause = partialIndexClause;
    }

    /**
     * create a committed index (when loading topology from existing schema)
     *
     * @param name
     * @param indexType
     * @param abstractLabel
     */
    Index(String name, IndexType indexType, AbstractLabel abstractLabel) {
        this.name = name;
        this.indexType = indexType;
        this.abstractLabel = abstractLabel;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return getName();
    }


    @Override
    public int hashCode() {
        return (this.abstractLabel.getName() + this.getName()).hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Index)) {
            return false;
        }
        Index otherIndex = (Index) other;
        return this.abstractLabel.equals(otherIndex.abstractLabel) && this.name.equals(otherIndex.name);
    }

    @Override
    public boolean isCommitted() {
        return this.committed;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    /**
     * add a committed property (when loading topology from existing schema)
     *
     * @param property
     */
    void addProperty(PropertyColumn property) {
        this.properties.add(property);
    }

    void afterCommit() {
        this.indexType = this.uncommittedIndexType;
        Iterator<PropertyColumn> propertyColumnIterator = this.uncommittedProperties.iterator();
        while (propertyColumnIterator.hasNext()) {
            PropertyColumn propertyColumn = propertyColumnIterator.next();
            this.properties.add(propertyColumn);
            propertyColumn.afterCommit();
            propertyColumnIterator.remove();
        }
        this.uncommittedIndexType = null;
        this.committed = true;
    }

    void afterRollback() {
        this.uncommittedIndexType = null;
        this.uncommittedProperties.clear();
    }

    private void addIndex(SqlgGraph sqlgGraph, SchemaTable schemaTable) {
        String prefix = this.abstractLabel instanceof VertexLabel ? VERTEX_PREFIX : EDGE_PREFIX;
        StringBuilder sql = new StringBuilder("CREATE ");
        if (IndexType.UNIQUE.equals(getIndexType())) {
            sql.append("UNIQUE ");
        }
        sql.append("INDEX ");
        SqlDialect sqlDialect = sqlgGraph.getSqlDialect();
        sql.append(sqlDialect.maybeWrapInQoutes(getName()));
        sql.append(" ON ");
        sql.append(sqlDialect.maybeWrapInQoutes(schemaTable.getSchema()));
        sql.append(".");
        sql.append(sqlDialect.maybeWrapInQoutes(prefix + schemaTable.getTable()));


        if (this.indexType.isGIN()) {
            sql.append(" USING GIN");
        }

        sql.append(" (");
        List<PropertyColumn> props = getProperties();
        if (IndexType.GIN_FULLTEXT.equals(getIndexType().getName())) {
            sql.append("to_tsvector(");
            String conf = indexType.getProperties().get(IndexType.GIN_CONFIGURATION);
            if (conf != null) {
                sql.append("'" + conf + "'"); // need single quotes, no double
                sql.append(",");
            }
            int count = 1;
            for (PropertyColumn property : props) {
                sql.append(sqlDialect.maybeWrapInQoutes(property.getName()));
                if (count++ < props.size()) {
                    sql.append(" || ' ' || ");
                }
            }
            sql.append(")");
        } else {
            int count = 1;
            for (PropertyColumn property : props) {
                sql.append(sqlDialect.maybeWrapInQoutes(property.getName()));
                //This is for mariadb. It needs to know how many characters to index.
                if (property.getPropertyType().isString() && sqlgGraph.getSqlDialect().requiresIndexLengthLimit()) {
                    //This number is for MariaDb TEXT data type.
                    //192 crashes with "Caused by: java.sql.SQLException: Specified key was too long; max key length is 767 bytes"
                    //Some or other Innodb byte count magic I can't be bothered to understand.
                    sql.append("(191)");
                }
                if (count++ < props.size()) {
                    sql.append(", ");
                }
            }
        }

        sql.append(")");
        if (partialIndexClause != null) {
            sql.append(" WHERE ");
            sql.append((sqlDialect.maybeWrapInQoutes(partialIndexClause.property.getName())));
            sql.append(" " + partialIndexClause.condition);
        }
        if (sqlDialect.needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected Optional<JsonNode> toNotifyJson() {
        Preconditions.checkState(this.abstractLabel.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread() && !this.uncommittedProperties.isEmpty());
        ObjectNode result = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        result.put("name", this.name);
        result.set("indexType", this.uncommittedIndexType.toNotifyJson());
        ArrayNode propertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (PropertyColumn property : this.uncommittedProperties) {
            propertyArrayNode.add(property.toNotifyJson());
        }
        result.set("uncommittedProperties", propertyArrayNode);
        if (partialIndexClause != null) {
            result.set("partialIndexClause", partialIndexClause.toNotifyJson());
        }
        return Optional.of(result);
    }

    public static Index fromNotifyJson(AbstractLabel abstractLabel, JsonNode indexNode) {
        IndexType indexType = IndexType.fromNotifyJson(indexNode.get("indexType"));
        String name = indexNode.get("name").asText();
        ArrayNode propertiesNode = (ArrayNode) indexNode.get("uncommittedProperties");
        List<PropertyColumn> properties = new ArrayList<>();
        for (JsonNode propertyNode : propertiesNode) {
            String propertyName = propertyNode.get("name").asText();
            PropertyType propertyType = PropertyType.valueOf(propertyNode.get("propertyType").asText());
            Optional<PropertyColumn> propertyColumnOptional = abstractLabel.getProperty(propertyName);
            Preconditions.checkState(propertyColumnOptional.isPresent(), "BUG: property %s for PropertyType %s not found.", propertyName, propertyType.name());
            //noinspection OptionalGetWithoutIsPresent
            properties.add(propertyColumnOptional.get());
        }
        PartialIndexClause partialIndexClause =
                PartialIndexClause.fromNotifyJson(abstractLabel, indexNode.get("partialIndexClause"));
        Index index = new Index(name, indexType, abstractLabel, properties, partialIndexClause);
        return index;
    }

    static Index createIndex(
            SqlgGraph sqlgGraph,
            AbstractLabel abstractLabel,
            String indexName,
            IndexType indexType,
            List<PropertyColumn> properties,
            PartialIndexClause partialIndexClause) {
        Index index = new Index(indexName, indexType, abstractLabel, properties, partialIndexClause);
        SchemaTable schemaTable = SchemaTable.of(abstractLabel.getSchema().getName(), abstractLabel.getLabel());
        index.addIndex(sqlgGraph, schemaTable);
        TopologyManager.addIndex(sqlgGraph, index);
        index.committed = false;
        return index;
    }

    List<Topology.TopologyValidationError> validateTopology(DatabaseMetaData metadata) throws SQLException {
        List<Topology.TopologyValidationError> validationErrors = new ArrayList<>();
        try (ResultSet propertyRs = metadata.getIndexInfo(null, this.abstractLabel.getSchema().getName(), this.abstractLabel.getPrefix() + this.abstractLabel.getLabel(), false, false)) {
            Map<String, List<String>> indexColumns = new HashMap<>();
            while (propertyRs.next()) {
                String columnName = propertyRs.getString("COLUMN_NAME");
                String indexName = propertyRs.getString("INDEX_NAME");
                List<String> columnNames;
                if (!indexColumns.containsKey(indexName)) {
                    columnNames = new ArrayList<>();
                    indexColumns.put(indexName, columnNames);
                } else {
                    columnNames = indexColumns.get(indexName);
                }
                columnNames.add(columnName);
            }
            if (!indexColumns.containsKey(this.getName())) {
                validationErrors.add(new Topology.TopologyValidationError(this));
            }
        }
        return validationErrors;

    }

    public AbstractLabel getParentLabel() {
        return abstractLabel;
    }

    public List<PropertyColumn> getProperties() {
        List<PropertyColumn> props = new ArrayList<>(properties);
        if (this.getParentLabel().getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            props.addAll(uncommittedProperties);
        }
        return Collections.unmodifiableList(props);
    }

    /**
     * delete the index from the database
     *
     * @param sqlgGraph
     */
    void delete(SqlgGraph sqlgGraph) {
        StringBuilder sql = new StringBuilder("DROP INDEX IF EXISTS ");
        SqlDialect sqlDialect = sqlgGraph.getSqlDialect();
        sql.append(sqlDialect.maybeWrapInQoutes(getParentLabel().getSchema().getName()));
        sql.append(".");
        sql.append(sqlDialect.maybeWrapInQoutes(getName()));
        if (sqlDialect.needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove(boolean preserveData) {
        getParentLabel().removeIndex(this, preserveData);
    }
}
