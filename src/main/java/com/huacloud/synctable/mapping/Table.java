package com.huacloud.synctable.mapping;

import com.huacloud.synctable.dialect.*;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 4/17/2019 12:16 PM
 */
public class Table {

    private String name;

    private String schema;

    private String catalog;

    private boolean quoted = true;

    private boolean schemaQuoted = true;

    private boolean catalogQuoted = true;

    private String comment;

    private List<Index> indexList;

    private PrimaryKey primaryKey;

    private Map<String, Column> columns = new LinkedHashMap<>();

    private Map<String, UniqueKey> uniqueKeys = new LinkedHashMap<>();

    private boolean isPartitionTable;

    private PartitionType partitionType;

    private List<PartitionTable> partitionTables;

    private String partitionColumnExpression;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public boolean isQuoted() {
        return quoted;
    }

    public void setQuoted(boolean quoted) {
        this.quoted = quoted;
    }

    public boolean isSchemaQuoted() {
        return schemaQuoted;
    }

    public void setSchemaQuoted(boolean schemaQuoted) {
        this.schemaQuoted = schemaQuoted;
    }

    public boolean isCatalogQuoted() {
        return catalogQuoted;
    }

    public void setCatalogQuoted(boolean catalogQuoted) {
        this.catalogQuoted = catalogQuoted;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    public void addColumn(Column column) {
        columns.put(column.getName(), column);
    }

    public void setColumns(Map columns) {
        this.columns = columns;
    }

    public String getName() {
        return name;
    }

    public boolean hasPrimaryKey() {
        if (primaryKey != null) {
            return true;
        }
        return false;
    }

    public boolean hasUniqueKey() {
        if (uniqueKeys.size() > 0) {
            return true;
        }
        return false;
    }

    public String getQualifiedName(Dialect dialect, String defaultCatalog, String defaultSchema) {
        String quotedName = getQuotedTabName(dialect);
        String usedSchema = schema == null ?
                defaultSchema :
                getQuotedSchema(dialect);
        String usedCatalog = catalog == null ?
                defaultCatalog :
                getQuotedCatalog(dialect);
        return qualify(usedCatalog, usedSchema, quotedName);
    }

    public static String qualify(String catalog, String schema, String table) {
        StringBuilder qualifiedName = new StringBuilder();
        if (catalog != null) {
            qualifiedName.append(catalog).append('.');
        }
        if (schema != null) {
            qualifiedName.append(schema).append('.');
        }
        return qualifiedName.append(table).toString();
    }

    public String getQuotedTabName(Dialect dialect) {
        return dialect.isSupportTableQuoted() ?
                dialect.openQuote() + name + dialect.closeQuote() :
                name;
    }

    public String getQuotedSchema(Dialect dialect) {
        return dialect.isSupportTableQuoted() ?
                dialect.openQuote() + schema + dialect.closeQuote() :
                schema;
    }

    public String getQuotedCatalog(Dialect dialect) {
        return catalogQuoted ?
                dialect.openQuote() + catalog + dialect.closeQuote() :
                catalog;
    }

    public void setName(String name) {
        if (name.charAt(0) == '`') {
            quoted = true;
            this.name = name.substring(1, name.length() - 1);
        } else {
            this.name = name;
        }
    }

    public Iterator<UniqueKey> getUniqueKeyIterator() {
        return uniqueKeys.values().iterator();
    }

    public Iterator<Column> getColumnIterator() {
        return columns.values().iterator();
    }

    public int getColumnSize() {
        return columns.values().size();
    }

    public String sqlCreateString(Dialect dialect) {
        StringBuilder buf = new StringBuilder(dialect.getCreateTableString())
                .append(' ')
                .append(getQuotedSchema(dialect))
                .append(".")
                .append(getQuotedTabName(dialect))
                .append(" (")
                .append("\n");

        Iterator<Column> iter = getColumnIterator();
        dialect.checkColumn(getColumnIterator());
        while (iter.hasNext()) {
            Column col = iter.next();

            buf.append("\t").append(col.getQuotedName(dialect)).append(' ');

            Integer jdbcType = col.getJdbcType();
            long length = col.getLength();
            int precision = col.getPrecision();
            int scale = col.getScale();
            String typeName = dialect.getTypeName(jdbcType, length, precision, scale);
            buf.append(typeName);

            if (dialect.isSupportColDefaultVal()) {
                String defaultValue = col.getDefaultValue();
                if (defaultValue != null) {

                    if (dialect instanceof GaussDbDialect &&
                            StringUtils.equalsIgnoreCase(defaultValue, "CURRENT_TIMESTAMP")) {
                        //gaussdb对于mysql的默认时间需要特殊处理
                        buf.append(" DEFAULT ").append("SYSTIMESTAMP");

                    } else if (dialect instanceof TBaseDialect &&
                            StringUtils.equalsIgnoreCase(defaultValue, "CURRENT_TIMESTAMP")) {
                        buf.append(" DEFAULT TIMESTAMP 'now()'");

                    } else if (dialect instanceof MySQLDialect &&
                            StringUtils.equalsIgnoreCase(defaultValue, "CURRENT_TIMESTAMP")) {
                        buf.append(" DEFAULT CURRENT_TIMESTAMP");

                    } else {
                        buf.append(" DEFAULT ")
                                .append("'")
                                .append(defaultValue)
                                .append("'");
                    }
                }
            }

            if (dialect.isSupportColNotNull()) {
                if (col.isNullable()) {
                    buf.append(dialect.getNullColumnString());
                } else {
                    buf.append(" NOT NULL ");
                }
            }

            String columnComment = col.getComment();
            if (StringUtils.isNotBlank(columnComment)) {
                buf.append(dialect.getColumnComment(columnComment));
            }

            if (iter.hasNext()) {
                buf.append(", ").append("\n");
            }
        }

        if (dialect.isSupportPK()) {
            if (getPrimaryKey() != null && getPrimaryKey().getColumnSize() > 0) {
                buf.append(", ").append("\n\t").append(getPrimaryKey().sqlConstraintString(dialect));
            }
        }

        buf.append("\n)");

        if (StringUtils.isNotBlank(comment)) {
            buf.append(dialect.getTableComment(comment));
        }

        buf.append(dialect.getTableTypeString());

        if (isPartitionTable && !(dialect instanceof SQLServerDialect)) {
            String partitionCreateSQL = dialect.getTablePartitionSQL(this);
            buf.append(partitionCreateSQL);
        }

        return buf.toString();
    }



    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public UniqueKey addUniqueKey(UniqueKey uniqueKey) {
        UniqueKey current = uniqueKeys.get( uniqueKey.getName() );
        if ( current != null ) {
            throw new RuntimeException( "UniqueKey " + uniqueKey.getName() + " already exists!" );
        }
        uniqueKeys.put( uniqueKey.getName(), uniqueKey );
        return uniqueKey;
    }

    public UniqueKey getUniqueKey(String keyName) {
        return uniqueKeys.get(keyName);
    }

    public Column getColumn(String columnName) {
        Column column = columns.get(columnName.toUpperCase());
        if (column != null) {
            return column;
        } else {
            return columns.get(columnName.toLowerCase());
        }
    }

    public boolean containColumn(String columnName) {
        return columns.containsKey(columnName);
    }

    public Column getColumn(int n) {
        Iterator<Column> iter = columns.values().iterator();
        for ( int i = 0; i < n - 1; i++ ) {
            iter.next();
        }
        return iter.next();
    }

    public Iterator<String> sqlCommentStrings(Dialect dialect) {
        List<String> comments = new ArrayList<>();
        if (dialect instanceof SQLServerDialect) {
            return comments.iterator();
        }
        if (dialect.supportsCommentOn()) {
            String tableName = getQuotedTabName(dialect);
            String schemaName = getQuotedSchema(dialect);
            if (StringUtils.isNotBlank(comment)) {
                StringBuilder buf = new StringBuilder()
                        .append("COMMENT ON TABLE ")
                        .append(schemaName)
                        .append(".")
                        .append(tableName)
                        .append(" is '")
                        .append(comment)
                        .append("'");
                comments.add(buf.toString());
            }
            Iterator iter = getColumnIterator();
            while (iter.hasNext()) {
                Column column = (Column) iter.next();
                String columnComment = column.getComment();
                if (StringUtils.isNotBlank(columnComment)) {
                    StringBuilder buf = new StringBuilder()
                            .append("COMMENT ON COLUMN ")
                            .append(schemaName)
                            .append(".")
                            .append(tableName)
                            .append('.')
                            .append(column.getQuotedName(dialect))
                            .append(" IS '")
                            .append(columnComment)
                            .append("'");
                    comments.add(buf.toString());
                }
            }
        }
        return comments.iterator();
    }

    public String getFullCreateTableSQL(Dialect dialect, String schemaName) {
        this.schema = schemaName;

        String createTabSql = this.sqlCreateString(dialect);
        Iterator<String> iterator = this.sqlCommentStrings(dialect);
        StringBuilder builder = new StringBuilder(createTabSql);
        builder.append(";\n");

        while (iterator.hasNext()) {
            String columnCommentSql = iterator.next();
            builder.append(columnCommentSql);
            builder.append(";\n");
        }


        builder.append(dialect.indexCreateSql(this));

        if (isPartitionTable && dialect.isAlonePartSQL() &&
                !(dialect instanceof SQLServerDialect)) {
            String partitionCreateSQL = dialect.getPartitionCreateSQL(this);
            builder.append(partitionCreateSQL);
        }

        return builder.toString();
    }

    public List<Index> getIndexList() {
        return indexList;
    }

    public void setIndexList(List<Index> indexList) {
        this.indexList = indexList;
    }

    public boolean isPartitionTable() {
        return isPartitionTable;
    }

    public void setPartitionTable(boolean partitionTable) {
        isPartitionTable = partitionTable;
    }

    public PartitionType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(PartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public List<PartitionTable> getPartitionTables() {
        return partitionTables;
    }

    public void setPartitionTables(List<PartitionTable> partitionTables) {
        this.partitionTables = partitionTables;
    }

    public String getPartColExp(Dialect dialect) {
        StringBuilder builder = new StringBuilder();
        if (StringUtils.contains(partitionColumnExpression, ",")) {
            String[] split = partitionColumnExpression.split(",");
            for (int i = 0; i < split.length; i++) {
                String partCol = split[i];
                builder.append(dialect.openQuote())
                        .append(partCol.trim())
                        .append(dialect.closeQuote());
                if ((i + 1) < split.length) {
                    builder.append(",");
                }
            }
        } else {
            builder.append(dialect.openQuote())
                    .append(partitionColumnExpression)
                    .append(dialect.closeQuote());
        }

        return builder.toString();
    }

    public void setPartitionColumnExpression(String partitionColumnExpression) {
        this.partitionColumnExpression = partitionColumnExpression;
    }
}
