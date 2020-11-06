package com.huacloud.synctable.dialect;

import com.huacloud.synctable.entity.DBType;
import com.huacloud.synctable.mapping.*;
import com.huacloud.synctable.utils.DbDataTypeConf;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.huacloud.synctable.mapping.Types.*;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 4/17/2019 12:09 PM
 */
public abstract class Dialect {

    /**
     * jdbc类型与数据库类型的映射
     */
    private final TypeNames typeNames = new TypeNames();

    /**
     * 数据库类型与jdbc类型的映射
     */
    private final Map<String, Integer> jdbcTypeMap = new HashMap<>();

    protected Dialect() {

        registerColumnType(BIT, "bit");
        registerColumnType(BOOLEAN, "boolean");
        registerColumnType(TINYINT, "tinyint");
        registerColumnType(SMALLINT, "smallint");
        registerColumnType(INTEGER, "integer");
        registerColumnType(BIGINT, "bigint");
        registerColumnType(FLOAT, "float");
        registerColumnType(DOUBLE, "double");
        registerColumnType(REAL, "real");

        registerColumnType(DATE, "date");
        registerColumnType(TIME, "time");
        registerColumnType(TIMESTAMP, "timestamp");
        registerColumnType(BLOB, "blob");

        registerColumnType(CHAR, "char($l)");
        registerColumnType(VARCHAR, "varchar($l)");
        registerColumnType(CLOB, "clob");

        registerJdbcType("bit", BIT);
        registerJdbcType("bigint", BIGINT);
        registerJdbcType("smallint", SMALLINT);
        registerJdbcType("tinyint", TINYINT);
        registerJdbcType("integer", INTEGER);
        registerJdbcType("int", INTEGER);
        registerJdbcType("char", CHAR);
        registerJdbcType("float", FLOAT);
        registerJdbcType("number", NUMERIC);
    }

    public Map<String, Integer> getJdbcTypeMap() {
        return jdbcTypeMap;
    }

    /**
     * Subclasses register a type name for the given type code and maximum
     * column length. <tt>$l</tt> in the type name with be replaced by the
     * column length (if appropriate).
     *
     * @param code     The {@link com.huacloud.synctable.mapping.Types} typecode
     * @param capacity The maximum length of database type
     * @param name     The database type name
     */
    protected void registerColumnType(int code, long capacity, String name) {
        typeNames.put(code, capacity, name);
    }

    /**
     * Subclasses register a type name for the given type code. <tt>$l</tt> in
     * the type name with be replaced by the column length (if appropriate).
     *
     * @param code The {@link com.huacloud.synctable.mapping.Types} typecode
     * @param name The database type name
     */
    protected void registerColumnType(int code, String name) {
        typeNames.put(code, name);
    }

    protected void registerJdbcType(String typeName, int jdbcTypeCode) {
        jdbcTypeMap.put(typeName.toLowerCase(), jdbcTypeCode);
    }


    public String getCreateTableString() {
        return "CREATE TABLE";
    }

    /**
     * The character specific to this dialect used to begin a quoted identifier.
     *
     * @return The dialect's specific open quote character.
     */
    public String openQuote() {
        return "\"";
    }

    /**
     * The character specific to this dialect used to close a quoted identifier.
     *
     * @return The dialect's specific close quote character.
     */
    public String closeQuote() {
        return "\"";
    }

    /**
     * The keyword used to specify a nullable column.
     *
     * @return String
     */
    public String getNullColumnString() {
        return "";
    }

    /**
     * Get the comment into a form supported for column definition.
     *
     * @param comment The comment to apply
     * @return The comment fragment
     */
    public String getColumnComment(String comment) {
        return "";
    }

    /**
     * Get the comment into a form supported for table definition.
     *
     * @param comment The comment to apply
     * @return The comment fragment
     */
    public String getTableComment(String comment) {
        return "";
    }

    public String getTableTypeString() {
        // grrr... for differentiation of mysql storage engines
        return "";
    }

    /**
     * Apply dialect-specific quoting.
     * <p/>
     * By default, the incoming value is checked to see if its first character
     * is the back-tick (`).  If so, the dialect specific quoting is applied.
     *
     * @param name The value to be quoted.
     * @return The quoted (or unmodified, if not starting with back-tick) value.
     * @see #openQuote()
     * @see #closeQuote()
     */
    public final String quote(String name) {
        if (name == null) {
            return null;
        }

        if (name.charAt(0) == '`') {
            return openQuote() + name.substring(1, name.length() - 1) + closeQuote();
        } else {
            return name;
        }
    }

    /**
     * The syntax used to add a primary key constraint to a table.
     *
     * @param constraintName The name of the PK constraint.
     * @return The "add PK" fragment
     */
    public String getAddPrimaryKeyConstraintString(String constraintName) {
        return " add constraint " + constraintName + " primary key ";
    }

    public String getTypeName(int code) {
        final String result = typeNames.get(code);
        if (result == null) {
            throw new RuntimeException("No default type mapping for (com.huacloud.synctable.mapping.Types) " + code);
        }
        return result;
    }

    public String getTypeName(int code, long length, int precision, int scale) {
        final String result = typeNames.get(code, length, precision, scale);
        if (result == null) {
            throw new RuntimeException(
                    String.format("No type mapping for com.huacloud.synctable.mapping.Types code: %s, length: %s",
                            code, length)
            );
        }
        return result;
    }

    public Integer getJdbcType(String jdbcType) {
        Integer jdbcTypeCode = jdbcTypeMap.get(jdbcType.toLowerCase());
        if (jdbcTypeCode == null) {
            throw new RuntimeException(
                    String.format("不支持的类型：%s", jdbcType)
            );
        }
        return jdbcTypeCode;
    }

    public String getTableCreationUniqueConstraintsFragment(UniqueKey uniqueKey) {

        final StringBuilder sb = new StringBuilder();
        sb.append("CONSTRAINT " + openQuote() + uniqueKey.getName() + closeQuote() + " ");

        sb.append("UNIQUE (");
        final Iterator<Column> columnIterator = uniqueKey.columnIterator();
        while (columnIterator.hasNext()) {
            final Column column = columnIterator.next();
            if (column == null) {
                continue;
            }
            sb.append(column.getQuotedName(this));
            if (columnIterator.hasNext()) {
                sb.append(", ");
            }
        }

        return sb.append(')').toString();
    }

    /**
     * 是否支持单独的备注语句
     *
     * @return boolean
     */
    public boolean supportsCommentOn() {
        return false;
    }

    public void registerDataTypeByConf(DBType dbType) {
        Map[] twoMap = DbDataTypeConf.parserConf(dbType);

        Map<String, Integer> db2CommonMap = twoMap[0];
        Map<Integer, String> common2DbMap = twoMap[1];

        for (Map.Entry<String, Integer> entry : db2CommonMap.entrySet()) {
            String key = entry.getKey();
            Integer value = entry.getValue();
            registerJdbcType(key, value);
        }

        for (Map.Entry<Integer, String> entry : common2DbMap.entrySet()) {
            Integer key = entry.getKey();
            String value = entry.getValue();
            registerColumnType(key, value);
        }

    }

    /**
     * 获取分区表的创建语句
     * @param table table
     * @return sql
     */
    public String getTablePartitionSQL(Table table) {
        List<PartitionTable> partitionTables = table.getPartitionTables();
        PartitionType partitionType = table.getPartitionType();
        StringBuilder buf = new StringBuilder();
        String partColExp = table.getPartColExp(this);

        //表的分区信息
        buf.append("\nPARTITION BY ")
                .append(partitionType.getName())
                .append(" (")
                .append(partColExp)
                .append(")");

        if (partitionType != PartitionType.HASH) {
            buf.append(" (\n");
            for (int i = 0, size = partitionTables.size(); i < size; i++) {
                PartitionTable partTable = partitionTables.get(i);
                String partTableName = partTable.getName();
                String value = partTable.getValue();

                buf.append("\tpartition ")
                        .append(partTableName);

                switch (partitionType) {

                    case LIST:
                        buf.append(" values (")
                                .append(value)
                                .append(")");
                        break;

                    case RANGE:
                        buf.append(" values less than (")
                                .append(value)
                                .append(")");
                        break;
                }
                boolean hasNext = (i + 1) < size;
                if (hasNext) {
                    buf.append(",\n");
                }
            }
            buf.append("\n)");
        } else {
            buf.append(" PARTITIONS ").append(partitionTables.size());
        }
        return buf.toString();
    }

    public String getPartitionCreateSQL(Table table) {
        return null;
    }

    public boolean isAlonePartSQL() {
        return false;
    }

    /**
     * 建表时字段是否支持非空(not null)
     * @return boolean
     */
    public boolean isSupportColNotNull() {
        return true;
    }

    /**
     * 建表时字段是否支持default语法
     * @return boolean
     */
    public boolean isSupportColDefaultVal() {
        return true;
    }

    /**
     * 建表时是否支持主键语法
     * @return boolean
     */
    public boolean isSupportPK() {
        return true;
    }

    public String indexCreateSql(Table table) {
        List<Index> indexList = table.getIndexList();
        StringBuilder sql = new StringBuilder();
        if (CollectionUtils.isNotEmpty(indexList)) {
            for (Index index : indexList) {
                sql.append("CREATE ");
                if (index.isUnique()) {
                    sql.append("UNIQUE ");
                }

                sql.append("INDEX ")
                        .append(index.getName())
                        .append(" ON ")
                        .append(table.getQuotedSchema(this))
                        .append(".")
                        .append(table.getQuotedTabName(this))
                        .append(" ( ");

                List<IndexColumn> columnList = index.getColumnList();
                for (int i = 0, size = columnList.size(); i < size; i++) {
                    IndexColumn column = columnList.get(i);
                    String columnName = column.getColumnName();
                    SortOrder sortOrder = column.getSortOrder();
                    sql.append(this.openQuote())
                            .append(columnName)
                            .append(this.closeQuote())
                            .append(" ")
                            .append(sortOrder.toSQL());

                    if ((i + 1) < size) {
                        sql.append(", ");
                    }
                }
                sql.append(");\n");
            }
        }
        return sql.toString();
    }

    /**
     * 是否支持在表名上增加quoted
     * @return 默认支持
     */
    public boolean isSupportTableQuoted() {
        return true;
    }

    public void checkColumn(Iterator<Column> columnIterator) {
    }

}
