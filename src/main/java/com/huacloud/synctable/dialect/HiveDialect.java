package com.huacloud.synctable.dialect;

import com.huacloud.synctable.mapping.Index;
import com.huacloud.synctable.mapping.IndexColumn;
import com.huacloud.synctable.mapping.SortOrder;
import com.huacloud.synctable.mapping.Table;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

import static com.huacloud.synctable.mapping.Types.*;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 4/29/2019 4:56 PM
 */
public class HiveDialect extends Dialect {

    public HiveDialect() {
        registerColumnType(TINYINT, "TINYINT");
        registerColumnType(SMALLINT, "SMALLINT");
        registerColumnType(INTEGER, "INT");
        registerColumnType(BIGINT, "BIGINT");
        registerColumnType(BOOLEAN, "BOOLEAN");
        registerColumnType(FLOAT, "FLOAT");
        registerColumnType(DOUBLE, "DOUBLE");
        registerColumnType(VARCHAR, "VARCHAR($l)");
        registerColumnType(NVARCHAR, "VARCHAR($l)");
        registerColumnType(BINARY, "BINARY");
        registerColumnType(TIMESTAMP, "TIMESTAMP");
        registerColumnType(DECIMAL, "DECIMAL($p,$s)");
        registerColumnType(NUMERIC, "DECIMAL($p,$s)");
        registerColumnType(DATE, "DATE");
        registerColumnType(CHAR, "CHAR($l)");
        registerColumnType(TIME, "TIMESTAMP");
        registerColumnType(BLOB, "BINARY");
        registerColumnType(CLOB, "STRING");
        registerColumnType(NCLOB, "STRING");
        registerColumnType(TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP");
        registerColumnType(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "TIMESTAMP");
        registerColumnType(TIME_WITH_TIMEZONE, "TIMESTAMP");
        registerColumnType(REAL, "FLOAT");
        registerColumnType(LONGVARCHAR, "STRING");
        registerColumnType(BIT, "STRING");
        registerColumnType(VARBINARY, "BINARY");
        registerColumnType(LONGVARBINARY, "BINARY");
        registerColumnType(INTERVAL_DAY_TO_SECOND, "DECIMAL(20,0)");
        registerColumnType(INTERVAL_YEAR_TO_MONTH, "DECIMAL(20,0)");

        //pg
        registerColumnType(BIT_VARYING, "BINARY");
        registerColumnType(INTERVAL_PG, "STRING");
        registerColumnType(JSON_PG, "STRING");
        registerColumnType(MONEY_PG, "DECIMAL(20,2)");
        registerColumnType(XML_PG, "STRING");
        registerColumnType(UUID_PG, "STRING");

        //registerDataTypeByConf(DBType.HIVE);
    }

    @Override
    public String getColumnComment(String comment) {
        return " comment '" + comment + "'";
    }

    @Override
    public String getTableComment(String comment) {
        return " comment '" + comment + "'";
    }

    @Override
    public boolean supportsCommentOn() {
        return false;
    }

    @Override
    public String openQuote() {
        return "`";
    }

    @Override
    public String closeQuote() {
        return "`";
    }

    @Override
    public boolean isSupportColNotNull() {
        return false;
    }

    @Override
    public boolean isSupportColDefaultVal() {
        return false;
    }

    @Override
    public boolean isSupportPK() {
        return false;
    }

    @Override
    public String indexCreateSql(Table table) {
        List<Index> indexList = table.getIndexList();
        StringBuilder sql = new StringBuilder();
        if (CollectionUtils.isNotEmpty(indexList)) {
            for (Index index : indexList) {
                sql.append("CREATE ");
                sql.append("INDEX ")
                        .append(index.getName())
                        .append(" ON TABLE ")
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
                sql.append(") AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' " +
                        "WITH DEFERRED REBUILD;\n");
            }
        }
        return sql.toString();
    }
}
