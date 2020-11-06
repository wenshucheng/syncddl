package com.huacloud.synctable.dialect;

import com.huacloud.synctable.mapping.Column;
import com.huacloud.synctable.mapping.PartitionTable;
import com.huacloud.synctable.mapping.PartitionType;
import com.huacloud.synctable.mapping.Table;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.huacloud.synctable.mapping.Types.*;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 4/23/2019 11:15 AM
 */
public class TBaseDialect extends Dialect {

    public TBaseDialect() {
        super();

        registerColumnType(SMALLINT, "smallint");
        registerColumnType(TINYINT, "smallint");
        registerColumnType(INTEGER, "integer");
        registerColumnType(BIGINT, "bigint");
        registerColumnType(DECIMAL, "decimal($p,$s)");
        registerColumnType(NUMERIC, "numeric($p,$s)");
        registerColumnType(REAL, "real");
        registerColumnType(FLOAT, "double precision");
        registerColumnType(DOUBLE, "double precision");
        registerColumnType(LONGVARCHAR, "text");

        registerColumnType(VARCHAR, "varchar($l)");
        registerColumnType(CHAR, "char($l)");
        registerColumnType(CLOB, "text");
        registerColumnType(BLOB, "bytea");

        registerColumnType(BINARY, "bytea");

        registerColumnType(TIMESTAMP, "timestamp($p)");
        registerColumnType(TIMESTAMP_WITH_TIMEZONE, "timestamp($p) with time zone");
        registerColumnType(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "timestamp($p) with time zone");
        registerColumnType(DATE, "date");
        registerColumnType(TIME, "time($p)");
        registerColumnType(TIME_WITH_TIMEZONE, "time($p) with time zone");

        registerColumnType(BOOLEAN, "boolean");
        registerColumnType(LONGVARBINARY, "bytea");
        registerColumnType(VARBINARY, "bytea");

        registerColumnType(BIT, "bit($p)");
        registerColumnType(BIT_VARYING, "bit varying($l)");
        registerColumnType(INTERVAL_PG, "interval");
        registerColumnType(JSON_PG, "json");
        registerColumnType(MONEY_PG, "money");
        registerColumnType(XML_PG, "xml");
        registerColumnType(UUID_PG, "uuid");

        registerColumnType(INTERVAL_DAY_TO_SECOND, "interval day to second");
        registerColumnType(INTERVAL_YEAR_TO_MONTH, "interval year to month");
        registerColumnType(NCLOB, "text");
        registerColumnType(NVARCHAR, "varchar($l)");
        registerColumnType(BIGSERIAL_PG, "bigserial");
        registerColumnType(SMALLSERIAL_PG, "smallserial");
        registerColumnType(SERIAL_PG, "serial");

        registerJdbcType("smallint", SMALLINT);
        registerJdbcType("integer", INTEGER);
        registerJdbcType("bigint", BIGINT);
        registerJdbcType("bit varying", BIT_VARYING);
        registerJdbcType("decimal", DECIMAL);
        registerJdbcType("numeric", NUMERIC);
        registerJdbcType("real", REAL);
        registerJdbcType("double precision", DOUBLE);
        registerJdbcType("smallserial", SMALLSERIAL_PG);
        registerJdbcType("serial", SERIAL_PG);
        registerJdbcType("bigserial", BIGSERIAL_PG);
        registerJdbcType("varchar", VARCHAR);
        registerJdbcType("character varying", VARCHAR);
        registerJdbcType("character", CHAR);
        registerJdbcType("text", CLOB);
        registerJdbcType("bytea", BINARY);
        registerJdbcType("timestamp", TIMESTAMP);
        registerJdbcType("timestamp with time zone", TIMESTAMP_WITH_TIMEZONE);
        registerJdbcType("time", TIME);
        registerJdbcType("time with time zone", TIME_WITH_TIMEZONE);
        registerJdbcType("date", DATE);
        registerJdbcType("json", JSON_PG);
        registerJdbcType("money", MONEY_PG);
        registerJdbcType("boolean", BOOLEAN);
        registerJdbcType("interval", INTERVAL_PG);
        registerJdbcType("timestamp without time zone", TIMESTAMP);
        registerJdbcType("time without time zone", TIME);
        registerJdbcType("xml", XML_PG);
        registerJdbcType("uuid", UUID_PG);

        //registerDataTypeByConf(DBType.TBase);

    }

    @Override
    public String openQuote() {
        return "\"";
    }

    @Override
    public String closeQuote() {
        return "\"";
    }

    @Override
    public boolean supportsCommentOn() {
        return true;
    }

    @Override
    public String getTablePartitionSQL(Table table) {
        PartitionType partitionType = table.getPartitionType();
        if (partitionType == PartitionType.HASH) {
            //TBase不支持Hash分区
            return "";
        }
        StringBuilder sql = new StringBuilder("\nPARTITION BY ");
        sql.append(partitionType.getName()).append(" (");
        String partColExp = table.getPartColExp(this);
        sql.append(partColExp);
        sql.append(")");
        return sql.toString();
    }

    @Override
    public String getPartitionCreateSQL(Table table) {

        PartitionType partitionType = table.getPartitionType();
        if (partitionType == PartitionType.HASH ||
                partitionType == PartitionType.KEY) {
            //TBase不支持Hash分区
            return "";
        }
        List<PartitionTable> partitionTables = table.getPartitionTables();

        StringBuilder sql = new StringBuilder();

        List<String> valList = new ArrayList<>();
        for (PartitionTable partitionTable : partitionTables) {
            String value = partitionTable.getValue();
            //to_date这种函数直接抽取时间出来
            //TO_DATE(' 2019-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
            if (StringUtils.containsIgnoreCase(value, "TO_DATE")) {
                int i1 = StringUtils.indexOf(value, "'");
                int i2 = StringUtils.indexOf(value, "'", i1 + 1);
                value = StringUtils.substring(value, i1, i2+1);
            }
            valList.add(value);
        }
        List<Pair<String, String>> valPairs = one2twoPartVal(valList);

        for (int i = 0, size = partitionTables.size(); i < size; i++) {
            PartitionTable partitionTable = partitionTables.get(i);
            sql.append("CREATE TABLE ");
            String name = partitionTable.getName();
            String value = partitionTable.getValue();
            String partitionTabName = table.getName() + "_" + name;

            sql.append(partitionTabName)
                .append(" PARTITION OF ")
                .append(this.openQuote())
                .append(table.getName())
                .append(this.closeQuote())
                .append("(");

            Iterator<Column> columnIterator = table.getColumnIterator();
            while (columnIterator.hasNext()) {
                Column column = columnIterator.next();
                String columnName = column.getName();
                sql.append(this.openQuote())
                        .append(columnName)
                        .append(this.closeQuote());
                if (columnIterator.hasNext()) {
                    sql.append(", ");
                }
            }
            sql.append(") FOR VALUES");

            if (partitionType == PartitionType.LIST) {
                sql.append(" IN (").append(value).append(")");

            } else if (partitionType == PartitionType.RANGE) {
                Pair<String, String> pair = valPairs.get(i);
                sql.append(" FROM (")
                    .append(pair.getKey())
                    .append(") TO (")
                    .append(pair.getValue())
                    .append(")");
            }

            sql.append(";\n");
        }
        return sql.toString();
    }

    /**
     * 将分区的范围值（less than t）转换成TBase格式（from t1 to t2）
     * 例如MySQL分区表：
     *     PARTITION p0 VALUES LESS THAN (10),
     *     PARTITION p1 VALUES LESS THAN (20)
     *
     * 转换TBase格式：
     * CREATE TABLE test_range_tab_p0 PARTITION of test_range_tab (id) FOR VALUES FROM (MINVALUE) TO (10);
     * CREATE TABLE test_range_tab_p1 PARTITION of test_range_tab (id) FOR VALUES FROM (10) TO (20);
     *
     */
    private List<Pair<String, String>> one2twoPartVal(List<String> partValues) {
        List<Pair<String, String>> pairList = new ArrayList<>();
        //分区字段的个数
        int paramSize = StringUtils.split(partValues.get(0), ",").length;
        for (int i = 0, size = partValues.size(); i < size; i++) {
            String key;
            String value;
            String val = partValues.get(i);

            if (i == 0) {
                value = val;
                StringBuilder keyStr = new StringBuilder();
                for (int j = 0; j < paramSize; j++) {
                    keyStr.append("MINVALUE");
                    if ((j + 1) < paramSize) {
                        keyStr.append(",");
                    }
                }
                key = keyStr.toString();

            } else {
                String previous = partValues.get(i - 1);
                key = previous;
                value = val;
            }

            Pair<String, String> pair = Pair.of(key, value);
            pairList.add(pair);
        }
        return pairList;
    }

    @Override
    public boolean isAlonePartSQL() {
        return true;
    }

}
