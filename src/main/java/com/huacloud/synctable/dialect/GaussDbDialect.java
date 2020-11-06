package com.huacloud.synctable.dialect;

import com.huacloud.synctable.entity.DBType;
import com.huacloud.synctable.mapping.PartitionTable;
import com.huacloud.synctable.mapping.Table;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

import static com.huacloud.synctable.mapping.Types.*;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/27/2019 12:26 PM
 */
public class GaussDbDialect extends Dialect {

    public GaussDbDialect() {

        registerColumnType(INTEGER, "int");
        registerColumnType(BIGINT, "bigint");
        registerColumnType(REAL, "real");
        registerColumnType(DOUBLE, "double");
        registerColumnType(FLOAT, "float");
        registerColumnType(NUMERIC, "number($p,$s)");
        registerColumnType(DECIMAL, "decimal($p,$s)");
        registerColumnType(CHAR, "char($l)");
        registerColumnType(NCHAR, "nchar($l)");
        registerColumnType(VARCHAR, "varchar($l)");
        registerColumnType(NVARCHAR, "nvarchar($l)");
        registerColumnType(DATE, "date");
        registerColumnType(TIME, "date");
        registerColumnType(TIME_WITH_TIMEZONE, "timestamp with time zone");
        registerColumnType(TIMESTAMP, "timestamp($s)");
        registerColumnType(TIMESTAMP_WITH_TIMEZONE, "timestamp($s) with time zone");
        registerColumnType(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "timestamp($s) with local time zone");
        registerColumnType(BOOLEAN, "boolean");
        registerColumnType(CLOB, "clob");
        registerColumnType(BLOB, "blob");
        registerColumnType(LONGVARBINARY, "blob");
        registerColumnType(BINARY, "binary($l)");
        registerColumnType(BIT, "varchar($p)");
        registerColumnType(TINYINT, "tinyint");
        registerColumnType(SMALLINT, "smallint");
        registerColumnType(VARBINARY, "varbinary($l)");
        registerColumnType(LONGVARCHAR, "clob");
        registerColumnType(NCLOB, "clob");
        registerColumnType(INTERVAL_DAY_TO_SECOND, "interval day($p) to second($s)");
        registerColumnType(INTERVAL_YEAR_TO_MONTH, "interval year($p) to month");


        //pg
        registerColumnType(BIT_VARYING, "varbinary($l)");
        registerColumnType(INTERVAL_PG, "varchar(50)");
        registerColumnType(JSON_PG, "clob");
        registerColumnType(MONEY_PG, "number(20,2)");
        registerColumnType(XML_PG, "clob");
        registerColumnType(UUID_PG, "varchar(50)");

        registerDataTypeByConf(DBType.GaussDB);
    }

    @Override
    public boolean supportsCommentOn() {
        return true;
    }

    @Override
    public String getTablePartitionSQL(Table table) {
        List<PartitionTable> partitionTables = table.getPartitionTables();
        for (PartitionTable partitionTable : partitionTables) {
            String value = partitionTable.getValue();
            if (StringUtils.containsIgnoreCase(value, "TO_DATE")) {
                int i1 = StringUtils.indexOf(value, "'");
                int i2 = StringUtils.indexOf(value, "'", i1 + 1);
                value = StringUtils.substring(value, i1, i2+1);
            }
            partitionTable.setValue(value);
        }
        String sql = super.getTablePartitionSQL(table);
        return sql;
    }

}
