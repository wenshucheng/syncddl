package com.huacloud.synctable.dialect;

import com.huacloud.synctable.entity.DBType;
import com.huacloud.synctable.mapping.Column;

import java.util.Iterator;

import static com.huacloud.synctable.mapping.Types.*;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 4/17/2019 12:17 PM
 */
public class MySQLDialect extends Dialect {

    public MySQLDialect() {
        super();
        registerColumnType(BIT, "bit");
        registerColumnType(BIGINT, "bigint");
        registerColumnType(SMALLINT, "smallint");
        registerColumnType(TINYINT, "tinyint");
        registerColumnType(INTEGER, "integer");
        registerColumnType(CHAR, "char($l)");
        registerColumnType(FLOAT, "float");
        registerColumnType(REAL, "float");
        registerColumnType(DOUBLE, "double");
        registerColumnType(DECIMAL, "decimal($p,$s)");
        registerColumnType(BOOLEAN, "bit");
        registerColumnType(DATE, "datetime");
        registerColumnType(TIME, "time");
        registerColumnType(TIME_WITH_TIMEZONE, "time");
        registerColumnType(TIMESTAMP, "timestamp");
        registerColumnType(TIMESTAMP_WITH_TIMEZONE, "timestamp");
        registerColumnType(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "timestamp");
        registerColumnType(VARBINARY, "longblob");
        registerColumnType(VARBINARY, 16777215, "mediumblob");
        registerColumnType(VARBINARY, 65535, "blob");
        registerColumnType(VARBINARY, 255, "tinyblob");
        registerColumnType(BINARY, "binary($l)");
        registerColumnType(LONGVARBINARY, "longblob");
        registerColumnType(LONGVARBINARY, 16777215, "mediumblob");
        registerColumnType(NUMERIC, "decimal($p,$s)");
        registerColumnType(BLOB, "longblob");
        registerColumnType(CLOB, "longtext");
        registerColumnType(NCLOB, "longtext");
        registerColumnType(VARCHAR, "longtext");
        registerColumnType(VARCHAR, 2000, "varchar($l)");
        registerColumnType(LONGVARCHAR, "longtext");
        registerColumnType(NVARCHAR, "varchar($l)");
        registerColumnType(INTERVAL_DAY_TO_SECOND, "bigint");
        registerColumnType(INTERVAL_YEAR_TO_MONTH, "bigint");

        //pg
        registerColumnType(BIT_VARYING, "varbinary($l)");
        registerColumnType(INTERVAL_PG, "varchar(50)");
        registerColumnType(JSON_PG, "longtext");
        registerColumnType(MONEY_PG, "number(20,2)");
        registerColumnType(XML_PG, "longtext");
        registerColumnType(UUID_PG, "varchar(50)");

        registerJdbcType("double", DOUBLE);
        registerJdbcType("double precision", DOUBLE);
        registerJdbcType("date", DATE);
        registerJdbcType("time", TIME);
        registerJdbcType("datetime", DATE);
        registerJdbcType("timestamp", TIMESTAMP);
        registerJdbcType("longblob", BLOB);
        registerJdbcType("mediumblob", BLOB);
        registerJdbcType("blob", BLOB);
        registerJdbcType("tinyblob", BLOB);
        registerJdbcType("binary", BINARY);
        registerJdbcType("decimal", NUMERIC);
        registerJdbcType("longtext", CLOB);
        registerJdbcType("varchar", VARCHAR);
        registerJdbcType("enum", VARCHAR);
        registerJdbcType("set", VARCHAR);
        registerJdbcType("mediumint", INTEGER);
        registerJdbcType("mediumtext", CLOB);
        registerJdbcType("text", CLOB);
        registerJdbcType("tinytext", CLOB);
        registerJdbcType("varbinary", VARBINARY);
        registerJdbcType("year", SMALLINT);
        registerJdbcType("bigint", BIGINT);
        registerJdbcType("bit", BIT);
        registerJdbcType("tinyint", TINYINT);
        registerJdbcType("char", CHAR);
        registerJdbcType("float", FLOAT);
        registerJdbcType("int", INTEGER);
        registerJdbcType("smallint", SMALLINT);

        registerDataTypeByConf(DBType.MYSQL);
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
    public String getColumnComment(String comment) {
        return " comment '" + comment + "'";
    }

    @Override
    public String getTableComment(String comment) {
        return " comment='" + comment + "'";
    }

    @Override
    public void checkColumn(Iterator<Column> columnIterator) {
    }
}
