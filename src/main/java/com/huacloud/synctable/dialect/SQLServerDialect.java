package com.huacloud.synctable.dialect;

import static com.huacloud.synctable.mapping.Types.*;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/15/2019 10:59 AM
 */
public class SQLServerDialect extends Dialect {

    /**
     * sqlserver number最大长度为38
     */
    public final int NUMBER_MAX_PRECISION = 38;

    public SQLServerDialect() {
        registerColumnType(BIGINT, "bigint");
        registerColumnType(BINARY, "binary($l)");
        registerColumnType(BIT, "varchar($p)");
        registerColumnType(CHAR, "char($l)");
        registerColumnType(DATE, "date");
        registerColumnType(TIMESTAMP, "datetime");
        registerColumnType(TIMESTAMP_WITH_TIMEZONE, "datetime");
        registerColumnType(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "datetimeoffset");
        registerColumnType(INTERVAL_DAY_TO_SECOND, "numeric(20,0)");
        registerColumnType(INTERVAL_YEAR_TO_MONTH, "numeric(20,0)");
        registerColumnType(DECIMAL, "decimal($p,$s)");
        registerColumnType(FLOAT, "float");
        registerColumnType(DOUBLE, "float");
        registerColumnType(LONGVARBINARY, "varbinary");
        registerColumnType(INTEGER, "int");
        registerColumnType(LONGVARCHAR, "text");
        registerColumnType(NUMERIC, "numeric($p,$s)");
        registerColumnType(VARCHAR, "varchar($l)");
        registerColumnType(NVARCHAR, "nvarchar($l)");
        registerColumnType(REAL, "real");
        registerColumnType(SMALLINT, "smallint");
        registerColumnType(TIME, "time");
        registerColumnType(TIME_WITH_TIMEZONE, "time");
        registerColumnType(TINYINT, "tinyint");
        registerColumnType(VARBINARY, "varbinary");
        registerColumnType(BLOB, "varbinary");
        registerColumnType(CLOB, "text");
        registerColumnType(NCLOB, "ntext");

        //pg
        registerColumnType(BIT_VARYING, "varbinary");
        registerColumnType(INTERVAL_PG, "varchar(50)");
        registerColumnType(JSON_PG, "text");
        registerColumnType(MONEY_PG, "money");
        registerColumnType(XML_PG, "xml");
        registerColumnType(UUID_PG, "varchar(50)");


        registerJdbcType("int", INTEGER);
        registerJdbcType("binary", BINARY);
        registerJdbcType("bit", BIT);
        registerJdbcType("bigint", BIGINT);
        registerJdbcType("datetime", DATE);
        registerJdbcType("datetime2", DATE);
        registerJdbcType("float", FLOAT);
        registerJdbcType("image", LONGVARBINARY);
        registerJdbcType("money", DECIMAL);
        registerJdbcType("nchar", CHAR);
        registerJdbcType("ntext", LONGVARCHAR);
        registerJdbcType("numeric", NUMERIC);
        registerJdbcType("decimal", DECIMAL);
        registerJdbcType("nvarchar", VARCHAR);
        registerJdbcType("real", REAL);
        registerJdbcType("smalldatetime", TIMESTAMP);
        registerJdbcType("smallint", SMALLINT);
        registerJdbcType("smallmoney", DECIMAL);
        registerJdbcType("text", LONGVARCHAR);
        registerJdbcType("time", TIME);
        registerJdbcType("date", DATE);
        registerJdbcType("datetimeoffset", TIMESTAMP_WITH_TIMEZONE);
        registerJdbcType("timestamp", TIMESTAMP);
        registerJdbcType("tinyint", TINYINT);
        registerJdbcType("udt", VARBINARY);
        registerJdbcType("uniqueidentifier", CHAR);
        registerJdbcType("varbinary", VARBINARY);
        registerJdbcType("varchar", VARCHAR);
        registerJdbcType("xml", LONGVARCHAR);
        registerJdbcType("geometry", VARBINARY);
        registerJdbcType("geography", VARBINARY);

        //registerDataTypeByConf(DBType.SQLServer);
    }

    @Override
    public String openQuote() {
        return "[";
    }

    @Override
    public String closeQuote() {
        return "]";
    }

    @Override
    public boolean supportsCommentOn() {
        return true;
    }

    @Override
    public String getTypeName(int code, long length, int precision, int scale) {
        if (code == NUMERIC || code == DECIMAL) {
            if (precision > NUMBER_MAX_PRECISION) {
                precision = NUMBER_MAX_PRECISION;
            }
        }
        return super.getTypeName(code, length, precision, scale);
    }

}
