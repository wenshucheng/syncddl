package com.huacloud.synctable.dialect;

import static com.huacloud.synctable.mapping.Types.*;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 4/17/2019 12:17 PM
 */
public class OracleDialect extends Dialect {

    /**
     * oracle number最大长度为38
     */
    public final int NUMBER_MAX_PRECISION = 38;

    public OracleDialect() {
        super();
        registerDateTimeTypeMappings();
        registerCharacterTypeMappings();
        registerNumericTypeMappings();
        registerLargeObjectTypeMappings();

        registerColumnType(REAL, "float");

        //registerDataTypeByConf(DBType.ORACLE);

        //pg
        registerColumnType(BIT_VARYING, "varchar2($l)");
        registerColumnType(INTERVAL_PG, "varchar(50)");
        registerColumnType(JSON_PG, "clob");
        registerColumnType(MONEY_PG, "number(20,2)");
        registerColumnType(XML_PG, "clob");
        registerColumnType(UUID_PG, "varchar(50)");
    }

    protected void registerDateTimeTypeMappings() {
        registerColumnType(DATE, "date");
        registerColumnType(TIME, "date");
        registerColumnType(TIMESTAMP, "timestamp");
        registerColumnType(TIMESTAMP_WITH_TIMEZONE, "timestamp with time zone");
        registerColumnType(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "timestamp with local time zone");
        registerColumnType(TIME_WITH_TIMEZONE, "date");
        registerColumnType(INTERVAL_DAY_TO_SECOND, "interval day($p) to second($s)");
        registerColumnType(INTERVAL_YEAR_TO_MONTH, "interval year($p) to month");

        registerJdbcType("date", DATE);
        registerJdbcType("timestamp", TIMESTAMP);
        registerJdbcType("timestamp with local time zone", TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        registerJdbcType("timestamp with time zone", TIMESTAMP_WITH_TIMEZONE);
    }

    protected void registerCharacterTypeMappings() {
        registerColumnType(CHAR, "char($l)");
        registerColumnType(VARCHAR, 4000, "varchar2($l)");
        registerColumnType(VARCHAR, "long");
        registerColumnType(NVARCHAR, "nvarchar2($l)");

        registerJdbcType("char", CHAR);
        registerJdbcType("varchar2", VARCHAR);
        registerJdbcType("long", VARCHAR);
        registerJdbcType("nvarchar2", NVARCHAR);
    }

    protected void registerNumericTypeMappings() {
        registerColumnType(BIT, "number($p)");
        registerColumnType(BIGINT, "number(19,0)");
        registerColumnType(SMALLINT, "number(5,0)");
        registerColumnType(TINYINT, "number(3,0)");
        registerColumnType(INTEGER, "number(10,0)");

        registerColumnType(FLOAT, "float");
        registerColumnType(DOUBLE, "double precision");
        registerColumnType(NUMERIC, "number($p,$s)");
        registerColumnType(DECIMAL, "number($p,$s)");

        registerColumnType(BOOLEAN, "number(1,0)");

        registerJdbcType("number", NUMERIC);
        registerJdbcType("float", FLOAT);
        registerJdbcType("double precision", DOUBLE);
        registerJdbcType("double", DOUBLE);
        registerJdbcType("binary_double", DOUBLE);
        registerJdbcType("binary_float", FLOAT);
        registerJdbcType("interval day to second", INTERVAL_DAY_TO_SECOND);
        registerJdbcType("interval year to month", INTERVAL_YEAR_TO_MONTH);
    }

    protected void registerLargeObjectTypeMappings() {
        registerColumnType(BINARY, 2000, "raw($l)");
        registerColumnType(BINARY, "long raw");

        registerColumnType(VARBINARY, 2000, "raw($l)");
        registerColumnType(VARBINARY, "long raw");

        registerColumnType(BLOB, "blob");
        registerColumnType(CLOB, "clob");
        registerColumnType(NCLOB, "nclob");

        registerColumnType(LONGVARCHAR, "clob");
        registerColumnType(LONGVARBINARY, "long raw");

        registerJdbcType("raw", BINARY);
        registerJdbcType("long raw", BINARY);
        registerJdbcType("blob", BLOB);
        registerJdbcType("clob", CLOB);
        registerJdbcType("long", LONGVARCHAR);
        registerJdbcType("long raw", LONGVARBINARY);
        registerJdbcType("nclob", NCLOB);
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
    public boolean isSupportTableQuoted() {
        return false;
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
