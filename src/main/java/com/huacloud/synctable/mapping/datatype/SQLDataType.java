package com.huacloud.synctable.mapping.datatype;

import java.sql.*;

public final class SQLDataType {

    // -------------------------------------------------------------------------
    // String types
    // -------------------------------------------------------------------------

    /**
     * The {@link Types#VARCHAR} type.
     */
    public static final DataType VARCHAR = new DefaultDataType(Types.VARCHAR,"varchar");

    /**
     * The {@link Types#VARCHAR} type.
     */
    public static final DataType VARCHAR(int length) {
        return VARCHAR.length(length);
    }

    /**
     * The {@link Types#CHAR} type.
     */
    public static final DataType CHAR = new DefaultDataType(Types.CHAR, "char");

    /**
     * The {@link Types#CHAR} type.
     */
    public static final DataType CHAR(int length) {
        return CHAR.length(length);
    }

    /**
     * The {@link Types#LONGVARCHAR} type.
     */
    public static final DataType LONGVARCHAR = new DefaultDataType(Types.LONGVARCHAR, "longvarchar");

    /**
     * The {@link Types#LONGVARCHAR} type.
     */
    public static final DataType LONGVARCHAR(int length) {
        return LONGVARCHAR.length(length);
    }

    /**
     * The {@link Types#CLOB} type.
     */
    public static final DataType CLOB = new DefaultDataType(Types.CLOB, "clob");

    /**
     * The {@link Types#CLOB} type.
     */
    public static final DataType CLOB(int length) {
        return CLOB.length(length);
    }

    /**
     * The {@link Types#NVARCHAR} type.
     */
    public static final DataType NVARCHAR = new DefaultDataType(Types.NVARCHAR, "nvarchar");

    /**
     * The {@link Types#NVARCHAR} type.
     */
    public static final DataType NVARCHAR(int length) {
        return NVARCHAR.length(length);
    }

    /**
     * The {@link Types#NCHAR} type.
     */
    public static final DataType NCHAR = new DefaultDataType(Types.NCHAR, "nchar");

    /**
     * The {@link Types#NCHAR} type.
     */
    public static final DataType NCHAR(int length) {
        return NCHAR.length(length);
    }

    /**
     * The {@link Types#LONGNVARCHAR} type.
     */
    public static final DataType LONGNVARCHAR = new DefaultDataType(Types.LONGNVARCHAR, "longnvarchar");

    /**
     * The {@link Types#LONGNVARCHAR} type.
     */
    public static final DataType LONGNVARCHAR(int length) {
        return LONGNVARCHAR.length(length);
    }

    /**
     * The {@link Types#NCLOB} type.
     */
    public static final DataType NCLOB = new DefaultDataType(Types.NCLOB, "nclob");

    /**
     * The {@link Types#NCLOB} type.
     */
    public static final DataType NCLOB(int length) {
        return NCLOB.length(length);
    }

    // -------------------------------------------------------------------------
    // Boolean types
    // -------------------------------------------------------------------------

    /**
     * The {@link Types#BOOLEAN} type.
     */
    public static final DataType BOOLEAN = new DefaultDataType(Types.BOOLEAN, "boolean");

    /**
     * The {@link Types#BIT} type.
     */
    public static final DataType BIT = new DefaultDataType(Types.BIT, "bit");

    // -------------------------------------------------------------------------
    // Integer types
    // -------------------------------------------------------------------------

    /**
     * The {@link Types#TINYINT} type.
     */
    public static final DataType TINYINT = new DefaultDataType(Types.TINYINT, "tinyint");

    /**
     * The {@link Types#SMALLINT} type.
     */
    public static final DataType SMALLINT = new DefaultDataType(Types.SMALLINT, "smallint");

    /**
     * The {@link Types#INTEGER} type.
     */
    public static final DataType INTEGER = new DefaultDataType(Types.INTEGER, "integer");

    /**
     * The {@link Types#BIGINT} type.
     */
    public static final DataType BIGINT = new DefaultDataType(Types.BIGINT, "bigint");

    /**
     * The zero-scale {@link Types#DECIMAL} type.
     */
    public static final DataType DECIMAL_INTEGER = new DefaultDataType(Types.DECIMAL, "decimal_integer");

    // -------------------------------------------------------------------------
    // Unsigned integer types
    // -------------------------------------------------------------------------

    /**
     * The unsigned {@link Types#TINYINT} type.
     */
    public static final DataType TINYINTUNSIGNED = new DefaultDataType(Types.TINYINT, "tinyint unsigned");

    /**
     * The unsigned {@link Types#SMALLINT} type.
     */
    public static final DataType SMALLINTUNSIGNED = new DefaultDataType(Types.SMALLINT, "smallint unsigned");

    /**
     * The unsigned {@link Types#INTEGER} type.
     */
    public static final DataType INTEGERUNSIGNED = new DefaultDataType(Types.INTEGER, "integer unsigned");

    /**
     * The unsigned {@link Types#BIGINT} type.
     */
    public static final DataType BIGINTUNSIGNED = new DefaultDataType(Types.BIGINT, "bigint unsigned");

    // -------------------------------------------------------------------------
    // Floating point types
    // -------------------------------------------------------------------------

    /**
     * The {@link Types#DOUBLE} type.
     */
    public static final DataType DOUBLE = new DefaultDataType(Types.DOUBLE, "double");

    /**
     * The {@link Types#FLOAT} type.
     */
    public static final DataType FLOAT = new DefaultDataType(Types.FLOAT, "float");

    /**
     * The {@link Types#REAL} type.
     */
    public static final DataType REAL = new DefaultDataType(Types.REAL, "real");

    // -------------------------------------------------------------------------
    // Numeric types
    // -------------------------------------------------------------------------

    /**
     * The {@link Types#NUMERIC} type.
     */
    public static final DataType NUMERIC = new DefaultDataType(Types.NUMERIC, "numeric");

    /**
     * The {@link Types#NUMERIC} type.
     */
    public static final DataType NUMERIC(int precision) {
        return NUMERIC.precision(precision);
    }

    /**
     * The {@link Types#NUMERIC} type.
     */
    public static final DataType NUMERIC(int precision, int scale) {
        return NUMERIC.precision(precision, scale);
    }

    /**
     * The {@link Types#DECIMAL} type.
     */
    public static final DataType DECIMAL = new DefaultDataType(Types.DECIMAL, "decimal");

    /**
     * The {@link Types#DECIMAL} type.
     */
    public static final DataType DECIMAL(int precision) {
        return DECIMAL.precision(precision);
    }

    /**
     * The {@link Types#DECIMAL} type.
     */
    public static final DataType DECIMAL(int precision, int scale) {
        return DECIMAL.precision(precision, scale);
    }

    // -------------------------------------------------------------------------
    // Datetime types
    // -------------------------------------------------------------------------

    /**
     * The {@link Types#DATE} type.
     */
    public static final DataType DATE = new DefaultDataType(Types.DATE, "date");

    /**
     * The {@link Types#TIMESTAMP} type.
     */
    public static final DataType TIMESTAMP = new DefaultDataType(Types.TIMESTAMP, "timestamp");

    /**
     * The {@link Types#TIMESTAMP} type.
     */
    public static final DataType TIMESTAMP(int precision) {
        return TIMESTAMP.precision(precision);
    }

    /**
     * The {@link Types#TIME} type.
     */
    public static final DataType TIME = new DefaultDataType(Types.TIME, "time");

    /**
     * The {@link Types#TIME} type.
     */
    public static final DataType TIME(int precision) {
        return TIME.precision(precision);
    }

    /**
     * The {@link Types#DATE} type.
     */
    public static final DataType LOCALDATE = new DefaultDataType(Types.DATE, "date");

    /**
     * The {@link Types#TIME} type.
     */
    public static final DataType LOCALTIME = new DefaultDataType(Types.TIME, "time");

    /**
     * The {@link Types#TIMESTAMP} type.
     */
    public static final DataType LOCALDATETIME = new DefaultDataType(Types.TIMESTAMP, "timestamp");

    /**
     * The {@link Types#TIME_WITH_TIMEZONE} type.
     */
    public static final DataType OFFSETTIME = new DefaultDataType(Types.TIME_WITH_TIMEZONE, "time with time zone");

    /**
     * The {@link Types#TIME_WITH_TIMEZONE} type.
     */
    public static final DataType OFFSETTIME(int precision) {
        return OFFSETTIME.precision(precision);
    }

    /**
     * The {@link Types#TIMESTAMP_WITH_TIMEZONE} type.
     */
    public static final DataType OFFSETDATETIME = new DefaultDataType(Types.TIMESTAMP_WITH_TIMEZONE, "timestamp with time zone");

    /**
     * The {@link Types#TIMESTAMP_WITH_TIMEZONE} type.
     */
    public static final DataType OFFSETDATETIME(int precision) {
        return OFFSETDATETIME.precision(precision);
    }

    /**
     * The {@link Types#TIME_WITH_TIMEZONE} type.
     * <p>
     * An alias for {@link #OFFSETTIME}
     */
    public static final DataType TIMEWITHTIMEZONE = OFFSETTIME;

    /**
     * The {@link Types#TIME_WITH_TIMEZONE} type.
     * <p>
     * An alias for {@link #OFFSETTIME}
     */
    public static final DataType TIMEWITHTIMEZONE(int precision) {
        return TIMEWITHTIMEZONE.precision(precision);
    }

    /**
     * The {@link Types#TIMESTAMP_WITH_TIMEZONE} type.
     * <p>
     * An alias for {@link #OFFSETDATETIME}
     */
    public static final DataType TIMESTAMPWITHTIMEZONE = OFFSETDATETIME;

    /**
     * The {@link Types#TIMESTAMP_WITH_TIMEZONE} type.
     * <p>
     * An alias for {@link #OFFSETDATETIME}
     */
    public static final DataType TIMESTAMPWITHTIMEZONE(int precision) {
        return TIMESTAMPWITHTIMEZONE.precision(precision);
    }


    // -------------------------------------------------------------------------
    // Binary types
    // -------------------------------------------------------------------------

    /**
     * The {@link Types#BINARY} type.
     */
    public static final DataType BINARY = new DefaultDataType(Types.BINARY, "binary");

    /**
     * The {@link Types#BINARY} type.
     */
    public static final DataType BINARY(int length) {
        return BINARY.length(length);
    }

    /**
     * The {@link Types#VARBINARY} type.
     */
    public static final DataType VARBINARY = new DefaultDataType(Types.VARBINARY, "varbinary");

    /**
     * The {@link Types#VARBINARY} type.
     */
    public static final DataType VARBINARY(int length) {
        return VARBINARY.length(length);
    }

    /**
     * The {@link Types#LONGVARBINARY} type.
     */
    public static final DataType LONGVARBINARY = new DefaultDataType(Types.LONGVARBINARY, "longvarbinary");

    /**
     * The {@link Types#LONGVARBINARY} type.
     */
    public static final DataType LONGVARBINARY(int length) {
        return LONGVARBINARY.length(length);
    }

    /**
     * The {@link Types#BLOB} type.
     */
    public static final DataType BLOB = new DefaultDataType(Types.BLOB, "blob");

    /**
     * The {@link Types#BLOB} type.
     */
    public static final DataType BLOB(int length) {
        return BLOB.length(length);
    }

    // -------------------------------------------------------------------------
    // Other types
    // -------------------------------------------------------------------------

    /**
     * The {@link Types#OTHER} type.
     */
    public static final DataType OTHER = new DefaultDataType(Types.OTHER, "other");

    //GaussDB

    public static final DataType INTERVALYEARTOMONTH = new DefaultDataType(301, "interval year to month");

    public static final DataType INTERVALYEARTOMONTH(int yearPrecision) {
        return INTERVALYEARTOMONTH.precision(yearPrecision);
    }

    public static final DataType INTERVALDAYTOSECOND = new DefaultDataType(302, "interval day to second");

    public static final DataType INTERVALDAYTOSECOND(int dayPrecision, int fractionalSecondsPrecision) {
        return INTERVALDAYTOSECOND.precision(dayPrecision, fractionalSecondsPrecision);
    }

    public static final DataType IMAGE = new DefaultDataType(303, "image");
    
    /**
     * No instances
     */
    private SQLDataType() {}

}
