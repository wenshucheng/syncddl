package com.huacloud.synctable.mapping.datatype;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 9/18/2019 11:02 AM
 */
public class OracleDataType {

    public static final DataType BFILE = new DefaultDataType("bfile");

    public static final DataType BINARY_DOUBLE = new DefaultDataType("binary_double");

    public static final DataType BINARY_FLOAT = new DefaultDataType("binary_float");

    public static final DataType BLOB = new DefaultDataType("blob");

    public static final DataType CHAR = new DefaultDataType("char");

    public static final DataType CLOB = new DefaultDataType("clob");

    public static final DataType DATE = new DefaultDataType("date");

    public static final DataType FLOAT = new DefaultDataType("float");

    public static final DataType INTERVAL_DAY_TO_SECOND(
            int dayPrecision, int fractionalSecondsPrecision) {
        return INTERVAL_DAY_TO_SECOND.precision(dayPrecision, fractionalSecondsPrecision);
    }

    public static final DataType INTERVAL_DAY_TO_SECOND =
            new DefaultDataType("interval day to second");

    public static final DataType INTERVAL_YEAR_TO_MONTH(int yearPrecision) {
        return INTERVAL_YEAR_TO_MONTH.precision(yearPrecision);
    }

    public static final DataType INTERVAL_YEAR_TO_MONTH =
            new DefaultDataType("interval year to month");

    public static final DataType LONG = new DefaultDataType("long");

    public static final DataType LONG_RAW = new DefaultDataType("long raw");

    public static final DataType NCHAR = new DefaultDataType("nchar");

    public static final DataType NCLOB = new DefaultDataType("nclob");

    public static final DataType NUMBER = new DefaultDataType("number");

    public static final DataType NVARCHAR2 = new DefaultDataType("nvarchar2");

    public static final DataType RAW = new DefaultDataType("raw");

    public static final DataType ROWID = new DefaultDataType("rowid");

    public static final DataType TIMESTAMP = new DefaultDataType("timestamp");

    public static final DataType TIMESTAMP(int precision) {
        return TIMESTAMP.precision(precision);
    }

    public static final DataType TIMESTAMP_WITH_LOCAL_TIME_ZONE =
            new DefaultDataType("timestamp with local time zone");

    public static final DataType TIMESTAMP_WITH_LOCAL_TIME_ZONE(int precision) {
        return TIMESTAMP_WITH_LOCAL_TIME_ZONE.precision(precision);
    }

    public static final DataType TIMESTAMP_WITH_TIMEZONE =
            new DefaultDataType("timestamp with time zone");

    public static final DataType TIMESTAMP_WITH_TIMEZONE(int precision) {
        return TIMESTAMP_WITH_TIMEZONE.precision(precision);
    }

    public static final DataType UROWID = new DefaultDataType("urowid");

    public static final DataType VARCHAR2 = new DefaultDataType("varchar2");

}
