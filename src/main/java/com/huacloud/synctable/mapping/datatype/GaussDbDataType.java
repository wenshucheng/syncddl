package com.huacloud.synctable.mapping.datatype;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 9/18/2019 3:22 PM
 */
public class GaussDbDataType {

    public static final DataType INT = new DefaultDataType("int");

    public static final DataType INTEGER = new DefaultDataType("integer");

    public static final DataType BINARY_INTEGER = new DefaultDataType("binary_integer");

    public static final DataType BIGINT = new DefaultDataType("bigint");

    public static final DataType BINARY_BIGINT = new DefaultDataType("binary_bigint");

    public static final DataType REAL = new DefaultDataType("real");

    public static final DataType DOUBLE = new DefaultDataType("double");

    public static final DataType FLOAT = new DefaultDataType("float");

    public static final DataType BINARY_DOUBLE = new DefaultDataType("binary_double");

    public static final DataType NUMBER = new DefaultDataType("number");

    public static final DataType NUMERIC = new DefaultDataType("numeric");

    public static final DataType DECIMAL = new DefaultDataType("decimal");

    public static final DataType CHAR = new DefaultDataType("char");

    public static final DataType NCHAR = new DefaultDataType("nchar");

    public static final DataType VARCHAR = new DefaultDataType("varchar");

    public static final DataType VARCHAR2 = new DefaultDataType("varchar2");

    public static final DataType NVARCHAR = new DefaultDataType("nvarchar");

    public static final DataType NVARCHAR2 = new DefaultDataType("nvarchar2");

    public static final DataType DATE = new DefaultDataType("date");

    public static final DataType DATETIME = new DefaultDataType("datetime");

    public static final DataType TIMESTAMP = new DefaultDataType("timestamp");

    public static final DataType TIMESTAMP_WITH_TIME_ZONE =
            new DefaultDataType("timestamp with time zone");

    public static final DataType TIMESTAMP_WITH_LOCAL_TIME_ZONE =
            new DefaultDataType("timestamp with local time zone");

    public static final DataType BOOLEAN = new DefaultDataType("boolean");

    public static final DataType INTERVAL_YEAR_TO_MONTH =
            new DefaultDataType("interval year to month");

    public static final DataType INTERVAL_DAY_TO_SECOND =
            new DefaultDataType("interval day to second");
}
