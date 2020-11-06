package com.huacloud.synctable.mapping;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 9/19/2019 5:29 PM
 */
public class Types {

    public final static int BIT = -7;

    public final static int TINYINT = -6;

    public final static int SMALLINT = 5;

    public final static int INTEGER = 4;

    public final static int BIGINT = -5;

    public final static int FLOAT = 6;

    public final static int REAL = 7;

    public final static int DOUBLE = 8;

    public final static int NUMERIC = 2;

    public final static int DECIMAL = 3;

    public final static int CHAR = 1;

    public final static int VARCHAR = 12;

    public final static int LONGVARCHAR = -1;

    public final static int DATE = 91;

    public final static int TIME = 92;

    public final static int TIMESTAMP = 93;

    public final static int BINARY = -2;

    public final static int VARBINARY = -3;

    public final static int LONGVARBINARY = -4;

    public final static int NULL = 0;

    public final static int OTHER = 1111;

    public final static int JAVA_OBJECT = 2000;

    public final static int DISTINCT = 2001;

    public final static int STRUCT = 2002;

    public final static int ARRAY = 2003;

    public final static int BLOB = 2004;

    public final static int CLOB = 2005;

    public final static int REF = 2006;

    public final static int DATALINK = 70;

    public final static int BOOLEAN = 16;

    public final static int ROWID = -8;

    public static final int NCHAR = -15;

    public static final int NVARCHAR = -9;

    public static final int LONGNVARCHAR = -16;

    public static final int NCLOB = 2011;

    public static final int SQLXML = 2009;

    public static final int REF_CURSOR = 2012;

    public static final int TIME_WITH_TIMEZONE = 2013;

    public static final int TIMESTAMP_WITH_TIMEZONE = 2014;

    public static final int TIMESTAMP_WITH_LOCAL_TIME_ZONE = 2201;

    public static final int INTERVAL_DAY_TO_SECOND = 1113;

    public static final int INTERVAL_YEAR_TO_MONTH = 1112;

    //PostgreSQL 10开头

    public final static int BIT_VARYING = 101;
    public final static int INTERVAL_PG = 102;
    public final static int JSON_PG = 103;
    public final static int MONEY_PG = 104;
    public final static int XML_PG = 105;
    public final static int UUID_PG = 106;
    public final static int BIGSERIAL_PG = 107;
    public final static int SMALLSERIAL_PG = 108;
    public final static int SERIAL_PG = 109;

    private Types() {
    }
}
