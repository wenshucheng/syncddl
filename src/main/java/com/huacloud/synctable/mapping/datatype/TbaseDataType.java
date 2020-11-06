package com.huacloud.synctable.mapping.datatype;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 9/25/2019 4:33 PM
 */
public class TbaseDataType {

    public static final DataType BIGINT = new DefaultDataType("bigint");

    public static final DataType BIGSERIAL = new DefaultDataType("bigserial");

    public static final DataType BIT = new DefaultDataType("bit");

    public static final DataType BIT_VARYING = new DefaultDataType("bit varying");

    public static final DataType BOOLEAN = new DefaultDataType("boolean");

    public static final DataType BOX = new DefaultDataType("box");

    public static final DataType BYTEA = new DefaultDataType("bytea");

    public static final DataType CHAR = new DefaultDataType("char");

    public static final DataType CHARACTER = new DefaultDataType("character");

    public static final DataType CHARACTER_VARYING = new DefaultDataType("character varying");

    public static final DataType CIDR = new DefaultDataType("cidr");

    public static final DataType CIRCLE = new DefaultDataType("circle");

    public static final DataType DATE = new DefaultDataType("date");

    public static final DataType INT = new DefaultDataType("int");

    public static final DataType DECIMAL = new DefaultDataType("decimal");

    public static final DataType DOUBLE_PRECISION = new DefaultDataType("double precision");

    public static final DataType FLOAT4 = new DefaultDataType("float4");

    public static final DataType FLOAT8 = new DefaultDataType("float8");

    public static final DataType INET = new DefaultDataType("inet");

    public static final DataType INT2 = new DefaultDataType("int2");

    public static final DataType INT4 = new DefaultDataType("int4");

    public static final DataType INT8 = new DefaultDataType("int8");

    public static final DataType INTEGER = new DefaultDataType("integer");

    public static final DataType INTERVAL = new DefaultDataType("interval");

    public static final DataType JSON = new DefaultDataType("json");

    public static final DataType JSONB = new DefaultDataType("jsonb");

    public static final DataType LINE = new DefaultDataType("line");

    public static final DataType LSEG = new DefaultDataType("lseg");

    public static final DataType MACADDR = new DefaultDataType("macaddr");

    public static final DataType MONEY = new DefaultDataType("money");

    public static final DataType NUMERIC = new DefaultDataType("numeric");

    public static final DataType PATH = new DefaultDataType("path");

    public static final DataType PG_LSN = new DefaultDataType("pg_lsn");

    public static final DataType POINT = new DefaultDataType("point");

    public static final DataType POLYGON = new DefaultDataType("polygon");

    public static final DataType REAL = new DefaultDataType("real");

    public static final DataType SERIAL = new DefaultDataType("serial");

    public static final DataType SERIAL2 = new DefaultDataType("serial2");

    public static final DataType SERIAL4 = new DefaultDataType("serial4");

    public static final DataType SERIAL8 = new DefaultDataType("serial8");

    public static final DataType SMALLINT = new DefaultDataType("smallint");

    public static final DataType SMALLSERIAL = new DefaultDataType("smallserial");

    public static final DataType TEXT = new DefaultDataType("text");

    public static final DataType TIME_WITHOUT_TIME_ZONE =
            new DefaultDataType("time without time zone");

    public static final DataType TIME_WITH_TIME_ZONE =
            new DefaultDataType("time with time zone");

    public static final DataType TIMESTAMP_WITHOUT_TIME_ZONE =
            new DefaultDataType("timestamp without time zone");

    public static final DataType TIMESTAMP_WITH_TIME_ZONE =
            new DefaultDataType("timestamp with time zone");

    public static final DataType TIMESTAMPTZ = new DefaultDataType("timestamptz");

    public static final DataType TIMETZ = new DefaultDataType("timetz");

    public static final DataType TSQUERY = new DefaultDataType("tsquery");

    public static final DataType TSVECTOR = new DefaultDataType("tsvector");

    public static final DataType TXID_SNAPSHOT = new DefaultDataType("txid_snapshot");

    public static final DataType UUID = new DefaultDataType("uuid");

    public static final DataType VARBIT = new DefaultDataType("varbit");

    public static final DataType VARCHAR = new DefaultDataType("varchar");

    public static final DataType XML = new DefaultDataType("xml");

}
