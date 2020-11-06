package com.huacloud.synctable;

import com.huacloud.synctable.dialect.*;
import com.huacloud.synctable.mapping.Column;
import com.huacloud.synctable.mapping.PrimaryKey;
import com.huacloud.synctable.mapping.Table;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class SQL2SQLTest {
    
    private static final Logger logger = LoggerFactory.getLogger(SQL2SQLTest.class);

    @Test
    public void testTbaseSQL2Other() {
        String tbaseSql =
            "create table tbase2oracle (" +
                "c1 smallint," +
                "c2 integer," +
                "c3 bigint," +
                "c4 decimal(10,2)," +
                "c5 numeric(10,3)," +
                "c6 real," +
                "c7 double precision," +
                "c811 smallserial," +
                "c8 serial," +
                "c9 bigserial," +
                "c10 char(2)," +
                "c11 character(20)," +
                "c12 varchar(50)," +
                "c13 character varying(20)," +
                "c14 text," +
                "c15 timestamp," +
                "c16 timestamp without time zone," +
                "c17 timestamp with   time   zone," +
                "c18 date," +
                "c19 time," +
                "c20 time without time zone," +
                "c21 time with time zone," +
                "c22 bytea," +
                "c23 boolean" +
            ")";

        ParserImpl parser = new ParserImpl();
        Table table = parser.parseTable(tbaseSql, new TBaseDialect());
        Dialect dialect = new OracleDialect();
        logger.info("oracle:" + table.sqlCreateString(dialect));

        Column c1 = table.getColumn("C1");
        Column c2 = table.getColumn("C2");
        Column c3 = table.getColumn("C3");
        Column c4 = table.getColumn("C4");
        Column c5 = table.getColumn("C5");
        Column c6 = table.getColumn("C6");
        Column c7 = table.getColumn("C7");
        Column c811 = table.getColumn("C811");
        Column c8 = table.getColumn("C8");
        Column c9 = table.getColumn("C9");
        Column c10 = table.getColumn("C10");
        Column c11 = table.getColumn("C11");
        Column c12 = table.getColumn("C12");
        Column c13 = table.getColumn("C13");
        Column c14 = table.getColumn("C14");
        Column c15 = table.getColumn("C15");
        Column c16 = table.getColumn("C16");
        Column c17 = table.getColumn("C17");
        Column c18 = table.getColumn("C18");
        Column c19 = table.getColumn("C19");
        Column c20 = table.getColumn("C20");
        Column c21 = table.getColumn("C21");
        Column c22 = table.getColumn("C22");
        Column c23 = table.getColumn("C23");

        assertEquals("number(5,0)", c1.getSqlType(dialect));
        assertEquals("number(10,0)", c2.getSqlType(dialect));
        assertEquals("number(19,0)", c3.getSqlType(dialect));
        assertEquals("number(10,2)", c4.getSqlType(dialect));
        assertEquals("number(10,3)", c5.getSqlType(dialect));
        assertEquals("float", c6.getSqlType(dialect));
        assertEquals("double precision", c7.getSqlType(dialect));
        assertEquals("number(10,0)", c811.getSqlType(dialect));
        assertEquals("number(10,0)", c8.getSqlType(dialect));
        assertEquals("number(19,0)", c9.getSqlType(dialect));
        assertEquals("char(2)", c10.getSqlType(dialect));
        assertEquals("char(20)", c11.getSqlType(dialect));
        assertEquals("varchar2(50)", c12.getSqlType(dialect));
        assertEquals("varchar2(20)", c13.getSqlType(dialect));
        assertEquals("clob", c14.getSqlType(dialect));
        assertEquals("timestamp", c15.getSqlType(dialect));
        assertEquals("timestamp", c16.getSqlType(dialect));
        assertEquals("timestamp with timezone", c17.getSqlType(dialect));
        assertEquals("date", c18.getSqlType(dialect));
        assertEquals("date", c19.getSqlType(dialect));
        assertEquals("date", c20.getSqlType(dialect));
        assertEquals("date", c21.getSqlType(dialect));
        assertEquals("blob", c22.getSqlType(dialect));
        assertEquals("number(1,0)", c23.getSqlType(dialect));

        dialect = new MySQLDialect();
        logger.info("mysql:" + table.sqlCreateString(dialect));
        assertEquals("smallint", c1.getSqlType(dialect));
        assertEquals("integer", c2.getSqlType(dialect));
        assertEquals("bigint", c3.getSqlType(dialect));
        assertEquals("decimal(10,2)", c4.getSqlType(dialect));
        assertEquals("decimal(10,3)", c5.getSqlType(dialect));
        assertEquals("float", c6.getSqlType(dialect));
        assertEquals("double", c7.getSqlType(dialect));
        assertEquals("integer", c811.getSqlType(dialect));
        assertEquals("integer", c8.getSqlType(dialect));
        assertEquals("bigint", c9.getSqlType(dialect));
        assertEquals("char(2)", c10.getSqlType(dialect));
        assertEquals("char(20)", c11.getSqlType(dialect));
        assertEquals("varchar(50)", c12.getSqlType(dialect));
        assertEquals("varchar(20)", c13.getSqlType(dialect));
        assertEquals("longtext", c14.getSqlType(dialect));
        assertEquals("timestamp", c15.getSqlType(dialect));
        assertEquals("timestamp", c16.getSqlType(dialect));
        assertEquals("timestamp", c17.getSqlType(dialect));
        assertEquals("date", c18.getSqlType(dialect));
        assertEquals("time", c19.getSqlType(dialect));
        assertEquals("time", c20.getSqlType(dialect));
        assertEquals("time", c21.getSqlType(dialect));
        assertEquals("longblob", c22.getSqlType(dialect));
        assertEquals("bit", c23.getSqlType(dialect));

        dialect = new HiveDialect();
        logger.info("Hive:" + table.sqlCreateString(dialect));

        assertEquals("SMALLINT", c1.getSqlType(dialect));
        assertEquals("INT", c2.getSqlType(dialect));
        assertEquals("BIGINT", c3.getSqlType(dialect));
        assertEquals("DECIMAL(10,2)", c4.getSqlType(dialect));
        assertEquals("DECIMAL(10,3)", c5.getSqlType(dialect));
        assertEquals("FLOAT", c6.getSqlType(dialect));
        assertEquals("DOUBLE", c7.getSqlType(dialect));
        assertEquals("INT", c811.getSqlType(dialect));
        assertEquals("INT", c8.getSqlType(dialect));
        assertEquals("BIGINT", c9.getSqlType(dialect));
        assertEquals("CHAR(2)", c10.getSqlType(dialect));
        assertEquals("CHAR(20)", c11.getSqlType(dialect));
        assertEquals("VARCHAR(50)", c12.getSqlType(dialect));
        assertEquals("VARCHAR(20)", c13.getSqlType(dialect));
        assertEquals("BINARY", c14.getSqlType(dialect));
        assertEquals("TIMESTAMP", c15.getSqlType(dialect));
        assertEquals("TIMESTAMP", c16.getSqlType(dialect));
        assertEquals("TIMESTAMP", c17.getSqlType(dialect));
        assertEquals("DATE", c18.getSqlType(dialect));
        assertEquals("TIMESTAMP", c19.getSqlType(dialect));
        assertEquals("TIMESTAMP", c20.getSqlType(dialect));
        assertEquals("TIMESTAMP", c21.getSqlType(dialect));
        assertEquals("BINARY", c22.getSqlType(dialect));
        assertEquals("BOOLEAN", c23.getSqlType(dialect));

        dialect = new SQLServerDialect();
        logger.info("SQLServer:\n" + table.sqlCreateString(dialect));

        assertEquals("smallint", c1.getSqlType(dialect));
        assertEquals("int", c2.getSqlType(dialect));
        assertEquals("bigint", c3.getSqlType(dialect));
        assertEquals("decimal(10,2)", c4.getSqlType(dialect));
        assertEquals("numeric(10,3)", c5.getSqlType(dialect));
        assertEquals("real", c6.getSqlType(dialect));
        assertEquals("float", c7.getSqlType(dialect));
        assertEquals("int", c811.getSqlType(dialect));
        assertEquals("int", c8.getSqlType(dialect));
        assertEquals("bigint", c9.getSqlType(dialect));
        assertEquals("char(2)", c10.getSqlType(dialect));
        assertEquals("char(20)", c11.getSqlType(dialect));
        assertEquals("varchar(50)", c12.getSqlType(dialect));
        assertEquals("varchar(20)", c13.getSqlType(dialect));
        assertEquals("text", c14.getSqlType(dialect));
        assertEquals("datetime", c15.getSqlType(dialect));
        assertEquals("datetime", c16.getSqlType(dialect));
        assertEquals("datetime", c17.getSqlType(dialect));
        assertEquals("date", c18.getSqlType(dialect));
        assertEquals("time", c19.getSqlType(dialect));
        assertEquals("time", c20.getSqlType(dialect));
        assertEquals("time", c21.getSqlType(dialect));
        assertEquals("image", c22.getSqlType(dialect));
        assertEquals("boolean", c23.getSqlType(dialect));
    }

    @Test
    public void testHiveSQL2Other() {
        String hiveSql =
            "CREATE TABLE hiveTable(" +
            "   c1 TINYINT," +
            "   c2 SMALLINT," +
            "   c3 INT," +
            "   c4 BIGINT," +
            "   c5 BOOLEAN," +
            "   c6 FLOAT," +
            "   c7 DOUBLE," +
            "   c8 DOUBLE PRECISION," +
            "   c9 STRING," +
            "   c10 BINARY," +
            "   c11 TIMESTAMP," +
            "   c12 DECIMAL," +
            "   c13 DECIMAL(10,2)," +
            "   c14 DATE," +
            "   c15 VARCHAR," +
            "   c16 CHAR," +
            "   c17 STRING COMMENT 'c17 comment'," +
            "   PRIMARY KEY(c1,c2)" +
            ")" +
            " COMMENT 'tab comment'";

        ParserImpl parser = new ParserImpl();
        Table table = parser.parseTable(hiveSql, new HiveDialect());
        PrimaryKey primaryKey = table.getPrimaryKey();

        assertEquals(17, table.getColumnSize());
        assertEquals(2, primaryKey.getColumns().size());
        assertEquals("tab comment", table.getComment());

        Dialect dialect = new TBaseDialect();
        String sql = table.getFullCreateTableSQL(dialect, "");
        logger.info("TBase:\n" + sql);

        Column c1 = table.getColumn("C1");
        Column c2 = table.getColumn("C2");
        Column c3 = table.getColumn("C3");
        Column c4 = table.getColumn("C4");
        Column c5 = table.getColumn("C5");
        Column c6 = table.getColumn("C6");
        Column c7 = table.getColumn("C7");
        Column c8 = table.getColumn("C8");
        Column c9 = table.getColumn("C9");
        Column c10 = table.getColumn("C10");
        Column c11 = table.getColumn("C11");
        Column c12 = table.getColumn("C12");
        Column c13 = table.getColumn("C13");
        Column c14 = table.getColumn("C14");
        Column c15 = table.getColumn("C15");
        Column c16 = table.getColumn("C16");
        Column c17 = table.getColumn("C17");

        assertEquals("smallint", c1.getSqlType(dialect));
        assertEquals("smallint", c2.getSqlType(dialect));
        assertEquals("integer", c3.getSqlType(dialect));
        assertEquals("bigint", c4.getSqlType(dialect));
        assertEquals("boolean", c5.getSqlType(dialect));
        assertEquals("double precision", c6.getSqlType(dialect));
        assertEquals("double precision", c7.getSqlType(dialect));
        assertEquals("double precision", c8.getSqlType(dialect));
        assertEquals("varchar(255)", c9.getSqlType(dialect));
        assertEquals("bytea", c10.getSqlType(dialect));
        assertEquals("timestamp without time zone", c11.getSqlType(dialect));
        assertEquals("decimal(19,2)", c12.getSqlType(dialect));
        assertEquals("decimal(10,2)", c13.getSqlType(dialect));
        assertEquals("date", c14.getSqlType(dialect));
        assertEquals("varchar(255)", c15.getSqlType(dialect));
        assertEquals("char(255)", c16.getSqlType(dialect));
        assertEquals("varchar(255)", c17.getSqlType(dialect));

        dialect = new MySQLDialect();
        logger.info("MySQL:\n" + table.getFullCreateTableSQL(dialect, ""));

        assertEquals("tinyint", c1.getSqlType(dialect));
        assertEquals("smallint", c2.getSqlType(dialect));
        assertEquals("integer", c3.getSqlType(dialect));
        assertEquals("bigint", c4.getSqlType(dialect));
        assertEquals("bit", c5.getSqlType(dialect));
        assertEquals("float", c6.getSqlType(dialect));
        assertEquals("double", c7.getSqlType(dialect));
        assertEquals("double", c8.getSqlType(dialect));
        assertEquals("varchar(255)", c9.getSqlType(dialect));
        assertEquals("binary(255)", c10.getSqlType(dialect));
        assertEquals("timestamp", c11.getSqlType(dialect));
        assertEquals("decimal(19,2)", c12.getSqlType(dialect));
        assertEquals("decimal(10,2)", c13.getSqlType(dialect));
        assertEquals("date", c14.getSqlType(dialect));
        assertEquals("varchar(255)", c15.getSqlType(dialect));
        assertEquals("char(255)", c16.getSqlType(dialect));
        assertEquals("varchar(255)", c17.getSqlType(dialect));

        dialect = new OracleDialect();
        logger.info("Oracle:\n" + table.getFullCreateTableSQL(dialect, ""));

        assertEquals("number(3,0)", c1.getSqlType(dialect));
        assertEquals("number(5,0)", c2.getSqlType(dialect));
        assertEquals("number(10,0)", c3.getSqlType(dialect));
        assertEquals("number(19,0)", c4.getSqlType(dialect));
        assertEquals("number(1,0)", c5.getSqlType(dialect));
        assertEquals("float", c6.getSqlType(dialect));
        assertEquals("double precision", c7.getSqlType(dialect));
        assertEquals("double precision", c8.getSqlType(dialect));
        assertEquals("varchar2(255)", c9.getSqlType(dialect));
        assertEquals("raw(255)", c10.getSqlType(dialect));
        assertEquals("timestamp", c11.getSqlType(dialect));
        assertEquals("number(19,2)", c12.getSqlType(dialect));
        assertEquals("number(10,2)", c13.getSqlType(dialect));
        assertEquals("date", c14.getSqlType(dialect));
        assertEquals("varchar2(255)", c15.getSqlType(dialect));
        assertEquals("char(255)", c16.getSqlType(dialect));
        assertEquals("varchar2(255)", c17.getSqlType(dialect));

        dialect = new SQLServerDialect();
        logger.info("SQLServer:\n" + table.getFullCreateTableSQL(dialect, ""));

        assertEquals("tinyint", c1.getSqlType(dialect));
        assertEquals("smallint", c2.getSqlType(dialect));
        assertEquals("int", c3.getSqlType(dialect));
        assertEquals("bigint", c4.getSqlType(dialect));
        assertEquals("boolean", c5.getSqlType(dialect));
        assertEquals("float", c6.getSqlType(dialect));
        assertEquals("float", c7.getSqlType(dialect));
        assertEquals("float", c8.getSqlType(dialect));
        assertEquals("varchar(255)", c9.getSqlType(dialect));
        assertEquals("binary", c10.getSqlType(dialect));
        assertEquals("datetime", c11.getSqlType(dialect));
        assertEquals("decimal(19,2)", c12.getSqlType(dialect));
        assertEquals("decimal(10,2)", c13.getSqlType(dialect));
        assertEquals("date", c14.getSqlType(dialect));
        assertEquals("varchar(255)", c15.getSqlType(dialect));
        assertEquals("char(255)", c16.getSqlType(dialect));
        assertEquals("varchar(255)", c17.getSqlType(dialect));
    }

    @Test
    public void testGaussDB2Other() {
        String oracleSql =
            "CREATE TABLE pxd.pxd_test_table (" +
                " C1 BINARY_INTEGER," +
                " C2 INTEGER," +
                " C3 BINARY_BIGINT," +
                " C4 BIGINT," +
                " C5 BINARY_DOUBLE," +
                " C6 DOUBLE," +
                " C7 FLOAT," +
                " C8 REAL," +
                " C9 NUMBER(10, 2)," +
                " C10 DECIMAL(15, 5)," +
                " C11 CHAR(3)," +
                " C12 NCHAR(3)," +
                " C13 CLOB," +
                " C14 VARCHAR(200)," +
                " C15 NVARCHAR(100)," +
                " C16 BINARY(50)," +
                " C17 VARBINARY(10)," +
                " C18 IMAGE," +
                " C19 BLOB," +
                " C20 DATETIME," +
                " C22 TIMESTAMP(6) WITH TIME ZONE," +
                " C23 TIMESTAMP(6) WITH LOCAL TIME ZONE," +
                " C21 TIMESTAMP," +
                " C24 BOOLEAN," +
                " C25 INTERVAL YEAR(4) TO MONTH," +
                " C26 INTERVAL DAY(5) TO SECOND (5)" +
            ")";
        Dialect gaussDbDialect = new GaussDbDialect();
        ParserImpl parser = new ParserImpl();
        Table table = parser.parseTable(oracleSql, gaussDbDialect);

        Dialect dialect = new MySQLDialect();
        logger.info("mysql:\n" + table.sqlCreateString(dialect));
    }
}
