package com.huacloud.synctable.db2sql;

import com.huacloud.synctable.dao.AbstractDbMetaInfoDao;
import com.huacloud.synctable.dialect.Dialect;
import com.huacloud.synctable.entity.DBType;
import com.huacloud.synctable.mapping.Column;
import com.huacloud.synctable.mapping.Table;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.Assert.assertEquals;

/**
 * 通过数据库转SQL语句的测试
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/14/2019 3:53 PM
 */
public class PostgreDb2SqlTest {

    public static final String ALL_DATATYPE_TEST = "pg_table_ddl_test";

    public static final String DATABASE_NAME = "database";

    private static JdbcTemplate jdbcTemplate;

    private static final String SCHEMA_NAME = "public";

    @BeforeClass
    public static void beforeClass() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(DBType.PostgreSql.getDriverName());
        dataSource.setUrl("jdbc:postgresql://172.16.18.18:5432/database");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        jdbcTemplate = new JdbcTemplate(dataSource);

        String sqlServerSql =
                "create table public.pg_table_ddl_test ( " +
                        "    c1 bigint,  " +
                        "    c2 bigserial,  " +
                        "    c3 bit(3),  " +
                        "    c4 bit varying(10),  " +
                        "    c5 boolean,  " +
                        "    c6 bytea,  " +
                        "    c7 character(10),  " +
                        "    c8 character varying(50),  " +
                        "    c9 date,  " +
                        "    c10 double precision,  " +
                        "    c11 integer,  " +
                        "    c12 interval,  " +
                        "    c13 json,  " +
                        "    c14 money,  " +
                        "    c15 numeric(10,2),  " +
                        "    c16 decimal(10,3),  " +
                        "    c17 real,  " +
                        "    c18 smallint,  " +
                        "    c19 float4,  " +
                        "    c20 int2,  " +
                        "    c21 int8,  " +
                        "    c22 serial8,  " +
                        "    c23 varbit(10),  " +
                        "    c24 bool,  " +
                        "    c25 char(5),  " +
                        "    c26 varchar(50),  " +
                        "    c27 float8,  " +
                        "    c28 int,  " +
                        "    c29 int4,  " +
                        "    c30 smallserial,  " +
                        "    c31 serial2,  " +
                        "    c32 serial,  " +
                        "    c33 serial4,  " +
                        "    c34 text,  " +
                        "    c35 time,  " +
                        "    c36 time(3),  " +
                        "    c37 time without time zone,  " +
                        "    c38 time(3) with time zone,  " +
                        "    c39 timetz(3),  " +
                        "    c40 timestamp,  " +
                        "    c41 timestamp(3),  " +
                        "    c42 timestamp(3) without time zone,  " +
                        "    c43 timestamp(2) with time zone,  " +
                        "    c44 timestamptz(2),  " +
                        "    c45 xml,  " +
                        "    c46 uuid  " +
                        ")";
        jdbcTemplate.execute(sqlServerSql);
    }

    @AfterClass
    public static void afterClass() {
        jdbcTemplate.execute("DROP TABLE public.pg_table_ddl_test");
    }

    private Table getTable() {
        AbstractDbMetaInfoDao dao = DBType.PostgreSql.getDbDao(jdbcTemplate);
        Dialect dialect = DBType.PostgreSql.getDialect();
        return dao.queryTable(DATABASE_NAME, SCHEMA_NAME, ALL_DATATYPE_TEST, dialect);
    }

    @Test
    public void testColumn2GaussDb() {
        Table table = getTable();
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
        Column c18 = table.getColumn("C18");
        Column c19 = table.getColumn("C19");
        Column c20 = table.getColumn("C20");
        Column c21 = table.getColumn("C21");
        Column c22 = table.getColumn("C22");
        Column c23 = table.getColumn("C23");
        Column c24 = table.getColumn("C24");
        Column c25 = table.getColumn("C25");
        Column c26 = table.getColumn("C26");
        Column c27 = table.getColumn("C27");
        Column c28 = table.getColumn("C28");
        Column c29 = table.getColumn("C29");
        Column c30 = table.getColumn("C30");
        Column c31 = table.getColumn("C31");
        Column c32 = table.getColumn("C32");
        Column c33 = table.getColumn("C33");
        Column c34 = table.getColumn("C34");
        Column c35 = table.getColumn("C35");
        Column c36 = table.getColumn("C36");
        Column c37 = table.getColumn("C37");
        Column c38 = table.getColumn("C38");
        Column c39 = table.getColumn("C39");
        Column c40 = table.getColumn("C40");
        Column c41 = table.getColumn("C41");
        Column c42 = table.getColumn("C42");
        Column c43 = table.getColumn("C43");
        Column c44 = table.getColumn("C44");
        Column c45 = table.getColumn("C45");
        Column c46 = table.getColumn("C46");

        Dialect destDialect = DBType.GaussDB.getDialect();
        assertEquals("bigint", c1.getSqlType(destDialect));
        assertEquals("bigint", c2.getSqlType(destDialect));
        assertEquals("varchar", c3.getSqlType(destDialect));
        assertEquals("varbinary(10)", c4.getSqlType(destDialect));
        assertEquals("boolean", c5.getSqlType(destDialect));
        assertEquals("binary", c6.getSqlType(destDialect));
        assertEquals("char(10)", c7.getSqlType(destDialect));
        assertEquals("varchar(50)", c8.getSqlType(destDialect));
        assertEquals("date", c9.getSqlType(destDialect));
        assertEquals("double", c10.getSqlType(destDialect));
        assertEquals("int", c11.getSqlType(destDialect));
        assertEquals("varchar(50)", c12.getSqlType(destDialect));
        assertEquals("clob", c13.getSqlType(destDialect));
        assertEquals("number(20,2)", c14.getSqlType(destDialect));
        assertEquals("number(10,2)", c15.getSqlType(destDialect));
        assertEquals("number(10,3)", c16.getSqlType(destDialect));
        assertEquals("real", c17.getSqlType(destDialect));
        assertEquals("smallint", c18.getSqlType(destDialect));
        assertEquals("real", c19.getSqlType(destDialect));
        assertEquals("smallint", c20.getSqlType(destDialect));
        assertEquals("bigint", c21.getSqlType(destDialect));
        assertEquals("bigint", c22.getSqlType(destDialect));
        assertEquals("varbinary(10)", c23.getSqlType(destDialect));
        assertEquals("boolean", c24.getSqlType(destDialect));
        assertEquals("char(5)", c25.getSqlType(destDialect));
        assertEquals("varchar(50)", c26.getSqlType(destDialect));
        assertEquals("double", c27.getSqlType(destDialect));
        assertEquals("int", c28.getSqlType(destDialect));
        assertEquals("int", c29.getSqlType(destDialect));
        assertEquals("smallint", c30.getSqlType(destDialect));
        assertEquals("smallint", c31.getSqlType(destDialect));
        assertEquals("int", c32.getSqlType(destDialect));
        assertEquals("int", c33.getSqlType(destDialect));
        assertEquals("clob", c34.getSqlType(destDialect));
        assertEquals("date", c35.getSqlType(destDialect));
        assertEquals("date", c36.getSqlType(destDialect));
        assertEquals("date", c37.getSqlType(destDialect));
        assertEquals("timestamp with time zone", c38.getSqlType(destDialect));
        assertEquals("timestamp with time zone", c39.getSqlType(destDialect));
        assertEquals("timestamp", c40.getSqlType(destDialect));
        assertEquals("timestamp", c41.getSqlType(destDialect));
        assertEquals("timestamp", c42.getSqlType(destDialect));
        assertEquals("timestamp with time zone", c43.getSqlType(destDialect));
        assertEquals("timestamp with time zone", c44.getSqlType(destDialect));
        assertEquals("clob", c45.getSqlType(destDialect));
        assertEquals("varchar(50)", c46.getSqlType(destDialect));
    }

    @Test
    public void testColumn2Tbase() {
        Table table = getTable();
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
        Column c18 = table.getColumn("C18");
        Column c19 = table.getColumn("C19");
        Column c20 = table.getColumn("C20");
        Column c21 = table.getColumn("C21");
        Column c22 = table.getColumn("C22");
        Column c23 = table.getColumn("C23");
        Column c24 = table.getColumn("C24");
        Column c25 = table.getColumn("C25");
        Column c26 = table.getColumn("C26");
        Column c27 = table.getColumn("C27");
        Column c28 = table.getColumn("C28");
        Column c29 = table.getColumn("C29");
        Column c30 = table.getColumn("C30");
        Column c31 = table.getColumn("C31");
        Column c32 = table.getColumn("C32");
        Column c33 = table.getColumn("C33");
        Column c34 = table.getColumn("C34");
        Column c35 = table.getColumn("C35");
        Column c36 = table.getColumn("C36");
        Column c37 = table.getColumn("C37");
        Column c38 = table.getColumn("C38");
        Column c39 = table.getColumn("C39");
        Column c40 = table.getColumn("C40");
        Column c41 = table.getColumn("C41");
        Column c42 = table.getColumn("C42");
        Column c43 = table.getColumn("C43");
        Column c44 = table.getColumn("C44");
        Column c45 = table.getColumn("C45");
        Column c46 = table.getColumn("C46");

        Dialect destDialect = DBType.TBase.getDialect();
        assertEquals("bigint", c1.getSqlType(destDialect));
        assertEquals("bigint", c2.getSqlType(destDialect));
        assertEquals("bit(3)", c3.getSqlType(destDialect));
        assertEquals("bit varying(10)", c4.getSqlType(destDialect));
        assertEquals("boolean", c5.getSqlType(destDialect));
        assertEquals("bytea", c6.getSqlType(destDialect));
        assertEquals("char(10)", c7.getSqlType(destDialect));
        assertEquals("varchar(50)", c8.getSqlType(destDialect));
        assertEquals("date", c9.getSqlType(destDialect));
        assertEquals("double precision", c10.getSqlType(destDialect));
        assertEquals("integer", c11.getSqlType(destDialect));
        assertEquals("interval", c12.getSqlType(destDialect));
        assertEquals("json", c13.getSqlType(destDialect));
        assertEquals("money", c14.getSqlType(destDialect));
        assertEquals("numeric(10,2)", c15.getSqlType(destDialect));
        assertEquals("numeric(10,3)", c16.getSqlType(destDialect));
        assertEquals("real", c17.getSqlType(destDialect));
        assertEquals("smallint", c18.getSqlType(destDialect));
        assertEquals("real", c19.getSqlType(destDialect));
        assertEquals("smallint", c20.getSqlType(destDialect));
        assertEquals("bigint", c21.getSqlType(destDialect));
        assertEquals("bigint", c22.getSqlType(destDialect));
        assertEquals("bit varying(10)", c23.getSqlType(destDialect));
        assertEquals("boolean", c24.getSqlType(destDialect));
        assertEquals("char(5)", c25.getSqlType(destDialect));
        assertEquals("varchar(50)", c26.getSqlType(destDialect));
        assertEquals("double precision", c27.getSqlType(destDialect));
        assertEquals("integer", c28.getSqlType(destDialect));
        assertEquals("integer", c29.getSqlType(destDialect));
        assertEquals("smallint", c30.getSqlType(destDialect));
        assertEquals("smallint", c31.getSqlType(destDialect));
        assertEquals("integer", c32.getSqlType(destDialect));
        assertEquals("integer", c33.getSqlType(destDialect));
        assertEquals("text", c34.getSqlType(destDialect));
        assertEquals("time(6)", c35.getSqlType(destDialect));
        assertEquals("time(3)", c36.getSqlType(destDialect));
        assertEquals("time(6)", c37.getSqlType(destDialect));
        assertEquals("time(3) with time zone", c38.getSqlType(destDialect));
        assertEquals("time(3) with time zone", c39.getSqlType(destDialect));
        assertEquals("timestamp(6)", c40.getSqlType(destDialect));
        assertEquals("timestamp(3)", c41.getSqlType(destDialect));
        assertEquals("timestamp(3)", c42.getSqlType(destDialect));
        assertEquals("timestamp(2) with time zone", c43.getSqlType(destDialect));
        assertEquals("timestamp(2) with time zone", c44.getSqlType(destDialect));
        assertEquals("xml", c45.getSqlType(destDialect));
        assertEquals("uuid", c46.getSqlType(destDialect));
    }

    @Test
    public void testColumn2Oracle() {
        Table table = getTable();
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
        Column c18 = table.getColumn("C18");
        Column c19 = table.getColumn("C19");
        Column c20 = table.getColumn("C20");
        Column c21 = table.getColumn("C21");
        Column c22 = table.getColumn("C22");
        Column c23 = table.getColumn("C23");
        Column c24 = table.getColumn("C24");
        Column c25 = table.getColumn("C25");
        Column c26 = table.getColumn("C26");
        Column c27 = table.getColumn("C27");
        Column c28 = table.getColumn("C28");
        Column c29 = table.getColumn("C29");
        Column c30 = table.getColumn("C30");
        Column c31 = table.getColumn("C31");
        Column c32 = table.getColumn("C32");
        Column c33 = table.getColumn("C33");
        Column c34 = table.getColumn("C34");
        Column c35 = table.getColumn("C35");
        Column c36 = table.getColumn("C36");
        Column c37 = table.getColumn("C37");
        Column c38 = table.getColumn("C38");
        Column c39 = table.getColumn("C39");
        Column c40 = table.getColumn("C40");
        Column c41 = table.getColumn("C41");
        Column c42 = table.getColumn("C42");
        Column c43 = table.getColumn("C43");
        Column c44 = table.getColumn("C44");
        Column c45 = table.getColumn("C45");
        Column c46 = table.getColumn("C46");

        Dialect destDialect = DBType.ORACLE.getDialect();
        assertEquals("number(19,0)", c1.getSqlType(destDialect));
        assertEquals("number(19,0)", c2.getSqlType(destDialect));
        assertEquals("number", c3.getSqlType(destDialect));
        assertEquals("varchar2(10)", c4.getSqlType(destDialect));
        assertEquals("number(1,0)", c5.getSqlType(destDialect));
        assertEquals("raw", c6.getSqlType(destDialect));
        assertEquals("char(10)", c7.getSqlType(destDialect));
        assertEquals("varchar2(50)", c8.getSqlType(destDialect));
        assertEquals("date", c9.getSqlType(destDialect));
        assertEquals("double precision", c10.getSqlType(destDialect));
        assertEquals("number(10,0)", c11.getSqlType(destDialect));
        assertEquals("varchar(50)", c12.getSqlType(destDialect));
        assertEquals("clob", c13.getSqlType(destDialect));
        assertEquals("number(20,2)", c14.getSqlType(destDialect));
        assertEquals("number(10,2)", c15.getSqlType(destDialect));
        assertEquals("number(10,3)", c16.getSqlType(destDialect));
        assertEquals("float", c17.getSqlType(destDialect));
        assertEquals("number(5,0)", c18.getSqlType(destDialect));
        assertEquals("float", c19.getSqlType(destDialect));
        assertEquals("number(5,0)", c20.getSqlType(destDialect));
        assertEquals("number(19,0)", c21.getSqlType(destDialect));
        assertEquals("number(19,0)", c22.getSqlType(destDialect));
        assertEquals("varchar2(10)", c23.getSqlType(destDialect));
        assertEquals("number(1,0)", c24.getSqlType(destDialect));
        assertEquals("char(5)", c25.getSqlType(destDialect));
        assertEquals("varchar2(50)", c26.getSqlType(destDialect));
        assertEquals("double precision", c27.getSqlType(destDialect));
        assertEquals("number(10,0)", c28.getSqlType(destDialect));
        assertEquals("number(10,0)", c29.getSqlType(destDialect));
        assertEquals("number(5,0)", c30.getSqlType(destDialect));
        assertEquals("number(5,0)", c31.getSqlType(destDialect));
        assertEquals("number(10,0)", c32.getSqlType(destDialect));
        assertEquals("number(10,0)", c33.getSqlType(destDialect));
        assertEquals("clob", c34.getSqlType(destDialect));
        assertEquals("date", c35.getSqlType(destDialect));
        assertEquals("date", c36.getSqlType(destDialect));
        assertEquals("date", c37.getSqlType(destDialect));
        assertEquals("date", c38.getSqlType(destDialect));
        assertEquals("date", c39.getSqlType(destDialect));
        assertEquals("timestamp", c40.getSqlType(destDialect));
        assertEquals("timestamp", c41.getSqlType(destDialect));
        assertEquals("timestamp", c42.getSqlType(destDialect));
        assertEquals("timestamp with time zone", c43.getSqlType(destDialect));
        assertEquals("timestamp with time zone", c44.getSqlType(destDialect));
        assertEquals("clob", c45.getSqlType(destDialect));
        assertEquals("varchar(50)", c46.getSqlType(destDialect));
    }

    @Test
    public void testColumn2SqlServer() {
        Table table = getTable();
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
        Column c18 = table.getColumn("C18");
        Column c19 = table.getColumn("C19");
        Column c20 = table.getColumn("C20");
        Column c21 = table.getColumn("C21");
        Column c22 = table.getColumn("C22");
        Column c23 = table.getColumn("C23");
        Column c24 = table.getColumn("C24");
        Column c25 = table.getColumn("C25");
        Column c26 = table.getColumn("C26");
        Column c27 = table.getColumn("C27");
        Column c28 = table.getColumn("C28");
        Column c29 = table.getColumn("C29");
        Column c30 = table.getColumn("C30");
        Column c31 = table.getColumn("C31");
        Column c32 = table.getColumn("C32");
        Column c33 = table.getColumn("C33");
        Column c34 = table.getColumn("C34");
        Column c35 = table.getColumn("C35");
        Column c36 = table.getColumn("C36");
        Column c37 = table.getColumn("C37");
        Column c38 = table.getColumn("C38");
        Column c39 = table.getColumn("C39");
        Column c40 = table.getColumn("C40");
        Column c41 = table.getColumn("C41");
        Column c42 = table.getColumn("C42");
        Column c43 = table.getColumn("C43");
        Column c44 = table.getColumn("C44");
        Column c45 = table.getColumn("C45");
        Column c46 = table.getColumn("C46");

        Dialect destDialect = DBType.SQLServer.getDialect();
        assertEquals("bigint", c1.getSqlType(destDialect));
        assertEquals("bigint", c2.getSqlType(destDialect));
        assertEquals("bit", c3.getSqlType(destDialect));
        assertEquals("varbinary", c4.getSqlType(destDialect));
        assertEquals("boolean", c5.getSqlType(destDialect));
        assertEquals("binary", c6.getSqlType(destDialect));
        assertEquals("char(10)", c7.getSqlType(destDialect));
        assertEquals("varchar(50)", c8.getSqlType(destDialect));
        assertEquals("date", c9.getSqlType(destDialect));
        assertEquals("float", c10.getSqlType(destDialect));
        assertEquals("int", c11.getSqlType(destDialect));
        assertEquals("varchar(50)", c12.getSqlType(destDialect));
        assertEquals("text", c13.getSqlType(destDialect));
        assertEquals("money", c14.getSqlType(destDialect));
        assertEquals("numeric(10,2)", c15.getSqlType(destDialect));
        assertEquals("numeric(10,3)", c16.getSqlType(destDialect));
        assertEquals("real", c17.getSqlType(destDialect));
        assertEquals("smallint", c18.getSqlType(destDialect));
        assertEquals("real", c19.getSqlType(destDialect));
        assertEquals("smallint", c20.getSqlType(destDialect));
        assertEquals("bigint", c21.getSqlType(destDialect));
        assertEquals("bigint", c22.getSqlType(destDialect));
        assertEquals("varbinary", c23.getSqlType(destDialect));
        assertEquals("boolean", c24.getSqlType(destDialect));
        assertEquals("char(5)", c25.getSqlType(destDialect));
        assertEquals("varchar(50)", c26.getSqlType(destDialect));
        assertEquals("float", c27.getSqlType(destDialect));
        assertEquals("int", c28.getSqlType(destDialect));
        assertEquals("int", c29.getSqlType(destDialect));
        assertEquals("smallint", c30.getSqlType(destDialect));
        assertEquals("smallint", c31.getSqlType(destDialect));
        assertEquals("int", c32.getSqlType(destDialect));
        assertEquals("int", c33.getSqlType(destDialect));
        assertEquals("text", c34.getSqlType(destDialect));
        assertEquals("time", c35.getSqlType(destDialect));
        assertEquals("time", c36.getSqlType(destDialect));
        assertEquals("time", c37.getSqlType(destDialect));
        assertEquals("time", c38.getSqlType(destDialect));
        assertEquals("time", c39.getSqlType(destDialect));
        assertEquals("datetime", c40.getSqlType(destDialect));
        assertEquals("datetime", c41.getSqlType(destDialect));
        assertEquals("datetime", c42.getSqlType(destDialect));
        assertEquals("datetime", c43.getSqlType(destDialect));
        assertEquals("datetime", c44.getSqlType(destDialect));
        assertEquals("xml", c45.getSqlType(destDialect));
        assertEquals("varchar(50)", c46.getSqlType(destDialect));
    }

    @Test
    public void testColumn2MySql() {
        Table table = getTable();
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
        Column c18 = table.getColumn("C18");
        Column c19 = table.getColumn("C19");
        Column c20 = table.getColumn("C20");
        Column c21 = table.getColumn("C21");
        Column c22 = table.getColumn("C22");
        Column c23 = table.getColumn("C23");
        Column c24 = table.getColumn("C24");
        Column c25 = table.getColumn("C25");
        Column c26 = table.getColumn("C26");
        Column c27 = table.getColumn("C27");
        Column c28 = table.getColumn("C28");
        Column c29 = table.getColumn("C29");
        Column c30 = table.getColumn("C30");
        Column c31 = table.getColumn("C31");
        Column c32 = table.getColumn("C32");
        Column c33 = table.getColumn("C33");
        Column c34 = table.getColumn("C34");
        Column c35 = table.getColumn("C35");
        Column c36 = table.getColumn("C36");
        Column c37 = table.getColumn("C37");
        Column c38 = table.getColumn("C38");
        Column c39 = table.getColumn("C39");
        Column c40 = table.getColumn("C40");
        Column c41 = table.getColumn("C41");
        Column c42 = table.getColumn("C42");
        Column c43 = table.getColumn("C43");
        Column c44 = table.getColumn("C44");
        Column c45 = table.getColumn("C45");
        Column c46 = table.getColumn("C46");

        Dialect destDialect = DBType.MYSQL.getDialect();
        assertEquals("bigint", c1.getSqlType(destDialect));
        assertEquals("bigint", c2.getSqlType(destDialect));
        assertEquals("bit", c3.getSqlType(destDialect));
        assertEquals("varbinary(10)", c4.getSqlType(destDialect));
        assertEquals("bit", c5.getSqlType(destDialect));
        assertEquals("binary", c6.getSqlType(destDialect));
        assertEquals("char(10)", c7.getSqlType(destDialect));
        assertEquals("varchar(50)", c8.getSqlType(destDialect));
        assertEquals("datetime", c9.getSqlType(destDialect));
        assertEquals("double", c10.getSqlType(destDialect));
        assertEquals("integer", c11.getSqlType(destDialect));
        assertEquals("varchar(50)", c12.getSqlType(destDialect));
        assertEquals("longtext", c13.getSqlType(destDialect));
        assertEquals("number(20,2)", c14.getSqlType(destDialect));
        assertEquals("decimal(10,2)", c15.getSqlType(destDialect));
        assertEquals("decimal(10,3)", c16.getSqlType(destDialect));
        assertEquals("float", c17.getSqlType(destDialect));
        assertEquals("smallint", c18.getSqlType(destDialect));
        assertEquals("float", c19.getSqlType(destDialect));
        assertEquals("smallint", c20.getSqlType(destDialect));
        assertEquals("bigint", c21.getSqlType(destDialect));
        assertEquals("bigint", c22.getSqlType(destDialect));
        assertEquals("varbinary(10)", c23.getSqlType(destDialect));
        assertEquals("bit", c24.getSqlType(destDialect));
        assertEquals("char(5)", c25.getSqlType(destDialect));
        assertEquals("varchar(50)", c26.getSqlType(destDialect));
        assertEquals("double", c27.getSqlType(destDialect));
        assertEquals("integer", c28.getSqlType(destDialect));
        assertEquals("integer", c29.getSqlType(destDialect));
        assertEquals("smallint", c30.getSqlType(destDialect));
        assertEquals("smallint", c31.getSqlType(destDialect));
        assertEquals("integer", c32.getSqlType(destDialect));
        assertEquals("integer", c33.getSqlType(destDialect));
        assertEquals("longtext", c34.getSqlType(destDialect));
        assertEquals("time", c35.getSqlType(destDialect));
        assertEquals("time", c36.getSqlType(destDialect));
        assertEquals("time", c37.getSqlType(destDialect));
        assertEquals("time", c38.getSqlType(destDialect));
        assertEquals("time", c39.getSqlType(destDialect));
        assertEquals("timestamp", c40.getSqlType(destDialect));
        assertEquals("timestamp", c41.getSqlType(destDialect));
        assertEquals("timestamp", c42.getSqlType(destDialect));
        assertEquals("timestamp", c43.getSqlType(destDialect));
        assertEquals("timestamp", c44.getSqlType(destDialect));
        assertEquals("longtext", c45.getSqlType(destDialect));
        assertEquals("varchar(50)", c46.getSqlType(destDialect));
    }

    @Test
    public void testColumn2Hive() {
        Table table = getTable();
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
        Column c18 = table.getColumn("C18");
        Column c19 = table.getColumn("C19");
        Column c20 = table.getColumn("C20");
        Column c21 = table.getColumn("C21");
        Column c22 = table.getColumn("C22");
        Column c23 = table.getColumn("C23");
        Column c24 = table.getColumn("C24");
        Column c25 = table.getColumn("C25");
        Column c26 = table.getColumn("C26");
        Column c27 = table.getColumn("C27");
        Column c28 = table.getColumn("C28");
        Column c29 = table.getColumn("C29");
        Column c30 = table.getColumn("C30");
        Column c31 = table.getColumn("C31");
        Column c32 = table.getColumn("C32");
        Column c33 = table.getColumn("C33");
        Column c34 = table.getColumn("C34");
        Column c35 = table.getColumn("C35");
        Column c36 = table.getColumn("C36");
        Column c37 = table.getColumn("C37");
        Column c38 = table.getColumn("C38");
        Column c39 = table.getColumn("C39");
        Column c40 = table.getColumn("C40");
        Column c41 = table.getColumn("C41");
        Column c42 = table.getColumn("C42");
        Column c43 = table.getColumn("C43");
        Column c44 = table.getColumn("C44");
        Column c45 = table.getColumn("C45");
        Column c46 = table.getColumn("C46");

        Dialect destDialect = DBType.HIVE.getDialect();
        assertEquals("BIGINT", c1.getSqlType(destDialect));
        assertEquals("BIGINT", c2.getSqlType(destDialect));
        assertEquals("BOOLEAN", c3.getSqlType(destDialect));
        assertEquals("BINARY", c4.getSqlType(destDialect));
        assertEquals("BOOLEAN", c5.getSqlType(destDialect));
        assertEquals("BINARY", c6.getSqlType(destDialect));
        assertEquals("CHAR(10)", c7.getSqlType(destDialect));
        assertEquals("VARCHAR(50)", c8.getSqlType(destDialect));
        assertEquals("DATE", c9.getSqlType(destDialect));
        assertEquals("DOUBLE", c10.getSqlType(destDialect));
        assertEquals("INT", c11.getSqlType(destDialect));
        assertEquals("STRING", c12.getSqlType(destDialect));
        assertEquals("STRING", c13.getSqlType(destDialect));
        assertEquals("DECIMAL(20,2)", c14.getSqlType(destDialect));
        assertEquals("DECIMAL(10,2)", c15.getSqlType(destDialect));
        assertEquals("DECIMAL(10,3)", c16.getSqlType(destDialect));
        assertEquals("FLOAT", c17.getSqlType(destDialect));
        assertEquals("SMALLINT", c18.getSqlType(destDialect));
        assertEquals("FLOAT", c19.getSqlType(destDialect));
        assertEquals("SMALLINT", c20.getSqlType(destDialect));
        assertEquals("BIGINT", c21.getSqlType(destDialect));
        assertEquals("BIGINT", c22.getSqlType(destDialect));
        assertEquals("BINARY", c23.getSqlType(destDialect));
        assertEquals("BOOLEAN", c24.getSqlType(destDialect));
        assertEquals("CHAR(5)", c25.getSqlType(destDialect));
        assertEquals("VARCHAR(50)", c26.getSqlType(destDialect));
        assertEquals("DOUBLE", c27.getSqlType(destDialect));
        assertEquals("INT", c28.getSqlType(destDialect));
        assertEquals("INT", c29.getSqlType(destDialect));
        assertEquals("SMALLINT", c30.getSqlType(destDialect));
        assertEquals("SMALLINT", c31.getSqlType(destDialect));
        assertEquals("INT", c32.getSqlType(destDialect));
        assertEquals("INT", c33.getSqlType(destDialect));
        assertEquals("STRING", c34.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c35.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c36.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c37.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c38.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c39.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c40.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c41.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c42.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c43.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c44.getSqlType(destDialect));
        assertEquals("STRING", c45.getSqlType(destDialect));
        assertEquals("STRING", c46.getSqlType(destDialect));
    }

}
