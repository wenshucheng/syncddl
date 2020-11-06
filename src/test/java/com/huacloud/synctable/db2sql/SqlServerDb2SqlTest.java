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
public class SqlServerDb2SqlTest {

    public static final String ALL_DATATYPE_TEST = "sqlserver_table_ddl_test";

    private static JdbcTemplate jdbcTemplate;

    private static final String SCHEMA_NAME = "dbo";

    @BeforeClass
    public static void beforeClass() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(DBType.SQLServer.getDriverName());
        dataSource.setUrl("jdbc:sqlserver://172.16.18.16:1433; DatabaseName=pxd_test");
        dataSource.setUsername("sa");
        dataSource.setPassword("admin@123");
        jdbcTemplate = new JdbcTemplate(dataSource);

        String sqlServerSql =
                "create table " + ALL_DATATYPE_TEST + " (" +
                        "c1 bigint," +
                        "c2 binary," +
                        "c3 bit," +
                        "c4 char(20)," +
                        "c5 date," +
                        "c6 datetime," +
                        "c7 decimal(10,3)," +
                        "c8 float," +
                        "c9 int," +
                        "c10 numeric(10,3)," +
                        "c11 real," +
                        "c12 smallint," +
                        "c13 text," +
                        "c14 time," +
                        "c15 timestamp," +  //注意：SQLServer对timestamp会默认使用NOT NULL
                        "c16 tinyint," +
                        "c17 varbinary(500)," +
                        "c18 varchar(200), " +
                        "c19 datetimeoffset(7)" +
                ")";
        jdbcTemplate.execute(sqlServerSql);
    }

    @AfterClass
    public static void afterClass() {
        jdbcTemplate.execute("DROP TABLE " + ALL_DATATYPE_TEST);
    }

    private Table getTable() {
        AbstractDbMetaInfoDao dao = DBType.SQLServer.getDbDao(jdbcTemplate);
        Dialect dialect = DBType.SQLServer.getDialect();
        return dao.queryTable(null, SCHEMA_NAME, ALL_DATATYPE_TEST, dialect);
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

        Dialect destDialect = DBType.GaussDB.getDialect();
        assertEquals("bigint", c1.getSqlType(destDialect));
        assertEquals("binary(1)", c2.getSqlType(destDialect));
        assertEquals("varchar(1)", c3.getSqlType(destDialect));
        assertEquals("char(20)", c4.getSqlType(destDialect));
        assertEquals("date", c5.getSqlType(destDialect));
        assertEquals("date", c6.getSqlType(destDialect));
        assertEquals("decimal(10,3)", c7.getSqlType(destDialect));
        assertEquals("float", c8.getSqlType(destDialect));
        assertEquals("int", c9.getSqlType(destDialect));
        assertEquals("number(10,3)", c10.getSqlType(destDialect));
        assertEquals("real", c11.getSqlType(destDialect));
        assertEquals("smallint", c12.getSqlType(destDialect));
        assertEquals("clob", c13.getSqlType(destDialect));
        assertEquals("date", c14.getSqlType(destDialect));
        assertEquals("timestamp", c15.getSqlType(destDialect));
        assertEquals("tinyint", c16.getSqlType(destDialect));
        assertEquals("varbinary(500)", c17.getSqlType(destDialect));
        assertEquals("varchar(200)", c18.getSqlType(destDialect));
        assertEquals("timestamp(6) with time zone", c19.getSqlType(destDialect));
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

        Dialect destDialect = DBType.TBase.getDialect();
        assertEquals("bigint", c1.getSqlType(destDialect));
        assertEquals("bytea", c2.getSqlType(destDialect));
        assertEquals("bit(1)", c3.getSqlType(destDialect));
        assertEquals("char(20)", c4.getSqlType(destDialect));
        assertEquals("date", c5.getSqlType(destDialect));
        assertEquals("date", c6.getSqlType(destDialect));
        assertEquals("decimal(10,3)", c7.getSqlType(destDialect));
        assertEquals("double precision", c8.getSqlType(destDialect));
        assertEquals("integer", c9.getSqlType(destDialect));
        assertEquals("numeric(10,3)", c10.getSqlType(destDialect));
        assertEquals("real", c11.getSqlType(destDialect));
        assertEquals("smallint", c12.getSqlType(destDialect));
        assertEquals("text", c13.getSqlType(destDialect));
        assertEquals("time(16)", c14.getSqlType(destDialect));
        assertEquals("timestamp", c15.getSqlType(destDialect));
        assertEquals("smallint", c16.getSqlType(destDialect));
        assertEquals("bytea", c17.getSqlType(destDialect));
        assertEquals("varchar(200)", c18.getSqlType(destDialect));
        assertEquals("timestamp(6) with time zone", c19.getSqlType(destDialect));
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

        Dialect destDialect = DBType.ORACLE.getDialect();
        assertEquals("number(19,0)", c1.getSqlType(destDialect));
        assertEquals("raw(1)", c2.getSqlType(destDialect));
        assertEquals("number(1)", c3.getSqlType(destDialect));
        assertEquals("char(20)", c4.getSqlType(destDialect));
        assertEquals("date", c5.getSqlType(destDialect));
        assertEquals("date", c6.getSqlType(destDialect));
        assertEquals("number(10,3)", c7.getSqlType(destDialect));
        assertEquals("float", c8.getSqlType(destDialect));
        assertEquals("number(10,0)", c9.getSqlType(destDialect));
        assertEquals("number(10,3)", c10.getSqlType(destDialect));
        assertEquals("float", c11.getSqlType(destDialect));
        assertEquals("number(5,0)", c12.getSqlType(destDialect));
        assertEquals("clob", c13.getSqlType(destDialect));
        assertEquals("date", c14.getSqlType(destDialect));
        assertEquals("timestamp", c15.getSqlType(destDialect));
        assertEquals("number(3,0)", c16.getSqlType(destDialect));
        assertEquals("raw(500)", c17.getSqlType(destDialect));
        assertEquals("varchar2(200)", c18.getSqlType(destDialect));
        assertEquals("timestamp with time zone", c19.getSqlType(destDialect));
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

        Dialect destDialect = DBType.SQLServer.getDialect();
        assertEquals("bigint", c1.getSqlType(destDialect));
        assertEquals("binary", c2.getSqlType(destDialect));
        assertEquals("bit", c3.getSqlType(destDialect));
        assertEquals("char(20)", c4.getSqlType(destDialect));
        assertEquals("date", c5.getSqlType(destDialect));
        assertEquals("date", c6.getSqlType(destDialect));
        assertEquals("decimal(10,3)", c7.getSqlType(destDialect));
        assertEquals("float", c8.getSqlType(destDialect));
        assertEquals("int", c9.getSqlType(destDialect));
        assertEquals("numeric(10,3)", c10.getSqlType(destDialect));
        assertEquals("real", c11.getSqlType(destDialect));
        assertEquals("smallint", c12.getSqlType(destDialect));
        assertEquals("text", c13.getSqlType(destDialect));
        assertEquals("time", c14.getSqlType(destDialect));
        assertEquals("datetime", c15.getSqlType(destDialect));
        assertEquals("tinyint", c16.getSqlType(destDialect));
        assertEquals("varbinary", c17.getSqlType(destDialect));
        assertEquals("varchar(200)", c18.getSqlType(destDialect));
        assertEquals("datetime", c19.getSqlType(destDialect));
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

        Dialect destDialect = DBType.MYSQL.getDialect();
        assertEquals("bigint", c1.getSqlType(destDialect));
        assertEquals("binary(1)", c2.getSqlType(destDialect));
        assertEquals("bit", c3.getSqlType(destDialect));
        assertEquals("char(20)", c4.getSqlType(destDialect));
        assertEquals("datetime", c5.getSqlType(destDialect));
        assertEquals("datetime", c6.getSqlType(destDialect));
        assertEquals("decimal(10,3)", c7.getSqlType(destDialect));
        assertEquals("float", c8.getSqlType(destDialect));
        assertEquals("integer", c9.getSqlType(destDialect));
        assertEquals("decimal(10,3)", c10.getSqlType(destDialect));
        assertEquals("float", c11.getSqlType(destDialect));
        assertEquals("smallint", c12.getSqlType(destDialect));
        assertEquals("longtext", c13.getSqlType(destDialect));
        assertEquals("time", c14.getSqlType(destDialect));
        assertEquals("timestamp", c15.getSqlType(destDialect));
        assertEquals("tinyint", c16.getSqlType(destDialect));
        assertEquals("blob", c17.getSqlType(destDialect));
        assertEquals("varchar(200)", c18.getSqlType(destDialect));
        assertEquals("timestamp", c19.getSqlType(destDialect));
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

        Dialect destDialect = DBType.HIVE.getDialect();
        assertEquals("BIGINT", c1.getSqlType(destDialect));
        assertEquals("BINARY", c2.getSqlType(destDialect));
        assertEquals("BOOLEAN", c3.getSqlType(destDialect));
        assertEquals("CHAR(20)", c4.getSqlType(destDialect));
        assertEquals("DATE", c5.getSqlType(destDialect));
        assertEquals("DATE", c6.getSqlType(destDialect));
        assertEquals("DECIMAL(10,3)", c7.getSqlType(destDialect));
        assertEquals("FLOAT", c8.getSqlType(destDialect));
        assertEquals("INT", c9.getSqlType(destDialect));
        assertEquals("DECIMAL(10,3)", c10.getSqlType(destDialect));
        assertEquals("FLOAT", c11.getSqlType(destDialect));
        assertEquals("SMALLINT", c12.getSqlType(destDialect));
        assertEquals("STRING", c13.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c14.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c15.getSqlType(destDialect));
        assertEquals("TINYINT", c16.getSqlType(destDialect));
        assertEquals("BINARY", c17.getSqlType(destDialect));
        assertEquals("VARCHAR(200)", c18.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c19.getSqlType(destDialect));
    }

}
