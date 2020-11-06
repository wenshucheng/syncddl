package com.huacloud.synctable;

import com.huacloud.synctable.dialect.*;
import com.huacloud.synctable.entity.DBType;
import com.huacloud.synctable.mapping.Column;
import com.huacloud.synctable.mapping.Table;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.Assert.assertEquals;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 9/17/2019 7:02 PM
 */
public class SQLServerSql2SqlTest {

    private static final Logger logger = LoggerFactory.getLogger(SQLServerSql2SqlTest.class);

    private static BasicDataSource dataSource;

    private static JdbcTemplate jdbcTemplate;

    private static final String schemaName = "dbo";

    @BeforeClass
    public static void beforeClass() {
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(DBType.SQLServer.getDriverName());
        dataSource.setUrl("jdbc:sqlserver://172.16.18.16:1433; DatabaseName=pxd_test");
        dataSource.setUsername("sa");
        dataSource.setPassword("admin@123");
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Test
    public void testSQLServer2Other() {
        String sqlServerSql =
                "create table sqlserver_table (" +
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
                        "c15 timestamp," +
                        "c16 tinyint," +
                        "c17 varbinary(500)," +
                        "c18 varchar(200)" +
                ")";

        ParserImpl parser = new ParserImpl();
        Table table = parser.parseTable(sqlServerSql, new SQLServerDialect());

        Dialect dialect = new TBaseDialect();
        String sql = table.getFullCreateTableSQL(dialect, schemaName);
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
        Column c18 = table.getColumn("C18");

        assertEquals("bigint", c1.getSqlType(dialect));
        assertEquals("bytea", c2.getSqlType(dialect));
        assertEquals("bit", c3.getSqlType(dialect));
        assertEquals("char(20)", c4.getSqlType(dialect));
        assertEquals("date", c5.getSqlType(dialect));
        assertEquals("timestamp without time zone", c6.getSqlType(dialect));
        assertEquals("decimal(10,3)", c7.getSqlType(dialect));
        assertEquals("double precision", c8.getSqlType(dialect));
        assertEquals("integer", c9.getSqlType(dialect));
        assertEquals("numeric(10,3)", c10.getSqlType(dialect));
        assertEquals("real", c11.getSqlType(dialect));
        assertEquals("smallint", c12.getSqlType(dialect));
        assertEquals("text", c13.getSqlType(dialect));
        assertEquals("time without time zone", c14.getSqlType(dialect));
        assertEquals("timestamp without time zone", c15.getSqlType(dialect));
        assertEquals("smallint", c16.getSqlType(dialect));
        assertEquals("bytea", c17.getSqlType(dialect));
        assertEquals("varchar(200)", c18.getSqlType(dialect));

        dialect = new MySQLDialect();
        logger.info("MySQL:\n" + table.getFullCreateTableSQL(dialect, schemaName));

        assertEquals("bigint", c1.getSqlType(dialect));
        assertEquals("binary(255)", c2.getSqlType(dialect));
        assertEquals("bit", c3.getSqlType(dialect));
        assertEquals("char(20)", c4.getSqlType(dialect));
        assertEquals("date", c5.getSqlType(dialect));
        assertEquals("timestamp", c6.getSqlType(dialect));
        assertEquals("decimal(10,3)", c7.getSqlType(dialect));
        assertEquals("float", c8.getSqlType(dialect));
        assertEquals("integer", c9.getSqlType(dialect));
        assertEquals("decimal(10,3)", c10.getSqlType(dialect));
        assertEquals("float", c11.getSqlType(dialect));
        assertEquals("smallint", c12.getSqlType(dialect));
        assertEquals("longtext", c13.getSqlType(dialect));
        assertEquals("time", c14.getSqlType(dialect));
        assertEquals("timestamp", c15.getSqlType(dialect));
        assertEquals("tinyint", c16.getSqlType(dialect));
        assertEquals("blob", c17.getSqlType(dialect));
        assertEquals("varchar(200)", c18.getSqlType(dialect));

        dialect = new OracleDialect();
        logger.info("Oracle:\n" + table.getFullCreateTableSQL(dialect, schemaName));

        assertEquals("number(19,0)", c1.getSqlType(dialect));
        assertEquals("raw(255)", c2.getSqlType(dialect));
        assertEquals("number(1,0)", c3.getSqlType(dialect));
        assertEquals("char(20)", c4.getSqlType(dialect));
        assertEquals("date", c5.getSqlType(dialect));
        assertEquals("timestamp", c6.getSqlType(dialect));
        assertEquals("number(10,3)", c7.getSqlType(dialect));
        assertEquals("float", c8.getSqlType(dialect));
        assertEquals("number(10,0)", c9.getSqlType(dialect));
        assertEquals("number(10,3)", c10.getSqlType(dialect));
        assertEquals("float", c11.getSqlType(dialect));
        assertEquals("number(5,0)", c12.getSqlType(dialect));
        assertEquals("clob", c13.getSqlType(dialect));
        assertEquals("date", c14.getSqlType(dialect));
        assertEquals("timestamp", c15.getSqlType(dialect));
        assertEquals("number(3,0)", c16.getSqlType(dialect));
        assertEquals("raw(500)", c17.getSqlType(dialect));
        assertEquals("varchar2(200)", c18.getSqlType(dialect));

        dialect = new HiveDialect();
        logger.info("Hive:\n" + table.getFullCreateTableSQL(dialect, schemaName));

        assertEquals("BIGINT", c1.getSqlType(dialect));
        assertEquals("BINARY", c2.getSqlType(dialect));
        assertEquals("BOOLEAN", c3.getSqlType(dialect));
        assertEquals("CHAR(20)", c4.getSqlType(dialect));
        assertEquals("DATE", c5.getSqlType(dialect));
        assertEquals("TIMESTAMP", c6.getSqlType(dialect));
        assertEquals("DECIMAL(10,3)", c7.getSqlType(dialect));
        assertEquals("FLOAT", c8.getSqlType(dialect));
        assertEquals("INT", c9.getSqlType(dialect));
        assertEquals("DECIMAL(10,3)", c10.getSqlType(dialect));
        assertEquals("FLOAT", c11.getSqlType(dialect));
        assertEquals("SMALLINT", c12.getSqlType(dialect));
        assertEquals("BINARY", c13.getSqlType(dialect));
        assertEquals("TIMESTAMP", c14.getSqlType(dialect));
        assertEquals("TIMESTAMP", c15.getSqlType(dialect));
        assertEquals("TINYINT", c16.getSqlType(dialect));
        assertEquals("BINARY", c17.getSqlType(dialect));
        assertEquals("VARCHAR(200)", c18.getSqlType(dialect));
    }

    @Test
    public void testIndexInfo() {
        String createTabSql =
                "CREATE TABLE DBO.INDEX_TEST_PXD (\n" +
                "    c1 BIGINT PRIMARY KEY,\n" +
                "    c2 BIGINT,\n" +
                "    c3 BIGINT,\n" +
                "    c4 VARCHAR(100),\n" +
                "    c5 VARCHAR(50)\n" +
                ")";

        String createIdexSql1 = "CREATE UNIQUE INDEX index_test_pxd_idx_c2 ON DBO.INDEX_TEST_PXD(c2)";
        String createIdexSql2 = "CREATE INDEX index_test_pxd_idx_c5 ON DBO.INDEX_TEST_PXD(c5)";
        String createIndeiesSql = "CREATE INDEX index_test_pxd_idx_c3_c4 ON DBO.INDEX_TEST_PXD(c3, c4)";
        try {
            jdbcTemplate.execute(createTabSql);
            jdbcTemplate.execute(createIdexSql1);
            jdbcTemplate.execute(createIdexSql2);
            jdbcTemplate.execute(createIndeiesSql);
        } catch (DataAccessException e) {
            e.printStackTrace();
        }

        String gaussDbSql = Db2SqlApp.db2sql(DBType.SQLServer, DBType.GaussDB,
                dataSource, schemaName, "INDEX_TEST_PXD", schemaName);
        System.out.println("GaussDb:\n" + gaussDbSql);

        String tbaseDbSql = Db2SqlApp.db2sql(DBType.SQLServer, DBType.TBase,
                dataSource, schemaName, "INDEX_TEST_PXD", schemaName);
        System.out.println("TBase:\n" + tbaseDbSql);

        try {
            jdbcTemplate.execute("DROP TABLE DBO.INDEX_TEST_PXD");
        } catch (DataAccessException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {

    }
}
