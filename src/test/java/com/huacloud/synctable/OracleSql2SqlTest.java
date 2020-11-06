package com.huacloud.synctable;

import com.huacloud.synctable.dialect.*;
import com.huacloud.synctable.entity.DBType;
import com.huacloud.synctable.mapping.Column;
import com.huacloud.synctable.mapping.Table;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.Assert.*;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 9/17/2019 7:02 PM
 */
public class OracleSql2SqlTest {

    private static final Logger logger = LoggerFactory.getLogger(OracleSql2SqlTest.class);

    private static BasicDataSource dataSource;

    private static JdbcTemplate jdbcTemplate;

    private static final String schemaName = "TEST_OGG";

    @BeforeClass
    public static void beforeClass() {
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(DBType.ORACLE.getDriverName());
        dataSource.setUrl("jdbc:oracle:thin:@//172.16.18.16:1521/oracle");
        dataSource.setUsername("test_ogg");
        dataSource.setPassword("pxd178");
        dataSource.setDefaultAutoCommit(true);

        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    /**
     * TODO: 还需仔细测试
     */
    @Test
    public void testOracleSQL2Other() {
        String oracleSql =
            "create table oracle2tbase (" +
                "c1 char(10) primary key," +
                "c2 varchar2(500) not null," +
                "c3 number(10) default '10'," +
                "c4 number(10, 2) default '5'," +
                "c5 date," +
                "c6 timestamp," +
                "c7 blob," +
                "c8 clob," +
                "c9 long" +
            ")";
        System.out.println(oracleSql);
        Dialect oracleDialect = new OracleDialect();
        ParserImpl parser = new ParserImpl();
        Table table = parser.parseTable(oracleSql, oracleDialect);

        Column c1 = table.getColumn("C1");
        Column c2 = table.getColumn("C2");
        Column c3 = table.getColumn("C3");
        Column c4 = table.getColumn("C4");
        Column c5 = table.getColumn("C5");
        Column c6 = table.getColumn("C6");
        Column c7 = table.getColumn("C7");
        Column c8 = table.getColumn("C8");
        Column c9 = table.getColumn("C9");

        Dialect dialect = new MySQLDialect();
        logger.info("mysql:\n" + table.sqlCreateString(dialect));

        assertEquals("char(10)", c1.getSqlType(dialect));
        assertEquals("varchar(500)", c2.getSqlType(dialect));
        assertEquals("decimal(10,0)", c3.getSqlType(dialect));
        assertEquals("decimal(10,2)", c4.getSqlType(dialect));
        assertEquals("date", c5.getSqlType(dialect));
        assertEquals("timestamp", c6.getSqlType(dialect));
        assertEquals("longblob", c7.getSqlType(dialect));
        assertEquals("longtext", c8.getSqlType(dialect));
        assertEquals("longtext", c9.getSqlType(dialect));

        dialect = new TBaseDialect();
        logger.info("tbase:\n" + table.sqlCreateString(dialect));

        assertEquals("char(10)", c1.getSqlType(dialect));
        assertTrue("Null字段", c1.isNullable());
        assertEquals("varchar(500)", c2.getSqlType(dialect));
        assertFalse("Not Null字段", c2.isNullable());
        assertEquals("numeric(10,0)", c3.getSqlType(dialect));
        assertEquals("默认值检查", "10", c3.getDefaultValue());
        assertEquals("numeric(10,2)", c4.getSqlType(dialect));
        assertEquals("默认值检查", "5", c4.getDefaultValue());
        assertEquals("date", c5.getSqlType(dialect));
        assertEquals("timestamp without time zone", c6.getSqlType(dialect));
        assertEquals("bytea", c7.getSqlType(dialect));
        assertEquals("text", c8.getSqlType(dialect));
        assertEquals("text", c9.getSqlType(dialect));

        dialect = new HiveDialect();
        logger.info("Hive:\n" + table.sqlCreateString(dialect));

        assertEquals("CHAR(10)", c1.getSqlType(dialect));
        assertEquals("VARCHAR(500)", c2.getSqlType(dialect));
        assertEquals("DECIMAL(10,0)", c3.getSqlType(dialect));
        assertEquals("DECIMAL(10,2)", c4.getSqlType(dialect));
        assertEquals("DATE", c5.getSqlType(dialect));
        assertEquals("TIMESTAMP", c6.getSqlType(dialect));
        assertEquals("BINARY", c7.getSqlType(dialect));
        assertEquals("BINARY", c8.getSqlType(dialect));
        assertEquals("VARCHAR", c9.getSqlType(dialect));

        dialect = new SQLServerDialect();
        logger.info("SQLServer:\n" + table.sqlCreateString(dialect));

        assertEquals("char(10)", c1.getSqlType(dialect));
        assertEquals("varchar(500)", c2.getSqlType(dialect));
        assertEquals("numeric(10,0)", c3.getSqlType(dialect));
        assertEquals("numeric(10,2)", c4.getSqlType(dialect));
        assertEquals("date", c5.getSqlType(dialect));
        assertEquals("datetime", c6.getSqlType(dialect));
        assertEquals("image", c7.getSqlType(dialect));
        assertEquals("text", c8.getSqlType(dialect));
        assertEquals("text", c9.getSqlType(dialect));
    }

    @Test
    public void testIndexInfo() {
        String createTabSql =
                "CREATE TABLE TEST_OGG.INDEX_TEST_PXD (\n" +
                        "    c1 NUMBER(10) PRIMARY KEY,\n" +
                        "    c2 NUMBER(10),\n" +
                        "    c3 NUMBER(10),\n" +
                        "    c4 VARCHAR2(100),\n" +
                        "    c5 VARCHAR2(50)\n" +
                ")";

        String createIdexSql1 = "CREATE UNIQUE INDEX index_test_pxd_idx_c2 ON TEST_OGG.INDEX_TEST_PXD(c2)";
        String createIdexSql2 = "CREATE INDEX index_test_pxd_idx_c5 ON TEST_OGG.INDEX_TEST_PXD(c5)";
        String createIndeiesSql = "CREATE INDEX index_test_pxd_idx_c3_c4 ON TEST_OGG.INDEX_TEST_PXD(c3, c4)";
        try {
            jdbcTemplate.execute(createTabSql);
            jdbcTemplate.execute(createIdexSql1);
            jdbcTemplate.execute(createIdexSql2);
            jdbcTemplate.execute(createIndeiesSql);
        } catch (DataAccessException e) {
            e.printStackTrace();
        }

        String gaussDbSql = Db2SqlApp.db2sql(DBType.ORACLE, DBType.GaussDB,
                dataSource, schemaName, "INDEX_TEST_PXD", "");
        System.out.println("GaussDb:\n" + gaussDbSql);

        String tbaseDbSql = Db2SqlApp.db2sql(DBType.ORACLE, DBType.TBase,
                dataSource, schemaName, "INDEX_TEST_PXD", "");
        System.out.println("TBase:\n" + tbaseDbSql);

        try {
            jdbcTemplate.execute("DROP TABLE TEST_OGG.INDEX_TEST_PXD");
        } catch (DataAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试分区信息到TBase和GaussDB
     */
    @Test
    public void testPartition2BigData() {

        String createTab1 =
                "create table TEST_OGG.p_range_test\n" +
                "(\n" +
                " id number(2),\n" +
                " done_date date,\n" +
                " data varchar2(50)\n" +
                ")\n" +
                "partition by range (done_date)\n" +
                "(\n" +
                "  partition part_1 values less than ( to_date('20160901', 'yyyymmdd') ),\n" +
                "  partition part_2 values less than ( to_date('20161001', 'yyyymmdd') ),\n" +
                "  partition part_3 values less than ( maxvalue )\n" +
                ")";

        String createTab2 =
                "create table TEST_OGG.p_hash_test\n" +
                "(\n" +
                " id number(2),\n" +
                " done_date date,\n" +
                " data varchar2(50)\n" +
                ")\n" +
                "partition by hash (done_date)\n" +
                "(\n" +
                "  partition part_1,\n" +
                "  partition part_2\n" +
                ")";

        String createTab3 =
                "create table TEST_OGG.p_list_test\n" +
                "(\n" +
                " id number(2),\n" +
                " name varchar(30),\n" +
                " data varchar2(50)\n" +
                ")\n" +
                "partition by list (id)\n" +
                "(\n" +
                "  partition part_1 values ( '1', '3' ),\n" +
                "  partition part_2 values ( '2', '4' )\n" +
                ")";

        try {
            jdbcTemplate.execute(createTab1);
            jdbcTemplate.execute(createTab2);
            jdbcTemplate.execute(createTab3);

            //GaussDB
            System.out.println("GaussDb1:\n" + Db2SqlApp.db2sql(DBType.ORACLE, DBType.GaussDB,
                    dataSource, schemaName, "P_RANGE_TEST", schemaName));

            System.out.println("GaussDb2:\n" + Db2SqlApp.db2sql(DBType.ORACLE, DBType.GaussDB,
                    dataSource, schemaName, "P_HASH_TEST", schemaName));

            System.out.println("GaussDb3:\n" + Db2SqlApp.db2sql(DBType.ORACLE, DBType.GaussDB,
                    dataSource, schemaName, "P_LIST_TEST", schemaName));

            //TBase
            System.out.println("TBase1:\n" + Db2SqlApp.db2sql(DBType.ORACLE, DBType.TBase,
                    dataSource, schemaName, "P_RANGE_TEST", schemaName));

            System.out.println("TBase2:\n" + Db2SqlApp.db2sql(DBType.ORACLE, DBType.TBase,
                    dataSource, schemaName, "P_HASH_TEST", schemaName));

            System.out.println("TBase3:\n" + Db2SqlApp.db2sql(DBType.ORACLE, DBType.TBase,
                    dataSource, schemaName, "P_LIST_TEST", schemaName));

            jdbcTemplate.execute("DROP TABLE TEST_OGG.P_RANGE_TEST");
            jdbcTemplate.execute("DROP TABLE TEST_OGG.P_HASH_TEST");
            jdbcTemplate.execute("DROP TABLE TEST_OGG.P_LIST_TEST");
        } catch (DataAccessException e) {
            e.printStackTrace();
        }
    }

}
