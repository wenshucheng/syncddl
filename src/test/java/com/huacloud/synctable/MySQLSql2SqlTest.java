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

import static org.junit.Assert.assertEquals;

/**
 * 通过数据库转SQL语句的测试
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/14/2019 3:53 PM
 */
public class MySQLSql2SqlTest {

    private static final Logger logger = LoggerFactory.getLogger(MySQLSql2SqlTest.class);

    private static BasicDataSource mySqlDs;

    @BeforeClass
    public static void beforeClass() {
        mySqlDs = new BasicDataSource();
        mySqlDs.setDriverClassName(DBType.MYSQL.getDriverName());
        mySqlDs.setUrl("jdbc:mysql://172.16.18.17:3306/test?useUnicode=true&characterEncoding=utf-8");
        mySqlDs.setUsername("root");
        mySqlDs.setPassword("root");
    }

    @Test
    public void testMySQLSql2Oracle() {
        String mysqlSql =
                "create table tbase2oracle (" +
                        "c1 tinyint," +
                        "c111 tinyint(20)," +
                        "c2 smallint," +
                        "c3 mediumint," +
                        "c4 int," +
                        "c5 integer," +
                        "c6 bigint," +
                        "c7 float," +
                        "c8 double," +
                        "c9 decimal(5,2)," +
                        "c10 date," +
                        "c11 time," +
                        "c12 datetime," +
                        "c13 timestamp," +
                        "c14 char(2)," +
                        "c15 varchar(50)," +
                        "c16 tinyblob," +
                        "c17 tinytext," +
                        "c18 blob," +
                        "c19 text," +
                        "c20 mediumblob," +
                        "c21 mediumtext," +
                        "c22 longblob," +
                        "c23 longtext" +
                ")";

        ParserImpl parser = new ParserImpl();
        Table table = parser.parseTable(mysqlSql, new MySQLDialect());

        Column c1 = table.getColumn("C1");
        Column c111 = table.getColumn("C111");
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

        Dialect dialect = new OracleDialect();
        logger.info("oracle:" + table.sqlCreateString(dialect));

        assertEquals("number(3,0)", c1.getSqlType(dialect));
        assertEquals("number(3,0)", c111.getSqlType(dialect));
        assertEquals("number(5,0)", c2.getSqlType(dialect));
        assertEquals("number(10,0)", c3.getSqlType(dialect));
        assertEquals("number(10,0)", c4.getSqlType(dialect));
        assertEquals("number(10,0)", c5.getSqlType(dialect));
        assertEquals("number(19,0)", c6.getSqlType(dialect));
        assertEquals("float", c7.getSqlType(dialect));
        assertEquals("double precision", c8.getSqlType(dialect));
        assertEquals("number(5,2)", c9.getSqlType(dialect));
        assertEquals("date", c10.getSqlType(dialect));
        assertEquals("date", c11.getSqlType(dialect));
        assertEquals("timestamp", c12.getSqlType(dialect));
        assertEquals("timestamp", c13.getSqlType(dialect));
        assertEquals("char(2)", c14.getSqlType(dialect));
        assertEquals("varchar2(50)", c15.getSqlType(dialect));
        assertEquals("blob", c16.getSqlType(dialect));
        assertEquals("varchar2(50)", c17.getSqlType(dialect));
        assertEquals("blob", c18.getSqlType(dialect));
        assertEquals("clob", c19.getSqlType(dialect));
        assertEquals("blob", c20.getSqlType(dialect));
        assertEquals("clob", c21.getSqlType(dialect));
        assertEquals("blob", c22.getSqlType(dialect));
        assertEquals("clob", c23.getSqlType(dialect));
    }

    @Test
    public void testMySQLSql2SqlServer() {
        String mysqlSql =
                "create table tbase2oracle (" +
                        "c1 tinyint," +
                        "c111 tinyint(20)," +
                        "c2 smallint," +
                        "c3 mediumint," +
                        "c4 int," +
                        "c5 integer," +
                        "c6 bigint," +
                        "c7 float," +
                        "c8 double," +
                        "c9 decimal(5,2)," +
                        "c10 date," +
                        "c11 time," +
                        "c12 datetime," +
                        "c13 timestamp," +
                        "c14 char(2)," +
                        "c15 varchar(50)," +
                        "c16 tinyblob," +
                        "c17 tinytext," +
                        "c18 blob," +
                        "c19 text," +
                        "c20 mediumblob," +
                        "c21 mediumtext," +
                        "c22 longblob," +
                        "c23 longtext" +
                        ")";

        ParserImpl parser = new ParserImpl();
        Table table = parser.parseTable(mysqlSql, new MySQLDialect());

        Column c1 = table.getColumn("C1");
        Column c111 = table.getColumn("C111");
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

        Dialect dialect = new SQLServerDialect();
        logger.info("SQLServer:" + table.sqlCreateString(dialect));

        assertEquals("tinyint", c1.getSqlType(dialect));
        assertEquals("tinyint", c111.getSqlType(dialect));
        assertEquals("smallint", c2.getSqlType(dialect));
        assertEquals("int", c3.getSqlType(dialect));
        assertEquals("int", c4.getSqlType(dialect));
        assertEquals("int", c5.getSqlType(dialect));
        assertEquals("bigint", c6.getSqlType(dialect));
        assertEquals("float", c7.getSqlType(dialect));
        assertEquals("float", c8.getSqlType(dialect));
        assertEquals("decimal(5,2)", c9.getSqlType(dialect));
        assertEquals("date", c10.getSqlType(dialect));
        assertEquals("time", c11.getSqlType(dialect));
        assertEquals("datetime", c12.getSqlType(dialect));
        assertEquals("datetime", c13.getSqlType(dialect));
        assertEquals("char(2)", c14.getSqlType(dialect));
        assertEquals("varchar(50)", c15.getSqlType(dialect));
        assertEquals("image", c16.getSqlType(dialect));
        assertEquals("varchar(50)", c17.getSqlType(dialect));
        assertEquals("image", c18.getSqlType(dialect));
        assertEquals("text", c19.getSqlType(dialect));
        assertEquals("image", c20.getSqlType(dialect));
        assertEquals("text", c21.getSqlType(dialect));
        assertEquals("image", c22.getSqlType(dialect));
        assertEquals("text", c23.getSqlType(dialect));
    }

    @Test
    public void testMySQLSql2Tbase() {
        String mysqlSql =
                "create table tbase2oracle (" +
                        "c1 tinyint," +
                        "c111 tinyint(20)," +
                        "c2 smallint," +
                        "c3 mediumint," +
                        "c4 int," +
                        "c5 integer," +
                        "c6 bigint," +
                        "c7 float," +
                        "c8 double," +
                        "c9 decimal(5,2)," +
                        "c10 date," +
                        "c11 time," +
                        "c12 datetime," +
                        "c13 timestamp," +
                        "c14 char(2)," +
                        "c15 varchar(50)," +
                        "c16 tinyblob," +
                        "c17 tinytext," +
                        "c18 blob," +
                        "c19 text," +
                        "c20 mediumblob," +
                        "c21 mediumtext," +
                        "c22 longblob," +
                        "c23 longtext" +
                        ")";

        ParserImpl parser = new ParserImpl();
        Table table = parser.parseTable(mysqlSql, new MySQLDialect());
        Dialect dialect = new TBaseDialect();
        logger.info("TBase:" + table.sqlCreateString(dialect));

        Column c1 = table.getColumn("C1");
        Column c111 = table.getColumn("C111");
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

        assertEquals("smallint", c1.getSqlType(dialect));
        assertEquals("smallint", c111.getSqlType(dialect));
        assertEquals("smallint", c2.getSqlType(dialect));
        assertEquals("integer", c3.getSqlType(dialect));
        assertEquals("integer", c4.getSqlType(dialect));
        assertEquals("integer", c5.getSqlType(dialect));
        assertEquals("bigint", c6.getSqlType(dialect));
        assertEquals("double precision", c7.getSqlType(dialect));
        assertEquals("double precision", c8.getSqlType(dialect));
        assertEquals("decimal(5,2)", c9.getSqlType(dialect));
        assertEquals("date", c10.getSqlType(dialect));
        assertEquals("time without time zone", c11.getSqlType(dialect));
        assertEquals("timestamp without time zone", c12.getSqlType(dialect));
        assertEquals("timestamp without time zone", c13.getSqlType(dialect));
        assertEquals("char(2)", c14.getSqlType(dialect));
        assertEquals("varchar(50)", c15.getSqlType(dialect));
        assertEquals("bytea", c16.getSqlType(dialect));
        assertEquals("varchar(50)", c17.getSqlType(dialect));
        assertEquals("bytea", c18.getSqlType(dialect));
        assertEquals("text", c19.getSqlType(dialect));
        assertEquals("bytea", c20.getSqlType(dialect));
        assertEquals("text", c21.getSqlType(dialect));
        assertEquals("bytea", c22.getSqlType(dialect));
        assertEquals("text", c23.getSqlType(dialect));
    }

    @Test
    public void testMySQLSql2Hive() {
        String mysqlSql =
                "create table tbase2oracle (" +
                        "c1 tinyint," +
                        "c111 tinyint(20)," +
                        "c2 smallint," +
                        "c3 mediumint," +
                        "c4 int," +
                        "c5 integer," +
                        "c6 bigint," +
                        "c7 float," +
                        "c8 double," +
                        "c9 decimal(5,2)," +
                        "c10 date," +
                        "c11 time," +
                        "c12 datetime," +
                        "c13 timestamp," +
                        "c14 char(2)," +
                        "c15 varchar(50)," +
                        "c16 tinyblob," +
                        "c17 tinytext," +
                        "c18 blob," +
                        "c19 text," +
                        "c20 mediumblob," +
                        "c21 mediumtext," +
                        "c22 longblob," +
                        "c23 longtext" +
                        ")";

        ParserImpl parser = new ParserImpl();
        Table table = parser.parseTable(mysqlSql, new MySQLDialect());

        Column c1 = table.getColumn("C1");
        Column c111 = table.getColumn("C111");
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

        Dialect dialect = new HiveDialect();
        logger.info("Hive:" + table.sqlCreateString(dialect));

        assertEquals("TINYINT", c1.getSqlType(dialect));
        assertEquals("TINYINT", c111.getSqlType(dialect));
        assertEquals("SMALLINT", c2.getSqlType(dialect));
        assertEquals("INT", c3.getSqlType(dialect));
        assertEquals("INT", c4.getSqlType(dialect));
        assertEquals("INT", c5.getSqlType(dialect));
        assertEquals("BIGINT", c6.getSqlType(dialect));
        assertEquals("FLOAT", c7.getSqlType(dialect));
        assertEquals("DOUBLE", c8.getSqlType(dialect));
        assertEquals("DECIMAL(5,2)", c9.getSqlType(dialect));
        assertEquals("DATE", c10.getSqlType(dialect));
        assertEquals("TIMESTAMP", c11.getSqlType(dialect));
        assertEquals("TIMESTAMP", c12.getSqlType(dialect));
        assertEquals("TIMESTAMP", c13.getSqlType(dialect));
        assertEquals("CHAR(2)", c14.getSqlType(dialect));
        assertEquals("VARCHAR(50)", c15.getSqlType(dialect));
        assertEquals("BINARY", c16.getSqlType(dialect));
        assertEquals("VARCHAR(50)", c17.getSqlType(dialect));
        assertEquals("BINARY", c18.getSqlType(dialect));
        assertEquals("BINARY", c19.getSqlType(dialect));
        assertEquals("BINARY", c20.getSqlType(dialect));
        assertEquals("BINARY", c21.getSqlType(dialect));
        assertEquals("BINARY", c22.getSqlType(dialect));
        assertEquals("BINARY", c23.getSqlType(dialect));
    }

/*
    @Test
    public void testIndexInfo2BigData() {
        String createTabSql =
                "CREATE TABLE index_test_pxd (\n" +
                        "    c1 BIGINT PRIMARY KEY,\n" +
                        "    c2 BIGINT,\n" +
                        "    c3 BIGINT,\n" +
                        "    c4 VARCHAR(100),\n" +
                        "    c5 VARCHAR(50)\n" +
                        ")";

        String createIdexSql1 = "CREATE UNIQUE INDEX index_test_pxd_idx_c2 ON index_test_pxd(c2 DESC)";
        String createIdexSql2 = "CREATE INDEX index_test_pxd_idx_c5 ON index_test_pxd(c5)";
        String createIndeiesSql = "CREATE INDEX index_test_pxd_idx_c3_c4 ON index_test_pxd(c3, c4)";
        jdbcTemplate.execute(createTabSql);
        jdbcTemplate.execute(createIdexSql1);
        jdbcTemplate.execute(createIdexSql2);
        jdbcTemplate.execute(createIndeiesSql);

        String gaussDbSql = Db2SqlApp.db2sql(DBType.MYSQL, DBType.GaussDB,
                mySqlDs, schemaName, "index_test_pxd", schemaName);
        System.out.println("GaussDb:\n" + gaussDbSql);

        String tbaseDbSql = Db2SqlApp.db2sql(DBType.MYSQL, DBType.TBase,
                mySqlDs, schemaName, "index_test_pxd", schemaName);
        System.out.println("TBase:\n" + tbaseDbSql);

        jdbcTemplate.execute("DROP TABLE IF EXISTS `index_test_pxd`");
    }

    @Test
    public void testPartitionTable() {
        jdbcTemplate.execute(
                "CREATE TABLE IF NOT EXISTS `test`.`p_range_date_test` (" +
                        "  `money` int(11), " +
                        "  `date` datetime " +
                        ") " +
                        " PARTITION BY RANGE (money) " +
                        "(PARTITION p2017 VALUES LESS THAN (2018), " +
                        " PARTITION p2018 VALUES LESS THAN (2019), " +
                        " PARTITION p2019 VALUES LESS THAN (2020)) ");

        jdbcTemplate.execute(
                "CREATE TABLE IF NOT EXISTS `test`.`p_list_test` ( " +
                        "  `a` int(11), " +
                        "  `b` int(11) " +
                        ") " +
                        " PARTITION BY LIST (b) " +
                        "(PARTITION p0 VALUES IN (1,3,5,7,9)," +
                        " PARTITION p1 VALUES IN (2,4,6,8,10))");

        jdbcTemplate.execute(
                "CREATE TABLE IF NOT EXISTS `test`.`p_hash4_tab_test` (" +
                        "  `id` int(11)," +
                        "  `date` datetime " +
                        ") " +
                        " PARTITION BY HASH (id) " +
                        "PARTITIONS 4 ");

        jdbcTemplate.execute(
                "CREATE TABLE test.p_range_2_test (" +
                        "    a INT," +
                        "    b INT" +
                        ")" +
                        "PARTITION BY RANGE COLUMNS(a, b) (" +
                        "    PARTITION p0 VALUES LESS THAN (5, 12)," +
                        "    PARTITION p3 VALUES LESS THAN (MAXVALUE, MAXVALUE)" +
                        ")");

        jdbcTemplate.execute(
                "CREATE TABLE test.p_list_2_test (" +
                        "    first_name VARCHAR(25)," +
                        "    last_name VARCHAR(25)," +
                        "    street_1 VARCHAR(30)," +
                        "    street_2 VARCHAR(30)," +
                        "    city VARCHAR(15)," +
                        "    renewal DATE" +
                        ")" +
                        "PARTITION BY LIST COLUMNS(renewal) (" +
                        "    PARTITION pWeek_1 VALUES IN('2010-02-01', '2010-02-02', '2010-02-03'," +
                        "        '2010-02-04', '2010-02-05', '2010-02-06', '2010-02-07')," +
                        "    PARTITION pWeek_2 VALUES IN('2010-02-08', '2010-02-09', '2010-02-10'," +
                        "        '2010-02-11', '2010-02-12', '2010-02-13', '2010-02-14')," +
                        "    PARTITION pWeek_3 VALUES IN('2010-02-15', '2010-02-16', '2010-02-17'," +
                        "        '2010-02-18', '2010-02-19', '2010-02-20', '2010-02-21')," +
                        "    PARTITION pWeek_4 VALUES IN('2010-02-22', '2010-02-23', '2010-02-24'," +
                        "        '2010-02-25', '2010-02-26', '2010-02-27', '2010-02-28')" +
                        ");");

        jdbcTemplate.execute(
                "CREATE TABLE test.p_linear_hash_test " +
                        "(col1 INT, col2 CHAR(5), col4 varchar(10), col3 DATE)" +
                        "    PARTITION BY LINEAR HASH(col1)" +
                        "    PARTITIONS 3");

        //GaussDB
        System.out.println("GaussDb1:\n" + Db2SqlApp.db2sql(DBType.MYSQL, DBType.GaussDB,
                mySqlDs, schemaName, "p_range_date_test", schemaName));

        System.out.println("GaussDb2:\n" + Db2SqlApp.db2sql(DBType.MYSQL, DBType.GaussDB,
                mySqlDs, schemaName, "p_list_test", schemaName));

        System.out.println("GaussDb3:\n" + Db2SqlApp.db2sql(DBType.MYSQL, DBType.GaussDB,
                mySqlDs, schemaName, "p_hash4_tab_test", schemaName));

        System.out.println("GaussDb4:\n" + Db2SqlApp.db2sql(DBType.MYSQL, DBType.GaussDB,
                mySqlDs, schemaName, "p_range_2_test", schemaName));

        System.out.println("GaussDb5:\n" + Db2SqlApp.db2sql(DBType.MYSQL, DBType.GaussDB,
                mySqlDs, schemaName, "p_list_2_test", schemaName));

        System.out.println("GaussDb6:\n" + Db2SqlApp.db2sql(DBType.MYSQL, DBType.GaussDB,
                mySqlDs, schemaName, "p_linear_hash_test", schemaName));

        //TBase
        System.out.println("TBase1:\n" + Db2SqlApp.db2sql(DBType.MYSQL, DBType.TBase,
                mySqlDs, schemaName, "p_range_date_test", schemaName));

        System.out.println("TBase2:\n" + Db2SqlApp.db2sql(DBType.MYSQL, DBType.TBase,
                mySqlDs, schemaName, "p_list_test", schemaName));

        System.out.println("TBase3:\n" + Db2SqlApp.db2sql(DBType.MYSQL, DBType.TBase,
                mySqlDs, schemaName, "p_hash4_tab_test", schemaName));

        System.out.println("TBase4:\n" + Db2SqlApp.db2sql(DBType.MYSQL, DBType.TBase,
                mySqlDs, schemaName, "p_range_2_test", schemaName));

        System.out.println("TBase5:\n" + Db2SqlApp.db2sql(DBType.MYSQL, DBType.TBase,
                mySqlDs, schemaName, "p_list_2_test", schemaName));

        System.out.println("TBase6:\n" + Db2SqlApp.db2sql(DBType.MYSQL, DBType.TBase,
                mySqlDs, schemaName, "p_linear_hash_test", schemaName));

        jdbcTemplate.execute("DROP TABLE IF EXISTS test.p_range_date_test");
        jdbcTemplate.execute("DROP TABLE IF EXISTS test.p_list_test");
        jdbcTemplate.execute("DROP TABLE IF EXISTS test.p_hash_tab_year_test");
        jdbcTemplate.execute("DROP TABLE IF EXISTS test.p_range_2_test");
        jdbcTemplate.execute("DROP TABLE IF EXISTS test.p_list_2_test");
        jdbcTemplate.execute("DROP TABLE IF EXISTS test.p_linear_hash_test");
    }
*/

}
