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
public class MySQLDb2SqlTest {

    public static final String ALL_DATATYPE_TEST = "all_datatype_test";

    private static JdbcTemplate jdbcTemplate;

    private static final String schemaName = "test";

    @BeforeClass
    public static void beforeClass() {
        BasicDataSource mySqlDs = new BasicDataSource();
        mySqlDs.setDriverClassName(DBType.MYSQL.getDriverName());
        mySqlDs.setUrl("jdbc:mysql://172.16.18.17:3306/test?useUnicode=true&characterEncoding=utf-8");
        mySqlDs.setUsername("root");
        mySqlDs.setPassword("root");
        jdbcTemplate = new JdbcTemplate(mySqlDs);

        String allColTabSql =
                "CREATE TABLE IF NOT EXISTS `all_datatype_test` ( " +
                        "  `c1` bigint(10) NOT NULL COMMENT 'c1', " +
                        "  `c2` binary(10), " +
                        "  `c3` bit(10), " +
                        "  `c4` tinyblob, " +
                        "  `c5` tinyint(1), " +
                        "  `c6` tinyint(5), " +
                        "  `c7` char(10) NOT NULL DEFAULT 'a', " +
                        "  `c8` date, " +
                        "  `c9` datetime(6), " +
                        "  `c10` decimal(10,2), " +
                        "  `c11` double(8,2), " +
                        "  `c12` enum('a','b'), " +
                        "  `c13` float(6,3), " +
                        "  `c14` int(10), " +
                        "  `c15` longblob, " +
                        "  `c16` longtext, " +
                        "  `c17` mediumblob, " +
                        "  `c18` mediumint(20), " +
                        "  `c19` mediumtext, " +
                        "  `c20` decimal(15,4), " +
                        "  `c21` double(10,3), " +
                        "  `c22` set('b','c'), " +
                        "  `c23` smallint(10), " +
                        "  `c24` tinytext, " +
                        "  `c25` time(6), " +
                        "  `c26` timestamp(6) NULL, " +
                        "  `c27` tinyblob, " +
                        "  `c28` tinyint(10), " +
                        "  `c29` tinytext, " +
                        "  `c30` varbinary(10), " +
                        "  `c31` varchar(50), " +
                        "  `c32` year(4), " +
                        "  PRIMARY KEY (`c1`) " +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8 ";
        jdbcTemplate.execute(allColTabSql);
    }

    @AfterClass
    public static void afterClass() {
        jdbcTemplate.execute("DROP TABLE IF EXISTS `all_datatype_test`");
    }

    private Table getTable() {
        AbstractDbMetaInfoDao dao = DBType.MYSQL.getDbDao(jdbcTemplate);
        Dialect dialect = DBType.MYSQL.getDialect();
        return dao.queryTable(null, schemaName, ALL_DATATYPE_TEST, dialect);
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

        Dialect destDialect = DBType.GaussDB.getDialect();

        assertEquals("bigint", c1.getSqlType(destDialect));
        assertEquals("binary(10)", c2.getSqlType(destDialect));
        assertEquals("varchar(10)", c3.getSqlType(destDialect));
        assertEquals("blob", c4.getSqlType(destDialect));
        assertEquals("tinyint", c5.getSqlType(destDialect));
        assertEquals("tinyint", c6.getSqlType(destDialect));
        assertEquals("char(10)", c7.getSqlType(destDialect));
        assertEquals("date", c8.getSqlType(destDialect));
        assertEquals("date", c9.getSqlType(destDialect));
        assertEquals("number(10,2)", c10.getSqlType(destDialect));
        assertEquals("double", c11.getSqlType(destDialect));
        assertEquals("varchar(1)", c12.getSqlType(destDialect));
        assertEquals("float", c13.getSqlType(destDialect));
        assertEquals("int", c14.getSqlType(destDialect));
        assertEquals("blob", c15.getSqlType(destDialect));
        assertEquals("clob", c16.getSqlType(destDialect));
        assertEquals("blob", c17.getSqlType(destDialect));
        assertEquals("int", c18.getSqlType(destDialect));
        assertEquals("clob", c19.getSqlType(destDialect));
        assertEquals("number(15,4)", c20.getSqlType(destDialect));
        assertEquals("double", c21.getSqlType(destDialect));
        assertEquals("varchar(3)", c22.getSqlType(destDialect));
        assertEquals("smallint", c23.getSqlType(destDialect));
        assertEquals("clob", c24.getSqlType(destDialect));
        assertEquals("date", c25.getSqlType(destDialect));
        assertEquals("timestamp", c26.getSqlType(destDialect));
        assertEquals("blob", c27.getSqlType(destDialect));
        assertEquals("tinyint", c28.getSqlType(destDialect));
        assertEquals("clob", c29.getSqlType(destDialect));
        assertEquals("varbinary(10)", c30.getSqlType(destDialect));
        assertEquals("varchar(50)", c31.getSqlType(destDialect));
        assertEquals("smallint", c32.getSqlType(destDialect));
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

        Dialect destDialect = DBType.TBase.getDialect();

        assertEquals("bigint", c1.getSqlType(destDialect));
        assertEquals("bytea", c2.getSqlType(destDialect));
        assertEquals("bit(10)", c3.getSqlType(destDialect));
        assertEquals("bytea", c4.getSqlType(destDialect));
        assertEquals("smallint", c5.getSqlType(destDialect));
        assertEquals("smallint", c6.getSqlType(destDialect));
        assertEquals("char(10)", c7.getSqlType(destDialect));
        assertEquals("date", c8.getSqlType(destDialect));
        assertEquals("date", c9.getSqlType(destDialect));
        assertEquals("numeric(10,2)", c10.getSqlType(destDialect));
        assertEquals("double precision", c11.getSqlType(destDialect));
        assertEquals("varchar(1)", c12.getSqlType(destDialect));
        assertEquals("double precision", c13.getSqlType(destDialect));
        assertEquals("integer", c14.getSqlType(destDialect));
        assertEquals("bytea", c15.getSqlType(destDialect));
        assertEquals("text", c16.getSqlType(destDialect));
        assertEquals("bytea", c17.getSqlType(destDialect));
        assertEquals("integer", c18.getSqlType(destDialect));
        assertEquals("text", c19.getSqlType(destDialect));
        assertEquals("numeric(15,4)", c20.getSqlType(destDialect));
        assertEquals("double precision", c21.getSqlType(destDialect));
        assertEquals("varchar(3)", c22.getSqlType(destDialect));
        assertEquals("smallint", c23.getSqlType(destDialect));
        assertEquals("text", c24.getSqlType(destDialect));
        assertEquals("time(6)", c25.getSqlType(destDialect));
        assertEquals("timestamp(6)", c26.getSqlType(destDialect));
        assertEquals("bytea", c27.getSqlType(destDialect));
        assertEquals("smallint", c28.getSqlType(destDialect));
        assertEquals("text", c29.getSqlType(destDialect));
        assertEquals("bytea", c30.getSqlType(destDialect));
        assertEquals("varchar(50)", c31.getSqlType(destDialect));
        assertEquals("smallint", c32.getSqlType(destDialect));
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

        Dialect destDialect = DBType.ORACLE.getDialect();

        assertEquals("number(19,0)", c1.getSqlType(destDialect));
        assertEquals("raw(10)", c2.getSqlType(destDialect));
        assertEquals("number(10)", c3.getSqlType(destDialect));
        assertEquals("blob", c4.getSqlType(destDialect));
        assertEquals("number(3,0)", c5.getSqlType(destDialect));
        assertEquals("number(3,0)", c6.getSqlType(destDialect));
        assertEquals("char(10)", c7.getSqlType(destDialect));
        assertEquals("date", c8.getSqlType(destDialect));
        assertEquals("date", c9.getSqlType(destDialect));
        assertEquals("number(10,2)", c10.getSqlType(destDialect));
        assertEquals("double precision", c11.getSqlType(destDialect));
        assertEquals("varchar2(1)", c12.getSqlType(destDialect));
        assertEquals("float", c13.getSqlType(destDialect));
        assertEquals("number(10,0)", c14.getSqlType(destDialect));
        assertEquals("blob", c15.getSqlType(destDialect));
        assertEquals("clob", c16.getSqlType(destDialect));
        assertEquals("blob", c17.getSqlType(destDialect));
        assertEquals("number(10,0)", c18.getSqlType(destDialect));
        assertEquals("clob", c19.getSqlType(destDialect));
        assertEquals("number(15,4)", c20.getSqlType(destDialect));
        assertEquals("double precision", c21.getSqlType(destDialect));
        assertEquals("varchar2(3)", c22.getSqlType(destDialect));
        assertEquals("number(5,0)", c23.getSqlType(destDialect));
        assertEquals("clob", c24.getSqlType(destDialect));
        assertEquals("date", c25.getSqlType(destDialect));
        assertEquals("timestamp", c26.getSqlType(destDialect));
        assertEquals("blob", c27.getSqlType(destDialect));
        assertEquals("number(3,0)", c28.getSqlType(destDialect));
        assertEquals("clob", c29.getSqlType(destDialect));
        assertEquals("raw(10)", c30.getSqlType(destDialect));
        assertEquals("varchar2(50)", c31.getSqlType(destDialect));
        assertEquals("number(5,0)", c32.getSqlType(destDialect));
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

        Dialect destDialect = DBType.SQLServer.getDialect();

        assertEquals("bigint", c1.getSqlType(destDialect));
        assertEquals("binary(10)", c2.getSqlType(destDialect));
        assertEquals("varchar(10)", c3.getSqlType(destDialect));
        assertEquals("varbinary", c4.getSqlType(destDialect));
        assertEquals("tinyint", c5.getSqlType(destDialect));
        assertEquals("tinyint", c6.getSqlType(destDialect));
        assertEquals("char(10)", c7.getSqlType(destDialect));
        assertEquals("date", c8.getSqlType(destDialect));
        assertEquals("date", c9.getSqlType(destDialect));
        assertEquals("numeric(10,2)", c10.getSqlType(destDialect));
        assertEquals("float", c11.getSqlType(destDialect));
        assertEquals("varchar(1)", c12.getSqlType(destDialect));
        assertEquals("float", c13.getSqlType(destDialect));
        assertEquals("int", c14.getSqlType(destDialect));
        assertEquals("varbinary", c15.getSqlType(destDialect));
        assertEquals("text", c16.getSqlType(destDialect));
        assertEquals("varbinary", c17.getSqlType(destDialect));
        assertEquals("int", c18.getSqlType(destDialect));
        assertEquals("text", c19.getSqlType(destDialect));
        assertEquals("numeric(15,4)", c20.getSqlType(destDialect));
        assertEquals("float", c21.getSqlType(destDialect));
        assertEquals("varchar(3)", c22.getSqlType(destDialect));
        assertEquals("smallint", c23.getSqlType(destDialect));
        assertEquals("text", c24.getSqlType(destDialect));
        assertEquals("time", c25.getSqlType(destDialect));
        assertEquals("datetime", c26.getSqlType(destDialect));
        assertEquals("varbinary", c27.getSqlType(destDialect));
        assertEquals("tinyint", c28.getSqlType(destDialect));
        assertEquals("text", c29.getSqlType(destDialect));
        assertEquals("varbinary", c30.getSqlType(destDialect));
        assertEquals("varchar(50)", c31.getSqlType(destDialect));
        assertEquals("smallint", c32.getSqlType(destDialect));
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

        Dialect destDialect = DBType.MYSQL.getDialect();

        assertEquals("bigint", c1.getSqlType(destDialect));
        assertEquals("binary(10)", c2.getSqlType(destDialect));
        assertEquals("bit(10)", c3.getSqlType(destDialect));
        assertEquals("longblob", c4.getSqlType(destDialect));
        assertEquals("tinyint", c5.getSqlType(destDialect));
        assertEquals("tinyint", c6.getSqlType(destDialect));
        assertEquals("char(10)", c7.getSqlType(destDialect));
        assertEquals("datetime", c8.getSqlType(destDialect));
        assertEquals("datetime", c9.getSqlType(destDialect));
        assertEquals("decimal(10,2)", c10.getSqlType(destDialect));
        assertEquals("double", c11.getSqlType(destDialect));
        assertEquals("varchar(1)", c12.getSqlType(destDialect));
        assertEquals("float", c13.getSqlType(destDialect));
        assertEquals("integer", c14.getSqlType(destDialect));
        assertEquals("longblob", c15.getSqlType(destDialect));
        assertEquals("longtext", c16.getSqlType(destDialect));
        assertEquals("longblob", c17.getSqlType(destDialect));
        assertEquals("integer", c18.getSqlType(destDialect));
        assertEquals("longtext", c19.getSqlType(destDialect));
        assertEquals("decimal(15,4)", c20.getSqlType(destDialect));
        assertEquals("double", c21.getSqlType(destDialect));
        assertEquals("varchar(3)", c22.getSqlType(destDialect));
        assertEquals("smallint", c23.getSqlType(destDialect));
        assertEquals("longtext", c24.getSqlType(destDialect));
        assertEquals("time", c25.getSqlType(destDialect));
        assertEquals("timestamp", c26.getSqlType(destDialect));
        assertEquals("longblob", c27.getSqlType(destDialect));
        assertEquals("tinyint", c28.getSqlType(destDialect));
        assertEquals("longtext", c29.getSqlType(destDialect));
        assertEquals("tinyblob", c30.getSqlType(destDialect));
        assertEquals("varchar(50)", c31.getSqlType(destDialect));
        assertEquals("smallint", c32.getSqlType(destDialect));
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

        Dialect destDialect = DBType.HIVE.getDialect();

        assertEquals("BIGINT", c1.getSqlType(destDialect));
        assertEquals("BINARY", c2.getSqlType(destDialect));
        assertEquals("STRING", c3.getSqlType(destDialect));
        assertEquals("BINARY", c4.getSqlType(destDialect));
        assertEquals("TINYINT", c5.getSqlType(destDialect));
        assertEquals("TINYINT", c6.getSqlType(destDialect));
        assertEquals("CHAR(10)", c7.getSqlType(destDialect));
        assertEquals("DATE", c8.getSqlType(destDialect));
        assertEquals("DATE", c9.getSqlType(destDialect));
        assertEquals("DECIMAL(10,2)", c10.getSqlType(destDialect));
        assertEquals("DOUBLE", c11.getSqlType(destDialect));
        assertEquals("VARCHAR(1)", c12.getSqlType(destDialect));
        assertEquals("FLOAT", c13.getSqlType(destDialect));
        assertEquals("INT", c14.getSqlType(destDialect));
        assertEquals("BINARY", c15.getSqlType(destDialect));
        assertEquals("STRING", c16.getSqlType(destDialect));
        assertEquals("BINARY", c17.getSqlType(destDialect));
        assertEquals("INT", c18.getSqlType(destDialect));
        assertEquals("STRING", c19.getSqlType(destDialect));
        assertEquals("DECIMAL(15,4)", c20.getSqlType(destDialect));
        assertEquals("DOUBLE", c21.getSqlType(destDialect));
        assertEquals("VARCHAR(3)", c22.getSqlType(destDialect));
        assertEquals("SMALLINT", c23.getSqlType(destDialect));
        assertEquals("STRING", c24.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c25.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c26.getSqlType(destDialect));
        assertEquals("BINARY", c27.getSqlType(destDialect));
        assertEquals("TINYINT", c28.getSqlType(destDialect));
        assertEquals("STRING", c29.getSqlType(destDialect));
        assertEquals("BINARY", c30.getSqlType(destDialect));
        assertEquals("VARCHAR(50)", c31.getSqlType(destDialect));
        assertEquals("SMALLINT", c32.getSqlType(destDialect));
    }

}
