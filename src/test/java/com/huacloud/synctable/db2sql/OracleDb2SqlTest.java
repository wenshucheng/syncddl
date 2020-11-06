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
public class OracleDb2SqlTest {

    public static final String ALL_DATATYPE_TEST = "ORCLE_TEST_TAB_PXD178";

    private static JdbcTemplate jdbcTemplate;

    private static final String SCHEMA_NAME = "TEST_OGG";

    @BeforeClass
    public static void beforeClass() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(DBType.ORACLE.getDriverName());
        dataSource.setUrl("jdbc:oracle:thin:@//172.16.18.16:1521/oracle");
        dataSource.setUsername("test_ogg");
        dataSource.setPassword("pxd178");
        jdbcTemplate = new JdbcTemplate(dataSource);

        String createTabSql =
                "CREATE TABLE " + ALL_DATATYPE_TEST +
                        "( " +
                        "  c1  NUMBER(10) PRIMARY KEY, " +
                        "  c2  BINARY_DOUBLE, " +
                        "  c3  BINARY_FLOAT, " +
                        "  c4  BLOB, " +
                        "  c5  CLOB, " +
                        "  c6  CHAR(8), " +
                        "  c7  DATE, " +
                        "  c8  INTERVAL DAY(2) TO SECOND(6), " +
                        "  c9  INTERVAL YEAR(2) TO MONTH, " +
                        "  c10 VARCHAR2(500), " +
                        "  c11 LONG RAW, " +
                        "  c12 NCLOB, " +
                        "  c13 NUMBER, " +
                        "  c14 NVARCHAR2(50), " +
                        "  c15 RAW(100), " +
                        "  c16 TIMESTAMP(6), " +
                        "  c17 TIMESTAMP(6) WITH LOCAL TIME ZONE, " +
                        "  c18 TIMESTAMP(6) WITH TIME ZONE, " +
                        "  c19 TIMESTAMP, " +
                        "  c20 FLOAT" +
                        ")";
        jdbcTemplate.execute(createTabSql);
    }

    @AfterClass
    public static void afterClass() {
        jdbcTemplate.execute("DROP TABLE " + ALL_DATATYPE_TEST);
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

        Dialect destDialect = DBType.GaussDB.getDialect();

        assertEquals("number(10,0)", c1.getSqlType(destDialect));
        assertEquals("double", c2.getSqlType(destDialect));
        assertEquals("float", c3.getSqlType(destDialect));
        assertEquals("blob", c4.getSqlType(destDialect));
        assertEquals("clob", c5.getSqlType(destDialect));
        assertEquals("char(8)", c6.getSqlType(destDialect));
        assertEquals("date", c7.getSqlType(destDialect));
        assertEquals("interval day(2) to second(6)", c8.getSqlType(destDialect));
        assertEquals("interval year(2) to month", c9.getSqlType(destDialect));
        assertEquals("varchar(500)", c10.getSqlType(destDialect));
        assertEquals("blob", c11.getSqlType(destDialect));
        assertEquals("clob", c12.getSqlType(destDialect));
        assertEquals("number", c13.getSqlType(destDialect));
        assertEquals("nvarchar(50)", c14.getSqlType(destDialect));
        assertEquals("binary(100)", c15.getSqlType(destDialect));
        assertEquals("timestamp(6)", c16.getSqlType(destDialect));
        assertEquals("timestamp(6) with local time zone", c17.getSqlType(destDialect));
        assertEquals("timestamp(6) with time zone", c18.getSqlType(destDialect));
        assertEquals("timestamp(6)", c19.getSqlType(destDialect));//oracle默认是6
        assertEquals("float", c20.getSqlType(destDialect));
    }

    private Table getTable() {
        AbstractDbMetaInfoDao dao = DBType.ORACLE.getDbDao(jdbcTemplate);
        Dialect dialect = DBType.ORACLE.getDialect();
        return dao.queryTable(null, SCHEMA_NAME, ALL_DATATYPE_TEST, dialect);
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

        Dialect destDialect = DBType.TBase.getDialect();
        assertEquals("numeric(10,0)", c1.getSqlType(destDialect));
        assertEquals("double precision", c2.getSqlType(destDialect));
        assertEquals("double precision", c3.getSqlType(destDialect));
        assertEquals("bytea", c4.getSqlType(destDialect));
        assertEquals("text", c5.getSqlType(destDialect));
        assertEquals("char(8)", c6.getSqlType(destDialect));
        assertEquals("date", c7.getSqlType(destDialect));
        assertEquals("interval day to second", c8.getSqlType(destDialect));
        assertEquals("interval year to month", c9.getSqlType(destDialect));
        assertEquals("varchar(500)", c10.getSqlType(destDialect));
        assertEquals("bytea", c11.getSqlType(destDialect));
        assertEquals("text", c12.getSqlType(destDialect));
        assertEquals("numeric", c13.getSqlType(destDialect));
        assertEquals("varchar(50)", c14.getSqlType(destDialect));
        assertEquals("bytea", c15.getSqlType(destDialect));
        assertEquals("timestamp", c16.getSqlType(destDialect));
        assertEquals("timestamp with time zone", c17.getSqlType(destDialect));
        assertEquals("timestamp with time zone", c18.getSqlType(destDialect));
        assertEquals("timestamp", c19.getSqlType(destDialect));//oracle默认是6
        assertEquals("double precision", c20.getSqlType(destDialect));
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

        Dialect destDialect = DBType.ORACLE.getDialect();
        assertEquals("number(10,0)", c1.getSqlType(destDialect));
        assertEquals("double precision", c2.getSqlType(destDialect));
        assertEquals("float", c3.getSqlType(destDialect));
        assertEquals("blob", c4.getSqlType(destDialect));
        assertEquals("clob", c5.getSqlType(destDialect));
        assertEquals("char(8)", c6.getSqlType(destDialect));
        assertEquals("date", c7.getSqlType(destDialect));
        assertEquals("interval day(2) to second(6)", c8.getSqlType(destDialect));
        assertEquals("interval year(2) to month", c9.getSqlType(destDialect));
        assertEquals("varchar2(500)", c10.getSqlType(destDialect));
        assertEquals("long raw", c11.getSqlType(destDialect));
        assertEquals("nclob", c12.getSqlType(destDialect));
        assertEquals("number", c13.getSqlType(destDialect));
        assertEquals("nvarchar2(50)", c14.getSqlType(destDialect));
        assertEquals("raw(100)", c15.getSqlType(destDialect));
        assertEquals("timestamp", c16.getSqlType(destDialect));
        assertEquals("timestamp with local time zone", c17.getSqlType(destDialect));
        assertEquals("timestamp with time zone", c18.getSqlType(destDialect));
        assertEquals("timestamp", c19.getSqlType(destDialect));
        assertEquals("float", c20.getSqlType(destDialect));
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

        Dialect destDialect = DBType.SQLServer.getDialect();
        assertEquals("numeric(10,0)", c1.getSqlType(destDialect));
        assertEquals("float", c2.getSqlType(destDialect));
        assertEquals("float", c3.getSqlType(destDialect));
        assertEquals("image", c4.getSqlType(destDialect));
        assertEquals("text", c5.getSqlType(destDialect));
        assertEquals("char(8)", c6.getSqlType(destDialect));
        assertEquals("date", c7.getSqlType(destDialect));
        assertEquals("numeric(20,0)", c8.getSqlType(destDialect));
        assertEquals("numeric(20,0)", c9.getSqlType(destDialect));
        assertEquals("varchar(500)", c10.getSqlType(destDialect));
        assertEquals("image", c11.getSqlType(destDialect));
        assertEquals("ntext", c12.getSqlType(destDialect));
        assertEquals("numeric", c13.getSqlType(destDialect));
        assertEquals("nvarchar(50)", c14.getSqlType(destDialect));
        assertEquals("binary", c15.getSqlType(destDialect));
        assertEquals("datetime", c16.getSqlType(destDialect));
        assertEquals("datetimeoffset", c17.getSqlType(destDialect));
        assertEquals("datetime", c18.getSqlType(destDialect));
        assertEquals("datetime", c19.getSqlType(destDialect));
        assertEquals("float", c20.getSqlType(destDialect));
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

        Dialect destDialect = DBType.MYSQL.getDialect();
        assertEquals("decimal(10,0)", c1.getSqlType(destDialect));
        assertEquals("double", c2.getSqlType(destDialect));
        assertEquals("float", c3.getSqlType(destDialect));
        assertEquals("longblob", c4.getSqlType(destDialect));
        assertEquals("longtext", c5.getSqlType(destDialect));
        assertEquals("char(8)", c6.getSqlType(destDialect));
        assertEquals("datetime", c7.getSqlType(destDialect));
        assertEquals("bigint", c8.getSqlType(destDialect));
        assertEquals("bigint", c9.getSqlType(destDialect));
        assertEquals("varchar(500)", c10.getSqlType(destDialect));
        assertEquals("mediumblob", c11.getSqlType(destDialect));
        assertEquals("longtext", c12.getSqlType(destDialect));
        assertEquals("decimal", c13.getSqlType(destDialect));
        assertEquals("varchar(50)", c14.getSqlType(destDialect));
        assertEquals("binary(100)", c15.getSqlType(destDialect));
        assertEquals("timestamp", c16.getSqlType(destDialect));
        assertEquals("timestamp", c17.getSqlType(destDialect));
        assertEquals("timestamp", c18.getSqlType(destDialect));
        assertEquals("timestamp", c19.getSqlType(destDialect));
        assertEquals("float", c20.getSqlType(destDialect));
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

        Dialect destDialect = DBType.HIVE.getDialect();
        assertEquals("DECIMAL(10,0)", c1.getSqlType(destDialect));
        assertEquals("DOUBLE", c2.getSqlType(destDialect));
        assertEquals("FLOAT", c3.getSqlType(destDialect));
        assertEquals("BINARY", c4.getSqlType(destDialect));
        assertEquals("STRING", c5.getSqlType(destDialect));
        assertEquals("CHAR(8)", c6.getSqlType(destDialect));
        assertEquals("DATE", c7.getSqlType(destDialect));
        assertEquals("DECIMAL(20,0)", c8.getSqlType(destDialect));
        assertEquals("DECIMAL(20,0)", c9.getSqlType(destDialect));
        assertEquals("VARCHAR(500)", c10.getSqlType(destDialect));
        assertEquals("BINARY", c11.getSqlType(destDialect));
        assertEquals("STRING", c12.getSqlType(destDialect));
        assertEquals("DECIMAL", c13.getSqlType(destDialect));
        assertEquals("VARCHAR(50)", c14.getSqlType(destDialect));
        assertEquals("BINARY", c15.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c16.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c17.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c18.getSqlType(destDialect));
        assertEquals("TIMESTAMP", c19.getSqlType(destDialect));
        assertEquals("FLOAT", c20.getSqlType(destDialect));
    }

}
