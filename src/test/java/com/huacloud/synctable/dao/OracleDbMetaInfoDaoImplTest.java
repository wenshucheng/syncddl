package com.huacloud.synctable.dao;

import com.huacloud.synctable.entity.DBType;
import com.huacloud.synctable.mapping.Index;
import com.huacloud.synctable.mapping.Table;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.List;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/20/2019 6:16 PM
 */
public class OracleDbMetaInfoDaoImplTest {

    private static OracleDbMetaInfoDaoImpl dao;

    private static BasicDataSource ds;

    @BeforeClass
    public static void beforeClass() {
        ds = new BasicDataSource();
        ds.setDriverClassName(DBType.ORACLE.getDriverName());
        ds.setUrl("jdbc:oracle:thin:@//172.16.18.16:1521/XE");
        ds.setUsername("ogg");
        ds.setPassword("ogg");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        dao = new OracleDbMetaInfoDaoImpl(jdbcTemplate);
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        ds.close();
    }

    @Test
    public void testQueryIndexInfo() {
        List<Index> indices = dao.queryIndexInfo("", "PXD", "PXD_TEST_DDL");
        System.out.println(indices.toString());
    }

    @Test
    public void testQueryPartitionInfo() {
        Table table = new Table();
        table.setSchema("SYS");
        table.setName("WRI$_OPTSTAT_SYNOPSIS$");
        dao.setPartitionInfo(table);

        System.out.println(table);
    }

}
