package com.huacloud.synctable.dao;

import com.huacloud.synctable.entity.ConstraintInfo;
import com.huacloud.synctable.mapping.Column;
import com.huacloud.synctable.mapping.Index;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import com.huacloud.synctable.mapping.Types;
import java.util.List;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/26/2019 6:10 PM
 */
public class GaussDbMetaInfoDaoImplTest {

    private static GaussDbMetaInfoDaoImpl dao;

    private static BasicDataSource ds;

    @BeforeClass
    public static void beforeClass() {
        ds = new BasicDataSource();
        ds.setDriverClassName("com.huawei.gauss.jdbc.ZenithDriver");
        ds.setUrl("jdbc:zenith:@172.16.121.28:1888");
        ds.setUsername("pxd");
        ds.setPassword("Pengxd@123");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        dao = new GaussDbMetaInfoDaoImpl(jdbcTemplate);
    }

    @Test
    public void testGD() {
        ds = new BasicDataSource();
        ds.setDriverClassName("com.huawei.gauss.jdbc.ZenithDriver");
        ds.setUrl("jdbc:zenith:@172.16.121.28:1888");
        ds.setUsername("pxd");
        ds.setPassword("Pengxd@123");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        dao = new GaussDbMetaInfoDaoImpl(jdbcTemplate);

        Object[] args = new Object[2];
        args[0] = "1";
        args[1] = "2019-09-04 19:36:15.199000000";
        int[] argTypes = {Types.VARCHAR, Types.VARCHAR};

        jdbcTemplate.update("insert into test_ogg.tt_test(c0, c1) values (?, ?)", args, argTypes);
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        ds.close();
    }

    @Test
    public void testQueryIndexInfo() {
        List<Index> indices = dao.queryIndexInfo("", "PXD", "TEST_QUERY_COL2");
        System.out.println(indices.toString());
    }

    @Test
    public void testQueryAllSchemaNames() {
        List<String> schemaNames = dao.queryAllSchemaNames();
        System.out.println(schemaNames);
    }

    @Test
    public void testQueryAllTabNames() {
        List<String> tabNames = dao.queryAllTabNames("SYS");
        System.out.println(tabNames);
    }

    @Test
    public void testQueryTableComment() {
        String tableComment = dao.queryTableComment("", "PXD", "TEST_TABLE");
        System.out.println(tableComment);
    }

    @Test
    public void testQueryColumnInfo() {
        List<Column> columnList = dao.queryColumnInfo("", "PXD", "TEST_QUERY_COL");
        System.out.println(columnList);
    }

    @Test
    public void testQueryConstraintInfo() {
        List<ConstraintInfo> constraintInfos =
                dao.queryConstraintInfo("", "PXD", "TEST_QUERY_COL2");
        System.out.println(constraintInfos);
    }

}
