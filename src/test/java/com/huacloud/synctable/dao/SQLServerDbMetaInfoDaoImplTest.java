package com.huacloud.synctable.dao;

import com.huacloud.synctable.entity.ConstraintInfo;
import com.huacloud.synctable.mapping.Column;
import com.huacloud.synctable.mapping.Index;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/16/2019 3:23 PM
 */
public class SQLServerDbMetaInfoDaoImplTest {

    private SQLServerDbMetaInfoDaoImpl dao;

    @Before
    public void beforeClass() {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        ds.setUrl("jdbc:sqlserver://172.16.18.16:1433; DatabaseName=pxd_test");
        ds.setUsername("sa");
        ds.setPassword("admin@123");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        dao = new SQLServerDbMetaInfoDaoImpl(jdbcTemplate);
    }

    @Test
    public void testQueryTableComment() {
        String tableComment = dao.queryTableComment("", "dbo", "sqlserver_test_pxd");
        System.out.println(tableComment);
    }

    @Test
    public void testQueryColumnInfo() {
        List<Column> columnList = dao.queryColumnInfo("", "dbo", "sqlserver_test_pxd");
        columnList.forEach(System.out::println);
    }

    @Test
    public void testQueryConstraintInfo() {
        List<ConstraintInfo> constraintInfos =
                dao.queryConstraintInfo("", "dbo", "sqlserver_test_pxd");
        constraintInfos.forEach(System.out::println);
    }

    @Test
    public void testQueryAllSchemaNames() {
        List<String> names = dao.queryAllSchemaNames();
        names.forEach(System.out::println);
    }

    @Test
    public void testQueryAllTabNames() {
        List<String> names = dao.queryAllTabNames("dbo");
        names.forEach(System.out::println);
    }

    @Test
    public void testQueryIndexInfo() {
        List<Index> indices = dao.queryIndexInfo("", "dbo", "sqlserver_test_pxd");
        System.out.println(indices.toString());
    }

}
