package com.huacloud.synctable.dao;

import com.huacloud.synctable.entity.DBType;
import com.huacloud.synctable.mapping.Index;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.List;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/21/2019 11:08 AM
 */
public class TBaseDbMetaInfoDaoImplTest {

    private static TBaseDbMetaInfoDaoImpl dao;

    private static BasicDataSource ds;

    @BeforeClass
    public static void beforeClass() {
        ds = new BasicDataSource();
        ds.setDriverClassName(DBType.TBase.getDriverName());
        ds.setUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        ds.setUsername("postgres");
        ds.setPassword("123456");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        dao = new TBaseDbMetaInfoDaoImpl(jdbcTemplate);

    }

    @AfterClass
    public static void afterClass() throws SQLException {
        ds.close();
    }

    @Test
    public void testQueryIndexInfo() {
        List<Index> indices = dao.queryIndexInfo("postgres", "test_ogg", "t_range");
        System.out.println(indices.toString());
    }

}
