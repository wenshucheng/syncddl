package com.huacloud.synctable.dao;

import com.huacloud.synctable.entity.DBType;
import com.huacloud.synctable.mapping.Column;
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
 * @date 8/20/2019 5:31 PM
 */
public class MySQLDbMetaInfoDaoImplTest {

    private static MySqlDbMetaInfoDaoImpl dao;

    private static BasicDataSource ds;

    @BeforeClass
    public static void beforeClass() {
        ds = new BasicDataSource();
        ds.setDriverClassName(DBType.MYSQL.getDriverName());
        ds.setUrl("jdbc:mysql://172.16.18.16:3306/test?useUnicode=true&characterEncoding=utf-8");
        ds.setUsername("root");
        ds.setPassword("root");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        dao = new MySqlDbMetaInfoDaoImpl(jdbcTemplate);
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        ds.close();
    }

    @Test
    public void testQueryColumnInfo() {
        List<Column> columns = dao.queryColumnInfo("", "hds_etl", "h_data_source");
        System.out.println(columns);
    }

    @Test
    public void testQueryIndexInfo() {
        List<Index> indices = dao.queryIndexInfo("", "test", "word");
        System.out.println(indices.toString());
    }

    @Test
    public void testQueryPartitionInfo() {
        Table table = new Table();
        table.setSchema("test_ogg");
        table.setName("tts");
        dao.setPartitionInfo(table);

        System.out.println(table);
    }

}
