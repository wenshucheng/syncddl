package com.huacloud.synctable.dao;

import com.huacloud.synctable.entity.ConstraintInfo;
import com.huacloud.synctable.entity.DBType;
import com.huacloud.synctable.mapping.Column;
import com.huacloud.synctable.mapping.Index;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/16/2019 5:50 PM
 */
public class HiveDbMetaInfoDaoImplTest {

    private static HiveDbMetaInfoDao dao;

    private static BasicDataSource ds;

    @BeforeClass
    public static void beforeClass() {
        ds = new BasicDataSource();
        ds.setDriverClassName(DBType.HIVE.getDriverName());
        ds.setUrl("jdbc:hive2://172.16.18.18:10000/default");
        ds.setUsername("hive");
        ds.setPassword("hive");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        dao = new HiveDbMetaInfoDao(jdbcTemplate);

        String tableName = "testHiveDriverTable";
        jdbcTemplate.execute("drop table if exists " + tableName);
        jdbcTemplate.execute(
                "create table " + tableName +
                        " (key int comment 'key col comment', value string comment 'value col comment') " +
                        "comment 'table comment'");

    }

    @AfterClass
    public static void afterClass() throws SQLException {
        ds.close();
    }

    @Test
    public void testQueryTableComment() {
        String tableComment = dao.queryTableComment("", "default", "testHiveDriverTable");
        System.out.println(tableComment);
        Assert.assertEquals("table comment", tableComment);
    }

    @Test
    public void testQueryColumnInfo() {
        List<Column> columnList = dao.queryColumnInfo("", "default", "testHiveDriverTable");
        columnList.forEach(System.out::println);
    }

    @Test
    public void testQueryConstraintInfo() {
        List<ConstraintInfo> constraintInfos =
                dao.queryConstraintInfo("", "default", "testHiveDriverTable");
        constraintInfos.forEach(System.out::println);
    }

    @Test
    public void testQueryAllSchemaNames() {
        List<String> names = dao.queryAllSchemaNames();
        names.forEach(System.out::println);
    }

    @Test
    public void testQueryAllTabNames() {
        List<String> names = dao.queryAllTabNames("default");
        names.forEach(System.out::println);
    }

    @Test
    public void testHiveDb() throws SQLException {
        try {
            Class.forName(DBType.HIVE.getDriverName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection(
                "jdbc:hive2://172.16.18.18:10000/default", "hive", "hive");
        Statement stmt = con.createStatement();
        String tableName = "testHiveDriverTable";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute(
                "create table " + tableName +
                " (key int comment 'key col comment', value string comment 'value col comment') " +
                        "comment 'table comment'");
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
    }

    @Test
    public void testHiveDb2() {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
        ds.setUrl("jdbc:hive2://172.16.18.18:10000/default");
        ds.setUsername("hive");
        ds.setPassword("hive");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        List<Map<String, Object>> table =
                jdbcTemplate.queryForList("desc default.testhivedrivertable");

        table.forEach(a -> {
            a.forEach((key, value) -> {
                System.out.println(key + " --> " + value);
            });
            System.out.println("----");
        });



    }

    @Test
    public void testQueryIndexInfo() {
        List<Index> indices = dao.queryIndexInfo("", "default", "test22");
        System.out.println(indices.toString());
    }
}
