package com.huacloud.synctable;

import com.huacloud.synctable.entity.DBType;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Types;

/**
 * 各种数据源获取工具
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 2020-07-29 17:18
 */
public class DataSourceUtils {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceUtils.class);

    public static BasicDataSource getDataSourceByType(DBType dbType) {
        BasicDataSource dataSource = new BasicDataSource();
        switch (dbType) {
            case MYSQL:
                dataSource.setDriverClassName(DBType.MYSQL.getDriverName());
                dataSource.setUrl("jdbc:mysql://172.16.18.16:3306/test?useUnicode=true&characterEncoding=utf-8");
                dataSource.setUsername("root");
                dataSource.setPassword("root");

            case ORACLE:
                dataSource.setDriverClassName(DBType.ORACLE.getDriverName());
                dataSource.setUrl("jdbc:oracle:thin:@//172.16.18.16:1521/oracle");
                dataSource.setUsername("ogg");
                dataSource.setPassword("ogg");

            case SQLServer:
                dataSource.setDriverClassName(DBType.SQLServer.getDriverName());
                dataSource.setUrl("jdbc:sqlserver://172.16.18.16:1433; DatabaseName=pxd_test");
                dataSource.setUsername("sa");
                dataSource.setPassword("admin@123");

        }
        return dataSource;
    }

    public static void initMySQLAllColTable(JdbcTemplate mySQLjdbcTemplate, String tableName) {
        mySQLjdbcTemplate.execute("DROP TABLE IF EXISTS " + tableName);
        String allColTabSql =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                        "  `c1` bigint(10) NOT NULL COMMENT 'c1'," +
                        "  `c2` binary(10)," +
                        "  `c3` bit(10)," +
                        "  `c4` tinyblob," +
                        "  `c5` tinyint(1)," +
                        "  `c6` tinyint(1)," +
                        "  `c7` char(10) NOT NULL DEFAULT 'a'," +
                        "  `c8` date," +
                        "  `c9` datetime(6)," +
                        "  `c10` decimal(10,2)," +
                        "  `c11` double(8,2)," +
                        "  `c12` enum('a','b')," +
                        "  `c13` float(6,3)," +
                        "  `c14` int(10)," +
                        "  `c15` longblob," +
                        "  `c16` longtext," +
                        "  `c17` mediumblob," +
                        "  `c18` mediumint(20)," +
                        "  `c19` mediumtext," +
                        "  `c20` decimal(15,4)," +
                        "  `c21` double(10,3)," +
                        "  `c22` set('b','c')," +
                        "  `c23` smallint(10)," +
                        "  `c24` tinytext," +
                        "  `c25` time(6)," +
                        "  `c26` timestamp(6) NULL," +
                        "  `c27` tinyblob," +
                        "  `c28` tinyint(10)," +
                        "  `c29` tinytext," +
                        "  `c30` varbinary(10)," +
                        "  `c31` varchar(50)," +
                        "  `c32` year(4)," +
                        "  PRIMARY KEY (`c1`)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        mySQLjdbcTemplate.execute(allColTabSql);
    }

    public static void initOracleAllColTable(JdbcTemplate jdbcTemplate, String tableName) {
        Integer count = jdbcTemplate.queryForObject("select COUNT(1) from all_tables t" +
                        "where t.OWNER = ? " +
                        "and t.TABLE_NAME = ?",
                new Object[]{"OGG", tableName.toUpperCase()},
                new int[]{Types.VARCHAR, Types.VARCHAR}, Integer.class);

        if (count > 0) {
            String allColTabSql =
                    "create table " + tableName +" (" +
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
            jdbcTemplate.execute(allColTabSql);
        } else {
            logger.warn("表已经存在：{}", tableName);
        }

    }

    public static void initSQLServerColTable(JdbcTemplate jdbcTemplate, String tableName) {
        String sqlServerSql =
                "create table " + tableName +" (" +
                        "c1 bigint," +
                        "c2 binary," +
                        "c3 bit," +
                        "c4 char(20)," +
                        "c5 date," +
                        "c6 datetime," +
                        "c7 decimal(10,3)," +
                        "c8 float," +
                        "c9 int," +
                        "c10 numeric(10,3)," +
                        "c11 real," +
                        "c12 smallint," +
                        "c13 text," +
                        "c14 time," +
                        "c15 timestamp," +  //注意：SQLServer对timestamp会默认使用NOT NULL
                        "c16 tinyint," +
                        "c17 varbinary(500)," +
                        "c18 varchar(200), " +
                        "c19 datetimeoffset(7)" +
                        ")";
        String tabComment = "execute sp_addextendedproperty 'MS_Description','表备注'," +
                "'user','dbo','table','" + tableName + "',null,null;";
        String columnComment = "execute sp_addextendedproperty 'MS_Description','c10字段备注'," +
                "'user','dbo','table','" + tableName + "','column','c10';";

        try {
            jdbcTemplate.execute(sqlServerSql);
            jdbcTemplate.execute(tabComment);
            jdbcTemplate.execute(columnComment);
        } catch (DataAccessException e) {
            logger.error("", e);
        }
    }

}
