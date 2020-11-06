package com.huacloud.synctable;

import com.huacloud.synctable.entity.DBType;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * db -> hive SQL语句相关测试
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 2020-07-29 17:12
 */
public class Db2HiveSQLTest {

    private static final Logger logger = LoggerFactory.getLogger(Db2HiveSQLTest.class);

    @Test
    public void testOracle2Hive() {
        String tableName = "ORACLE_HIVE_ALLCOL";
        BasicDataSource ds = DataSourceUtils.getDataSourceByType(DBType.ORACLE);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        DataSourceUtils.initOracleAllColTable(jdbcTemplate, tableName);
        String hiveSql = Db2SqlApp.db2sql(DBType.ORACLE, DBType.HIVE,
                ds, "ogg", tableName, "default");
        logger.info(hiveSql);
    }

    @Test
    public void testSQLServer2Hive() {
        String tableName = "sqlserver_hive_allcol";
        BasicDataSource ds = DataSourceUtils.getDataSourceByType(DBType.SQLServer);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        DataSourceUtils.initSQLServerColTable(jdbcTemplate, tableName);
        String hiveSql = Db2SqlApp.db2sql(DBType.SQLServer, DBType.HIVE,
                ds, "dbo", tableName, "default");
        logger.info(hiveSql);
    }

}
