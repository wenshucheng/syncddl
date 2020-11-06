package com.huacloud.synctable.entity;

import com.huacloud.synctable.dao.*;
import com.huacloud.synctable.dialect.*;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/15/2019 9:50 AM
 */
public enum DBType {

    MYSQL(0, "com.mysql.cj.jdbc.Driver", "mysql") {

        @Override
        public AbstractDbMetaInfoDao getDbDao(JdbcTemplate jdbcTemplate) {
            return new MySqlDbMetaInfoDaoImpl(jdbcTemplate);
        }

        @Override
        public Dialect getDialect() {
            return new MySQLDialect();
        }

    },
    ORACLE(1, "oracle.jdbc.driver.OracleDriver", "oracle") {

        @Override
        public AbstractDbMetaInfoDao getDbDao(JdbcTemplate jdbcTemplate) {
            return new OracleDbMetaInfoDaoImpl(jdbcTemplate);
        }

        @Override
        public Dialect getDialect() {
            return new OracleDialect();
        }

    },
    SQLServer(2, "com.microsoft.sqlserver.jdbc.SQLServerDriver", "sqlserver") {

        @Override
        public AbstractDbMetaInfoDao getDbDao(JdbcTemplate jdbcTemplate) {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setUser("sa");
            ds.setPassword("admin@123");
            ds.setServerName("172.16.18.16");
            ds.setPortNumber(1433);
            ds.setDatabaseName("pxd_test");

            JdbcTemplate jdbcTemplate2 = new JdbcTemplate(ds);

            return new SQLServerDbMetaInfoDaoImpl(jdbcTemplate2);
        }

        @Override
        public Dialect getDialect() {
            return new SQLServerDialect();
        }

    },
    TBase(3, "org.postgresql.Driver", "tbase") {

        @Override
        public AbstractDbMetaInfoDao getDbDao(JdbcTemplate jdbcTemplate) {
            return new TBaseDbMetaInfoDaoImpl(jdbcTemplate);
        }

        @Override
        public Dialect getDialect() {
            return new TBaseDialect();
        }

    },
    HIVE(4, "org.apache.hive.jdbc.HiveDriver", "hive") {

        @Override
        public AbstractDbMetaInfoDao getDbDao(JdbcTemplate jdbcTemplate) {
            return new HiveDbMetaInfoDao(jdbcTemplate);
        }

        @Override
        public Dialect getDialect() {
            return new HiveDialect();
        }

    },
    GaussDB(5, "com.huawei.gauss.jdbc.ZenithDriver", "gaussdb") {
        @Override
        public AbstractDbMetaInfoDao getDbDao(JdbcTemplate jdbcTemplate) {
            return new GaussDbMetaInfoDaoImpl(jdbcTemplate);
        }

        @Override
        public Dialect getDialect() {
            return new GaussDbDialect();
        }
    },
    PostgreSql(6, "org.postgresql.Driver", "postgresql") {

        @Override
        public AbstractDbMetaInfoDao getDbDao(JdbcTemplate jdbcTemplate) {
            return new TBaseDbMetaInfoDaoImpl(jdbcTemplate);
        }

        @Override
        public Dialect getDialect() {
            return new TBaseDialect();
        }

    };

    private int type;

    private String driverName;

    /**
     * 数据库名称
     */
    private String dbName;

    DBType(int type, String driverName, String dbName) {
        this.type = type;
        this.driverName = driverName;
        this.dbName = dbName;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public abstract AbstractDbMetaInfoDao getDbDao(JdbcTemplate jdbcTemplate);

    public abstract Dialect getDialect();

    public static DBType int2Enum(int i) {
        switch (i) {
            case 0:
                return DBType.MYSQL;
            case 1:
                return DBType.ORACLE;
            case 2:
                return DBType.SQLServer;
            case 3:
                return DBType.TBase;
            case 4:
                return DBType.HIVE;
            case 5:
                return DBType.GaussDB;
            default:
                throw new RuntimeException("无法识别的数据库类型：" + i);
        }
    }

}
