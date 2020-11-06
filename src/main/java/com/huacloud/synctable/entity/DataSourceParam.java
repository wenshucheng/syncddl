package com.huacloud.synctable.entity;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/14/2019 11:46 AM
 */
public class DataSourceParam {

    private String url;

    private String userName;

    private String password;

    private Integer dbType;

    private String schemaName;

    private String tableName;

    private Integer targetDbType;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getDbType() {
        return dbType;
    }

    public void setDbType(Integer dbType) {
        this.dbType = dbType;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getTargetDbType() {
        return targetDbType;
    }

    public void setTargetDbType(Integer targetDbType) {
        this.targetDbType = targetDbType;
    }
}
