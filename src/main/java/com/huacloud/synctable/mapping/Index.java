package com.huacloud.synctable.mapping;

import java.util.ArrayList;
import java.util.List;

/**
 * 索引
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/20/2019 4:35 PM
 */
public class Index {

    private Table table;

    private String name;

    private boolean unique;

    private boolean primaryKey;

    private List<IndexColumn> columnList = new ArrayList<>();

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    public List<IndexColumn> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<IndexColumn> columnList) {
        this.columnList = columnList;
    }

    @Override
    public String toString() {
        return "Index{" +
                "table=" + table +
                ", name='" + name + '\'' +
                ", unique=" + unique +
                ", primaryKey=" + primaryKey +
                ", columnList=" + columnList +
                '}';
    }
}
