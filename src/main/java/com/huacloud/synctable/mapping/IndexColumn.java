package com.huacloud.synctable.mapping;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/20/2019 4:45 PM
 */
public class IndexColumn {

    private String columnName;

    private SortOrder sortOrder;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public SortOrder getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    @Override
    public String toString() {
        return "IndexColumn{" +
                "columnName='" + columnName + '\'' +
                ", sortOrder=" + sortOrder +
                '}';
    }
}
