package com.huacloud.synctable.entity;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 4/19/2019 5:14 PM
 */
public class ConstraintInfo {

    private String constraintType;

    private String constraintName;

    private String columnName;

    private Integer ordinalPosition;

    public String getConstraintType() {
        return constraintType;
    }

    public void setConstraintType(String constraintType) {
        this.constraintType = constraintType;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public Integer getOrdinalPosition() {
        return ordinalPosition;
    }

    public void setOrdinalPosition(Integer ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    @Override
    public String toString() {
        return "ConstraintInfo{" +
                "constraintType='" + constraintType + '\'' +
                ", constraintName='" + constraintName + '\'' +
                ", columnName='" + columnName + '\'' +
                ", ordinalPosition=" + ordinalPosition +
                '}';
    }
}
