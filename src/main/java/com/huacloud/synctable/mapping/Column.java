package com.huacloud.synctable.mapping;

import com.huacloud.synctable.dialect.Dialect;
import com.huacloud.synctable.mapping.datatype.DataType;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 4/17/2019 12:16 PM
 */
public class Column {

    private long length;

    private int precision;

    private int scale;

    private int typeIndex;

    private String name;

    private boolean nullable = true;

    private boolean unique;

    private String sqlType;

    private Integer jdbcType;

    private Integer sqlTypeCode;

    private boolean quoted = true;

    int uniqueInteger;

    private String checkConstraint;

    private String comment;

    private String defaultValue;

    private boolean autoIncrement = false;

    private DataType dataType;

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public boolean isQuoted() {
        return quoted;
    }

    public void setQuoted(boolean quoted) {
        this.quoted = quoted;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getQuotedName() {
        return quoted ?
                "`" + name + "`" :
                name;
    }

    public String getQuotedName(Dialect d) {
        return quoted ?
                d.openQuote() + name + d.closeQuote() :
                name;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable=nullable;
    }

    public int getTypeIndex() {
        return typeIndex;
    }

    public void setTypeIndex(int typeIndex) {
        this.typeIndex = typeIndex;
    }

    public boolean isUnique() {
        return unique;
    }

    public String getDefaultValue() {
        return this.defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getSqlType() {
        return sqlType;
    }

    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }

    public Integer getJdbcType() {
        return jdbcType;
    }

    public void setJdbcType(Integer jdbcType) {
        this.jdbcType = jdbcType;
    }

    public String getSqlType(Dialect dialect) {
        return dialect.getTypeName(jdbcType, length, precision, scale);
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
        this.jdbcType = dataType.id();
        this.length = dataType.length();
        this.precision = dataType.precision();
        this.scale = dataType.scale();
    }

    @Override
    public String toString() {
        return "Column{" +
                "length=" + length +
                ", precision=" + precision +
                ", scale=" + scale +
                ", name='" + name + '\'' +
                ", nullable=" + nullable +
                ", sqlType='" + sqlType + '\'' +
                ", comment='" + comment + '\'' +
                ", defaultValue='" + defaultValue + '\'' +
                '}';
    }
}
