package com.huacloud.synctable.mapping.datatype;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/22/2019 7:25 PM
 */
public class DefaultDataType implements DataType {

    /**
     * 唯一标识该类型
     */
    private int id;

    private boolean identity;

    private int precision;

    private int scale;

    private int length;

    private final String typeName;

    public DefaultDataType(int jdbcType, String typeName) {
        this.id = jdbcType;
        this.typeName = typeName;
    }

    public DefaultDataType(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public int id() {
        return this.id;
    }

    @Override
    public DataType id(int jdbcType) {
        this.id = jdbcType;
        return this;
    }

    @Override
    public DataType identity(boolean identity) {
        this.identity = identity;
        return this;
    }

    @Override
    public boolean identity() {
        return this.identity;
    }

    @Override
    public DataType precision(int precision) {
        this.precision = precision;
        return this;
    }

    @Override
    public DataType precision(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
        return this;
    }

    @Override
    public int precision() {
        return this.precision;
    }

    @Override
    public boolean hasPrecision() {
        return false;
    }

    @Override
    public DataType scale(int scale) {
        this.scale = scale;
        return this;
    }

    @Override
    public int scale() {
        return this.scale;
    }

    @Override
    public boolean hasScale() {
        return false;
    }

    @Override
    public DataType length(int length) {
        this.length = length;
        return this;
    }

    @Override
    public int length() {
        return this.length;
    }

    @Override
    public boolean hasLength() {
        return false;
    }

    public String getTypeName() {
        return typeName;
    }
}
