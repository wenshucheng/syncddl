package com.huacloud.synctable.mapping.datatype;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/22/2019 7:18 PM
 */
public interface DataType {

    int id();

    DataType id(int jdbcType);

    DataType identity(boolean identity);

    boolean identity();

    DataType precision(int precision);

    DataType precision(int precision, int scale);

    int precision();

    boolean hasPrecision();

    DataType scale(int scale);

    int scale();

    boolean hasScale();

    DataType length(int length);

    int length();

    boolean hasLength();

    String getTypeName();

}
