package com.huacloud.synctable.mapping;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 9/12/2019 12:30 PM
 */
public enum PartitionType {

    LIST("LIST"),
    HASH("HASH"),
    RANGE("RANGE"),
    KEY("KEY");

    private String name;

    PartitionType(String name) {
        this.name = name;
    }

    public static PartitionType toEnum(String partitionType) {
        if (StringUtils.equalsIgnoreCase("LIST", partitionType)) {
            return LIST;
        } else if (StringUtils.equalsIgnoreCase("HASH", partitionType)) {
            return HASH;
        } else if (StringUtils.equalsIgnoreCase("RANGE", partitionType)) {
            return RANGE;
        } else if (StringUtils.equalsIgnoreCase("RANGE COLUMNS", partitionType)) {
            return RANGE;
        } else if (StringUtils.equalsIgnoreCase("LIST COLUMNS", partitionType)) {
            return LIST;
        } else if (StringUtils.equalsIgnoreCase("LINEAR HASH", partitionType)) {
            return HASH;
        } else if (StringUtils.equalsIgnoreCase("KEY", partitionType)) {
            return KEY;
        } else {
            throw new RuntimeException("不支持的分区类型：" + partitionType);
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
