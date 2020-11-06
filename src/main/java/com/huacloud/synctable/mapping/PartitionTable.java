package com.huacloud.synctable.mapping;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 9/12/2019 2:32 PM
 */
public class PartitionTable {

    /**
     * 分区表名
     */
    private String name;

    /**
     * 分区表的排序位置
     */
    private int position;

    /**
     * 分区字段的值
     */
    private String value;

    /**
     * 子分区的字段信息
     */
    private String subPartCol;

    /**
     * 子分区
     */
    private List<PartitionTable> subPartTab = new ArrayList<>();

    /**
     * 子分区的类型
     */
    private PartitionType subParttype;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public List<PartitionTable> getSubPartTab() {
        return subPartTab;
    }

    public void setSubPartTab(List<PartitionTable> subPartTab) {
        this.subPartTab = subPartTab;
    }

    public PartitionType getSubParttype() {
        return subParttype;
    }

    public void setSubParttype(PartitionType subParttype) {
        this.subParttype = subParttype;
    }

    public String getSubPartCol() {
        return subPartCol;
    }

    public void setSubPartCol(String subPartCol) {
        this.subPartCol = subPartCol;
    }
}
