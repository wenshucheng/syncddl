package com.huacloud.synctable.mapping;

import com.huacloud.synctable.dialect.Dialect;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class UniqueKey extends Constraint {

    private Map<Column, String> columnOrderMap = new HashMap<>();

    @Override
    public String sqlConstraintString(
            Dialect dialect,
            String constraintName,
            String defaultCatalog,
            String defaultSchema) {
        // Not used.
        return "";
    }

    public void addColumn(Column column, String order) {
        addColumn(column);
        if (StringUtils.isNotEmpty(order)) {
            columnOrderMap.put(column, order);
        }
    }

    public Map<Column, String> getColumnOrderMap() {
        return columnOrderMap;
    }

    @Override
    public String generatedConstraintNamePrefix() {
        return "UK_";
    }

}
