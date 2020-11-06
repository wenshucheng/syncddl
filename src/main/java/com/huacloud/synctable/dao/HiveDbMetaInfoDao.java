package com.huacloud.synctable.dao;

import com.huacloud.synctable.entity.ConstraintInfo;
import com.huacloud.synctable.mapping.Column;
import com.huacloud.synctable.mapping.Index;
import com.huacloud.synctable.mapping.IndexColumn;
import com.huacloud.synctable.mapping.SortOrder;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/19/2019 2:29 PM
 */
public class HiveDbMetaInfoDao extends AbstractDbMetaInfoDao {

    public HiveDbMetaInfoDao(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override
    public String queryTableComment(String catalog, String schema, String tableName) {
        List<Map<String, Object>> table =
                jdbcTemplate.queryForList("desc formatted " + schema + "." + tableName);

        for (Map<String, Object> rowMap : table) {
            Object dataTypeObj = rowMap.get("data_type");
            if (dataTypeObj == null) {
                continue;
            }
            String dataType = dataTypeObj.toString().trim();
            if (StringUtils.equalsIgnoreCase(dataType, "comment")) {
                Object comment1 = rowMap.get("comment");
                if (comment1 == null) {
                    return null;
                } else {
                    return comment1.toString().trim();
                }
            }
        }
        return null;
    }

    @Override
    public List<Column> queryColumnInfo(String catalog, String schema, String tableName) {
        List<Map<String, Object>> table =
                jdbcTemplate.queryForList("desc " + schema + "." + tableName);
        List<Column> columnList = new ArrayList<>();
        for (Map<String, Object> rowMap : table) {
            String columnName = (String) rowMap.get("col_name");
            String dataType = (String) rowMap.get("data_type");
            String comment = (String) rowMap.get("comment");

            Column column = new Column();
            column.setName(columnName);
            column.setComment(comment);

            if (StringUtils.contains(dataType, "(")) {
                String jdbcDataType = dataType.substring(0, dataType.indexOf('('));
                String dataLen = dataType.substring(dataType.indexOf('(') + 1, dataType.indexOf(')'));
                if (StringUtils.contains(dataLen, ",")) {
                    String[] split = dataLen.split(",");
                    int precision = Integer.valueOf(split[0].trim());
                    int scale = Integer.valueOf(split[1].trim());
                    column.setPrecision(precision);
                    column.setScale(scale);

                } else {
                    int len = Integer.valueOf(dataLen.trim());
                    column.setLength(len);
                    column.setPrecision(len);
                }
                column.setSqlType(jdbcDataType);
            } else {
                column.setSqlType(dataType);
            }
            columnList.add(column);
        }
        return columnList;
    }

    @Override
    public List<ConstraintInfo> queryConstraintInfo(String catalog, String schema, String tableName) {
        return new ArrayList<>();
    }

    @Override
    public List<String> queryAllSchemaNames() {
        String sql = "show databases";
        List<String> schemaNameList = jdbcTemplate.queryForList(sql, String.class);
        return schemaNameList;
    }

    @Override
    public List<String> queryAllTabNames(String schema) {
        String sql = "show tables in " + schema;
        List<String> schemaNameList = jdbcTemplate.queryForList(sql, String.class);
        return schemaNameList;
    }

    @Override
    public List<Index> queryIndexInfo(String catalog, String schema, String tableName) {
        String sql = "SHOW FORMATTED INDEXES ON " + tableName + " in " + schema;
        List<Map<String, Object>> dataMapList = jdbcTemplate.queryForList(sql, schema, tableName);

        List<Index> indexList = new ArrayList<>();
        for (Map<String, Object> columnMap : dataMapList) {
            String idxName = (String) columnMap.get("IDX_NAME");
            String columnNames = (String) columnMap.get("COL_NAMES");
            String[] columnIndes = StringUtils.split(columnNames, ",");

            Index index = new Index();
            index.setName(idxName);
            indexList.add(index);

            for (String colName : columnIndes) {
                IndexColumn indexColumn = new IndexColumn();
                indexColumn.setColumnName(colName.trim());
                indexColumn.setSortOrder(SortOrder.DEFAULT);
                index.getColumnList().add(indexColumn);
            }
        }
        return indexList;
    }

}
