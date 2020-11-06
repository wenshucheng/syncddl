package com.huacloud.synctable.dao;

import com.huacloud.synctable.entity.ConstraintInfo;
import com.huacloud.synctable.mapping.Column;
import com.huacloud.synctable.mapping.Index;
import com.huacloud.synctable.mapping.IndexColumn;
import com.huacloud.synctable.mapping.SortOrder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/16/2019 9:28 AM
 */
public class SQLServerDbMetaInfoDaoImpl extends AbstractDbMetaInfoDao {

    public SQLServerDbMetaInfoDaoImpl(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override
    public String queryTableComment(String catalog, String schema, String tableName) {
        String sql = "select CONVERT(nvarchar(2000), ep.value) AS value " +
                "from sys.schemas s " +
                "inner join sys.tables t on s.schema_id = t.schema_id " +
                "left join sys.extended_properties ep on t.object_id = ep.major_id and ep.minor_id = 0 " +
                "where s.name = ? and t.name = ?";
        List<String> comments = jdbcTemplate.queryForList(sql, new String[]{schema, tableName}, String.class);
        if (CollectionUtils.isEmpty(comments)) {
            return null;

        } else {
            return comments.get(0);
        }

    }

    @Override
    public List<Column> queryColumnInfo(String catalog, String schema, String tableName) {
        String sql =
                "select c.name column_name, tt.name data_type, c.max_length, " +
                " c.precision, c.scale, c.is_nullable, c.is_identity, " +
                " dc.definition column_default, CONVERT(nvarchar(2000), ep.value) column_comment " +
                "from sys.schemas s " +
                " inner join sys.tables t on s.schema_id = t.schema_id " +
                " left join sys.columns c on t.object_id = c.object_id " +
                " left join sys.types tt on tt.system_type_id = c.system_type_id " +
                        "and tt.user_type_id = c.user_type_id " +
                " left join sys.default_constraints dc " +
                        "on c.default_object_id = dc.object_id and dc.type = 'D' " +
                " left join sys.extended_properties ep " +
                        "on c.column_id = ep.minor_id and t.object_id = ep.major_id " +
                "where s.name = ? and t.name = ? " +
                "order by c.column_id";
        List<Map<String, Object>> columnMapList = jdbcTemplate.queryForList(sql, schema, tableName);
        List<Column> columnList = new ArrayList<>();
        for (Map<String, Object> columnMap : columnMapList) {
            String columnName = (String) columnMap.get("COLUMN_NAME");
            String dataType = (String) columnMap.get("DATA_TYPE");
            Short maxLength = (Short) columnMap.get("MAX_LENGTH");
            Short numericPrecision = (Short) columnMap.get("PRECISION");
            Short numericScale = (Short) columnMap.get("SCALE");
            boolean isNullable = (boolean) columnMap.get("IS_NULLABLE");
            String columnDefault = (String) columnMap.get("COLUMN_DEFAULT");
            String columnComment = (String) columnMap.get("COLUMN_COMMENT");

            if (StringUtils.equalsIgnoreCase("timestamp", dataType)) {
                //SQLServer对于timestamp的长度使用len而不是scale，为了统一需要改成scale。
                numericScale = null;
            } else if (StringUtils.equalsIgnoreCase("datetimeoffset", dataType)) {
                if (numericScale != null && numericScale > 6) {
                    //最多6位
                    numericScale = 6;
                }
                numericPrecision = numericScale;
            }

            if (StringUtils.equalsAnyIgnoreCase(dataType,
                    "sysname", "uniqueidentifier")) {
                throw new UnsupportedOperationException("不支持的类型：" + columnName + " " + dataType);
            }

            if (maxLength != null && maxLength == -1) {
                throw new UnsupportedOperationException("不支持字段长度为MAX：" + columnName);
            }

            if (columnDefault != null) {
                columnDefault = StringUtils.removeStart(columnDefault, "((");
                columnDefault = StringUtils.removeEnd(columnDefault, "))");
            }

            Column column = new Column();
            column.setComment(columnComment);
            column.setSqlType(dataType);
            column.setName(columnName);
            column.setDefaultValue(columnDefault);
            column.setNullable(isNullable);
            if (maxLength != null) {
                column.setLength(maxLength);
            }
            if (numericPrecision != null) {
                column.setPrecision(numericPrecision);
            }
            if (numericScale != null) {
                column.setScale(numericScale);
            }
            columnList.add(column);
        }
        return columnList;
    }

    @Override
    public List<ConstraintInfo> queryConstraintInfo(String catalog, String schema, String tableName) {
        String constraintSql =
                "SELECT " +
                    "obj.xtype constraintType, " +
                    "k.constraint_name constraintName, " +
                    "k.column_name columnName, " +
                    "k.ordinal_position ordinalPosition " +
                    "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k " +
                    "   left join SYSOBJECTS obj on k.constraint_name = obj.name " +
                    "where k.table_schema = ? and k.table_name = ?";
        List<ConstraintInfo> constraintInfo = jdbcTemplate.query(
                constraintSql, rowMapper, schema, tableName);
        for (ConstraintInfo con : constraintInfo) {
            String constraintType = con.getConstraintType();
            if (StringUtils.equalsIgnoreCase(constraintType, "PK")) {
                con.setConstraintType("P");
            } else if (StringUtils.equalsIgnoreCase(constraintType, "UQ")) {
                con.setConstraintType("U");
            }
        }
        return constraintInfo;
    }

    @Override
    public List<String> queryAllSchemaNames() {
        String sql = "select name from sys.schemas";
        return jdbcTemplate.queryForList(sql, String.class);
    }

    @Override
    public List<String> queryAllTabNames(String schema) {
        String sql =
                "select t.name " +
                    "from sys.schemas s " +
                    "inner join sys.tables t on s.schema_id = t.schema_id " +
                    "where s.name = ?";
        return jdbcTemplate.queryForList(sql, new String[]{schema}, String.class);
    }

    @Override
    public List<Index> queryIndexInfo(String catalog, String schema, String tableName) {
        String sql =
                "select " +
                "   ind.name indexName, " +
                "   ind.is_unique, " +
                "   Col.name columnName, " +
                "   ind.is_primary_key, " +
                "   index_columns.key_ordinal, " +
                "   index_columns.is_descending_key " +
                "from sys.indexes ind " +
                "   inner join sys.tables tab " +
                "       on ind.Object_id = tab.object_id " +
                "   inner join sys.schemas s " +
                "       on tab.schema_id = s.schema_id " +
                "   inner join sys.index_columns index_columns " +
                "       on tab.object_id = index_columns.object_id and ind.index_id = index_columns.index_id " +
                "   inner join sys.columns Col " +
                "       on tab.object_id = Col.object_id and index_columns.column_id = Col.column_id " +
                "where s.name = ? and tab.name = ? " +
                "order by ind.name, index_columns.key_ordinal";
        List<Map<String, Object>> dataMapList = jdbcTemplate.queryForList(sql, schema, tableName);

        Map<String, Index> indexMap = new HashMap<>();
        for (Map<String, Object> columnMap : dataMapList) {
            Boolean unique = (Boolean) columnMap.get("IS_UNIQUE");
            String keyName = (String) columnMap.get("INDEXNAME");
            if (StringUtils.startsWithIgnoreCase(keyName, "PK__")) {
                //主键索引不需要
                continue;
            }
            String columnName = (String) columnMap.get("COLUMNNAME");
            Boolean isDescendingKey = (Boolean) columnMap.get("IS_DESCENDING_KEY");

            IndexColumn indexColumn = new IndexColumn();
            indexColumn.setColumnName(columnName);
            indexColumn.setSortOrder(isDescendingKey ?
                    SortOrder.DESC : SortOrder.ASC);

            if (indexMap.containsKey(keyName)) {
                Index index = indexMap.get(keyName);
                index.getColumnList().add(indexColumn);

            } else {
                Index index = new Index();
                index.setName(keyName);
                index.setUnique(unique);
                index.getColumnList().add(indexColumn);
                indexMap.put(keyName, index);
            }
        }
        return new ArrayList<>(indexMap.values());
    }
}
