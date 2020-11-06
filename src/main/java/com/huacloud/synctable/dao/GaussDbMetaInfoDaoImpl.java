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
import java.util.List;
import java.util.Map;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/27/2019 9:04 AM
 */
public class GaussDbMetaInfoDaoImpl extends AbstractDbMetaInfoDao {

    public GaussDbMetaInfoDaoImpl(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override
    public String queryTableComment(String catalog, String schema, String tableName) {
        String tableCommentSql =
                "select comments from ADM_TAB_COMMENTS " +
                "where owner = ? and table_name = ?";
        List<Map<String, Object>> tableCommentMapList = jdbcTemplate.queryForList(tableCommentSql,
                schema.toUpperCase(),
                tableName.toUpperCase());
        if (CollectionUtils.isEmpty(tableCommentMapList)) {
            return null;
        } else {
            Map<String, Object> tableCommentMap = tableCommentMapList.get(0);
            String tableComment = (String) tableCommentMap.get("COMMENTS");
            return tableComment;
        }

    }

    @Override
    public List<Column> queryColumnInfo(String catalog, String schema, String tableName) {
        String columnInfoSql =
                "select " +
                    " c.column_id," +
                    " c.table_name," +
                    " c.column_name," +
                    " c.data_type," +
                    " c.data_length," +
                    " c.data_precision," +
                    " c.data_scale," +
                    " c.nullable," +
                    " c.DATA_DEFAULT," +
                    " cc.comments " +
                "from ADM_TAB_COLS c " +
                "left join ADM_COL_COMMENTS cc on c.owner = cc.owner " +
                "and c.table_name = cc.table_name " +
                "and c.column_name = cc.column_name " +
                "where c.owner = ? and c.table_name = ? " +
                "order by c.column_id";

        List<Map<String, Object>> columnMapList = jdbcTemplate.queryForList(columnInfoSql,
                schema.toUpperCase(), tableName.toUpperCase());
        List<Column> columnList = new ArrayList<>();

        for (Map<String, Object> columnMap : columnMapList) {
            String columnName = (String) columnMap.get("COLUMN_NAME");
            String dataType = (String) columnMap.get("DATA_TYPE");
            Integer dataLength = (Integer) columnMap.get("DATA_LENGTH");
            Integer dataPrecision = (Integer) columnMap.get("DATA_PRECISION");
            Integer dataScale = (Integer) columnMap.get("DATA_SCALE");
            String nullable = (String) columnMap.get("NULLABLE");
            String dataDefault = (String) columnMap.get("DATA_DEFAULT");
            String comment = (String) columnMap.get("COMMENTS");

            Column column = new Column();
            column.setName(columnName);
            column.setSqlType(dataType);
            column.setLength(dataLength != null ? dataLength.intValue() : 0);
            if (StringUtils.isNotBlank(dataDefault)) {
                column.setDefaultValue(dataDefault);
            }
            column.setComment(comment);
            column.setPrecision(dataPrecision != null ? dataPrecision.intValue() : 0);
            column.setScale(dataScale != null ? dataScale.intValue() : 0);

            if (StringUtils.equalsIgnoreCase(nullable, "Y")) {
                column.setNullable(true);
            } else {
                column.setNullable(false);
            }
            columnList.add(column);
        }
        return columnList;
    }

    @Override
    public List<ConstraintInfo> queryConstraintInfo(String catalog, String schema, String tableName) {
        String constraintSql =
                "select " +
                    " CONS_COLS, " +
                    " CONSTRAINT_NAME, " +
                    " CONSTRAINT_TYPE " +
                "from ADM_CONSTRAINTS " +
                "where OWNER = ? and TABLE_NAME = ?";

        List<Map<String, Object>> constraintInfo = jdbcTemplate.queryForList(
                constraintSql, schema.toUpperCase(), tableName.toUpperCase());
        List<ConstraintInfo> constraintInfos = new ArrayList<>();
        for (Map<String, Object> columnMap : constraintInfo) {
            String constraintName = (String) columnMap.get("CONSTRAINT_NAME");
            String consCols = (String) columnMap.get("CONS_COLS");
            String constraintType = (String) columnMap.get("CONSTRAINT_TYPE");

            String[] colNames = consCols.split(", ");
            for (String columnName : colNames) {
                ConstraintInfo constraint = new ConstraintInfo();
                constraint.setConstraintType(constraintType);
                constraint.setConstraintName(constraintName);
                constraint.setColumnName(columnName);
                constraintInfos.add(constraint);
            }
        }
        return constraintInfos;
    }

    @Override
    public List<String> queryAllSchemaNames() {
        String sql = "select USERNAME from db_users";
        List<String> schemaNameList = jdbcTemplate.queryForList(sql, String.class);
        return schemaNameList;
    }

    @Override
    public List<String> queryAllTabNames(String schema) {
        String sql = "select table_name from ADM_DBLINK_TABLES where owner = ?";
        List<String> schemaNameList = jdbcTemplate.queryForList(sql, new String[]{schema}, String.class);
        return schemaNameList;
    }

    @Override
    public List<Index> queryIndexInfo(String catalog, String schema, String tableName) {
        String sql =
                "select " +
                    "idx.index_name," +
                    "idx.index_type," +
                    "idx.is_primary," +
                    "idx.is_unique," +
                    "idx.columns " +
                "from ADM_INDEXES idx " +
                "where owner = ? and table_name = ?";
        List<Map<String, Object>> dataMapList = jdbcTemplate.queryForList(sql, schema, tableName);

        List<Index> indexList = new ArrayList<>();
        for (Map<String, Object> columnMap : dataMapList) {
            String unique = (String) columnMap.get("IS_UNIQUE");
            String idxName = (String) columnMap.get("INDEX_NAME");
            String columnNames = (String) columnMap.get("COLUMNS");
            String primaryKey = (String) columnMap.get("IS_PRIMARY");

            Index index = new Index();
            index.setUnique(StringUtils.equalsIgnoreCase(unique, "Y"));
            index.setName(idxName);
            index.setPrimaryKey(StringUtils.equalsIgnoreCase(primaryKey, "Y"));

            String[] columnNameArray = StringUtils.split(columnNames, ", ");
            for (String columnName : columnNameArray) {
                IndexColumn indexColumn = new IndexColumn();
                indexColumn.setColumnName(columnName);
                indexColumn.setSortOrder(SortOrder.DEFAULT);
                index.getColumnList().add(indexColumn);
            }
            indexList.add(index);
        }
        return indexList;
    }
}
