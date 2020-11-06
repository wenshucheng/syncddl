package com.huacloud.synctable.dao;

import com.huacloud.synctable.dialect.Dialect;
import com.huacloud.synctable.entity.ConstraintInfo;
import com.huacloud.synctable.mapping.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 4/19/2019 5:28 PM
 */
public abstract class AbstractDbMetaInfoDao {

    protected JdbcTemplate jdbcTemplate;

    public RowMapper<ConstraintInfo> rowMapper = (rs, rowNum) -> {
        ConstraintInfo constraintInfo1 = new ConstraintInfo();
        constraintInfo1.setConstraintType(rs.getString("constraintType"));
        constraintInfo1.setColumnName(rs.getString("columnName"));
        constraintInfo1.setConstraintName(rs.getString("constraintName"));
        constraintInfo1.setOrdinalPosition(rs.getInt("ordinalPosition"));
        return constraintInfo1;
    };

    public AbstractDbMetaInfoDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public abstract String queryTableComment(String catalog, String schema, String tableName);

    public abstract List<Column> queryColumnInfo(String catalog, String schema, String tableName);

    public abstract List<ConstraintInfo> queryConstraintInfo(String catalog, String schema, String tableName);

    public abstract List<String> queryAllSchemaNames();

    public abstract List<String> queryAllTabNames(String schema);

    public Table queryTable(String catalog, String schema, String tableName, Dialect dialect) {
        String tableComment = queryTableComment(catalog, schema, tableName);
        Table table = new Table();
        table.setName(tableName);
        table.setCatalog(catalog);
        table.setSchema(schema);
        table.setComment(tableComment);

        List<Column> columnList = queryColumnInfo(catalog, schema, tableName);
        for (Column column : columnList) {
            Integer jdbcType = dialect.getJdbcType(column.getSqlType());
            column.setJdbcType(jdbcType);
            table.addColumn(column);
        }

        List<ConstraintInfo> constraintInfos = queryConstraintInfo(catalog, schema, tableName);

        PrimaryKey primaryKey = new PrimaryKey();
        primaryKey.setTable(table);
        table.setPrimaryKey(primaryKey);

        setPartitionInfo(table);

        for (ConstraintInfo constraintInfo : constraintInfos) {
            String columnName = constraintInfo.getColumnName();
            String constraintName = constraintInfo.getConstraintName();
            String constraintType = constraintInfo.getConstraintType();
            Integer position = constraintInfo.getOrdinalPosition();

            Column column = table.getColumn(columnName);

            if (StringUtils.equalsIgnoreCase(constraintType, "P")) {
                primaryKey.setName(constraintName);
                primaryKey.addColumn(column);

            } else if (StringUtils.equalsIgnoreCase(constraintType, "U")) {
                UniqueKey uniqueKey = table.getUniqueKey(constraintName);
                if (uniqueKey == null) {
                    uniqueKey = new UniqueKey();
                    uniqueKey.setName(constraintName);
                    uniqueKey.setTable(table);
                    uniqueKey.addColumn(column, position + "");
                    table.addUniqueKey(uniqueKey);
                } else {
                    uniqueKey.addColumn(column, position + "");
                }
            }
        }

        List<Index> indexList = queryIndexInfo(catalog, schema, tableName);
        table.setIndexList(indexList);

        return table;
    }

    public abstract List<Index> queryIndexInfo(String catalog, String schema, String tableName);

    public void setPartitionInfo(Table table) {

    }
}
