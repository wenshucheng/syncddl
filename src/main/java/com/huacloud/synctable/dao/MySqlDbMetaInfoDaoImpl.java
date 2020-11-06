package com.huacloud.synctable.dao;

import com.huacloud.synctable.entity.ConstraintInfo;
import com.huacloud.synctable.mapping.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigInteger;
import java.util.*;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 4/18/2019 6:35 PM
 */
public class MySqlDbMetaInfoDaoImpl extends AbstractDbMetaInfoDao {

    public MySqlDbMetaInfoDaoImpl(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override
    public String queryTableComment(String catalog, String schema, String tableName) {
        String tableCommentSql =
                "SELECT " +
                "    t.`TABLE_COMMENT` " +
                "FROM" +
                "    information_schema.TABLES t " +
                "WHERE t.`TABLE_SCHEMA` = ? " +
                "  AND t.`TABLE_NAME` = ? ";
        String tableComment = null;
        try {
            tableComment = jdbcTemplate.queryForObject(tableCommentSql, String.class, schema, tableName);
        } catch (IncorrectResultSizeDataAccessException e) {
            //do nothing
        }
        return tableComment;
    }

    @Override
    public List<Column> queryColumnInfo(String catalog, String schema, String tableName) {
        String columnSql =
                "SELECT " +
                "    c.`COLUMN_NAME`," +
                "    c.`DATA_TYPE`," +
                "    c.`CHARACTER_MAXIMUM_LENGTH`," +
                "    c.`NUMERIC_PRECISION`," +
                "    c.`DATETIME_PRECISION`," +
                "    c.`COLUMN_TYPE`," +
                "    c.`NUMERIC_SCALE`," +
                "    c.`IS_NULLABLE`," +
                "    c.`COLUMN_DEFAULT`," +
                "    c.`COLUMN_COMMENT` " +
                "FROM information_schema.columns c " +
                "WHERE c.`table_schema` = ? " +
                "    AND c.`table_name` = ?";

        List<Column> columnList = jdbcTemplate.query(
                columnSql, (rs, rowNum) -> {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    long characterMaximumLength = rs.getLong("CHARACTER_MAXIMUM_LENGTH");
                    int numericPrecision = rs.getInt("NUMERIC_PRECISION");
                    int datetimePrecision = rs.getInt("DATETIME_PRECISION");
                    int numericScale = rs.getInt("NUMERIC_SCALE");
                    String isNullable = rs.getString("IS_NULLABLE");
                    String columnDefault = rs.getString("COLUMN_DEFAULT");
                    String columnComment = rs.getString("COLUMN_COMMENT");

                    Column column = new Column();
                    column.setComment(columnComment);
                    column.setSqlType(dataType);
                    column.setName(columnName);
                    column.setDefaultValue(columnDefault);
                    if (StringUtils.equalsIgnoreCase(isNullable, "YES")) {
                        column.setNullable(true);
                    } else {
                        column.setNullable(false);
                    }
                    if (characterMaximumLength != 0) {
                        column.setLength(characterMaximumLength);
                    }
                    if (numericPrecision != 0) {
                        column.setPrecision(numericPrecision);
                    }
                    if (datetimePrecision != 0) {
                        column.setPrecision(datetimePrecision);
                    }
                    if (numericScale != 0) {
                        column.setScale(numericScale);
                    }
                    return column;

                }, schema, tableName);

        return columnList;
    }

    @Override
    public List<ConstraintInfo> queryConstraintInfo(String catalog, String schema, String tableName) {
        String constraintSql =
                "SELECT " +
                "    tc.`CONSTRAINT_TYPE` as constraintType," +
                "    k.`CONSTRAINT_NAME` as constraintName," +
                "    k.`COLUMN_NAME` as columnName," +
                "    k.`ORDINAL_POSITION` as ordinalPosition " +
                "FROM " +
                "    INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc," +
                "    INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` k " +
                "WHERE tc.`CONSTRAINT_CATALOG` = k.`CONSTRAINT_CATALOG` " +
                "    AND tc.`CONSTRAINT_SCHEMA` = k.`CONSTRAINT_SCHEMA` " +
                "    AND tc.`TABLE_NAME` = k.`TABLE_NAME` " +
                "    AND tc.`CONSTRAINT_NAME` = k.`CONSTRAINT_NAME` " +
                "    AND tc.`CONSTRAINT_SCHEMA` = ? " +
                "    AND tc.table_name = ?";
        List<ConstraintInfo> constraintInfo = jdbcTemplate.query(
                constraintSql, rowMapper, schema, tableName);
        for (ConstraintInfo con : constraintInfo) {
            String constraintType = con.getConstraintType();
            if (StringUtils.equalsIgnoreCase(constraintType, "PRIMARY KEY")) {
                con.setConstraintType("P");
            } else if (StringUtils.equalsIgnoreCase(constraintType, "UNIQUE")) {
                con.setConstraintType("U");
            }
        }
        return constraintInfo;
    }

    @Override
    public List<String> queryAllSchemaNames() {
        String sql = "SELECT schema_name FROM `information_schema`.`SCHEMATA`";
        return jdbcTemplate.queryForList(sql, String.class);
    }

    @Override
    public List<String> queryAllTabNames(String schema) {
        String sql =
                "SELECT table_name FROM `information_schema`.`TABLES` " +
                "WHERE table_schema = ?";
        return jdbcTemplate.queryForList(sql, new String[]{schema}, String.class);
    }

    @Override
    public List<Index> queryIndexInfo(String catalog, String schema, String tableName) {
        String sql = "SHOW INDEX FROM " + schema + "." + tableName;
        List<Map<String, Object>> dataMapList = jdbcTemplate.queryForList(sql);

        Map<String, Index> indexMap = new HashMap<>();
        for (Map<String, Object> columnMap : dataMapList) {
            long unique = (Long) columnMap.get("NON_UNIQUE");
            String keyName = (String) columnMap.get("KEY_NAME");
            String columnName = (String) columnMap.get("COLUMN_NAME");
            String indexType = (String) columnMap.get("INDEX_TYPE");

            if (StringUtils.equalsIgnoreCase(keyName, "PRIMARY") ||
                    StringUtils.equalsIgnoreCase(indexType, "FULLTEXT")) {
                continue;
            }

            IndexColumn indexColumn = new IndexColumn();
            indexColumn.setColumnName(columnName);
            indexColumn.setSortOrder(SortOrder.DEFAULT);

            if (indexMap.containsKey(keyName)) {
                Index index = indexMap.get(keyName);
                index.getColumnList().add(indexColumn);

            } else {
                Index index = new Index();
                index.setName(keyName);
                index.setUnique(unique == 0);
                index.getColumnList().add(indexColumn);
                indexMap.put(keyName, index);
            }

        }
        return new ArrayList<>(indexMap.values());
    }

    @Override
    public void setPartitionInfo(Table table) {
        String schemaName = table.getSchema();
        String tableName = table.getName();
        String sql =
                "SELECT " +
                "    p.`PARTITION_NAME`," +
                "    p.`PARTITION_ORDINAL_POSITION`," +
                "    p.`PARTITION_METHOD`," +
                "    p.`PARTITION_EXPRESSION`," +
                "    p.`PARTITION_DESCRIPTION`, " +
                "    p.`SUBPARTITION_EXPRESSION`, " +
                "    p.`SUBPARTITION_METHOD`, " +
                "    p.`SUBPARTITION_NAME`, " +
                "    p.`SUBPARTITION_ORDINAL_POSITION` " +
                "FROM " +
                "    information_schema.PARTITIONS p " +
                "WHERE p.`TABLE_SCHEMA` = ? " +
                "    AND p.`TABLE_NAME` = ? " +
                "    AND p.`PARTITION_NAME` IS NOT NULL " +
                "ORDER BY p.`PARTITION_ORDINAL_POSITION`, p.`SUBPARTITION_ORDINAL_POSITION`";
        List<Map<String, Object>> partitionInfos = jdbcTemplate.queryForList(sql, schemaName, tableName);

        String partitionTypeStr = null;
        String columnExpression = null;
        Map<String, PartitionTable> partTabMap = new TreeMap<>();
        for (Map<String, Object> dataMap : partitionInfos) {
            String partitionName = (String) dataMap.get("PARTITION_NAME");
            BigInteger ordinalPosition = (BigInteger) dataMap.get("PARTITION_ORDINAL_POSITION");
            partitionTypeStr = (String) dataMap.get("PARTITION_METHOD");
            columnExpression = (String) dataMap.get("PARTITION_EXPRESSION");
            String description = (String) dataMap.get("PARTITION_DESCRIPTION");

            PartitionTable partitionTable = new PartitionTable();
            partitionTable.setName(partitionName);
            partitionTable.setPosition(ordinalPosition.intValue());
            partitionTable.setValue(description);

            String subPartName = (String) dataMap.get("SUBPARTITION_NAME");
            if (StringUtils.isNotEmpty(subPartName)) {
                String subPartExpression = (String) dataMap.get("SUBPARTITION_EXPRESSION");
                String subPartMethod = (String) dataMap.get("SUBPARTITION_METHOD");

                BigInteger subOrdinalPosition = (BigInteger) dataMap.get("SUBPARTITION_ORDINAL_POSITION");

                PartitionTable subPartTab = new PartitionTable();
                subPartTab.setName(subPartName);
                subPartTab.setPosition(subOrdinalPosition.intValue());

                partitionTable.setSubParttype(PartitionType.toEnum(subPartMethod));
                partitionTable.setSubPartCol(subPartExpression);

                if (partTabMap.containsKey(partitionName)) {
                    partTabMap.get(partitionName).getSubPartTab().add(subPartTab);
                } else {
                    partitionTable.getSubPartTab().add(subPartTab);
                    partTabMap.put(partitionName, partitionTable);
                }
            } else {
                partTabMap.put(partitionName, partitionTable);
            }
        }
        columnExpression = StringUtils.replace(columnExpression, "`", "");
        table.setPartitionColumnExpression(columnExpression);
        table.setPartitionTables(new ArrayList<>(partTabMap.values()));

        if (partitionTypeStr != null) {
            PartitionType type = PartitionType.toEnum(partitionTypeStr);
            table.setPartitionType(type);
        }

        if (CollectionUtils.isEmpty(partitionInfos)) {
            table.setPartitionTable(false);
        } else {
            table.setPartitionTable(true);
        }

    }

}
