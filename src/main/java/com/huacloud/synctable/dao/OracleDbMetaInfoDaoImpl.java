package com.huacloud.synctable.dao;

import com.huacloud.synctable.entity.ConstraintInfo;
import com.huacloud.synctable.mapping.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.util.*;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 4/18/2019 5:49 PM
 */
public class OracleDbMetaInfoDaoImpl extends AbstractDbMetaInfoDao {

    private static final Logger logger = LoggerFactory.getLogger(OracleDbMetaInfoDaoImpl.class);

    public OracleDbMetaInfoDaoImpl(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override
    public String queryTableComment(String catalog, String schema, String tableName) {
        String tableCommentSql =
                "select t.COMMENTS " +
                        "  from all_tab_comments t " +
                        " where t.OWNER = ? AND t.TABLE_NAME = ? ";
        Map<String, Object> tableCommentMap;
        try {
            tableCommentMap = jdbcTemplate.queryForMap(tableCommentSql, schema.toUpperCase(), tableName);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
        return (String) tableCommentMap.get("COMMENTS");
    }

    @Override
    public List<Column> queryColumnInfo(String catalog, String schema, String tableName) {
        String columnInfoSql =
                "select c.COLUMN_ID," +
                    "   c.TABLE_NAME," +
                    "   c.COLUMN_NAME," +
                    "   c.DATA_TYPE," +
                    "   c.DATA_LENGTH," +
                    "   c.DATA_PRECISION," +
                    "   c.DATA_SCALE," +
                    "   c.NULLABLE," +
                    "   c.DATA_DEFAULT," +
                    "   cm.COMMENTS " +
                    "from all_tab_columns c, all_col_comments cm " +
                    "where c.COLUMN_NAME = cm.COLUMN_NAME " +
                    "   and c.OWNER = cm.OWNER " +
                    "   and c.TABLE_NAME = cm.TABLE_NAME " +
                    "   and c.OWNER = ? " +
                    "   and c.table_name = ? " +
                    "order by c.COLUMN_ID ";

        List<Map<String, Object>> columnMapList = jdbcTemplate.queryForList(
                columnInfoSql, schema.toUpperCase(), tableName);
        logger.info("字段长度：{}", columnMapList.size());
        List<Column> columnList = new ArrayList<>();

        for (Map<String, Object> columnMap : columnMapList) {
            String columnName = (String) columnMap.get("COLUMN_NAME");
            String dataType = (String) columnMap.get("DATA_TYPE");
            BigDecimal dataLength = (BigDecimal) columnMap.get("DATA_LENGTH");
            int dataLen = 0;
            if (dataLength != null) {
                dataLen = dataLength.intValue();
            }

            BigDecimal dataPrecision = (BigDecimal) columnMap.get("DATA_PRECISION");
            BigDecimal dataScale = (BigDecimal) columnMap.get("DATA_SCALE");
            String nullable = (String) columnMap.get("NULLABLE");
            String dataDefault = (String) columnMap.get("DATA_DEFAULT");
            String comment = (String) columnMap.get("COMMENTS");

            if (StringUtils.containsIgnoreCase(dataType, "interval day")) {
                dataType = "interval day to second";

            } else if (StringUtils.containsIgnoreCase(dataType, "interval year")) {
                dataType = "interval year to month";

            } else if (StringUtils.containsIgnoreCase(dataType, "timestamp") &&
                    StringUtils.containsIgnoreCase(dataType, "with local time zone")) {
                dataType = "timestamp with local time zone";

            } else if (StringUtils.containsIgnoreCase(dataType, "timestamp") &&
                    StringUtils.containsIgnoreCase(dataType, "with time zone")) {
                dataType = "timestamp with time zone";

            } else if (StringUtils.containsIgnoreCase(dataType, "timestamp")) {
                dataType = "timestamp";

            } else if (StringUtils.containsIgnoreCase(dataType, "nvarchar2")) {
                if (dataLen > 0) {
                    dataLen = dataLen / 3;
                }
            }

            Column column = new Column();
            column.setName(columnName);
            column.setSqlType(dataType);
            column.setLength(dataLen);
            column.setDefaultValue(dataDefault);
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
                "  col.COLUMN_NAME as columnName," +
                "  con.CONSTRAINT_NAME as constraintName," +
                "  con.CONSTRAINT_TYPE as constraintType," +
                "  col.POSITION as ordinalPosition" +
                "  from all_constraints con, all_cons_columns col" +
                " where con.constraint_name = col.constraint_name" +
                "   and col.owner = ?" +
                "   and col.table_name = ?";

        List<ConstraintInfo> constraintInfo = jdbcTemplate.query(
                constraintSql, rowMapper,
                schema.toUpperCase(), tableName);
        for (ConstraintInfo con : constraintInfo) {
            String constraintType = con.getConstraintType();
            if (StringUtils.equalsIgnoreCase(constraintType, "P")) {
                con.setConstraintType("P");
            } else if (StringUtils.equalsIgnoreCase(constraintType, "U")) {
                con.setConstraintType("U");
            }
        }
        return constraintInfo;
    }

    @Override
    public List<String> queryAllSchemaNames() {
        String sql = "select username from all_users";
        return jdbcTemplate.queryForList(sql, String.class);
    }

    @Override
    public List<String> queryAllTabNames(String schema) {
        String sql = "select table_name from all_tab_columns where owner = ?";
        return jdbcTemplate.queryForList(sql,
                new String[]{schema.toUpperCase()}, String.class);
    }

    @Override
    public List<Index> queryIndexInfo(String catalog, String schema, String tableName) {
        String sql =
                "select i.index_name," +
                "       i.uniqueness," +
                "       t.COLUMN_NAME," +
                "       t.COLUMN_POSITION," +
                "       t.DESCEND " +
                "  from user_ind_columns t, user_indexes i " +
                " where t.index_name = i.index_name " +
                "   and i.table_owner = ? " +
                "   and i.table_name = ? " +
                "   and i.index_type = 'NORMAL' " +
                " order by i.index_name, t.COLUMN_POSITION";
        List<Map<String, Object>> dataMapList = jdbcTemplate.queryForList(sql, schema, tableName);

        Map<String, Index> indexMap = new HashMap<>();
        for (Map<String, Object> columnMap : dataMapList) {
            String unique = (String) columnMap.get("UNIQUENESS");
            String keyName = (String) columnMap.get("INDEX_NAME");
            String columnName = (String) columnMap.get("COLUMN_NAME");
            String descend = (String) columnMap.get("DESCEND");

            IndexColumn indexColumn = new IndexColumn();
            indexColumn.setColumnName(columnName);
            indexColumn.setSortOrder(
                    StringUtils.equalsIgnoreCase(descend, "ASC") ?
                            SortOrder.ASC : SortOrder.DESC);

            if (indexMap.containsKey(keyName)) {
                Index index = indexMap.get(keyName);
                index.getColumnList().add(indexColumn);

            } else {
                Index index = new Index();
                index.setName(keyName);
                index.setUnique(StringUtils.equalsIgnoreCase(unique, "UNIQUE"));
                index.getColumnList().add(indexColumn);
                indexMap.put(keyName, index);
            }
        }
        return new ArrayList<>(indexMap.values());
    }

    @Override
    public void setPartitionInfo(Table table) {
        String schemaName = table.getSchema().toUpperCase();
        String tableName = table.getName().toUpperCase();
        String queryPartitionInfoSQL =
                "select tp.partitioning_type, tp.subpartitioning_type " +
                "from ALL_PART_TABLES tp " +
                "where tp.owner = ? and tp.table_name = ? ";
        Map<String, Object> partTypeMap;
        try {
            partTypeMap = jdbcTemplate.queryForMap(queryPartitionInfoSQL, schemaName, tableName);
        } catch (EmptyResultDataAccessException e) {
            return;//说明没有分区信息
        }

        String partitionType = (String) partTypeMap.get("PARTITIONING_TYPE");
        String subPartitionType = (String) partTypeMap.get("SUBPARTITIONING_TYPE");

        if (StringUtils.isBlank(partitionType)) {
            return;
        }

        PartitionType type = PartitionType.toEnum(partitionType);
        table.setPartitionTable(true);
        table.setPartitionType(type);

        String partitionColumnSQL =
                "select pc.column_name " +
                "  from ALL_PART_KEY_COLUMNS pc " +
                " where pc.object_type = 'TABLE'  " +
                "   and pc.owner = ?  " +
                "   and pc.name = ? " +
                "order by pc.column_position";

        List<String> partitionColumns = jdbcTemplate.queryForList(partitionColumnSQL,
                String.class, schemaName, tableName);
        table.setPartitionColumnExpression(StringUtils.join(partitionColumns, ","));

        String partitionInfoSQL =
                "select tp.partition_name, tp.partition_position, tp.high_value " +
                "from ALL_TAB_PARTITIONS tp " +
                "where tp.table_owner = ? and tp.table_name = ? " +
                "order by tp.partition_position";
        List<Map<String, Object>> partitionInfos =
                jdbcTemplate.queryForList(partitionInfoSQL, schemaName, tableName);

        Map<String, PartitionTable> partTabMap = new TreeMap<>();
        for (Map<String, Object> dataMap : partitionInfos) {
            String partitionName = (String) dataMap.get("PARTITION_NAME");
            BigDecimal partitionPosition = (BigDecimal) dataMap.get("PARTITION_POSITION");
            String highValue = (String) dataMap.get("HIGH_VALUE");

            PartitionTable partitionTable = new PartitionTable();
            partitionTable.setName(partitionName);
            partitionTable.setPosition(partitionPosition.intValue());
            partitionTable.setValue(highValue);
            partTabMap.put(partitionName, partitionTable);
        }


        //如果有分区子表，才进行查询
        if (StringUtils.isNotEmpty(subPartitionType)) {
            String subPartColSql =
                    "select pc.column_name " +
                            "  from ALL_SUBPART_KEY_COLUMNS pc " +
                            " where pc.object_type = 'TABLE'  " +
                            "   and pc.owner = ?  " +
                            "   and pc.name = ? " +
                            "order by pc.column_position";

            List<String> subPartitionColumns = jdbcTemplate.queryForList(subPartColSql,
                    String.class, schemaName, tableName);
            String subPartitionColumnExpression = StringUtils.join(subPartitionColumns, ",");

            String subPartSql =
                    "select " +
                        "ts.partition_name, " +
                        "ts.subpartition_name, " +
                        "ts.high_value, " +
                        "ts.subpartition_position " +
                    "from ALL_TAB_SUBPARTITIONS ts " +
                    "where ts.table_owner = ?  " +
                    "and ts.table_name = ? " +
                    "order by ts.partition_name, ts.subpartition_position";

            List<Map<String, Object>> subPartitionInfos =
                    jdbcTemplate.queryForList(subPartSql, schemaName, tableName);
            for (Map<String, Object> dataMap : subPartitionInfos) {
                String partitionName = (String) dataMap.get("PARTITION_NAME");
                String subPartitionName = (String) dataMap.get("SUBPARTITION_NAME");
                BigDecimal subPartitionPosition = (BigDecimal) dataMap.get("SUBPARTITION_POSITION");
                String highValue = (String) dataMap.get("HIGH_VALUE");

                PartitionTable subPartTab = new PartitionTable();
                subPartTab.setName(subPartitionName);
                subPartTab.setPosition(subPartitionPosition.intValue());
                subPartTab.setValue(highValue);

                PartitionTable partitionTable = partTabMap.get(partitionName);
                partitionTable.setSubPartCol(subPartitionColumnExpression);
                partitionTable.setSubParttype(PartitionType.toEnum(subPartitionType));
                partitionTable.getSubPartTab().add(subPartTab);
            }
        }
        table.setPartitionTables(new ArrayList<>(partTabMap.values()));
    }

}
