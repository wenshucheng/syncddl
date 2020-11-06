package com.huacloud.synctable.dao;

import com.huacloud.synctable.entity.ConstraintInfo;
import com.huacloud.synctable.mapping.Column;
import com.huacloud.synctable.mapping.Index;
import com.huacloud.synctable.mapping.IndexColumn;
import com.huacloud.synctable.mapping.SortOrder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 4/23/2019 6:03 PM
 */
public class TBaseDbMetaInfoDaoImpl extends AbstractDbMetaInfoDao {

    public TBaseDbMetaInfoDaoImpl(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override
    public String queryTableComment(String catalog, String schema, String tableName) {
        String sql =
                "select d.description " +
                "from pg_class cls, pg_description d, pg_namespace n " +
                "where cls.oid = d.objoid and d.objsubid = 0  " +
                "and cls.relnamespace = n.oid  " +
                "and n.nspname = ? and cls.relname = ?";
        List<Map<String, Object>> resultMap = jdbcTemplate.queryForList(sql, schema, tableName);
        if (CollectionUtils.isNotEmpty(resultMap)) {
            return (String) resultMap.get(0).get("description");
        } else {
            return null;
        }
    }

    @Override
    public List<Column> queryColumnInfo(String catalog, String schema, String tableName) {
        String sql =
                "select " +
                "   col.column_name," +
                "   col.data_type," +
                "   col.character_maximum_length," +
                "   col.numeric_precision," +
                "   col.datetime_precision," +
                "   col.numeric_scale," +
                "   col.is_nullable," +
                "   col.column_default," +
                "   d.description " +
                "from information_schema.columns col " +
                "inner join pg_class c " +
                        "on c.relname = col.table_name " +
                "inner join pg_namespace n " +
                        "on n.nspname = col.table_schema and c.relnamespace = n.oid " +
                "left join pg_attribute a " +
                        "on a.attrelid = c.oid and a.attname = col.column_name " +
                "left join pg_type t " +
                        "on a.atttypid = t.oid " +
                "left join pg_description d " +
                        "on d.objsubid = a.attnum and d.objoid = a.attrelid " +
                "where col.table_catalog = ? " +
                "   and col.table_schema = ? " +
                "   and col.table_name = ? " +
                "order by col.ordinal_position";
        List<Map<String, Object>> columnMapList = jdbcTemplate.queryForList(
                sql, catalog, schema, tableName);
        List<Column> columnList = new ArrayList<>();
        for (Map<String, Object> columnMap : columnMapList) {
            String columnName = (String) columnMap.get("column_name");
            String dataType = (String) columnMap.get("data_type");
            Integer characterMaximumLength = (Integer) columnMap.get("character_maximum_length");
            Integer numericPrecision = (Integer) columnMap.get("numeric_precision");
            Integer datetimePrecision = (Integer) columnMap.get("datetime_precision");
            Integer numericScale = (Integer) columnMap.get("numeric_scale");
            String isNullable = (String) columnMap.get("is_nullable");
            String columnDefault = (String) columnMap.get("column_default");
            if (StringUtils.containsIgnoreCase(columnDefault, "nextval(")) {
                columnDefault = null;
            }

            String columnComment = (String) columnMap.get("description");

            Column column = new Column();
            column.setComment(columnComment);
            column.setSqlType(dataType);
            column.setName(columnName);
            column.setDefaultValue(columnDefault);
            column.setNullable(StringUtils.equalsIgnoreCase(isNullable, "YES"));
            if (characterMaximumLength != null) {
                column.setLength(characterMaximumLength);
            }
            if (numericPrecision != null) {
                column.setPrecision(numericPrecision);
            }
            if (datetimePrecision != null) {
                column.setPrecision(datetimePrecision);
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
        String sql =
                "select " +
                "   con.contype as constraintType, " +
                "   con.conname as constraintName, " +
                "   a.attname as columnName, " +
                "   a.attnum as ordinalPosition " +
                "from pg_constraint con " +
                "inner join pg_class cls ON con.conrelid = cls.oid and cls.relname = ? " +
                "inner join pg_attribute a on a.attrelid = cls.oid and  con.conkey @> array[a.attnum] " +
                "inner join pg_namespace n on cls.relnamespace = n.oid and n.nspname = ? " +
                "order by con.conname";
        List<ConstraintInfo> constraintInfo = jdbcTemplate.query(
                sql, rowMapper, tableName, schema);
        for (ConstraintInfo con : constraintInfo) {
            String constraintType = con.getConstraintType();
            if (StringUtils.equalsIgnoreCase(constraintType, "p")) {
                con.setConstraintType("P");
            } else if (StringUtils.equalsIgnoreCase(constraintType, "u")) {
                con.setConstraintType("U");
            }
        }
        return constraintInfo;
    }

    @Override
    public List<String> queryAllSchemaNames() {
        String sql = "select nspname from pg_namespace";
        return jdbcTemplate.queryForList(sql, String.class);
    }

    @Override
    public List<String> queryAllTabNames(String schema) {
        String sql = "select tablename from pg_tables where schemaname = ?";
        return jdbcTemplate.queryForList(sql, new String[]{schema}, String.class);
    }

    @Override
    public List<Index> queryIndexInfo(String catalog, String schema, String tableName) {
        String sql =
                "select " +
                "   A.INDEXNAME, " +
                "   C.INDNATTS, " +
                "   C.INDISUNIQUE, " +
                "   C.INDISPRIMARY,  " +
                "   c.indkey " +
                "from PG_AM B  " +
                "   left join PG_CLASS F on B.OID = F.RELAM  " +
                "   left join PG_STAT_ALL_INDEXES E on F.OID = E.INDEXRELID  " +
                "   left join PG_INDEX C on E.INDEXRELID = C.INDEXRELID  " +
                "   left outer join PG_DESCRIPTION D " +
                "       on C.INDEXRELID = D.OBJOID, PG_INDEXES A " +
                "where A.SCHEMANAME = E.SCHEMANAME " +
                "   and A.TABLENAME = E.RELNAME " +
                "   and A.INDEXNAME = E.INDEXRELNAME " +
                "   and E.SCHEMANAME = ? " +
                "   and E.RELNAME = ?";
        List<Map<String, Object>> dataMapList = jdbcTemplate.queryForList(sql, schema, tableName);
        List<Column> columns = queryColumnInfo(catalog, schema, tableName);

        List<Index> indexList = new ArrayList<>();
        for (Map<String, Object> columnMap : dataMapList) {
            Boolean unique = (Boolean) columnMap.get("INDISUNIQUE");
            String keyName = (String) columnMap.get("INDEXNAME");
            PGobject columnObj = (PGobject) columnMap.get("INDKEY");
            String value = columnObj.getValue();
            String[] columnIndes = StringUtils.split(value, " ");

            Index index = new Index();
            index.setName(keyName);
            index.setUnique(unique);
            indexList.add(index);

            for (String colIndex : columnIndes) {
                IndexColumn indexColumn = new IndexColumn();
                indexColumn.setColumnName(columns.get(Integer.parseInt(colIndex)-1).getName());
                indexColumn.setSortOrder(SortOrder.DEFAULT);
                index.getColumnList().add(indexColumn);
            }
        }
        return indexList;
    }

}
