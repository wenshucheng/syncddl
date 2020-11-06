package com.huacloud.synctable;

import com.huacloud.synctable.dao.AbstractDbMetaInfoDao;
import com.huacloud.synctable.dialect.Dialect;
import com.huacloud.synctable.entity.DBType;
import com.huacloud.synctable.mapping.Table;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 9/16/2019 11:13 AM
 */
public class Db2SqlApp {

    private static final Logger logger = LoggerFactory.getLogger(Db2SqlApp.class);

    public static String db2sql(
            DBType srcDb, DBType destDb,
            DataSource dataSource, String schemaName,
            String tableName, String destSchemaName) {
        return db2sql(srcDb, destDb, dataSource, null, schemaName, tableName, destSchemaName);
    }

    /**
     * transform from Db to SQL
     * @param srcDb source type
     * @param destDb destination type
     * @param dataSource datasource name
     * @param dbName catalog name of postgresql
     * @param schemaName schema name
     * @param tableName table name
     * @param destSchemaName destination's scheme name
     * @return SQL text
     */
    public static String db2sql(
            DBType srcDb, DBType destDb,
            DataSource dataSource, String dbName, String schemaName,
            String tableName, String destSchemaName) {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        AbstractDbMetaInfoDao dao = srcDb.getDbDao(jdbcTemplate);
        Dialect dialect = srcDb.getDialect();
        Dialect dbDialect = destDb.getDialect();

        List<String> tabNames = new ArrayList<>();
        StringBuilder allSql = new StringBuilder();
        if (StringUtils.isNotEmpty(tableName)) {

            if (StringUtils.contains(tableName, ",")) {
                String[] split = StringUtils.split(tableName, ",");
                tabNames.addAll(Arrays.asList(split));
            } else {
                tabNames.add(tableName);
            }

        } else {
            tabNames.addAll(dao.queryAllTabNames(schemaName));
        }

        if (tabNames.size() <= 20) {
            for (String tabName : tabNames) {
                Table table = dao.queryTable(dbName, schemaName, tabName, dialect);
                String sql = table.getFullCreateTableSQL(dbDialect, destSchemaName);
                allSql.append(sql);
            }
        } else {
            logger.info("使用并发执行DDL: table count = {}", tabNames.size());
            String manySql = concurrenceDdl(tabNames, dao, schemaName, destSchemaName, dialect, dbDialect);
            allSql.append(manySql);
        }
        return allSql.toString();
    }

    static class NameableThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        NameableThreadFactory(String name) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = name + "-pool-" + poolNumber.getAndIncrement() + "-thread-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }

    static class DDLTask implements Runnable {

        private String tableName;

        private CountDownLatch countDownLatch;

        private AbstractDbMetaInfoDao dao;

        private StringBuffer sqlBuffer;

        private String schemaName;

        private String destSchemaName;

        private Dialect srcDialect;

        private Dialect destDialect;

        public DDLTask(String tableName,
                       CountDownLatch countDownLatch,
                       AbstractDbMetaInfoDao dao,
                       StringBuffer sql,
                       String schemaName, String destSchemaName,
                       Dialect srcDialect, Dialect destDialect) {
            this.tableName = tableName;
            this.countDownLatch = countDownLatch;
            this.dao = dao;
            this.sqlBuffer = sql;
            this.schemaName = schemaName;
            this.destSchemaName = destSchemaName;
            this.srcDialect = srcDialect;
            this.destDialect = destDialect;
        }

        @Override
        public void run() {
            logger.info("DDL执行: {}", this.tableName);
            try {
                Table table = dao.queryTable(null, schemaName, tableName, srcDialect);
                String sql = table.getFullCreateTableSQL(destDialect, destSchemaName);
                sqlBuffer.append(sql);
            } finally {
                countDownLatch.countDown();
            }
            logger.info("DDL执行完成");
        }
    }

    private static String concurrenceDdl(List<String> tabNames, AbstractDbMetaInfoDao dao,
                      String schemaName, String destSchemaName,
                      Dialect srcDialect, Dialect destDialect) {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(
                4, 8, 1000,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new NameableThreadFactory("ddl-task"),
                new ThreadPoolExecutor.AbortPolicy());
        CountDownLatch countDownLatch = new CountDownLatch(tabNames.size());
        StringBuffer sql = new StringBuffer();
        for (String tableName : tabNames) {
            poolExecutor.submit(new DDLTask(tableName, countDownLatch, dao, sql,
                    schemaName, destSchemaName, srcDialect, destDialect));
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        }
        poolExecutor.shutdownNow();
        logger.info("并发执行DDL完成");
        return sql.toString();
    }

}




