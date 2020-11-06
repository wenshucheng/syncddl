package com.huacloud.synctable.utils;

import com.huacloud.synctable.entity.DBType;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileUrlResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 8/26/2019 2:43 PM
 */
public class DbDataTypeConf {

    public final static Logger logger = LoggerFactory.getLogger(DbDataTypeConf.class);

    public static Map[] parserConf(DBType dbType) {

        Map<String, Integer> db2CommonMap = new HashMap<>();
        Map<Integer, String> common2DbMap = new HashMap<>();

        String appPath = System.getProperty("user.dir") +
                File.separator + "conf" + File.separator +
                dbType.getDbName() + ".datatype.properties";

        Properties appProperties = null;
        try {
            if (!new File(appPath).exists()) {
                return new Map[]{MapUtils.EMPTY_MAP, MapUtils.EMPTY_MAP};
            }
            appProperties = PropertiesLoaderUtils.loadProperties(new FileUrlResource(appPath));
        } catch (IOException e) {
            logger.error("加载配置失败：path=" + appPath, e);
        }

        Enumeration<String> names = (Enumeration<String>) appProperties.propertyNames();
        while (names.hasMoreElements()) {
            String key = names.nextElement();
            String[] split = StringUtils.split(key, ".");
            String type = split[0];
            String dataType = split[1];
            String value = appProperties.getProperty(key);
            if (StringUtils.equalsIgnoreCase("db2common", type)) {
                db2CommonMap.put(dataType, Integer.valueOf(value));

            } else if (StringUtils.equalsIgnoreCase("common2db", type)) {
                common2DbMap.put(Integer.valueOf(dataType), value);

            } else {
                String err = "无法识别的类型：" + type;
                logger.error(err);
                throw new RuntimeException(err);
            }
        }

        return new Map[]{db2CommonMap, common2DbMap};
    }

}
