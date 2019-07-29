package structuredStreaming.sink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * @Description:配置文件读取工具类
 * @Author: chenli
 * @Date: 2019-05-08 13:58
 */
public class PropertiesUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtil.class);

    private static Properties properties;

    static {
        try {
            properties = new Properties();
            properties.load(PropertiesUtil.class.getResourceAsStream("/dw_task.properties"));
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException("dw_task.properties load failed!");
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static Integer getIntegerProperty(String key) {
        String property = properties.getProperty(key);
        return property == null ? null : Integer.valueOf(property);
    }

    public static Long getLongProperty(String key) {
        String property = properties.getProperty(key);
        return property == null ? null : Long.valueOf(property);
    }

    public static Double getDoubleProperty(String key) {
        String property = properties.getProperty(key);
        return property == null ? null : Double.valueOf(property);
    }

    public static boolean getBooleanProperty(String key) {
        String property = properties.getProperty(key);
        return property == null ? null : Boolean.valueOf(property);
    }

}
