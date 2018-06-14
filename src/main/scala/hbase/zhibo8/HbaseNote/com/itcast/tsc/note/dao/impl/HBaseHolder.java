package hbase.zhibo8.HbaseNote.com.itcast.tsc.note.dao.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * <p>
 * HBase工具类
 * </p>
 *
 * @author 用户名 2015年6月18日 上午8:58:38
 * @version V1.0
 * @modify by user: {修改人} 2015年6月18日
 * @modify by reason:{方法名}:{原因}
 */
public class HBaseHolder implements Serializable, Closeable {

    // 日志记录器
    protected static final Logger LOGGER = LoggerFactory.getLogger(HBaseHolder.class);
    private static final int DEFAULT_MAX_VERSIONS = 3;
    // HBase配置
    private Configuration config;
    private Admin admin;
    private Connection connection;

    public HBaseHolder() {
        config = HBaseConfiguration.create();
    }

    public HBaseHolder(Configuration config) {
        this.config = config;
    }

    /**
     * 从连接池获取HTable对象
     *
     * @param tableName
     * @return
     * @throws IOException
     * @author
     */
    public Table getTable(String tableName) throws IOException {
        return getConnection().getTable(TableName.valueOf(tableName));
    }

    /**
     * 获取HAdmin对象，建表等操作
     *
     * @return
     * @throws IOException
     * @author
     */
    public Admin getHBaseAdmin() throws IOException {
        if (admin == null) {
            admin = getConnection().getAdmin();
        }
        return admin;
    }

    /**
     * 关闭HTable对象
     *
     * @param table
     * @author
     */
    public void doCloseTable(Table table) {
        if (table == null) {
            return;
        }
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表操作
     *
     * @param tableName
     * @param families
     * @author
     */
    public void createTable(String tableName, String[] families) {
        createTable(tableName, DEFAULT_MAX_VERSIONS, null, families);
    }

    /**
     * 创建表操作
     *
     * @param tableName
     * @param splitKeys
     * @param families
     * @author
     */
    public void createTable(String tableName, byte[][] splitKeys, String[] families) {
        createTable(tableName, DEFAULT_MAX_VERSIONS, splitKeys, families);
    }

    /**
     * 创建表操作
     *
     * @param tableName
     * @param maxVersions
     * @param families
     * @author
     */
    public void createTable(String tableName, int maxVersions, String[] families) {
        createTable(tableName, maxVersions, null, families);
    }

    /**
     * 创建表操作
     *
     * @param tableName
     * @param families
     * @author
     */
    public void createTable(String tableName, int maxVersions, byte[][] splitKeys, String[] families) {
        // 参数判空
        if (StringUtils.isBlank(tableName) || families == null || families.length <= 0) {
            return;
        }
        try {
            // 表不存在则创建
            if (!getHBaseAdmin().tableExists(TableName.valueOf(tableName))) {
                HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
                for (String family : families) {
                    HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
                    columnDescriptor.setCompressionType(Algorithm.SNAPPY);
                    columnDescriptor.setMaxVersions(maxVersions);
                    desc.addFamily(columnDescriptor);
                }
                if (splitKeys != null) {
                    getHBaseAdmin().createTable(desc, splitKeys);
                } else {
                    getHBaseAdmin().createTable(desc);
                }
            } else {
                LOGGER.warn("Table " + tableName + " already exists.");
            }
        } catch (IOException e) {
            LOGGER.error("", e);
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     * @author
     */
    public void dropTable(String tableName) {
        Admin admin = null;
        try {
            admin = getHBaseAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
            }
        } catch (IOException e) {
            LOGGER.error("drop table error." + e);
        } finally {
            if (null != admin) {
                try {
                    admin.close();
                } catch (IOException e) {
                    LOGGER.error("close admin error " + e);
                }
            }
        }
    }

    /**
     * 获取单个列值
     *
     * @param tableName
     * @param rowkey
     * @return
     * @author
     */
    public byte[] get(String tableName, String rowkey, String family, String qualifier) {
        Table table = null;
        try {
            Get get = new Get(Bytes.toBytes(rowkey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            table = getTable(tableName);
            if (getHBaseAdmin().tableExists(TableName.valueOf(tableName))) {
                Result result = table.get(get);
                return result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            } else {
                LOGGER.warn("Table " + tableName + " does not exist.");
            }
        } catch (IOException e) {
            LOGGER.error("获取列值失败！ " + e);
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取单个列值，字符串返回
     *
     * @param tableName
     * @param rowkey
     * @return
     * @author zhanglei11
     */
    public String getString(String tableName, String rowkey, String family, String qualifier) {
        return Bytes.toString(get(tableName, rowkey, family, qualifier));
    }

    /**
     * 获取一行中某列族的值
     *
     * @param tableName
     * @param rowkey
     * @return
     * @author
     */
    public Map<String, byte[]> getMapByKeyAndFamily(String tableName, String rowkey, String family) {
        Map<String, byte[]> map = new HashMap<String, byte[]>();
        Table table = null;
        try {
            Get get = new Get(Bytes.toBytes(rowkey));
            get.addFamily(Bytes.toBytes(family));
            table = getTable(tableName);
            if (getHBaseAdmin().tableExists(TableName.valueOf(tableName))) {
                Result result = table.get(get);
                for (Cell cell : result.rawCells()) {
                    byte[] q = CellUtil.cloneQualifier(cell);
                    byte[] v = CellUtil.cloneValue(cell);
                    map.put(Bytes.toString(q), v);
                }
            } else {
                LOGGER.warn("Table " + tableName + " does not exist.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    /**
     * 获取一整行的值
     *
     * @param tableName
     * @param rowkey
     * @return
     * @author
     */
    public Map<String, byte[]> getRowMap(String tableName, String rowkey) {
        Map<String, byte[]> map = new HashMap<String, byte[]>();
        Table table = null;
        try {
            Get get = new Get(Bytes.toBytes(rowkey));
            table = getTable(tableName);
            if (getHBaseAdmin().tableExists(TableName.valueOf(tableName))) {
                Result result = table.get(get);
                for (Cell cell : result.rawCells()) {
                    byte[] q = CellUtil.cloneQualifier(cell);
                    byte[] v = CellUtil.cloneValue(cell);
                    map.put(Bytes.toString(q), v);
                }
            } else {
                LOGGER.warn("Table " + tableName + " does not exist.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    /**
     * 获取记录
     *
     * @param tableName
     * @param rowkeys
     * @return
     */
    public Result[] getRecodes(String tableName, List<String> rowkeys) {
        Table table = null;
        if (rowkeys == null || rowkeys.size() == 0) {
            LOGGER.warn("Has no rowkeys to get.");
            return null;
        }
        try {
            List<Get> gets = new ArrayList<>();
            Get get = null;
            for (String rowkey : rowkeys) {
                get = new Get(Bytes.toBytes(rowkey));
                gets.add(get);
            }
            table = getTable(tableName);
            if (getHBaseAdmin().tableExists(TableName.valueOf(tableName))) {
                Result[] results = table.get(gets);
                return results;
            } else {
                LOGGER.warn("Table " + tableName + " does not exist.");
                return null;
            }
        } catch (IOException e) {
            LOGGER.error("get table [{}] recodes error", tableName, e);
            return null;
        }
    }

    /**
     * 获取记录
     *
     * @param tableName
     * @param rowkey
     * @return
     */
    public Result getRecode(String tableName, String rowkey) {
        Table table = null;
        try {
            Get get = new Get(Bytes.toBytes(rowkey));
            table = getTable(tableName);
            if (getHBaseAdmin().tableExists(TableName.valueOf(tableName))) {
                Result result = table.get(get);
                return result;
            } else {
                LOGGER.warn("Table " + tableName + " does not exist.");
                return null;
            }
        } catch (IOException e) {
            LOGGER.error("get table [{}] recodes error", tableName, e);
            return null;
        }
    }

    /**
     * 获取记录
     *
     * @param tableName
     * @param gets
     * @return
     */
    public Result[] getRecodesByGets(String tableName, List<Get> gets) {
        Table table = null;
        if (gets == null || gets.size() == 0) {
            LOGGER.warn("Has no gets to get.");
            return null;
        }
        try {
            table = getTable(tableName);
            if (getHBaseAdmin().tableExists(TableName.valueOf(tableName))) {
                Result[] results = table.get(gets);
                return results;
            } else {
                LOGGER.warn("Table " + tableName + " does not exist.");
                return null;
            }
        } catch (IOException e) {
            LOGGER.error("get table [{}] recodes error", tableName, e);
            return null;
        }
    }

    /**
     * 获取记录
     *
     * @param tableName
     * @param get
     * @return
     */
    public Result getRecodeByGet(String tableName, Get get) {
        Table table = null;
        try {
            table = getTable(tableName);
            if (getHBaseAdmin().tableExists(TableName.valueOf(tableName))) {
                Result result = table.get(get);
                return result;
            } else {
                LOGGER.warn("Table " + tableName + " does not exist.");
                return null;
            }
        } catch (IOException e) {
            LOGGER.error("get table [{}] recodes error", tableName, e);
            return null;
        }
    }
    /**
     * 存数据
     */
    public void getRecode(){

    }
    /**
     * 批量存数据
     */
    public void getRecodes() throws Exception {
        TableName tableName = TableName.valueOf("testHbaseTable2");
        Table table = connection.getTable(tableName);
        Random random = new Random();
        List<Put> batPut = new ArrayList<Put>();
        for (int i = 0; i < 10; i++) {
            //构建put的参数是rowkey   rowkey_i (Bytes工具类，各种java基础数据类型和字节数组之间的相互转换)
            Put put = new Put(Bytes.toBytes("rowkey_" + i)); //put 十条数据，每条是独立个体，rowkey不同，但要有组织性

            put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("username"), Bytes.toBytes("un_" + i));
            put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("age"), Bytes.toBytes(random.nextInt(50) + 1));
            put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("birthday"), Bytes.toBytes("20170" + i + "01"));
            put.addColumn(Bytes.toBytes("j"), Bytes.toBytes("phone"), Bytes.toBytes("电话_" + i));
            put.addColumn(Bytes.toBytes("j"), Bytes.toBytes("email"), Bytes.toBytes("email_" + i));
            //单记录put
//            table.put(put);
            batPut.add(put);
        }
        table.put(batPut); //批量写入List<Put>

        System.out.println("表插入数据成功！");
    }


    /**
     * 检测HBase服务是否可用
     *
     * @return
     * @author
     */
    public boolean isHBaseAvailable() {
        try {
            HBaseAdmin.checkHBaseAvailable(config);
        } catch (ZooKeeperConnectionException zkce) {
            LOGGER.error("", zkce);
            return false;
        } catch (MasterNotRunningException e) {
            LOGGER.error("", e);
            return false;
        } catch (Exception e) {
            LOGGER.error("Check HBase available throws an Exception. We don't know whether HBase is running or not.", e);
            return false;
        }
        return true;
    }

    public Connection getConnection() {
        if (connection == null) {
            try {
                connection = ConnectionFactory.createConnection(config);
            } catch (IOException e) {
                LOGGER.error("Create HBase connect error.", e);
            }
        }
        return connection;
    }

    @Override
    public void close() throws IOException {
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    public static void main(String[] args) {
        HBaseHolder hBaseHolder = new HBaseHolder();
        hBaseHolder.createTable("comment",new String[]{"pl"});//pinglun 评论
    }
}