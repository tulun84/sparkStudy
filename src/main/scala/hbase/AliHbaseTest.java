package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;


public class AliHbaseTest {

    private static final String TABLE_NAME = "mytable2";
    private static final String CF_DEFAULT = "cf";
    public static final byte[] QUALIFIER = "col1".getBytes();
    private static final byte[] ROWKEY = "rowkey1".getBytes();
    public static void main(String[] args) {
        //建立Hbase连接
        Configuration config = HBaseConfiguration.create();
        String zkAddress = "hb-proxy-pub-bp12mmg4lu45o4ivy-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp12mmg4lu45o4ivy-002.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp12mmg4lu45o4ivy-003.hbase.rds.aliyuncs.com:2181";
        config.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            //创建Hbase中的表结构
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            tableDescriptor.addFamily(new HColumnDescriptor(CF_DEFAULT));
            System.out.print("Creating table. ");
            Admin admin = connection.getAdmin();
            admin.createTable(tableDescriptor);
            System.out.println(" Done.");
            //获取Hbase中的已创建的表对象
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            try {
                //往Hbase中写数据
                Put put = new Put(ROWKEY);
                put.addColumn(CF_DEFAULT.getBytes(), QUALIFIER, "this is value".getBytes());
                table.put(put);
                //从Hbase中读数据
                Get get = new Get(ROWKEY);
                Result r = table.get(get);
                byte[] b = r.getValue(CF_DEFAULT.getBytes(), QUALIFIER);  // returns current version of value
                System.out.println(new String(b));
            } finally {
                if (table != null) table.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
