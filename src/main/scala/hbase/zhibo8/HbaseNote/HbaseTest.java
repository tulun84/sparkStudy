package hbase.zhibo8.HbaseNote;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HbaseTest {
    public Connection connection;
    //用hbaseconfiguration初始化配置信息时会自动加载当前应用classpath下的hbase-site.xml
    public static Configuration configuration = HBaseConfiguration.create();
    public Table table;
    public Admin admin;

    public HbaseTest() throws Exception {
        //对connection初始化

//        connection = ConnectionFactory.createConnection(configuration);
//        admin = connection.getAdmin();


        // 取得一个数据库连接的配置参数对象
        Configuration conf = HBaseConfiguration.create();

        // 设置连接参数：HBase数据库所在的主机IP
        conf.set("hbase.zookeeper.quorum", "hb-proxy-pub-bp12mmg4lu45o4ivy-001.hbase.rds.aliyuncs.com,hb-proxy-pub-bp12mmg4lu45o4ivy-002.hbase.rds.aliyuncs.com,hb-proxy-pub-bp12mmg4lu45o4ivy-003.hbase.rds.aliyuncs.com");
        // 设置连接参数：HBase数据库使用的端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        // 取得一个数据库连接对象
        connection = ConnectionFactory.createConnection(conf);

        // 取得一个数据库元数据操作对象
        admin = connection.getAdmin();
    }

    //创建表
    public void createTable(String tablename, String... cf1) throws Exception {
        //获取admin对象
        Admin admin = connection.getAdmin();
        //创建tablename对象描述表的名称信息
        TableName tname = TableName.valueOf(tablename);//bd17:mytable
        //创建HTableDescriptor对象，描述表信息
        HTableDescriptor tDescriptor = new HTableDescriptor(tname);
        //判断是否表已存在
        if (admin.tableExists(tname)) {
            System.out.println("表" + tablename + "已存在");
            return;
        }
        //添加表列簇信息
        for (String cf : cf1) {
            HColumnDescriptor famliy = new HColumnDescriptor(cf);
            tDescriptor.addFamily(famliy);
        }
        //调用admin的createtable方法创建表
        admin.createTable(tDescriptor);
        System.out.println("表" + tablename + "创建成功");
    }

    //删除表
    public void deleteTable(String tablename) throws Exception {
        Admin admin = connection.getAdmin();
        TableName tName = TableName.valueOf(tablename);
        if (admin.tableExists(tName)) {
            admin.disableTable(tName);
            admin.deleteTable(tName);
            System.out.println("删除表" + tablename + "成功！");
        } else {
            System.out.println("表" + tablename + "不存在。");
        }
    }

    //新增数据到表里面Put
    public void putData() throws Exception {
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

    //查询数据
    public void getData() throws Exception {
        TableName tableName = TableName.valueOf("testHbaseTable2");
        table = connection.getTable(tableName);
        //构建get对象
        List<Get> gets = new ArrayList<Get>();
        for (int i = 0; i < 5; i++) {
            //同过rowkey构建Get对象
            Get get = new Get(Bytes.toBytes("rowkey_" + i));
            gets.add(get);
        }
        //通过List<Get>对象批量获取Result
        Result[] results = table.get(gets);
        for (Result result : results) {
            //一行一行读取数据
//            NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> maps = result.getMap();
//            for(byte[] cf:maps.keySet()){
//                NavigableMap<byte[],NavigableMap<Long,byte[]>> valueWithColumnQualify = maps.get(cf);
//                for(byte[] columnQualify:valueWithColumnQualify.keySet()){
//                    NavigableMap<Long,byte[]> valueWithTimeStamp = valueWithColumnQualify.get(columnQualify);
//                    for(Long ts:valueWithTimeStamp.keySet()){
//                        byte[] value = valueWithTimeStamp.get(ts);
//                        System.out.println("rowkey:"+Bytes.toString(result.getRow())+",columnFamliy:"+
//                                Bytes.toString(cf)+",comlumnQualify:"+Bytes.toString(columnQualify)+",timestamp:"
//                                +new Date(ts)+",value:"+Bytes.toString(value)
//                                );
//                    }
//                }
//            }

            //使用字段名称和列簇名称来获取value值
//            System.out.println("rowkey:"+Bytes.toString(result.getRow())+",columnfamily:i,columnqualify:username,value:"+
//                    Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("username")))
//                    );
//            System.out.println("rowkey:"+Bytes.toString(result.getRow())+",columnfamily:i,columnqualify:age,value:"+
//                    Bytes.toInt(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("age")))
//                    );

            //使用cell获取result里面的数据
            CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                //从单元格cell中把数据获取并输出
                //使用 CellUtil工具类，从cell中把数据获取出来
                String famliy = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("rowkey:" + rowkey + ",columnfamily:" + famliy + ",qualify:" + qualify + ",value:" + value);
            }
        }
    }

    //关闭连接
    public void cleanUp() throws Exception {
        connection.close();
    }

    public static void main(String[] args) throws Exception {
        HbaseTest hbaseTest = new HbaseTest();
//        hbaseTest.createTable("testHbaseTable2", "i", "j");
//        hbaseTest.deleteTable("testHbaseTable2");
//        hbaseTest.putData();
//        hbaseTest.getData();
        hbaseTest.cleanUp();
    }
}