package hbase.zhibo8.HbaseNote.com.itcast.tsc.note.dao.impl;

import com.zhibo8.HbaseNote.com.itcast.tsc.note.dao.HBaseDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class HBaseDAOImp implements HBaseDAO, Serializable {

    HConnection hTablePool = null;

    public HBaseDAOImp() {
        Configuration conf = new Configuration();
        String zk_list = "CDH01:2181,CDH02:2181,CDH03:2181";
        conf.set("hbase.zookeeper.quorum", zk_list);
        try {
            hTablePool = HConnectionManager.createConnection(conf);   //connetion
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void save(Put put, String tableName) {
        // TODO Auto-generated method stub
        HTableInterface table = null;
        try {
            table = hTablePool.getTable(tableName);
            table.put(put);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void insert(String tableName, String rowKey, String family,
                       String quailifer, String value) {
        // TODO Auto-generated method stub
        HTableInterface table = null;
        try {
            table = hTablePool.getTable(tableName);
            Put put = new Put(rowKey.getBytes());
            put.add(family.getBytes(), quailifer.getBytes(), value.getBytes());
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void insert(String tableName, String rowKey, String family, String quailifer[], String value[]) {
        HTableInterface table = null;
        try {
            table = hTablePool.getTable(tableName);
            Put put = new Put(rowKey.getBytes());
            // 批量添加
            for (int i = 0; i < quailifer.length; i++) {
                String col = quailifer[i];
                String val = value[i];
                put.add(family.getBytes(), col.getBytes(), val.getBytes());
            }
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void save(List<Put> Put, String tableName) {
        // TODO Auto-generated method stub
        HTableInterface table = null;
        try {
            table = hTablePool.getTable(tableName);
            table.put(Put);
        } catch (Exception e) {
            // TODO: handle exception
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    @Override
    public Result getOneRow(String tableName, String rowKey) {
        // TODO Auto-generated method stub
        HTableInterface table = null;
        Result rsResult = null;
        try {
            table = hTablePool.getTable(tableName);
            Get get = new Get(rowKey.getBytes());
            rsResult = table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return rsResult;
    }

    @Override
    public List<Result> getRows(String tableName, String rowKeyLike) {
        // TODO Auto-generated method stub
        HTableInterface table = null;
        List<Result> list = null;
        try {
            table = hTablePool.getTable(tableName);
            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<Result>();
            for (Result rs : scanner) {
                list.add(rs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    @Override
    public List<Result> getRows(String tableName, String rowKeyLike, String cols[]) {
        // TODO Auto-generated method stub
        HTableInterface table = null;
        List<Result> list = null;
        try {
            table = hTablePool.getTable(tableName);
            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
            Scan scan = new Scan();
            for (int i = 0; i < cols.length; i++) {
                scan.addColumn("cf".getBytes(), cols[i].getBytes());
            }
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<Result>();
            for (Result rs : scanner) {
                list.add(rs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    @Override
    public List<Result> getRows(String tableName, String startRow, String stopRow) {
        HTableInterface table = null;
        List<Result> list = null;
        try {
            table = hTablePool.getTable(tableName);
            Scan scan = new Scan();
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(stopRow.getBytes());
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<Result>();
            for (Result rsResult : scanner) {
                list.add(rsResult);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    @Override
    public void deleteRecords(String tableName, String rowKeyLike) {
        HTableInterface table = null;
        try {
            table = hTablePool.getTable(tableName);
            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            List<Delete> list = new ArrayList<Delete>();
            for (Result rs : scanner) {
                Delete del = new Delete(rs.getRow());
                list.add(del);
            }
            table.delete(list);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    public static void main(String[] args) {
        // TODO Auto-generated method stub
        HBaseDAO dao = new HBaseDAOImp();
        List<Put> list = new ArrayList<Put>();
        Put put = new Put("r1".getBytes());
        put.add("cf".getBytes(), "name".getBytes(), "wangwu".getBytes());
        list.add(put);
        dao.save(put, "test");

        dao.insert("test", "r1", "cf", "age", "30");
        dao.insert("test", "testrow", "cf", "cardid", "12312312335");
        dao.insert("test", "testrow", "cf", "tel", "13512312345");
        List<Result> list1 = dao.getRows("test", "2018-02-24");
        List<Result> list2 = dao.getRows("test", "r1", new String[]{"name", "age"});
        for (Result rs : list2) {
            for (KeyValue keyValue : rs.raw()) {
                System.out.println("rowkey:" + new String(keyValue.getRow()));
                System.out.println("Qualifier:" + new String(keyValue.getQualifier()));
                System.out.println("Value:" + new String(keyValue.getValue()));
                System.out.println("----------------");
            }
        }


    }

}
