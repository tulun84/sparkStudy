package hbase.zhibo8.HbaseNote.com.itcast.tsc.note.dao;

import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public interface TableDao {

	public void creatTable(String tableName, String[] familys) throws IOException;
	public void createTableSplit(String tableName, String[] familys) throws IOException;
	public void createTableSplit2(String tableName, String[] familys) throws IOException;
	public void deleteTable(String tableName) throws IOException;
	public Table getTable(String tableName) throws IOException;

}
