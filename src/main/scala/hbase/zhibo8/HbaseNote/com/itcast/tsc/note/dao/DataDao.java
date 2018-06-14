package hbase.zhibo8.HbaseNote.com.itcast.tsc.note.dao;

import com.zhibo8.HbaseNote.com.itcast.tsc.note.bean.Note;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.List;


public interface DataDao {
	public boolean insertData(String tableName, String rowKey, String family,
                              String qualifier, String value) ;

	public boolean insertData(String tableName, String rowKey,
                              String[][] famQuaVals) ;

	public boolean deleteData(String tableName, String rowKey) ;

	public ResultScanner queryAll(String tableName) throws IOException;

	public ResultScanner querySome(String tableName, List<String> rowKeys)
			throws IOException;


	public ResultScanner queryByReg(String tableName, String reg)
			throws IOException;

	public void copyData(String fromTableName, String toTableName, String fromRowKey, String toRowKey) throws IOException;

	public List<String> queryByRowKeyString(String notebookTableName,
                                            String noteBookRowkey);

	public Note queryNoteByRowKey(String noteRowkey);

	public List<Note> queryNoteListByRowKey(String rowkey);

	public Result queryByRowKey(String noteTableName, String rowKey);
}
