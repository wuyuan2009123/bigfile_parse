package bigfile;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 数据处理类
 * @author admin
 *
 */
public class DataHandler {
	private DBHelper db;
	
	private PreparedStatement statement;
	
	private String sql = "insert into web_request_multiple1(time,src_ip,request_url,dest_ip,dest_port,method,user_agent,connection,server,status,protocol) values(?,?,?,?,?,?,?,?,?,?,?)";
	private long totalLine = 0;
	private int counter = 0;
	private int maxBatch = 200;
	
	public DataHandler() {
		db = new DBHelper();
		db.openConnection();
		
		try {
			statement = db.conn.prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 处理一行读取的数据：解析添加到批量SQL中，达到入库最大批次后批量入库
	 * @param content    一行文本
	 * @param isLastLine 最后一行标识
	 * @throws SQLException
	 */
	public void handle(String content,boolean isLastLine) throws SQLException {
		totalLine++;
		counter++;
		String[] datas = content.split(",");
		for (int i = 1; i <= datas.length; i++) {
			if(i==1 ) {
				statement.setObject(i, Long.parseLong(datas[i - 1]));
			}else if(i==5){
				statement.setObject(i, Integer.parseInt(datas[i - 1]));
			}else {
				statement.setObject(i, datas[i - 1]);
			}
		}
		statement.addBatch();

		// 达到一个批次最大值，入库
		if (counter == maxBatch) {
			int[] r= statement.executeBatch();
			System.out.println(r.length);
			statement.clearBatch();
			counter = 0;
		}
		
		//最后一行则收尾
		if(isLastLine && counter>0) {
			statement.executeBatch();
			statement.clearBatch();
			statement.close();
			db.closeConnection();
			System.out.println(Thread.currentThread().getName()+" parse total line is:"+totalLine);
		}
	}
}
