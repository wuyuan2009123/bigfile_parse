package bigfile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BigFileReader {
	public static void main(String[] args) throws IOException, SQLException {
		String filePath = "E:" + File.separator + "bigfileTwoGB.txt";
		File file = new File(filePath);

		//按块读取
//		readByBlock(file);
		
		//按行读取不解析
//		readByLine(file);
		
		//按行读取解析并入库
		readByLineAndParse(file);
	}

	/**
	 * 按缓冲区读取文件，每次读取指定长度的数据到内存中
	 * java虚拟机默认的最大内存是64MB
	 * @param file
	 * @throws IOException
	 */
	public static void readByBlock(File file) throws IOException {
		long begin = System.currentTimeMillis();
		
		FileInputStream fileInputStream = new FileInputStream(file);
		//设定缓冲区长度为64MB
		byte[] bytes = new byte[64 * 1024 * 1024];
		int length = 0;
		while ((length = fileInputStream.read(bytes)) != -1) {
			System.out.println(new String(bytes, 0, length));
		}
		fileInputStream.close();
		
		long end = System.currentTimeMillis();
		System.out.println("readByBlock用时:" + (end - begin) / 1000+"(s)");
	}

	/**
	 * @throws IOException 
	 * 
	 */
	public static void readByLine(File file) throws IOException {
		long begin = System.currentTimeMillis();
		
		FileInputStream is = new FileInputStream(file);
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		
		String content = null;
		long counter = 0;
		while ((content = reader.readLine()) != null) {
			System.out.println(content);
			counter++;
		}

		is.close();

		long end = System.currentTimeMillis();

		System.out.println(counter+"readByLine用时:" + (end - begin) / 1000+"(s)");
	}

	/**
	 * 逐行读取大文件内容并解析、批量入库
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void readByLineAndParse(File file) throws SQLException, IOException {
		long begin = System.currentTimeMillis();

		FileInputStream is = new FileInputStream(file);
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));

		int batch = 0;
		int maxBatch = 2000;
		int counter = 0;
		DBHelper db = new DBHelper();
		db.openConnection();
		Connection conn = db.getConnection();
		PreparedStatement stated = conn.prepareStatement(
				"insert into web_request_single(time,src_ip,request_url,dest_ip,dest_port,method,user_agent,connection,server,status,protocol) values(?,?,?,?,?,?,?,?,?,?,?)");
		String lineTxt = reader.readLine();//第一行是Title
		while ((lineTxt = reader.readLine()) != null) {
			counter++;
			String[] datas = lineTxt.split(",");
			for (int i = 1; i <= datas.length; i++) {
				if(i==1 ) {
					stated.setObject(i, Long.parseLong(datas[i - 1]));
				}else if(i==5){
					stated.setObject(i, Integer.parseInt(datas[i - 1]));
				}else {
					stated.setObject(i, datas[i - 1]);
				}
			}
			stated.addBatch();

			// 达到一个批次最大值，入库
			if (counter == maxBatch) {
				batch++;
				System.out.println("current batch :" + batch);
				stated.executeBatch();
				stated.clearBatch();
				counter = 0;
			}
		}

		//最后一个批量不足maxRow的记录入库
		if(counter>0) {
			stated.executeBatch();
		}
		
		is.close();

		long end = System.currentTimeMillis();

		System.out.println("readByLineAndParse用时"+ (end - begin) / 1000+"(s)");
	}
}
