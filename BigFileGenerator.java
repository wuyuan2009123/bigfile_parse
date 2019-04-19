package bigfile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class BigFileGenerator {
	
	public static void main(String[] args) {
//		String path = "E:/bigfile2GB.txt";
		String path = "E:/bigfile2KB.txt";
		try {
//			creatBigFile(path,(long)1073741824*2);
			creatBigFile(path,(long)1024*1024*2);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 创建一个指定byte的CSV文件
	 * @param path
	 * @param size
	 * @throws IOException
	 */
	public static void creatBigFile(String path,long size) throws IOException {
		File file = new File(path);
		BufferedWriter writer = new BufferedWriter( new FileWriter(file));
		
		long start = System.currentTimeMillis();
		
		String title = "time,src_ip,request_url,dest_ip,dest_port,method,user_agent,connection,server,status,protocol\r\n";
		writer.write(title);
		
		long total  = title.getBytes().length;
		long line = 0;
		while (total<size) {
			line++;
			String  temp = formatRecord();
			writer.write(temp);
			total +=temp.getBytes().length;
		}
		
		long end = System.currentTimeMillis();
		writer.flush();
		writer.close();
		System.out.println("total line:"+line);
		System.out.println("cost :"+(end-start)/1000);
	}

	public static String formatRecord() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(System.currentTimeMillis()).append(",");
		buffer.append(getSrcIp()).append(",");
		buffer.append(getRequestUrl()).append(",");
		buffer.append(getDestIp()).append(",");
		buffer.append(new Random().nextInt(65535)).append(",");
		buffer.append(getMethodType()).append(",");
		buffer.append(getUseragent()).append(",");
		buffer.append(getConnection()).append(",");
		buffer.append(getServer()).append(",");
		buffer.append(getStatus()).append(",");
		buffer.append(getProtocolType()).append("\r\n");
		
		return buffer.toString();
	}


	private static String getRequestUrl() {
        String appAccount[] = {"/log","/logout","/getUsers","/countUsers","/list","/status","/IPP","/Jabber","/Netflix"};
        Random r = new Random();
        int next = r.nextInt();
        next = Math.abs(next);
        return appAccount[next%appAccount.length];
    }
	
	private static String getSrcIp(){
		String[] ips = {"192.168.19.10","192.168.19.11","192.168.19.12","192.168.19.13","192.168.19.14","192.168.19.15","192.168.19.16","192.168.19.17","192.168.19.18","192.168.19.19","192.168.19.20",
				"192.168.10.10","192.168.10.11","192.168.10.12","192.168.10.13","192.168.10.14","192.168.10.15","192.168.10.16","192.168.10.17","192.168.10.18","192.168.10.19","192.168.10.20"
		};
		Random r = new Random();
		int next = r.nextInt();
		next = Math.abs(next);
		return ips[next%20];
	}
	
	private static String getDestIp(){
		String[] s = {"192.168.11.10","192.168.11.11","192.168.11.12","192.168.11.13","192.168.11.14","192.168.11.15","192.168.11.16","192.168.11.17","192.168.11.18","192.168.11.19","192.168.11.20",
				"192.168.15.10","192.168.15.11","192.168.15.12","192.168.15.13","192.168.15.14","192.168.15.15","192.168.15.16","192.168.15.17","192.168.15.18","192.168.15.19","192.168.15.20"
		};
		Random r = new Random();
		int next = r.nextInt();
		next = Math.abs(next);
		return s[next%20];
	}
	
	private static String getProtocolType() {
        String appAccount[] = {"FTP","Telnet","http","Telnet","AFP","ICMP","IPP","Jabber","Netflix","PPTP","QQ","Quake","SAP","SNMP","SSH","SSL","Teredo"};
        Random r = new Random();
        int next = r.nextInt();
        next = Math.abs(next);
        return appAccount[next%appAccount.length];
    }
	
	private static String getMethodType() {
		String appAccount[] = {"trace","put","delete","get","post"};
		Random r = new Random();
		int next = r.nextInt();
		next = Math.abs(next);
		return appAccount[next%appAccount.length];
	}
	
	private static String getUseragent() {
		String appAccount[] = {"Firefox","Edge","windows","chrome","Wechat"};
		Random r = new Random();
		int next = r.nextInt();
		next = Math.abs(next);
		return appAccount[next%appAccount.length];
	}
	
	private static String getStatus() {
		String appAccount[] = {"0","1"};
		Random r = new Random();
		int next = r.nextInt();
		next = Math.abs(next);
		return appAccount[next%appAccount.length];
	}
	
	private static String getConnection() {
		String appAccount[] = {"no-catched","keep-alived"};
		Random r = new Random();
		int next = r.nextInt();
		next = Math.abs(next);
		return appAccount[next%appAccount.length];
	}
	
	private static String getServer() {
		String appAccount[] = {"Mac","Windows","Linux","Android"};
		Random r = new Random();
		int next = r.nextInt();
		next = Math.abs(next);
		return appAccount[next%appAccount.length];
	}
}
