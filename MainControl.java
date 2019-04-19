package bigfile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 总控类
 * 
 * @author admin
 *
 */
public class MainControl {
	public static void main(String[] args) throws IOException {
		//大文件路径
		String filePath = "E:" + File.separator + "bigfile2GB.txt";
//		String filePath = "E:" + File.separator + "bigfile2KB.txt";

		// 只读模式创建RandomAccessFile对象
		final RandomAccessFile randomAccessFile = new RandomAccessFile(new File(filePath), "r");

		//首行标题，切分时需要跳过
		String title = "time,src_ip,request_url,dest_ip,dest_port,method,user_agent,connection,server,status,protocol\r\n";
		
		int splitCount = 10;//切分块
		long startPosition = title.length();//切分起始位置
		long totalSize = randomAccessFile.length();//文件总长度
		
		//对文件进行切分
		List<FilePartition> partition1 = FilePartitionUtil.partition1(startPosition, splitCount, totalSize,
				randomAccessFile);

		//创建线程池
		final ScheduledExecutorService service = Executors.newScheduledThreadPool(splitCount);
		
		//记录解析开始时间
		final long startTime = System.currentTimeMillis();

		// 线程协作类：等待所有任务都执行完成后，主线程则执行收尾工作
		CyclicBarrier barrier = new CyclicBarrier(partition1.size(), new Runnable() {
			@Override
			public void run() {
				long endTime = System.currentTimeMillis();
				
				//关闭线程池
				service.shutdown();
				
				//关闭文件资源
				try {
					randomAccessFile.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				//统计操作总耗时
				System.out.println(Thread.currentThread().getName() + " all work is done ,cost:"
						+ (endTime - startTime) / 1000 + ("(s)"));
			}
		});

		// 根据文件切分块数，提交N个解析任务
		for (FilePartition p : partition1) {
			service.submit(new ParseWorker(randomAccessFile, p, barrier));
//			 RandomAccessFile randomAccessFile1 = new RandomAccessFile(new File(filePath), "r");
//			service.submit(new NomapParseWorker(randomAccessFile1, p, barrier));
		}
	}
}
