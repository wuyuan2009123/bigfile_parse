package bigfile;

import java.io.ByteArrayOutputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.CyclicBarrier;

/**
 * 大文件解析线程任务
 * 
 * @author admin
 *
 */
public class NomapParseWorker implements Runnable {

	/**
	 * 文件访问类
	 */
	private RandomAccessFile fileAccess;

	/**
	 * 处理的文件切分信息
	 */
	private FilePartition filePartition;

	/**
	 * 线程写作控制类
	 */
	private CyclicBarrier barrier;

	/**
	 * 文件切分的大小
	 */
	private long sliceSize;

	/**
	 * 文件读取到内存中的缓冲区
	 */
	private byte[] buffer;

	/**
	 * 缓冲区大小1M
	 */
	private int bufferSize = 1024 * 1024;

	/**
	 * 数据处理类
	 */
	private DataHandler handler;

	/**
	 * 构造函数：文件解析必须的信息
	 * 
	 * @param fileAccess
	 * @param filePartition
	 * @param barrier
	 */
	public NomapParseWorker(RandomAccessFile fileAccess, FilePartition filePartition, CyclicBarrier barrier) {
		this.fileAccess = fileAccess;
		this.filePartition = filePartition;
		this.barrier = barrier;
		this.sliceSize = filePartition.getEnd() - filePartition.getStart() + 1;
		this.buffer = new byte[bufferSize];// 1M的缓冲区
		this.handler = new DataHandler();
	}

	/**
	 * 解析逻辑:1、将文件切分区域内的数据映射到内存镜像中 2、读取内存镜像中的数据到缓冲区中，以便进行数据处理 3、循环处理缓冲区中的数据，以 \r\n
	 * 为一行数据，交由DataHandler进行解析 4、切分区域内的数据读取完成，则通知栅栏
	 */
	public void run() {
		try {

			long start = System.currentTimeMillis();
			ByteArrayOutputStream bos = new ByteArrayOutputStream();

			fileAccess.seek(filePartition.getStart());
			
			// 缓冲区大小固定为bufferSize，每次只处理这么多，循环处理直到完成
			for (int offset = 0; offset < sliceSize; offset += bufferSize) {
				int readLength;
				if (offset + bufferSize <= sliceSize) {
					readLength = bufferSize;
				} else {// 最后一次读取的真正的长度
					readLength = (int) (sliceSize - offset);
				}

				// 读取内存镜像中的数据到指定缓冲区
				fileAccess.read(buffer,0,readLength);

				// 处理缓冲区中的数据
				for (int i = 0; i < readLength; i++) {
					byte tmp = buffer[i];

					// 一行数据，则交由处理一行
					if (tmp == '\r') {
						continue;
					} else if (tmp == '\n') {// 找到完整一行记录，则交由数据处理类解析
						handler.handle(new String(bos.toByteArray()), false);
						bos.reset();
					} else {// 读取到字节数组中
						bos.write(tmp);
					}
				}
			}

			// 最后一行数据
			if (bos.size() > 0) {
				handler.handle(new String(bos.toByteArray()), true);
			}

			// 数据处理完成，通知栅栏修改状态
			long end = System.currentTimeMillis();
			barrier.await();

			System.out.println(Thread.currentThread().getName() + "Finish ed:" + (end - start) / 1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
