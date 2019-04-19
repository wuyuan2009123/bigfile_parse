package bigfile;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 文件切分工具类：使用递归完成对文件的切分，并返回切分集合
 * @author admin
 *
 */
public class FilePartitionUtil {
	
	/**
	 * 两个for循环完成对大文件的切分
	 * @param start      文件切分起点位置
	 * @param splitCount 需要切分的个数
	 * @param totalSize  文件总长度
	 * @param randomAccessFile 大文件访问类
	 * @return
	 * @throws IOException
	 */
	public static List<FilePartition> partition1(long start, int splitCount, long totalSize, RandomAccessFile randomAccessFile) throws IOException{
		if(splitCount<2) {
			throw new IllegalArgumentException("切分块不能小于2");
		}
		
		//返回结果：所有切分区域信息
		List<FilePartition> result = new ArrayList<FilePartition>(splitCount);
		
		//每个切分区域的总长度
		long length = totalSize/splitCount;
		
		//初始化切分块信息
		for(int i=0;i<splitCount;i++) {
			//创建一条记录
			FilePartition partition = new FilePartition();
			
			//平均切分时每个区域的起止位置：类似分页计算页面起止位置的算法
			long currentStart = start+length*i;
			
			//位置是从0开始的，所以需要减1
			long currentEnd = currentStart+length-1;	
			
			partition.setStart(currentStart);
			partition.setEnd(currentEnd<totalSize?currentEnd:totalSize-1);
			
			result.add(partition);
		}
		
	    //从第二个切分块开始，修正起、止位置：保证每块的终止位置处是\r\n换行符
		long index = result.get(0).getEnd();
		for(int i=1;i<splitCount;i++) {
			   //定位到上一个切块的end处，往后寻找换行符号
			   randomAccessFile.seek(index);
	           byte oneByte = randomAccessFile.readByte();
	           
	           //判断是否是换行符号,如果不是换行符，那么读取到换行符为止
	           while(oneByte != '\n' && oneByte != '\r') {
	               randomAccessFile.seek(index++);
	               oneByte = randomAccessFile.readByte();
	           }

	           FilePartition previous = result.get(i-1);
			   FilePartition current = result.get(i);
			   
			   //循环结束时，index此时位于\r位置
			   //此时还差一个\n符号，修正上一个切块的end和当前切块的start
			   previous.setEnd(index+1);
			   current.setStart(index+2);
			   
			   //修正下次寻找的位置为当前切块的end
			   index = current.getEnd();
		}
		
		return result;
	}
	
   /**
    *
    * 基本思想：保证数据的完整，所以在切分过程中保证每次切分的end是一个完整的行。
    * @program: io.util.DataUtil
    * @description: 分片数据
    * @auther: xiaof
    * @date: 2019/2/22 15:20
    */
   public static Set<FilePartition> partition(long start, long length, long totalSize, RandomAccessFile randomAccessFile) throws IOException {
       if(start > totalSize - 1) {
           return null;
       }

       //最终返回的切分集合
       Set<FilePartition> partitionPairs = new HashSet<FilePartition>();
       
       //第一个切分记录
       FilePartition partitionPair = new FilePartition();
       partitionPair.setStart(start);
       
       //判断这个start+length是否是换行符，如果不是，则需要找到换行符所在的位置，修正该切分的end值
       long index = start + length;

       //递归终止条件
       if(index > totalSize - 1) {
           //最后一个递归终止
           partitionPair.setEnd(totalSize - 1);
           partitionPairs.add(partitionPair);
           return partitionPairs;

       } else {
           //设置位置并读取一个字节
           randomAccessFile.seek(index);
           byte oneByte = randomAccessFile.readByte();
           //判断是否是换行符号,如果不是换行符，那么读取到换行符为止
           while(oneByte != '\n' && oneByte != '\r') {
               //不能越界：先加后用
               if(++index > totalSize - 1) {
                   index = totalSize-1;
                   break;
               }

               randomAccessFile.seek(index);
               oneByte = randomAccessFile.readByte();
           }

           //以换行符真正所处的位置修正切分的end值
           //执行到此时，oneByte=\r，此时还差一个\n符号
           
           if(index+1<totalSize) {
        	   randomAccessFile.seek(index+1);
        	   oneByte = randomAccessFile.readByte();
        	   
        	   if(oneByte == '\n' || oneByte=='\r') {
        		   partitionPair.setEnd(index++);
        	   }
           }
           
           //加入当前切分块
           partitionPairs.add(partitionPair);

           //递归下一个位置
           partitionPairs.addAll(partition(index + 1, length, totalSize, randomAccessFile));
       }

       return partitionPairs;
   }
}
