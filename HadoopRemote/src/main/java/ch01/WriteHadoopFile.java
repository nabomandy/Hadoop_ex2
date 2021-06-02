package ch01;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHadoopFile {
	public static void main(String[] args) {
		String filename = "hdfs://192.168.42.3:9000/user/f01/sample3.txt";
		String content = "=========== 한글입니다11111 \r\n=========== 한글이라구요22222\r\n";
		
		try {
			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(new URI("hdfs://192.168.42.3:9000"), conf, "hadoop");
			// FileSystem hdfs = FileSystem.get(conf);	// Wrong FS = hdfs://192.168.42.3:9000/user/test2.txt
			
			Path path = new Path(filename);
			
			if (hdfs.exists(path)) hdfs.delete(path, true);
			
			FSDataOutputStream outputStream = hdfs.create(path);
			
			outputStream.writeUTF(content);
			outputStream.close();
			
			FSDataInputStream inputStream = hdfs.open(path);
			
			String inputString = inputStream.readUTF();
			inputStream.close();
			
			System.out.println("Input Data: " + inputString);
		} catch (Exception e) { e.printStackTrace(); }
	}
}
