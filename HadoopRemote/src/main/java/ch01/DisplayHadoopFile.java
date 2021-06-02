package ch01;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DisplayHadoopFile {
	public static void main(String[] args) {
		try {
			String filepath = "hdfs://192.168.42.3:9000/user/f01/sample2.txt";
			Path pt = new Path(filepath);
			Configuration conf = new Configuration();
			
			conf.set("fs.defaultFS", filepath);
			
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(
					new InputStreamReader(fs.open(pt), "UTF-8"));
			String line = br.readLine();
			
			while (line != null) {
				System.out.println(line);
				line = br.readLine();
			}
			
			br.close();
		} catch(Exception e) { e.printStackTrace(); }
	}
}
