package com.laboros.hdfs;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author manerm Created HDFSService to copy files from local file system to hdfs file system
 *
 */
//java -cp /path/to/jars com.laboros.hdfs.HDFSServicePut [configurations] WordCount.txt /user/trainings
public class HDFSServicePut extends Configured implements Tool {

	public static void main(String[] args) {

		// Total 3 steps
		// Validation
		if (args.length < 2) {
			System.out.println("Java Usage "+HDFSServicePut.class.getName()+" [configuration] /path/to/edge/node/local/file /path/to/hdfs/destination/dir");
			return;
		}
		// Loading Configuration
		Configuration conf = new Configuration(Boolean.TRUE);
		conf.set("fs.defaultFS", "hdfs://localhost:8020");
		
		
//		print(conf);
		
		// Invoke ToolRunner.run
		
		try {
			int i=ToolRunner.run(conf, new HDFSServicePut(), args);
			if(i==0)
			{
				System.out.println("SUCCESS");
			}
		} catch (Exception e) {
			System.out.println("FAILED");
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		//Creating a file on hdfs = creating metadata + Add data
		
		/**
		 * Step:1 : Creating metadata = Creating empty file 
		 * on hdfs and put the metadata in the namenode 
		*/ 
		
		//Get the edge node file = get the file from argumnet
		final String edgeNodeInputFile = args[0];
		System.out.println("Input EdgeNode Local File:"+edgeNodeInputFile);
		//get the hdfs destination directory == get the information from argument
		
		final String hdfsDestinationDir=args[1];
		System.out.println("HDFS Destination dir:"+hdfsDestinationDir);
		
		//Convert into URI because hdfs understand URI
		final Path hdfsDestPathWithFileName = new Path(hdfsDestinationDir, edgeNodeInputFile);
		
		//Create the FileSystem
		
		Configuration conf = super.getConf();
		
		FileSystem hdfs = FileSystem.get(conf);
		
		FSDataOutputStream fsdos=hdfs.create(hdfsDestPathWithFileName);
		
		/**Step:2 
		 * 
		 * Adding Data
		 * 
		 * Splitting Data into blocks
		 * Identify the Datanode for data block -- DataStreamer
		 * Writing datablock to DN
		 * Replication
		 * Sync with NN
		 * Handling Failures.
		 */
		
		InputStream is = new FileInputStream(edgeNodeInputFile);
		
		IOUtils.copyBytes(is, fsdos, conf, Boolean.TRUE);
		

		return 0;
	}
	
//	private static void print(Configuration conf)
//	{
//		for (Map.Entry<String, String> entry : conf) {
//			System.out.println(entry.getKey()+"===="+entry.getValue());
//		}
//	}

}
