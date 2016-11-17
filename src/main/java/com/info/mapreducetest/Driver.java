package com.info.mapreducetest;
// Created ne package com.info.mapreducetest

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
// Import IntWritable package to value type as integer. IntWritable is primitive data type for Int in Hadoop (Not used in driver class)
import org.apache.hadoop.io.Text;
//Import Text package used as a key for mapper and reducers (Not used in driver class)
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// File Input package to set input file path for job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// File Output package to set output file path for the job
import org.apache.hadoop.mapreduce.Job;
// Hadoop map package to initialize job 


// Driver class 
public class Driver {
    
	/*
	 * Main method : Hadoop job is submitted from main method. Program execution starts here.
	 * @param String args command line arguments. while running MR program we are sending input file path, output file path as CLI parameters 
	 * @throws IOException,InterruptedException,ClassNotFoundException
	 */
	
    public static void main(String... args) throws IOException, InterruptedException, ClassNotFoundException{
    	// check input file and output file path specified or not in CLI.
    	// If number of arguments less than 2 then we are exiting job 
        if(args.length<2){
            System.out.println("Usage is [generic option] <input path> <output path>");
            System.exit(1);
        }
        // Create new job object(Job is not started yet)
        Job job=new Job();
        
        //Set driver
        job.setJarByClass(Driver.class);
        //Assign job name
        job.setJobName("wordCount");
        
        //Set input file name specified in CLI to job
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // set output file name specified in CLI to job
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // Set mapper class 
        job.setMapperClass(MapperTest.class);
        //set reducer class 
        job.setReducerClass(ReducerTest.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        System.exit(job.waitForCompletion(true)?0:1);
        
    }
}
