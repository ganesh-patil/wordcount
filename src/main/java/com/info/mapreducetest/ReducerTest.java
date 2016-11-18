package com.info.mapreducetest;
// Reducer class 
import java.io.IOException;
import org.apache.hadoop.io.IntWritable; // package class to store Value 
import org.apache.hadoop.io.Text; // package  class to store key 
import org.apache.hadoop.mapreduce.Reducer; // import hadoop reducer class 


/**
 * 
 * Reducer class for MR job
 * Only one reducer is initiated for all the maps from one slave.
 * data types:
 * Text  = input key type 
 * IntWritable = input value type 
 * Text = output key type 
 * IntWritable = output value type 
 */
public class ReducerTest extends Reducer<Text,IntWritable,Text,IntWritable>{
    
    @Override
    /**
     * reduce method 
     * @param key Text
     * @param values Int 
     * @param context 
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
        int count=0;  // set initial count to 0
        // Output is send to reducer in formar (how, 1 1 1 1)
        // using Iterable interface to iterate on values.
        for(IntWritable one:values){
            count=one.get()+count;  // increase count 
        }
        context.write(key, new IntWritable(count));  // write output in format(how, 4) 
    }
    
}
