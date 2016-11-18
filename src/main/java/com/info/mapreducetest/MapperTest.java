package com.info.mapreducetest;
// mapper class 
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.LongWritable; // package for intWriable class (key type which is hash  )
import org.apache.hadoop.io.Text; // package for intWriable class (key type whihc is text  )
import org.apache.hadoop.mapreduce.Mapper; // hadoop mapper class 

/**
 * Mapper class for MR job 
 * Here we actually write our logic. mapper is getting input from record reader in key value format.
 * on each input split this method is called.
 * We are providing following datatypes for 
 * LongWritable - input key type (hash of line)
 * Text - input value type (actual line)
 * Text - Output type (word)
 * IntWriteable - output Value (1)
 */

/*
 * mapper class must extend to mapper class 
 */
public class MapperTest extends Mapper<LongWritable,Text,Text,IntWritable>{
    IntWritable one=new IntWritable(1);
    
    @Override
    
    // as we are extending Mapper must override this map method.
    /**
     * 
     * @param key  input key
     * @param value input value 
     * @param context Context object: allows the Mapper/Reducer to interact with the rest of the Hadoop system.
     * @throws IOException
     * @throws InterruptedException
     * mapper sends output as (key, value)  pairs.
     */
    public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
        // assume input value is received like value = "Hi how are you"
        String[] line=value.toString().split(" "); //split value by space and store in string array 
        for(String word:line){  // iterate on each word from string 
            context.write(new Text(word.toLowerCase()), one);  // write kry values in to context like (hi,1),(how,1)(are,1)
        }
        
    }
}
