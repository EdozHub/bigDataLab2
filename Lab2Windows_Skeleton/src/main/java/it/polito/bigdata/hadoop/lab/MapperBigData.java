package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    private String prefix;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        prefix = context.getConfiguration().get("word.prefix");
    }

    protected void map(
                LongWritable key,   // Input key type
                Text value,         // Input value type
                Context context) throws IOException, InterruptedException {
                
        String line = value.toString();
        String parts[] = line.split("\\t");
        if (parts[0].startsWith(prefix)){
            int frequency = Integer.parseInt(parts[1]);
            context.write(new Text(parts[0]), new IntWritable(frequency));
        }

    }
}
