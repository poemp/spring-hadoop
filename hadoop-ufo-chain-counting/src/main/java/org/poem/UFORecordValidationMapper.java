package org.poem;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * @author Administrator
 */
public class UFORecordValidationMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {
        String line = text.toString();
        if (validate(line)){
            outputCollector.collect(longWritable, text);
        }
    }
    private boolean validate(String str){
        String[] parts = str.split("\t");
        return parts.length == 6;
    }
}
