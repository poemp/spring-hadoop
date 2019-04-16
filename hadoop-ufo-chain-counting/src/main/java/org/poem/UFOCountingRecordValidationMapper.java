package org.poem;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class UFOCountingRecordValidationMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

    public enum  LineCount{
        BAD_LINE,
        TOO_MANY_TABS,
        TOO_FEW_TABS
    }

    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {
        String line = text.toString();
        if (validate(line, reporter)){
            outputCollector.collect(longWritable,text);
        }
    }

    private boolean validate(String str, Reporter reporter){
        String[] parts = str.split("\t");
        if (parts.length != 6){
            if (parts.length < 6){
                reporter.incrCounter(LineCount.TOO_FEW_TABS,1);
            }else{
                reporter.incrCounter(LineCount.TOO_MANY_TABS,1);
            }
            reporter.incrCounter(LineCount.BAD_LINE,1);
            if (reporter.getCounter(LineCount.BAD_LINE).getCounter() %10 == 0){
                reporter.setStatus("Got 10 bad line");
                System.out.println("Read Other 10 bad Line");
            }
            return false;
        }
        return true;
    }
}
