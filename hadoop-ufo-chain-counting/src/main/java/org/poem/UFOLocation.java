package org.poem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Administrator
 */
public class UFOLocation {

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        JobConf jobConf = new JobConf( configuration, UFOLocation.class );
        jobConf.setJobName( "UFOLocation" );

        jobConf.setOutputKeyClass( Text.class );
        jobConf.setOutputValueClass( LongWritable.class );

        JobConf mapConf1 = new JobConf( false );
        ChainMapper.addMapper( jobConf,
                UFOCountingRecordValidationMapper.class,
                LongWritable.class,
                Text.class,
                LongWritable.class,
                Text.class,
                true,
                mapConf1 );

        JobConf mapconf2 = new JobConf( false );
        ChainMapper.addMapper( jobConf,
                MapClass.class,
                LongWritable.class,
                Text.class,
                Text.class,
                LongWritable.class,
                true,
                mapconf2 );

        jobConf.setMapperClass( ChainMapper.class );
        jobConf.setCombinerClass( LongSumReducer.class );
        jobConf.setReducerClass( LongSumReducer.class );

        FileInputFormat.setInputPaths( jobConf, args[0] );
        FileOutputFormat.setOutputPath( jobConf, new Path( args[1] ) );

        JobClient.runJob( jobConf );
    }

    public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable( 1 );
        private static Pattern locationPattern = Pattern.compile( "[a-zA-Z]{2}[^a-zA-Z]*$*" );

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            String[] fields = line.split( "\t" );
            String location = fields[2].trim();
            if (location.length() >= 2) {
                Matcher matcher = locationPattern.matcher( location );
                if (matcher.find()) {
                    int start = matcher.start();
                    String data = location.substring( start, start + 2 );
                    outputCollector.collect( new Text( data.toUpperCase() ), one );
                }
            }
        }
    }
}
