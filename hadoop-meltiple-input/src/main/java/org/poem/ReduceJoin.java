package org.poem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

;


public class ReduceJoin {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = new Job( configuration, "Reduce Job" );

        job.setJarByClass( ReduceJoin.class );
        job.setReducerClass( ReduceJoinReducer.class );
        job.setOutputValueClass( Text.class );
        job.setOutputKeyClass( Text.class );


        MultipleInputs.addInputPath( job, new Path( args[0] ), TextInputFormat.class, SalesRecordMapper.class );
        MultipleInputs.addInputPath( job, new Path( args[1] ), TextInputFormat.class, AccountRecordMapper.class );

        Path outputPath = new Path( args[2] );
        FileOutputFormat.setOutputPath( job, outputPath );
        outputPath.getFileSystem( configuration ).delete( outputPath );

        System.exit( job.waitForCompletion( true ) ? 0 : 1 );
    }

    public static class SalesRecordMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split( "\t" );

            context.write( new Text( parts[0] ), new Text( "sales \t" + parts[1] ) );
        }
    }

    public static class AccountRecordMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split( "\t" );
            context.write( new Text( parts[0] ), new Text( "accounts \t" + parts[1] ) );
        }

    }

    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = "";
            double total = 0.0;
            int count = 0;

            for (Text t : values) {
                String[] parts = t.toString().split( "\t" );
                if (parts[0].equalsIgnoreCase( "sales" )) {
                    count++;
                    total += Float.valueOf( parts[1] );
                } else if (parts[0].equalsIgnoreCase( "accounts" )) {
                    name = parts[1];
                }
            }
            String str = String.format( "%d\t%f", count, total );
            context.write( new Text( name ), new Text( str ) );
        }
    }
}
