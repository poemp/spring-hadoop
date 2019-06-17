package org.poem;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;

/**
 * @author Administrator
 */
public class WriteableTests {

    public static void main(String[] args) {
        System.out.println( "******* Primitive Writables ******" );
        BooleanWritable booleanWritable = new BooleanWritable( true );
        System.out.println();
    }

    public static class IntArrayWritable extends ArrayWritable {

        public IntArrayWritable() {
            super( IntArrayWritable.class );
        }
    }


}
