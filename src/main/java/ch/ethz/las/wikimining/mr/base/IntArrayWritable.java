package ch.ethz.las.wikimining.mr.base;

import org.apache.hadoop.io.IntWritable;

/**
 * Object to set the Text type to ArrayWritable, so that we can use
 * "hadoop fs -text" for reading arrays.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class IntArrayWritable extends ArrayWritableToString {

  public IntArrayWritable() {
    super(IntWritable.class);
  }

  public IntArrayWritable(IntWritable[] values) {
    super(IntWritable.class, values);
  }
}
