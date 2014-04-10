package ch.ethz.las.wikimining.mr.base;

import org.apache.hadoop.io.Text;

/**
 * Object to set the IntWritable type to ArrayWritable, so that we can use
 * "hadoop fs -text" for reading arrays.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class TextArrayWritable extends ArrayWritableToString {

  public TextArrayWritable() {
    super(Text.class);
  }

  public TextArrayWritable(Text[] values) {
    super(Text.class, values);
  }
}
