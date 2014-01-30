package ch.ethz.las.wikimining.mr;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * Decorates ArrayWritable with a toString method.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public abstract class ArrayWritableToString extends ArrayWritable {

  public ArrayWritableToString(Class<? extends Writable> valueClass) {
    super(valueClass);
  }

  public ArrayWritableToString(
      Class<? extends Writable> valueClass, Writable[] values) {
    super(valueClass, values);
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    for (String element : toStrings()) {
      stringBuilder.append(element).append(' ');
    }

    return stringBuilder.toString();
  }
}
