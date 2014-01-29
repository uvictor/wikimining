
package ch.ethz.las.wikimining.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * Stores a hash value along with the band (hash function) that generated it.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class HashBandWritable implements WritableComparable<HashBandWritable> {

  private int hash;
  private int band;

  public HashBandWritable() { }

  public HashBandWritable(int theHashCode, int theBand) {
    hash = theHashCode;
    band = theBand;
  }

  public int getHash() {
    return hash;
  }

  public int getBand() {
    return band;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(hash);
    out.writeInt(band);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    hash = in.readInt();
    band = in.readInt();
  }

  @Override
  public int compareTo(HashBandWritable other) {
    if (other == null) {
      return 1;
    }

    if (hash == other.hash) {
      return band - other.band;
    }

    return hash - other.hash;
  }

  @Override
  public int hashCode() {
    int hashCode = 7;
    hashCode = 59 * hashCode + hash;
    hashCode = 59 * hashCode + band;

    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    final HashBandWritable other = (HashBandWritable) obj;
    if (hash != other.hash) {
      return false;
    }

    return band == other.band;
  }

  @Override
  public String toString() {
    return hash + "-" + band;
  }
}
