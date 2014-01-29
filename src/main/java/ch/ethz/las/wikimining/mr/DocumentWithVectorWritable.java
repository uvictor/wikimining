
package ch.ethz.las.wikimining.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.VectorWritable;

/**
 * Stores a document id along with its tf-idf vectors.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class DocumentWithVectorWritable implements Writable {
  private Text id;
  private VectorWritable vector;

  public DocumentWithVectorWritable() { }

  public DocumentWithVectorWritable(Text theId, VectorWritable theVector) {
    id = theId;
    vector = theVector;
  }

  public Text getId() {
    return id;
  }

  public VectorWritable getVector() {
    return vector;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    id.write(out);
    vector.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = new Text();
    vector = new VectorWritable();

    id.readFields(in);
    vector.readFields(in);
  }
}
