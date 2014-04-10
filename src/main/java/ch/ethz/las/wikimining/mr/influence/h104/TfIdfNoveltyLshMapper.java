package ch.ethz.las.wikimining.mr.influence.h104;

import ch.ethz.las.wikimining.mr.base.Defaults;
import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.mr.base.HashBandWritable;
import ch.ethz.las.wikimining.mr.io.h104.MatrixSequenceFileReader;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.VectorFunction;

/**
 * Finds k-nearest documents from the past, for each document.
 * Uses Locality Sensitive Hashing with Random Projections (for cosine
 * similarity).
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class TfIdfNoveltyLshMapper extends MapReduceBase implements Mapper<
    Text, VectorWritable, HashBandWritable, DocumentWithVectorWritable> {

  private static final Logger logger =
      Logger.getLogger(TfIdfNoveltyLshMapper.class);

  private Matrix basisMatrix;
  private int bandCount;
  private int rowCount;

  @Override
  public void configure(JobConf config) {
    try {
      final Path basisPath =
          new Path(config.get(Fields.BASIS.get()));
      FileSystem fs = FileSystem.get(config);

      final MatrixSequenceFileReader basisReader =
          new MatrixSequenceFileReader(basisPath.suffix("/part-m-00000"),
              fs, config);
      basisMatrix = basisReader.read();
    } catch (IOException e) {
      logger.fatal("Error loading basis matrix!", e);
    }

    bandCount = config.getInt(Fields.BANDS.get(), Defaults.BANDS.get());
    rowCount = config.getInt(Fields.ROWS.get(), Defaults.ROWS.get());
  }

  @Override
  public void map(Text docId, VectorWritable value,
      OutputCollector<HashBandWritable, DocumentWithVectorWritable> output,
      Reporter reporter) throws IOException {
    reporter.getCounter(TfIdfNovelty.Records.TOTAL).increment(1);
    final Vector vector = value.get();

    final Vector rowHashes =
        basisMatrix.aggregateRows(new VectorFunction() {
          @Override
          /**
           * Signum of the dot product with a random projection.
           * Cosine similarity.
           */
          public double apply(Vector projector) {
            return projector.dot(vector) > 0 ? 1231 : 1237;
          }
        });

    // TODO(uvictor): consider using multiple hash functions?
    for (int band = 0; band < bandCount; band++) {
      final Vector bandVector = rowHashes.viewPart(band * rowCount, rowCount);
      final int hash = new VectorWritable(bandVector).hashCode();
      output.collect(new HashBandWritable(hash, band),
          new DocumentWithVectorWritable(docId, value));
    }
  }
}
