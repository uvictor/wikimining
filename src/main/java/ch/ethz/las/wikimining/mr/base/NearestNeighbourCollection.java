
package ch.ethz.las.wikimining.mr.base;

import java.util.Collection;

/**
 *
 * @author uvictor
 */
public interface NearestNeighbourCollection<E> extends Collection<E> {
  public abstract E getNearestNeighbour(E current);
}
