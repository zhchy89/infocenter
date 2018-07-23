package py.infocenter.service.selection;

import java.util.Collection;
import java.util.List;

/**
 * An interface which defines a method to select specified number of elements from a given collection.
 * <p>
 * {@link RandomSelectionStrategy} implements this interface as a strategy to random select elements from collection.
 * 
 * @author zjm
 *
 */
public interface SelectionStrategy {
    public <T> List<T> select(Collection<T> objs, int n);
}
