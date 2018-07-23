package py.infocenter.service.selection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.emory.mathcs.backport.java.util.Collections;

/**
 * A class implements the selection strategy interface {@link SelectionStrategy}. The strategy compares each elements in
 * the collection to select expected number of elements.
 * 
 * @author zjm
 */
public class ComparisonSelectionStrategy<T> implements SelectionStrategy {
    private Comparator<T> comparator;

    public ComparisonSelectionStrategy(Comparator<T> comparable) {
        this.comparator = comparable;
    }

    @Override
    public <T> List<T> select(Collection<T> objs, int n) {
        List<T> objList = new ArrayList<T>();
        for (T obj : objs) {
            objList.add(obj);
        }

        Collections.sort(objList, comparator);

        List<T> selectedObjList = new ArrayList<T>();
        for (int i = 0; i < n; i++) {
            selectedObjList.add(objList.get(i));
        }

        return selectedObjList;
    }
}
