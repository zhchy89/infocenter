package py.infocenter.service.selection;

import java.util.Collection;
import java.util.List;

import py.driver.DriverContainerCandidate;
import py.driver.DriverMetadata;
import py.instance.Instance;

public interface DriverContainerSelectionStrategy {
    
    public List<DriverContainerCandidate> select(Collection<Instance> instances, List<DriverMetadata> drivers);

}
