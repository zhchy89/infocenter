package py.infocenter.service.selection;

import java.util.Collection;
import java.util.List;

import py.icshare.InstanceMetadata;

public interface InstanceSelectionStrategy {

    List<InstanceMetadata> select(Collection<InstanceMetadata> instances);
}
