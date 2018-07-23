package py.infocenter.store;

import java.util.List;

import py.icshare.InstanceMetadata;

public interface StorageStore {
    public void save(InstanceMetadata instanceMetadata);

    public InstanceMetadata get(long instanceId);

    public List<InstanceMetadata> list();

    public void delete(long instanceId);
    
    public int size();

    public void clearMemoryData();
}
