package py.infocenter.store;

import java.util.List;

import py.icshare.ArchiveInformation;

public interface ArchiveStore {
    
    public void update(ArchiveInformation archiveInformation);

    public void save(ArchiveInformation driverInformation);
    
    public List<ArchiveInformation> getByInstanceId(long instanceId);
    
    public ArchiveInformation get(long archiveId);
    
    public List<ArchiveInformation> list();
    
    public int deleteByInstanceId(long instanceId);
    
    public int delete(long archiveId);
}
