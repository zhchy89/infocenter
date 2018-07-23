package py.infocenter.store;

import java.util.List;




public interface OrphanVolumeStore {

    // add orphan volume 
    public void addOrphanVolume(long volumeId);

    // remove orphan volume 
    public void removeOrphanVolume(long volumeId);
    
    // get orphan volume 
    public List<Long> getOrphanVolume();
    
}
