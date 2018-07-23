package py.infocenter.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import py.infocenter.*;

import org.apache.log4j.Logger;

public class OrphanVolumeStoreImpl implements OrphanVolumeStore {
    private static final Logger logger = Logger.getLogger(OrphanVolumeStoreImpl.class);

    private Map<Long, Long> orphanVolumeStore = new ConcurrentHashMap<Long, Long>();
    private long volumeToBeOrphanTime;

    @Override
    public void addOrphanVolume(long volumeId) {
        if (!orphanVolumeStore.containsKey(volumeId)) {
            orphanVolumeStore.put(volumeId, System.currentTimeMillis());
        }
    }

    @Override
    public void removeOrphanVolume(long volumeId) {
        orphanVolumeStore.remove(volumeId);
    }

    @Override
    public List<Long> getOrphanVolume() {

        List<Long> allOrphanVolumes = new ArrayList<Long>();
        Iterator<Map.Entry<Long, Long>> iterator = orphanVolumeStore.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Long> entry = iterator.next();
            if (System.currentTimeMillis() - entry.getValue() > volumeToBeOrphanTime) {
                allOrphanVolumes.add(entry.getKey());
            }
        }
        return allOrphanVolumes;
    }

    public long getVolumeToBeOrphanTime() {
        return volumeToBeOrphanTime;
    }

    public void setVolumeToBeOrphanTime(long volumeToBeOrphanTime) {
        this.volumeToBeOrphanTime = volumeToBeOrphanTime;
    }

}
