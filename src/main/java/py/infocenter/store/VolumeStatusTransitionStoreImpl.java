package py.infocenter.store;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import py.volume.VolumeMetadata;

public class VolumeStatusTransitionStoreImpl implements VolumeStatusTransitionStore {
    private Map<Long, VolumeMetadata> volumeSweeperMap;

    public VolumeStatusTransitionStoreImpl() {
        volumeSweeperMap = new ConcurrentHashMap<>();
    }

    /**
     * Add the volume need to process to the store
     */
    @Override
    public void addVolumeToStore(VolumeMetadata volume) {
        volumeSweeperMap.put(volume.getVolumeId(), volume);
    }

    /**
     * Pop all the volumes which need to process
     * After this method, the map will be empty;
     */
    @Override
    public int drainTo(Collection<VolumeMetadata> volumes) {
        int num = 0;
        for (Object obj : volumeSweeperMap.values()) {
            VolumeMetadata volume = (VolumeMetadata) obj;
            volumes.add(volume);
            num++;
        }
        volumeSweeperMap.clear();
        return num;
    }

    /**
     * Clear all the volume need to process
     */
    @Override
    public void clear() {
        volumeSweeperMap.clear();
    }
}
