package py.infocenter.store;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py.volume.VolumeMetadata;

/**
 * Thread-safe volume store
 *
 * @author chenlia
 */
public class TwoLevelVolumeStoreImpl implements VolumeStore {
    private static final Logger logger = LoggerFactory.getLogger(TwoLevelVolumeStoreImpl.class);

    private VolumeStore inMemoryVolumeStore;
    private VolumeStore dbVolumeStore;

    public TwoLevelVolumeStoreImpl(VolumeStore inMemoryStore, VolumeStore dbStore) {
        this.inMemoryVolumeStore = inMemoryStore;
        this.dbVolumeStore = dbStore;
    }

    @Override
    public synchronized void saveVolume(VolumeMetadata volumeMetadata) {
        // first get from memory,if it's null. then get from DB
        VolumeMetadata volumeData = getVolume(volumeMetadata.getVolumeId());

        // if volumeMetadata has not been in memory map, or volumeMetadata has changed
        if (volumeData != null && volumeData.getDeadTime() != 0) {
            logger.debug("saved deadtime: {} going to save deadtime: {}", volumeData.getDeadTime(),
                    volumeMetadata.getDeadTime());
            // we do this way to make sure that deadTime won't be covered by 0 value
            volumeMetadata.setDeadTime(volumeData.getDeadTime());
        }
        if (!volumeMetadata.equals(volumeData)) {
            dbVolumeStore.saveVolume(volumeMetadata);
        }
        inMemoryVolumeStore.saveVolume(volumeMetadata);
    }

    @Override
    public synchronized void deleteVolume(VolumeMetadata volumeMetadata) {
        // the interface of this implement should be modified to throw some
        // exceptions out if it execute failed.
        dbVolumeStore.deleteVolume(volumeMetadata);
        inMemoryVolumeStore.deleteVolume(volumeMetadata);
    }

    @Override
    public synchronized List<VolumeMetadata> listVolumes() {
        /*
         * we can just get volumes from the memory cache,because we had make the data from database is the same with the
         * data in memory
         */
        return inMemoryVolumeStore.listVolumes();
    }

    @Override
    public void loadVolumeFromDbToMemory() {
        List<VolumeMetadata> volumesInDb = dbVolumeStore.listVolumes();
        List<VolumeMetadata> volumesInMemory = inMemoryVolumeStore.listVolumes();
        List<VolumeMetadata> volumesInDbButNotInMemory = new ArrayList<VolumeMetadata>();
        for (VolumeMetadata volumeInDb : volumesInDb) {
            boolean exist = false;
            for (VolumeMetadata volumeInMemory : volumesInMemory) {
                if (volumeInDb.getVolumeId() == volumeInMemory.getVolumeId())
                    exist = true;
            }
            if (!exist)
                volumesInDbButNotInMemory.add(volumeInDb);
        }

        for (VolumeMetadata volumeInDbButNotInMemory : volumesInDbButNotInMemory) {
            inMemoryVolumeStore.saveVolume(volumeInDbButNotInMemory);
        }
    }

    @Override
    public void loadVolumeInDb() {
        List<VolumeMetadata> volumesInDb = dbVolumeStore.listVolumes();
        List<VolumeMetadata> volumesInMemery = inMemoryVolumeStore.listVolumes();
        List<VolumeMetadata> volumesInDbButNotInMemery = new ArrayList<>();
        for (VolumeMetadata volumeInDb : volumesInDb) {
            boolean exist = false;
            for (VolumeMetadata volumeInMemery : volumesInMemery) {
                if (volumeInDb.getVolumeId() == volumeInMemery.getVolumeId())
                    exist = true;
            }
            if (!exist)
                volumesInDbButNotInMemery.add(volumeInDb);
        }
        for (VolumeMetadata volumeInDbButNotInMemery : volumesInDbButNotInMemery) {
            logger.warn("found a volume in db but not in memory , delete it in db {}", volumeInDbButNotInMemery);
            dbVolumeStore.deleteVolume(volumeInDbButNotInMemery);
        }
    }

    @Override
    public synchronized List<VolumeMetadata> listRootVolumes() {
//        Long accountIdOverride = overrideAccountId(accountId);
        /*
         * we can just get volumes from the memory cache,because we had make the data from database is the same with the
         * data in memory
         */
        return inMemoryVolumeStore.listRootVolumes();
    }

    @Override
    public synchronized VolumeMetadata getVolume(Long volumeId) {
        VolumeMetadata volume = inMemoryVolumeStore.getVolume(volumeId);

        if (volume == null) {
            volume = dbVolumeStore.getVolume(volumeId);
            logger.debug("DB volume: {}", volume);
            if (volume != null) {
                logger.debug(
                        "TwoLevelVolumeStoreImpl::getVolume.Save volume[{}] into Memory.\nCurrent volumes in memory are {[]}",
                        volume.getName(), inMemoryVolumeStore.toString());
                inMemoryVolumeStore.saveVolume(volume);
            }
        }
        VolumeMetadata volumeToReturn = new VolumeMetadata();
        return volumeToReturn.deepCopy(volume);
    }

    @Override
    public synchronized List<VolumeMetadata> listVolumesFromRoot(long rootId) {
        List<VolumeMetadata> volumesCopied = new ArrayList<>();
        for (VolumeMetadata volume : inMemoryVolumeStore.listVolumesFromRoot(rootId)) {
            volumesCopied.add(new VolumeMetadata().deepCopy(volume));
        }
        return volumesCopied;
    }

    @Override
    public List<VolumeMetadata> listVolumesWithSameRoot(long rootId) {
        List<VolumeMetadata> volumesCopied = new ArrayList<>();
        for (VolumeMetadata volume : inMemoryVolumeStore.listVolumesWithSameRoot(rootId)) {
            volumesCopied.add(new VolumeMetadata().deepCopy(volume));
        }
        return volumesCopied;
    }

    @Override
    public synchronized void clearData() {
        inMemoryVolumeStore.clearData();
    }

    @Override
    public int updateDeadTime(long volumeId, long deadTime) {
        // update db first
        dbVolumeStore.updateDeadTime(volumeId, deadTime);
        return inMemoryVolumeStore.updateDeadTime(volumeId, deadTime);
    }

    @Override
    public int updateExtendingSize(long volumeId, long extendingSize) {
        dbVolumeStore.updateExtendingSize(volumeId, extendingSize);
        return inMemoryVolumeStore.updateExtendingSize(volumeId, extendingSize);
    }

    @Override
    public synchronized int updateStatusAndVolumeInAction(long volumeId, String status, String volumeInAction) {
        // update db first
        dbVolumeStore.updateStatusAndVolumeInAction(volumeId, status, volumeInAction);
        return inMemoryVolumeStore.updateStatusAndVolumeInAction(volumeId, status, volumeInAction);
    }

    @Override
    public int updatePersistedItems(long volumeId, long volumeSize, String volumeName, String volumeType, String layout,
            boolean simpleConfigured, int segmentNumToCreateEachTime, long domainId, long storagePoolId,
            Date volumeCreatedTime, Date lastExtendedTime, String volumeSource, String readWrite) {
        return dbVolumeStore
                .updatePersistedItems(volumeId, volumeSize, volumeName, volumeType, layout, simpleConfigured,
                        segmentNumToCreateEachTime, domainId, storagePoolId, volumeCreatedTime, lastExtendedTime,
                        volumeSource, readWrite);
    }

    @Override
    public synchronized VolumeMetadata getVolumeNotDeadByName(String name) {
        // Get VolumeMetadata from Memory first for quickly search,
        // if not in memory, then search for DB
        VolumeMetadata volume = inMemoryVolumeStore.getVolumeNotDeadByName(name);
        if (null == volume) {
            volume = dbVolumeStore.getVolumeNotDeadByName(name);
        }
        return volume;
    }

    @Override
    public synchronized String updateVolumeLayout(long volumeId, int startSegmentIndex, int newSegmentsCount,
            String volumeLayout) {
        String layoutFromMemory = inMemoryVolumeStore
                .updateVolumeLayout(volumeId, startSegmentIndex, newSegmentsCount, volumeLayout);
        return dbVolumeStore.updateVolumeLayout(volumeId, startSegmentIndex, newSegmentsCount, layoutFromMemory);

    }
}
