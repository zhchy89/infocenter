package py.infocenter.worker;

import py.archive.RawArchiveMetadata;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePool;

import java.util.*;

public class StoragePoolSpaceCalculator {
    /**
     * @param storagePool
     * @param instanceId2InstanceMetadata all OK datanode
     * @param archiveId2Archive
     * @param volumeRequiredGroupCount    For PSS volume, group count is 3; for PSA volume, group count is 2.
     * @return
     */
    public static long calculateFreeSpace(StoragePool storagePool, Map<Long, InstanceMetadata> instanceId2InstanceMetadata,
            Map<Long, RawArchiveMetadata> archiveId2Archive, int volumeRequiredGroupCount, long segmentSize) {
        Set<Integer> allGroupIds = new HashSet<>();

        ObjectCounter<Integer> freeSpaceCounterByGroup = new TreeSetObjectCounter<>();
        for (Long archiveId : storagePool.getArchivesInDataNode().values()) {
            RawArchiveMetadata archiveMetadata = archiveId2Archive.get(archiveId);
            if (archiveMetadata != null) {
                int groupId = instanceId2InstanceMetadata.get(archiveMetadata.getInstanceId().getId()).getGroup().getGroupId();
                allGroupIds.add(groupId);
                freeSpaceCounterByGroup.increment(groupId, archiveMetadata.getLogicalFreeSpace());
            }
        }

        if (allGroupIds.size() < 3) {
            return 0;
        }

        BucketWithBarrier spaceCalculator = new BucketWithBarrier(volumeRequiredGroupCount);
        Iterator<Integer> iterator = freeSpaceCounterByGroup.descendingIterator();
        while (iterator.hasNext()) {
            spaceCalculator.fill(freeSpaceCounterByGroup.get(iterator.next()));
        }
        long spaceAvailableInStoragePool = spaceCalculator.getLowest();

        return spaceAvailableInStoragePool - spaceAvailableInStoragePool % segmentSize;
    }
}
