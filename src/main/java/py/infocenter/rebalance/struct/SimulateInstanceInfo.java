package py.infocenter.rebalance.struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.RawArchiveMetadata;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePool;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.store.StorageStore;
import py.instance.InstanceId;

import javax.annotation.Nonnull;
import java.util.*;

/**
 * instance information simulation
 */
public class SimulateInstanceInfo implements Comparable<SimulateInstanceInfo> {
    private static final Logger logger = LoggerFactory.getLogger(SimulateInstanceInfo.class);

    protected InstanceId instanceId;
    private final int groupId;
    private final int diskCount;
    private final long segmentSize;
    private List<Long> archiveIds;
    private long freeSpace;
    private int freeFlexibleSegmentUnitCount;
    private int weight;

    public SimulateInstanceInfo(InstanceId instanceId, Collection<Long> disks, int groupId, long freeSpace,
                            int freeFlexibleSegmentUnitCount, long segmentSize, int weight) {
        this.instanceId = instanceId;
        this.groupId = groupId;
        this.diskCount = disks.size();
        this.archiveIds = new ArrayList<>(disks);
        this.freeSpace = freeSpace;
        this.segmentSize = segmentSize;
        this.freeFlexibleSegmentUnitCount = freeFlexibleSegmentUnitCount;
        this.weight = weight;
    }

    public InstanceId getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(InstanceId instanceId) {
        this.instanceId = instanceId;
    }

    public int getGroupId() {
        return groupId;
    }

    public int getDiskCount() {
        return diskCount;
    }

    public long getSegmentSize() {
        return segmentSize;
    }

    public List<Long> getArchiveIds() {
        return archiveIds;
    }

    public void setArchiveIds(List<Long> archiveIds) {
        this.archiveIds = archiveIds;
    }

    public long getFreeSpace() {
        return freeSpace;
    }

    public void setFreeSpace(long freeSpace) {
        this.freeSpace = freeSpace;
    }

    public int getFreeFlexibleSegmentUnitCount() {
        return freeFlexibleSegmentUnitCount;
    }

    public void setFreeFlexibleSegmentUnitCount(int freeFlexibleSegmentUnitCount) {
        this.freeFlexibleSegmentUnitCount = freeFlexibleSegmentUnitCount;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    @Override
    public final int compareTo(@Nonnull SimulateInstanceInfo other) {
        int compare = Double.compare(weight, other.weight);
        if (compare == 0) {
            return Long.compare(instanceId.getId(), other.getInstanceId().getId());
        } else {
            return compare;
        }
    }

    @Override
    public String toString() {
        return "SimulateInstanceInfo{" +
                "instanceId=" + instanceId +
                ", groupId=" + groupId +
                ", diskCount=" + diskCount +
                ", segmentSize=" + segmentSize +
                ", archiveIds=" + archiveIds +
                ", freeSpace=" + freeSpace +
                ", freeFlexibleSegmentUnitCount=" + freeFlexibleSegmentUnitCount +
                ", weight=" + weight +
                '}';
    }
}
