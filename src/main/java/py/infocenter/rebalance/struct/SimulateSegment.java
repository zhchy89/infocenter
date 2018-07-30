package py.infocenter.rebalance.struct;

import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.instance.InstanceId;
import py.volume.VolumeType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * segment simulation
 */
public class SimulateSegment {
    private final long volumeId;
    private final VolumeType volumeType;
    private final SegId segId;
    private Map<InstanceId, SimulateSegmentUnit> instanceId2SimulateSegUnitMap = new HashMap<>();
    private InstanceId primaryId;
    private Set<InstanceId> secondaryIdSet = new HashSet<>();
    private Set<InstanceId> arbiterIdSet = new HashSet<>();

    public SimulateSegment(SegmentMetadata segmentMetadata){
        this.volumeId = segmentMetadata.getVolume().getVolumeId();
        this.volumeType = segmentMetadata.getVolume().getVolumeType();
        this.segId = segmentMetadata.getSegId();

        for (InstanceId instanceId : segmentMetadata.getSegmentUnitMetadataTable().keySet()){
            SegmentUnitMetadata segmentUnit = segmentMetadata.getSegmentUnitMetadataTable().get(instanceId);
            instanceId2SimulateSegUnitMap.put(instanceId, new SimulateSegmentUnit(segmentUnit));
            if (segmentUnit.getStatus() == SegmentUnitStatus.Primary){
                primaryId = instanceId;
            } else if (segmentUnit.getStatus() == SegmentUnitStatus.Secondary){
                secondaryIdSet.add(instanceId);
            } else if (segmentUnit.getStatus() == SegmentUnitStatus.Arbiter){
                arbiterIdSet.add(instanceId);
            }
        }
    }

    public long getVolumeId() {
        return volumeId;
    }

    public SegId getSegId() {
        return segId;
    }

    public Map<InstanceId, SimulateSegmentUnit> getInstanceId2SimulateSegUnitMap() {
        return instanceId2SimulateSegUnitMap;
    }

    public VolumeType getVolumeType() {
        return volumeType;
    }

    public InstanceId getPrimaryId() {
        return primaryId;
    }

    public void setPrimaryId(InstanceId primaryId) {
        this.primaryId = primaryId;
    }

    public Set<InstanceId> getSecondaryIdSet() {
        return secondaryIdSet;
    }

    public void setSecondaryIdSet(Set<InstanceId> secondaryIdSet) {
        this.secondaryIdSet = secondaryIdSet;
    }

    public Set<InstanceId> getArbiterIdSet() {
        return arbiterIdSet;
    }

    public void setArbiterIdSet(Set<InstanceId> arbiterIdSet) {
        this.arbiterIdSet = arbiterIdSet;
    }

    /**
     * simulate secondary migrate
     *
     * phase 1: new a simulate segment unit on dest
     * phase 2: move src segment unit to dest
     * phase 3: update segment's instance map and secondary set
     *
     * @param src   src instance id
     * @param dest  dest instance id
     * @return  true:if simulate migrate success
     */
    public boolean migrateSecondary(InstanceId src, InstanceId dest){
        if (!secondaryIdSet.contains(src)){
            return false;
        }

        SimulateSegmentUnit srcSegUnit = instanceId2SimulateSegUnitMap.get(src);
        SimulateSegmentUnit destSegUnit = new SimulateSegmentUnit(dest, srcSegUnit);

        instanceId2SimulateSegUnitMap.remove(src);
        instanceId2SimulateSegUnitMap.put(dest, destSegUnit);

        secondaryIdSet.remove(src);
        secondaryIdSet.add(dest);
        return true;
    }

    /**
     * simulate primary migrate
     *
     * phase 1: swap src primary and dest secondary,update segment unit status
     * phase 2: update segment's primary id to dest instance id, and update secondary set
     *
     * @param src   src instance id
     * @param dest  dest instance id
     * @return  true:if simulate migrate success
     */
    public boolean migratePrimary(InstanceId src, InstanceId dest){
        if (primaryId != src && !secondaryIdSet.contains(dest)){
            return false;
        }

        SimulateSegmentUnit srcSegUnit = instanceId2SimulateSegUnitMap.get(src);
        SimulateSegmentUnit destSegUnit = instanceId2SimulateSegUnitMap.get(dest);

        srcSegUnit.setStatus(SegmentUnitStatus.Secondary);
        destSegUnit.setStatus(SegmentUnitStatus.Primary);

        primaryId = dest;
        secondaryIdSet.remove(dest);
        secondaryIdSet.add(src);
        return true;
    }

    @Override
    public String toString() {
        return "SimulateSegment{" +
                "volumeId=" + volumeId +
                ", volumeType=" + volumeType +
                ", segId=" + segId +
                ", instanceId2SimulateSegUnitMap=" + instanceId2SimulateSegUnitMap +
                ", primaryId=" + primaryId +
                ", secondaryIdSet=" + secondaryIdSet +
                ", arbiterIdSet=" + arbiterIdSet +
                '}';
    }
}
