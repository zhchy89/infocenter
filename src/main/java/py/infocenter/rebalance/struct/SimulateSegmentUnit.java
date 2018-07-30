package py.infocenter.rebalance.struct;

import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.instance.InstanceId;
import py.volume.VolumeType;

/**
 * segment unit simulation
 */
public class SimulateSegmentUnit {
    private final InstanceId instanceId;
    private final SegId segId;
    private final long archiveId;
    private final VolumeType volumeType;
    private SegmentUnitStatus status;

    public SimulateSegmentUnit(InstanceId instanceId, SegId segId, long archiveId, VolumeType volumeType, SegmentUnitStatus status){
        this.instanceId = instanceId;
        this.segId = segId;
        this.archiveId = archiveId;
        this.status = status;
        this.volumeType = volumeType;
    }

    public SimulateSegmentUnit(InstanceId instanceId, SimulateSegmentUnit simulateSegmentUnit){
        this.instanceId = instanceId;
        this.segId = simulateSegmentUnit.getSegId();
        this.archiveId = simulateSegmentUnit.getArchiveId();
        this.status = simulateSegmentUnit.getStatus();
        this.volumeType = simulateSegmentUnit.getVolumeType();
    }

    public SimulateSegmentUnit(SegmentUnitMetadata segmentUnitMetadata){
        this.instanceId = segmentUnitMetadata.getInstanceId();
        this.segId = segmentUnitMetadata.getSegId();
        this.archiveId = segmentUnitMetadata.getArchiveId();
        this.status = segmentUnitMetadata.getStatus();
        this.volumeType = segmentUnitMetadata.getVolumeType();
    }

    public InstanceId getInstanceId() {
        return instanceId;
    }

    public SegId getSegId() {
        return segId;
    }

    public long getArchiveId() {
        return archiveId;
    }

    public VolumeType getVolumeType() {
        return volumeType;
    }

    public SegmentUnitStatus getStatus() {
        return status;
    }

    public void setStatus(SegmentUnitStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "SimulateSegmentUnit{" +
                "instanceId=" + instanceId +
                ", segId=" + segId +
                ", archiveId=" + archiveId +
                ", volumeType=" + volumeType +
                ", status=" + status +
                '}';
    }
}
