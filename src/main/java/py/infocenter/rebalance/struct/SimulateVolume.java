package py.infocenter.rebalance.struct;

import py.archive.segment.SegmentMetadata;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

import java.util.HashMap;
import java.util.Map;

/**
 * volume simulation
 */
public class SimulateVolume {
    private final long poolId;
    private final long volumeId;
    private final VolumeType volumeType;
    private final int segmentCount;
    private Map<Integer, SimulateSegment> segIndex2SimulateSegmentMap = new HashMap<>();

    public SimulateVolume(VolumeMetadata volumeMetadata){
        this.poolId = volumeMetadata.getStoragePoolId();
        this.volumeId = volumeMetadata.getVolumeId();
        this.volumeType = volumeMetadata.getVolumeType();
        this.segmentCount = volumeMetadata.getSegmentCount();

        if (volumeMetadata.getSegmentTable() != null){
            for (int segIndex : volumeMetadata.getSegmentTable().keySet()){
                SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(segIndex);
                segIndex2SimulateSegmentMap.put(segIndex, new SimulateSegment(segmentMetadata));
            }
        }
    }

    public int getSegmentCount() {
        return segmentCount;
    }

    public long getPoolId() {
        return poolId;
    }

    public long getVolumeId() {
        return volumeId;
    }

    public Map<Integer, SimulateSegment> getSegIndex2SimulateSegmentMap() {
        return segIndex2SimulateSegmentMap;
    }

    public VolumeType getVolumeType() {
        return volumeType;
    }

    @Override
    public String toString() {
        return "SimulateVolume{" +
                "poolId=" + poolId +
                ", volumeId=" + volumeId +
                ", volumeType=" + volumeType +
                ", segmentCount=" + segmentCount +
                ", segIndex2SimulateSegmentMap=" + segIndex2SimulateSegmentMap +
                '}';
    }
}
