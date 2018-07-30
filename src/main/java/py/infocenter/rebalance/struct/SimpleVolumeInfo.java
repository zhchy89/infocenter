package py.infocenter.rebalance.struct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import py.volume.VolumeStatus;
import py.volume.VolumeType;

@Deprecated
public class SimpleVolumeInfo {

    private final long volumeId;
    private final Map<Integer, SimpleSegmentInfo> segments;
    private final VolumeType volumeType;
    private final long storagePoolId;

    private VolumeStatus status;

    public SimpleVolumeInfo(long volumeId, VolumeType volumeType, long storagePoolId) {
        this.volumeId = volumeId;
        this.volumeType = volumeType;
        this.storagePoolId = storagePoolId;
        this.segments = new HashMap<>();
    }

    public void addSegment(SimpleSegmentInfo segment) {
        segments.put(segment.getSegId().getIndex(), segment);
        segment.setVolume(this);
    }

    public SimpleSegmentInfo getSegment(int index) {
        return segments.get(index);
    }

    public long getVolumeId() {
        return volumeId;
    }

    public VolumeStatus getStatus() {
        return status;
    }

    public void setStatus(VolumeStatus status) {
        this.status = status;
    }

    public List<SimpleSegmentInfo> getSegments() {
        return new ArrayList<>(segments.values());
    }

    public VolumeType getVolumeType() {
        return volumeType;
    }

    public long getStoragePoolId() {
        return storagePoolId;
    }

}
