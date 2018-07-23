package py.infocenter.store;

import java.util.List;

import py.icshare.SegmentId;
import py.icshare.SegmentUnitInformation;

public interface SegmentStore {
    
    public void update(SegmentUnitInformation segmentInformation);

    public void save(SegmentUnitInformation segmentInformation);
    
    public List<SegmentUnitInformation> getByVolumeId(long volumeId);
    
    public SegmentUnitInformation getBySegmentId(SegmentId segmentId);
    
    public List<SegmentUnitInformation> list();
    
    public int deleteByVolumeId(long volumeId);
    
    public int delete(SegmentId segmentId);
}
