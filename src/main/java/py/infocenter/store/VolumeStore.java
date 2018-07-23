package py.infocenter.store;

import py.icshare.exception.AccessDeniedException;
import py.volume.VolumeMetadata;

import java.util.Date;
import java.util.List;

public interface VolumeStore {
    // load volume in db when infocenter starts
    public void loadVolumeInDb();

    // delete a record from volume table
    public void deleteVolume(VolumeMetadata volumeMetadata);

    // update or insert a record(volumeMetadata)
    public void saveVolume(VolumeMetadata volumeMetadata);

    // List all volumes belonging to an account, if account Id is null, then return all volumes
    public List<VolumeMetadata> listVolumes();

    // list all root volumes belonging to an account. If account id is null, then return all root volumes
    public List<VolumeMetadata> listRootVolumes();

    // find all volumes belonging to one root volume. root volume is included
    public List<VolumeMetadata> listVolumesFromRoot(long rootId);

    List<VolumeMetadata> listVolumesWithSameRoot(long rootId);

    public VolumeMetadata getVolume(Long volumeId);

    public VolumeMetadata getVolumeNotDeadByName(String name);

    public void clearData();

    public int updateDeadTime(long volumeId, long deadTime);

    int updateStatusAndVolumeInAction(long volumeId, String status, String volumeInAction);

    int updateExtendingSize(long volumeId, long extendingSize);

    int updatePersistedItems(long volumeId, long volumeSize, String volumeName, String volumeType,
            String volumeLayout, boolean simpleConfigured, int segmentNumToCreateEachTime, long domainId,
            long storagePoolId, Date volumeCreatedTime, Date lastExtendedTime, String volumeSource,
            String readWrite);

    String updateVolumeLayout(long volumeId, int startSegmentIndex, int newSegmentsCount, String volumeLayout);

    void loadVolumeFromDbToMemory();
}
