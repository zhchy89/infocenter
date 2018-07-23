package py.infocenter.service;

import org.junit.Test;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.DeleteSnapshotRequest;
import py.thrift.share.ServiceHavingBeenShutdown_Thrift;
import py.thrift.share.ServiceIsNotAvailable_Thrift;
import py.thrift.share.SnapshotIsInCloningException_Thrift;
import py.thrift.share.SnapshotNotFoundException_Thrift;
import py.thrift.share.SnapshotRollingBackException_Thrift;
import py.thrift.share.SnapshotVersionMismatchException_Thrift;
import py.thrift.share.VolumeNotFoundException_Thrift;
import py.volume.VolumeMetadata;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FilterSnapshotCloneTest extends TestBase {

    @Test(expected = SnapshotIsInCloningException_Thrift.class)
    public void testDeleteSnapshotWhenClone() throws SnapshotIsInCloningException_Thrift,
            VolumeNotFoundException_Thrift, SnapshotVersionMismatchException_Thrift,
            ServiceHavingBeenShutdown_Thrift, SnapshotNotFoundException_Thrift, ServiceIsNotAvailable_Thrift,
            SnapshotRollingBackException_Thrift {
        long snapshotVolumeId = 999L;
        int snapshotId = 1;
        long rootVolumeId = 111L;

        InformationCenterImpl infoCenter = new InformationCenterImpl();
        VolumeStore volumeStore = mock(VolumeStore.class);
        InfoCenterAppContext appContext = mock(InfoCenterAppContext.class);
        infoCenter.setVolumeStore(volumeStore);
        infoCenter.setAppContext(appContext);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        List<VolumeMetadata> rootVolumeMetadatas = new ArrayList<>();
        VolumeMetadata rootVolumeMetadata = new VolumeMetadata();
        rootVolumeMetadata.setVolumeId(rootVolumeId);
        rootVolumeMetadata.setRootVolumeId(rootVolumeId);
        rootVolumeMetadata.setCloningVolumeId(snapshotVolumeId);
        rootVolumeMetadata.setCloningSnapshotId(snapshotId);
        rootVolumeMetadata.setSegmentSize(1);

        rootVolumeMetadatas.add(rootVolumeMetadata);
        when(volumeStore.listRootVolumes()).thenReturn(rootVolumeMetadatas);
        when(volumeStore.getVolume(snapshotVolumeId)).thenReturn(rootVolumeMetadata);

        DeleteSnapshotRequest request = new DeleteSnapshotRequest();
        request.setRequestId(123456);
        request.setVolumeId(snapshotVolumeId);
        request.setSnapshotId(snapshotId);
        infoCenter.deleteSnapshot(request);
    }

}
