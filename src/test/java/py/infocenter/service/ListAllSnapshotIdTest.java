package py.infocenter.service;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.common.tree.GenericTree;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.store.VolumeStore;
import py.test.TestBase;
import py.thrift.icshare.ListAllSnapshotsRequest;
import py.thrift.icshare.ListAllSnapshotsResponse;
import py.thrift.share.VolumeMetadata_Thrift;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;
import py.volume.snapshot.*;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * The class use to test get snapshot information by different request.
 * Created by wangjing on 17-11-23.
 */
public class ListAllSnapshotIdTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(ListAllSnapshotIdTest.class);
    private InformationCenterImpl icImpl;
    @Mock
    private InfoCenterAppContext appContext;
    @Mock
    private VolumeStore volumeStore;
    @Mock
    private VolumeSnapshotManager snapshotManager;
    private long volumeId1 = 111111L;
    private long volumeSize = 512;

    @Before
    public void init() throws Exception {
        super.init();
        List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
        icImpl = new InformationCenterImpl();
        icImpl.setAppContext(appContext);
        icImpl.setVolumeStore(volumeStore);
        List<SnapshotMetadata> snapshotMetadataList = new ArrayList<>();
        VolumeMetadata volume1 = createVolumeMetadata(volumeId1, "test1");
        volumeMetadataList.add(volume1);

        long volumeId2 = 222222L;
        VolumeMetadata volume2 = createVolumeMetadata(volumeId2, "test2");
        when(volumeStore.getVolume(volumeId1)).thenReturn(volume1);
        when(volumeStore.getVolume(volumeId2)).thenReturn(volume2);
        when(volumeStore.listVolumesFromRoot(volumeId1)).thenReturn(volumeMetadataList);
        when(snapshotManager.listSnapshots()).thenReturn(snapshotMetadataList);
    }

    /**
     * request given volumeId list ,and it contain just one volume(volumeId1)
     * @throws TException
     */
    @Test
    public void listSnapshotWithVolumesRequest() throws TException {
        Set<Long> volumeIdList = new HashSet<>();
        volumeIdList.add(volumeId1);
        ListAllSnapshotsRequest request = new ListAllSnapshotsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeIds(volumeIdList);

        List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
        volumeMetadataList.add(volumeStore.getVolume(volumeId1));
        when (volumeStore.listVolumesFromRoot(volumeId1)).thenReturn(volumeMetadataList);

        ListAllSnapshotsResponse response = icImpl.listAllSnapshots(request);
        Map<VolumeMetadata_Thrift, ByteBuffer> volumeId2SnapshotManager = response.getVolume2SnapshotManagerBinary();
        assertEquals(1, volumeId2SnapshotManager.size());
        ByteBuffer snapshotByteBuffer = null;
        for (ByteBuffer byteBuffer : volumeId2SnapshotManager.values()) {
            snapshotByteBuffer = byteBuffer;
        }
        GenericVolumeSnapshotManager snapshotManager = GenericVolumeSnapshotManager.parseFromByteArray(
                snapshotByteBuffer.array(), volumeId1);
        List<GenericTree.TreeNode<SnapshotMetadata>> treeNodes = snapshotManager.getSnapshotTree().asList();
        for (GenericTree.TreeNode<SnapshotMetadata> treeNode : treeNodes) {
            treeNode.parent();
            treeNode.children();
        }
        logger.debug("snapshotManager is {}",snapshotManager);
        assertEquals(2, snapshotManager.listSnapshotIds().size());
    }

    /**
     * request has given empty volumeIds, will not return any snapshot
     * @throws TException
     */
    @Test
    public void listSnapshotWithNullRequest() throws TException {
        ListAllSnapshotsRequest request = new ListAllSnapshotsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeIds(new HashSet<>());
        ListAllSnapshotsResponse response = icImpl.listAllSnapshots(request);
        assertEquals(0, response.getVolume2SnapshotManagerBinarySize());
    }

    private VolumeMetadata createVolumeMetadata(long volumeId, String volumeName) throws Exception {
        VolumeMetadata volume = new VolumeMetadata();
        volume.setRootVolumeId(volumeId);
        volume.setVolumeId(volumeId);
        volume.setName(volumeName);
        volume.setDomainId(111111111L);
        volume.setStoragePoolId(11111111L);
        GenericVolumeSnapshotManager snapshotManager = new GenericVolumeSnapshotManager(volumeId);
        volume.setSnapshotManager(snapshotManager);
        snapshotManager.addSnapshot(1, "snap1", "snap1", 20171123,
                0, volumeSize);
        snapshotManager.addSnapshot(2, "snap2", "snap2", 20171123,
                1, volumeSize);

        volume.setVolumeType(VolumeType.REGULAR);
        volume.setCacheType(CacheType.MEMORY);
        volume.setVolumeStatus(VolumeStatus.Available);
        volume.setAccountId(1862755152385798555L);
        volume.setSegmentSize(1008);
        volume.setVolumeCreatedTime(new Date());
        volume.setLastExtendedTime(new Date());
        volume.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
        volume.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
        volume.setPageWrappCount(128);
        volume.setSegmentWrappCount(10);
        return volume;
    }
}
