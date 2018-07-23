package py.infocenter.service;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.test.TestUtils.*;
import static py.test.TestUtils.generateVolumeMetadataWithSingleSegment;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py.archive.RawArchiveMetadata;
import py.archive.ArchiveStatus;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.common.RequestIdBuilder;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePoolStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.instance.*;
import py.membership.SegmentMembership;
import py.test.TestBase;
import py.thrift.share.*;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

/**
 * Created by fq on 17-3-20.
 * use for test FixVolumeTest method
 */
public class FixVolumeTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(FixVolumeTest.class);
    private static final Long TEST_PRIMARY_ID = 1L;
    private static final Long TEST_SECONDARY1_ID = 2L;
    private static final Long TEST_SECONDARY2_ID = 3L;
    private static final Long TEST_JOINING_SECONDARY_ID = 3L;
    private static final Long TEST_ARBITER_ID = 3L;

    @Mock
    private StorageStore storageStore;

    @Mock
    private InstanceStore instanceStore;
    @Mock
    private VolumeStore volumeStore;
    @Mock
    private DomainStore domainStore;
    @Mock
    private StoragePoolStore storagePoolStore;
    private InformationCenterImpl icImpl;

    @Before
    public void init() throws Exception {
        super.init();
        InfoCenterAppContext appContext = mock(InfoCenterAppContext.class);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        icImpl = new InformationCenterImpl();
        icImpl.setStorageStore(storageStore);
        icImpl.setVolumeStore(volumeStore);
        icImpl.setDomainStore(domainStore);
        icImpl.setInstanceStore(instanceStore);
        icImpl.setStoragePoolStore(storagePoolStore);
        icImpl.setAppContext(appContext);

    }

    // all node and disk are good
    @Test
    public void testFixVolumeWithoutFix() throws TException {
        FixVolumeRequest_Thrift fixVolumeRequest = new FixVolumeRequest_Thrift();
        fixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertTrue(volumeMetadata != null);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);

        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();

        for (Long i = 1L; i <= 3L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadataList.add(archiveMetadata);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDatanodeStatus(OK);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        for (Long datanodeId = 1L; datanodeId <= 3L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            InstanceId instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        FixVolumeResponse_Thrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
        int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
        assertEquals(0, lostDatanodesSize);

        boolean bVolumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
        assertTrue(bVolumeCompletely);

        boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
        assertFalse(bneedFixVolume);
    }

    @Test
    public void testFixVolumeWithAllDataNodeDied() throws TException {
        FixVolumeRequest_Thrift fixVolumeRequest = new FixVolumeRequest_Thrift();
        fixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertTrue(volumeMetadata != null);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();
        for (Long i = 1L; i <= 3L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadataList.add(archiveMetadata);
            dataNodeMetadata.setArchives(archiveMetadataList);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        for (Long datanodeId = 1L; datanodeId <= 3L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(null);
            InstanceId instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        FixVolumeResponse_Thrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
        int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
        assertTrue(lostDatanodesSize == 3);
        boolean bVolumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
        assertTrue(!bVolumeCompletely);
        boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
        assertTrue(bneedFixVolume);
    }

    @Test
    public void testFixVolumeWithInstanceStatusError() throws TException {
        FixVolumeRequest_Thrift fixVolumeRequest = new FixVolumeRequest_Thrift();
        fixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertTrue(volumeMetadata != null);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();
        for (Long i = 1L; i <= 3L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.FAILED);
            assertNotNull(datanodeInstance);

            volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadataList.add(archiveMetadata);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDatanodeStatus(OK);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        for (Long datanodeId = 1L; datanodeId <= 3L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            InstanceId instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        FixVolumeResponse_Thrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
        int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
        assertTrue(lostDatanodesSize == 3);
        boolean bVolumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
        assertTrue(!bVolumeCompletely);
        boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
        assertTrue(bneedFixVolume);
    }

    @Test
    public void testFixVolumeWithArchiveName() throws TException {
        FixVolumeRequest_Thrift fixVolumeRequest = new FixVolumeRequest_Thrift();
        fixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertTrue(volumeMetadata != null);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();
        for (Long i = 1L; i <= 3L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Error Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadataList.add(archiveMetadata);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDatanodeStatus(OK);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        for (Long datanodeId = 1L; datanodeId <= 3L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            InstanceId instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        FixVolumeResponse_Thrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
        int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
        assertTrue(lostDatanodesSize == 0);
        boolean bVolumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
        assertTrue(!bVolumeCompletely);
        boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
        assertTrue(bneedFixVolume);
    }

    @Test
    public void testFixVolumeWithArchiveStatusError() throws TException {
        FixVolumeRequest_Thrift fixVolumeRequest = new FixVolumeRequest_Thrift();
        fixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertTrue(volumeMetadata != null);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();
        for (Long i = 1L; i <= 3L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");

            if (i == 1l) {
                archiveMetadata.setStatus(ArchiveStatus.OFFLINED);
            } else if (i == 2l) {
                archiveMetadata.setStatus(ArchiveStatus.BROKEN);
            } else {
                archiveMetadata.setStatus(ArchiveStatus.OFFLINING);
            }

            archiveMetadataList.add(archiveMetadata);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDatanodeStatus(OK);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        for (Long datanodeId = 1L; datanodeId <= 3L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            InstanceId instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        FixVolumeResponse_Thrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
        int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
        assertTrue(lostDatanodesSize == 0);
        boolean bVolumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
        assertTrue(!bVolumeCompletely);
        boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
        assertTrue(bneedFixVolume);
    }

    @Test
    public void testFixVolumeWithSAliveatPSS() throws TException {
        FixVolumeRequest_Thrift fixVolumeRequest = new FixVolumeRequest_Thrift();
        fixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertTrue(volumeMetadata != null);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();
        for (Long i = 1L; i <= 3L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadataList.add(archiveMetadata);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDatanodeStatus(OK);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
        when(storageStore.get(TEST_SECONDARY2_ID)).thenReturn(instanceMetadataList.get(2));
        InstanceId instanceId = new InstanceId(TEST_SECONDARY2_ID);
        when(instanceStore.get(instanceId)).thenReturn(instanceList.get(2));

        fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        FixVolumeResponse_Thrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
        int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
        assertTrue(lostDatanodesSize == 2);
        boolean bVolumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
        assertTrue(!bVolumeCompletely);
        boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
        assertTrue(bneedFixVolume);
    }

    @Test
    public void testFixVolumeWithPAliveatPSS() throws TException {
        FixVolumeRequest_Thrift fixVolumeRequest = new FixVolumeRequest_Thrift();
        fixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertTrue(volumeMetadata != null);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();
        for (Long i = 1L; i <= 3L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadataList.add(archiveMetadata);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDatanodeStatus(OK);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        when(storageStore.get(TEST_SECONDARY2_ID)).thenReturn(null);
        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
        when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(instanceMetadataList.get(0));
        InstanceId instanceId = new InstanceId(TEST_PRIMARY_ID);
        when(instanceStore.get(instanceId)).thenReturn(instanceList.get(0));

        fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        FixVolumeResponse_Thrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
        int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
        assertTrue(lostDatanodesSize == 2);
        boolean bVolumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
        assertTrue(bVolumeCompletely);
        boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
        assertTrue(bneedFixVolume);
    }

    @Test
    public void testFixVolumeWithJAliveatPSJ() throws TException {
        FixVolumeRequest_Thrift fixVolumeRequest = new FixVolumeRequest_Thrift();
        fixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertTrue(volumeMetadata != null);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();
        for (Long i = 1L; i <= 3L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadataList.add(archiveMetadata);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDatanodeStatus(OK);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        SegmentMembership memship = volumeMetadata.getMembership(0);
        InstanceId id = new InstanceId(3l);
        SegmentMembership newMemship = memship.removeSecondary(id);

        id = new InstanceId(3l);
        memship = newMemship.addJoiningSecondary(id);
        volumeMetadata.updateMembership(0, memship);
        volumeMetadatasList.add(volumeMetadata);

        SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(0);
        for (InstanceId instanceId : memship.getMembers()) {
            SegmentUnitMetadata segmentUnitMetadata = segmentMetadata.getSegmentUnitMetadata(instanceId);
            segmentUnitMetadata.setMembership(memship);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
        when(storageStore.get(TEST_JOINING_SECONDARY_ID)).thenReturn(instanceMetadataList.get(2));
        InstanceId instanceId = new InstanceId(TEST_JOINING_SECONDARY_ID);
        when(instanceStore.get(instanceId)).thenReturn(instanceList.get(2));

        fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        FixVolumeResponse_Thrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
        int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
        assertTrue(lostDatanodesSize == 2);
        boolean bVolumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
        assertTrue(!bVolumeCompletely);
        boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
        assertTrue(bneedFixVolume);
    }

    @Test
    public void testFixVolumeWithAAliveatPSA() throws TException {
        FixVolumeRequest_Thrift fixVolumeRequest = new FixVolumeRequest_Thrift();
        fixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
        assertTrue(volumeMetadata != null);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();
        for (Long i = 1L; i <= 3L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadataList.add(archiveMetadata);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDatanodeStatus(OK);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
        when(storageStore.get(TEST_ARBITER_ID)).thenReturn(instanceMetadataList.get(2));
        InstanceId instanceId = new InstanceId(TEST_ARBITER_ID);
        when(instanceStore.get(instanceId)).thenReturn(instanceList.get(2));

        fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        FixVolumeResponse_Thrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
        int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
        assertTrue(lostDatanodesSize == 2);
        boolean bVolumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
        assertTrue(!bVolumeCompletely);
        boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
        assertTrue(bneedFixVolume);
    }

    @Test
    public void testFixVolumeWithPAliveatPSA() throws TException {
        FixVolumeRequest_Thrift fixVolumeRequest = new FixVolumeRequest_Thrift();
        fixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
        assertTrue(volumeMetadata != null);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();
        for (Long i = 1L; i <= 3L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadataList.add(archiveMetadata);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDatanodeStatus(OK);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        when(storageStore.get(TEST_ARBITER_ID)).thenReturn(null);
        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
        when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(instanceMetadataList.get(0));
        InstanceId instanceId = new InstanceId(TEST_PRIMARY_ID);
        when(instanceStore.get(instanceId)).thenReturn(instanceList.get(0));

        fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        FixVolumeResponse_Thrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
        int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
        assertTrue(lostDatanodesSize == 2);
        boolean bVolumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
        assertTrue(bVolumeCompletely);
        boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
        assertTrue(bneedFixVolume);
    }

    @Test
    public void testFixVolumeWithOnlyPNotAliveAtPJA() throws TException {
        FixVolumeRequest_Thrift fixVolumeRequest = new FixVolumeRequest_Thrift();
        fixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
        assertTrue(volumeMetadata != null);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();
        for (Long i = 1L; i <= 3L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadataList.add(archiveMetadata);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDatanodeStatus(OK);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        SegmentMembership membership = volumeMetadata.getMembership(0);
        InstanceId id = new InstanceId(2l);
        SegmentMembership newMembership = membership.removeSecondary(id);

        membership = newMembership.addJoiningSecondary(id);
        volumeMetadata.updateMembership(0, membership);
        volumeMetadatasList.add(volumeMetadata);

        SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(0);
        for (InstanceId instanceId : membership.getMembers()) {
            SegmentUnitMetadata segmentUnitMetadata = segmentMetadata.getSegmentUnitMetadata(instanceId);
            segmentUnitMetadata.setMembership(membership);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        when(storageStore.get(TEST_ARBITER_ID)).thenReturn(instanceMetadataList.get(2));
        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(instanceMetadataList.get(1));
        when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
        InstanceId instanceId = new InstanceId(TEST_SECONDARY1_ID);
        when(instanceStore.get(instanceId)).thenReturn(instanceList.get(1));
        InstanceId instanceId2 = new InstanceId(TEST_ARBITER_ID);
        when(instanceStore.get(instanceId2)).thenReturn(instanceList.get(2));

        fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        FixVolumeResponse_Thrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
        int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();

        assertTrue(lostDatanodesSize == 1);
        assertEquals(fixVolumeRsp.getLostDatanodes().iterator().next(), TEST_PRIMARY_ID);
        boolean bVolumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
        assertFalse(bVolumeCompletely);
        boolean needFixVolume = fixVolumeRsp.isNeedFixVolume();
        assertTrue(needFixVolume);
    }
}
