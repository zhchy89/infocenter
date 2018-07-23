package py.infocenter.service;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.icshare.InstanceMetadata.DatanodeType.NORMAL;
import static py.test.TestUtils.*;
import static py.test.TestUtils.generateVolumeMetadataWithSingleSegment;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py.archive.RawArchiveMetadata;
import py.archive.ArchiveStatus;
import py.archive.StorageType;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.common.RequestIdBuilder;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePool;
import py.icshare.StoragePoolStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.instance.*;
import py.membership.SegmentMembership;
import py.test.TestBase;
import py.thrift.share.*;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

/**
 * Created by fq on 17-3-21.
 * use for test ConfirmFixVolumeTest method
 */

public class ConfirmFixVolumeTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(ConfirmFixVolumeTest.class);

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
    @Mock
    private SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager;

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
        icImpl.setSegmentUnitsDistributionManager(segmentUnitsDistributionManager);
        icImpl.setSegmentWrappCount(10);
    }

    // datanode in request < datanode is died in volume
    @Test
    public void testConfirmFixVolumeWithNotAllDataNode() throws TException {
        ConfirmFixVolumeRequest_Thrift confirmFixVolumeRequest = new ConfirmFixVolumeRequest_Thrift();
        confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertNotNull(volumeMetadata);

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
            when(storageStore.get(datanodeId)).thenReturn(null);
        }

        Set<Long> lostDataNode = new HashSet<>();
        confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());
        confirmFixVolumeRequest.setLostDatanodes(lostDataNode);

        boolean caughtLackNodeException = false;
        try {
            icImpl.confirmFixVolume(confirmFixVolumeRequest);
        } catch (LackDatanodeException_Thrift e) {
            caughtLackNodeException = true;
        }
        assertTrue(caughtLackNodeException);
    }

    //all ArchiveDied , do not need datanode in request
    @Test
    public void testConfirmFixVolumeWithAllArchiveDied() throws TException, IOException, SQLException {
        ConfirmFixVolumeRequest_Thrift confirmFixVolumeRequest = new ConfirmFixVolumeRequest_Thrift();
        confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertNotNull(volumeMetadata);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();

        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(volumeMetadata.getStoragePoolId());
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(volumeMetadata.getStoragePoolId());
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        storagePool.setArchivesInDataNode(archivesInDataNode);

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
            archiveMetadata.setStatus(ArchiveStatus.OFFLINED);
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadataList.add(archiveMetadata);
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        for (Long i = 4L; i <= 5L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            archiveMetadataList.add(archiveMetadata);

            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(Long.valueOf(i), 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        for (Long datanodeId = 1L; datanodeId <= 5L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            InstanceId instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        when(storageStore.list()).thenReturn(instanceMetadataList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

        Domain domain = mock(Domain.class);
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        boolean caughtLackNodeException = false;
        try {
            icImpl.confirmFixVolume(confirmFixVolumeRequest);
        } catch (LackDatanodeException_Thrift e) {
            caughtLackNodeException = true;
        }
        assertFalse(caughtLackNodeException);

    }

    // datanode in request = datanode is died in volume
    @Test
    public void testConfirmFixVolumeWithAllDataNode() throws TException, IOException, SQLException {
        ConfirmFixVolumeRequest_Thrift confirmFixVolumeRequest = new ConfirmFixVolumeRequest_Thrift();
        confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertNotNull(volumeMetadata);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();

        Set<Long> storagePoolIdList = new HashSet<Long>();
        storagePoolIdList.add(volumeMetadata.getStoragePoolId());
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(volumeMetadata.getStoragePoolId());
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        storagePool.setArchivesInDataNode(archivesInDataNode);

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
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadataList.add(archiveMetadata);
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        for (Long i = 4L; i <= 5L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            archiveMetadataList.add(archiveMetadata);

            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        for (Long datanodeId = 1L; datanodeId <= 2L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(null);
        }
        for (Long datanodeId = 3L; datanodeId <= 5L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            InstanceId instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        when(storageStore.list()).thenReturn(instanceMetadataList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

        Domain domain = mock(Domain.class);
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        Set<Long> lostDataNode = new HashSet<>();
        lostDataNode.add(1L);
        lostDataNode.add(2L);
        confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
        boolean caughtLackNodeException = false;
        try {
            icImpl.confirmFixVolume(confirmFixVolumeRequest);
        } catch (LackDatanodeException_Thrift e) {
            caughtLackNodeException = true;
        }
        assertFalse(caughtLackNodeException);

    }

    @Test
    public void testConfirmFixVolumeWithPAliveatPSS() throws TException, IOException, SQLException {
        ConfirmFixVolumeRequest_Thrift confirmFixVolumeRequest = new ConfirmFixVolumeRequest_Thrift();
        confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertNotNull(volumeMetadata);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();

        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(volumeMetadata.getStoragePoolId());
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(volumeMetadata.getStoragePoolId());
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        storagePool.setArchivesInDataNode(archivesInDataNode);

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
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadataList.add(archiveMetadata);
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        for (Long i = 4L; i <= 5L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            archiveMetadataList.add(archiveMetadata);

            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
        when(storageStore.get(TEST_SECONDARY2_ID)).thenReturn(null);
        when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(instanceMetadataList.get(0));
        InstanceId instanceId = new InstanceId(TEST_PRIMARY_ID);
        when(instanceStore.get(instanceId)).thenReturn(instanceList.get(0));

        for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        when(storageStore.list()).thenReturn(instanceMetadataList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

        Domain domain = mock(Domain.class);
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        Set<Long> lostDataNode = new HashSet<>();
        lostDataNode.add(TEST_SECONDARY1_ID);
        lostDataNode.add(TEST_SECONDARY2_ID);
        confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
        boolean caughtLackNodeException = false;
        ConfirmFixVolumeResponse fixVolumeRsp = null;
        try {
            fixVolumeRsp = icImpl.confirmFixVolume(confirmFixVolumeRequest);
        } catch (LackDatanodeException_Thrift e) {
            caughtLackNodeException = true;
        }
        assertFalse(caughtLackNodeException);

        assertNotNull(fixVolumeRsp);
        assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
        Map<SegId_Thrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp.getCreateSegmentUnits();
        assertEquals(createSegmentUnitList.size(), 1);
        for (Map.Entry<SegId_Thrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList.entrySet()) {
            List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
            assertEquals(createSegmentUnitInfoList.size(), 1);
            for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
                assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 2);
                for (Map.Entry<InstanceIdAndEndPoint_Thrift, SegmentMembership_Thrift> entry1 : createSegmentUnitInfo
                        .getSegmentMembershipMap().entrySet()) {
                    Set<Long> joinsendaryInstanceId = entry1.getValue().getJoiningSecondaries();
                    if (!joinsendaryInstanceId.contains(entry1.getKey().getInstanceId())) {
                        fail();
                    }
                    assertEquals(entry1.getValue().getPrimary(), 1L);
                    assertEquals(entry1.getValue().getEpoch(), 2);
                    assertEquals(entry1.getValue().getGeneration(), 0);
                }
                assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRole_Thrift.JoiningSecondary);
            }

        }
    }

    @Test
    public void testConfirmFixVolumeWithSAliveatPSS() throws TException, IOException, SQLException {
        ConfirmFixVolumeRequest_Thrift confirmFixVolumeRequest = new ConfirmFixVolumeRequest_Thrift();
        confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertNotNull(volumeMetadata);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();

        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(volumeMetadata.getStoragePoolId());
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(volumeMetadata.getStoragePoolId());
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        storagePool.setArchivesInDataNode(archivesInDataNode);

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
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadataList.add(archiveMetadata);
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        for (Long i = 4L; i <= 5L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            archiveMetadataList.add(archiveMetadata);

            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
        when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
        when(storageStore.get(TEST_SECONDARY2_ID)).thenReturn(instanceMetadataList.get(2));
        InstanceId instanceId = new InstanceId(TEST_SECONDARY2_ID);
        when(instanceStore.get(instanceId)).thenReturn(instanceList.get(2));

        for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        when(storageStore.list()).thenReturn(instanceMetadataList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

        Domain domain = mock(Domain.class);
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        Set<Long> lostDataNode = new HashSet<>();
        lostDataNode.add(TEST_SECONDARY1_ID);
        lostDataNode.add(TEST_PRIMARY_ID);
        confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
        boolean caughtLackNodeException = false;
        ConfirmFixVolumeResponse fixVolumeRsp = null;
        try {
            fixVolumeRsp = icImpl.confirmFixVolume(confirmFixVolumeRequest);
        } catch (LackDatanodeException_Thrift e) {
            caughtLackNodeException = true;
        }
        assertFalse(caughtLackNodeException);

        assertNotNull(fixVolumeRsp);
        assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
        Map<SegId_Thrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp.getCreateSegmentUnits();
        assertEquals(createSegmentUnitList.size(), 1);
        for (Map.Entry<SegId_Thrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList.entrySet()) {
            List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
            assertEquals(createSegmentUnitInfoList.size(), 1);
            for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
                assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 2);
                for (Map.Entry<InstanceIdAndEndPoint_Thrift, SegmentMembership_Thrift> entry1 : createSegmentUnitInfo
                        .getSegmentMembershipMap().entrySet()) {
                    Set<Long> joiningSecondaries = entry1.getValue().getJoiningSecondaries();
                    Long primaryId = entry1.getValue().getPrimary();
                    if (!joiningSecondaries.contains(entry1.getKey().getInstanceId())) {
                        fail();
                    }
                    assertEquals(primaryId, TEST_SECONDARY2_ID);
                    assertEquals(entry1.getValue().getEpoch(), 2);
                    assertEquals(entry1.getValue().getGeneration(), 0);
                }
                assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRole_Thrift.JoiningSecondary);
            }

        }
    }

    @Test
    public void testConfirmFixVolumeWithJAliveatPSS() throws TException, IOException, SQLException {
        ConfirmFixVolumeRequest_Thrift confirmFixVolumeRequest = new ConfirmFixVolumeRequest_Thrift();
        confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertNotNull(volumeMetadata);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        SegmentMembership memship = volumeMetadata.getMembership(0);
        InstanceId id = new InstanceId(TEST_SECONDARY2_ID);
        SegmentMembership newMemship = memship.removeSecondary(id);

        id = new InstanceId(TEST_JOINING_SECONDARY_ID);
        memship = newMemship.addJoiningSecondary(id);
        volumeMetadata.updateMembership(0, memship);

        SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(0);
        for (InstanceId instanceId : memship.getMembers()) {
            SegmentUnitMetadata segmentUnitMetadata = segmentMetadata.getSegmentUnitMetadata(instanceId);
            segmentUnitMetadata.setMembership(memship);
        }

        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();

        Set<Long> storagePoolIdList = new HashSet<Long>();
        storagePoolIdList.add(volumeMetadata.getStoragePoolId());
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(volumeMetadata.getStoragePoolId());
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        storagePool.setArchivesInDataNode(archivesInDataNode);

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
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadataList.add(archiveMetadata);
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        for (Long i = 4L; i <= 5L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            archiveMetadataList.add(archiveMetadata);

            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
        when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
        when(storageStore.get(TEST_JOINING_SECONDARY_ID)).thenReturn(instanceMetadataList.get(2));
        InstanceId instanceId = new InstanceId(TEST_JOINING_SECONDARY_ID);
        when(instanceStore.get(instanceId)).thenReturn(instanceList.get(2));

        for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        when(storageStore.list()).thenReturn(instanceMetadataList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

        Domain domain = mock(Domain.class);
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        Set<Long> lostDataNode = new HashSet<>();
        lostDataNode.add(TEST_SECONDARY1_ID);
        lostDataNode.add(TEST_PRIMARY_ID);
        confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
        boolean caughtLackNodeException = false;
        ConfirmFixVolumeResponse fixVolumeRsp = null;
        try {
            fixVolumeRsp = icImpl.confirmFixVolume(confirmFixVolumeRequest);
        } catch (LackDatanodeException_Thrift e) {
            caughtLackNodeException = true;
        }
        assertFalse(caughtLackNodeException);

        assertNotNull(fixVolumeRsp);
        assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
        Map<SegId_Thrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp.getCreateSegmentUnits();
        assertEquals(createSegmentUnitList.size(), 1);
        for (Map.Entry<SegId_Thrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList.entrySet()) {
            List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
            assertEquals(createSegmentUnitInfoList.size(), 1);
            for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
                assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 2);
                for (Map.Entry<InstanceIdAndEndPoint_Thrift, SegmentMembership_Thrift> entry1 : createSegmentUnitInfo
                        .getSegmentMembershipMap().entrySet()) {
                    Set<Long> joinsendaryInstanceId = entry1.getValue().getJoiningSecondaries();
                    if (!joinsendaryInstanceId.contains(TEST_JOINING_SECONDARY_ID)) {
                        fail();
                    }
                    assertEquals(entry1.getValue().getPrimary(), entry1.getKey().getInstanceId());
                    assertEquals(entry1.getValue().getEpoch(), 2);
                    assertEquals(entry1.getValue().getGeneration(), 0);
                }
                assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRole_Thrift.Primary);
            }

        }
    }

    @Test
    public void testConfirmFixVolumeWithNoAliveatPSS() throws TException, IOException, SQLException {
        ConfirmFixVolumeRequest_Thrift confirmFixVolumeRequest = new ConfirmFixVolumeRequest_Thrift();
        confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
        assertNotNull(volumeMetadata);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();

        Set<Long> storagePoolIdList = new HashSet<Long>();
        storagePoolIdList.add(volumeMetadata.getStoragePoolId());
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(volumeMetadata.getStoragePoolId());
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        storagePool.setArchivesInDataNode(archivesInDataNode);

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
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadataList.add(archiveMetadata);
            archiveMetadata.setLogicalFreeSpace(1l);
            archiveMetadata.setStorageType(StorageType.SATA);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(Long.valueOf(i), Long.valueOf(0));
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        for (Long i = 4l; i <= 5l; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            archiveMetadataList.add(archiveMetadata);

            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
        when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
        when(storageStore.get(TEST_SECONDARY2_ID)).thenReturn(null);

        for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            InstanceId instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        when(storageStore.list()).thenReturn(instanceMetadataList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

        Domain domain = mock(Domain.class);
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        Set<Long> lostDataNode = new HashSet<>();
        lostDataNode.add(TEST_SECONDARY2_ID);
        lostDataNode.add(TEST_SECONDARY1_ID);
        lostDataNode.add(TEST_PRIMARY_ID);
        confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
        boolean caughtLackNodeException = false;
        ConfirmFixVolumeResponse fixVolumeRsp = null;
        try {
            fixVolumeRsp = icImpl.confirmFixVolume(confirmFixVolumeRequest);
        } catch (LackDatanodeException_Thrift e) {
            caughtLackNodeException = true;
        }
        assertFalse(caughtLackNodeException);

        assertNotNull(fixVolumeRsp);
        assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
        Map<SegId_Thrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp.getCreateSegmentUnits();
        assertEquals(createSegmentUnitList.size(), 1);
        for (Map.Entry<SegId_Thrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList.entrySet()) {
            List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
            assertEquals(createSegmentUnitInfoList.size(), 2);
            for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
                assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 1);
                for (Map.Entry<InstanceIdAndEndPoint_Thrift, SegmentMembership_Thrift> entry1 : createSegmentUnitInfo
                        .getSegmentMembershipMap().entrySet()) {
                    Set<Long> secondaryID = entry1.getValue().getSecondaries();
                    if (entry1.getValue().getPrimary() == entry1.getKey().getInstanceId()) {
                        assertEquals(entry1.getValue().getEpoch(), 2);
                        assertEquals(entry1.getValue().getGeneration(), 0);
                        assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRole_Thrift.Primary);
                    } else if (secondaryID.contains(entry1.getKey().getInstanceId())) {
                        assertEquals(entry1.getValue().getEpoch(), 2);
                        assertEquals(entry1.getValue().getGeneration(), 0);
                        assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRole_Thrift.Secondary);
                    } else {
                        fail();
                    }
                }
            }
        }
    }

    @Test
    public void testConfirmFixVolumeWithPAliveatPSA() throws TException, IOException, SQLException {
        ConfirmFixVolumeRequest_Thrift confirmFixVolumeRequest = new ConfirmFixVolumeRequest_Thrift();
        confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
        assertNotNull(volumeMetadata);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();

        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(volumeMetadata.getStoragePoolId());
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(volumeMetadata.getStoragePoolId());
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        storagePool.setArchivesInDataNode(archivesInDataNode);

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
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadataList.add(archiveMetadata);
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        for (Long i = 4L; i <= 5L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            archiveMetadataList.add(archiveMetadata);

            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(Long.valueOf(i), Long.valueOf(0));
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
        when(storageStore.get(TEST_ARBITER_ID)).thenReturn(null);
        when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(instanceMetadataList.get(0));
        InstanceId instanceId = new InstanceId(TEST_PRIMARY_ID);
        when(instanceStore.get(instanceId)).thenReturn(instanceList.get(0));

        for (Long datanodeId = 4l; datanodeId <= 5l; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        when(storageStore.list()).thenReturn(instanceMetadataList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

        Domain domain = mock(Domain.class);
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        Set<Long> lostDataNode = new HashSet<>();
        lostDataNode.add(TEST_SECONDARY1_ID);
        lostDataNode.add(TEST_ARBITER_ID);
        confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
        boolean caughtLackNodeException = false;
        ConfirmFixVolumeResponse fixVolumeRsp = null;
        try {
            fixVolumeRsp = icImpl.confirmFixVolume(confirmFixVolumeRequest);
        } catch (LackDatanodeException_Thrift e) {
            caughtLackNodeException = true;
        }
        assertFalse(caughtLackNodeException);

        assertNotNull(fixVolumeRsp);
        assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
        Map<SegId_Thrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp.getCreateSegmentUnits();
        assertEquals(createSegmentUnitList.size(), 1);
        for (Map.Entry<SegId_Thrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList.entrySet()) {
            List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
            assertEquals(createSegmentUnitInfoList.size(), 1);
            for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
                assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 2);
                for (Map.Entry<InstanceIdAndEndPoint_Thrift, SegmentMembership_Thrift> entry1 : createSegmentUnitInfo
                        .getSegmentMembershipMap().entrySet()) {
                    Set<Long> arbiterId = entry1.getValue().getArbiters();
                    if (!arbiterId.contains(entry1.getKey().getInstanceId())) {
                        fail();
                    }
                    assertEquals(entry1.getValue().getPrimary(), 1L);
                    assertEquals(entry1.getValue().getEpoch(), 2);
                    assertEquals(entry1.getValue().getGeneration(), 0);
                }
                assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRole_Thrift.Arbiter);
            }

        }
    }

    @Test
    public void testConfirmFixVolumeWithSAliveatPSA() throws TException, IOException, SQLException {
        ConfirmFixVolumeRequest_Thrift confirmFixVolumeRequest = new ConfirmFixVolumeRequest_Thrift();
        confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
        assertNotNull(volumeMetadata);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();

        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(volumeMetadata.getStoragePoolId());
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(volumeMetadata.getStoragePoolId());
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        storagePool.setArchivesInDataNode(archivesInDataNode);

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
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadataList.add(archiveMetadata);
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        for (Long i = 4L; i <= 5L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            archiveMetadataList.add(archiveMetadata);

            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        when(storageStore.get(TEST_ARBITER_ID)).thenReturn(null);
        when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);

        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(instanceMetadataList.get(1));
        InstanceId instanceId = new InstanceId(TEST_SECONDARY1_ID);
        when(instanceStore.get(instanceId)).thenReturn(instanceList.get(1));

        for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        when(storageStore.list()).thenReturn(instanceMetadataList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

        Domain domain = mock(Domain.class);
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        Set<Long> lostDataNode = new HashSet<>();
        lostDataNode.add(TEST_ARBITER_ID);
        lostDataNode.add(TEST_PRIMARY_ID);
        confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
        boolean caughtLackNodeException = false;
        ConfirmFixVolumeResponse fixVolumeRsp = null;
        try {
            fixVolumeRsp = icImpl.confirmFixVolume(confirmFixVolumeRequest);
        } catch (LackDatanodeException_Thrift e) {
            caughtLackNodeException = true;
        }
        assertFalse(caughtLackNodeException);

        assertNotNull(fixVolumeRsp);
        assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
        Map<SegId_Thrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp.getCreateSegmentUnits();
        assertEquals(createSegmentUnitList.size(), 1);
        for (Map.Entry<SegId_Thrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList.entrySet()) {
            List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
            assertEquals(createSegmentUnitInfoList.size(), 1);
            for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
                assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 2);
                for (Map.Entry<InstanceIdAndEndPoint_Thrift, SegmentMembership_Thrift> entry1 : createSegmentUnitInfo
                        .getSegmentMembershipMap().entrySet()) {
                    Set<Long> arbiterId = entry1.getValue().getArbiters();
                    Long primaryId = entry1.getValue().getPrimary();
                    if (!arbiterId.contains(entry1.getKey().getInstanceId())) {
                        fail();
                    }
                    assertEquals(primaryId, TEST_SECONDARY1_ID);
                    assertEquals(entry1.getValue().getEpoch(), 2);
                    assertEquals(entry1.getValue().getGeneration(), 0);
                }
                assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRole_Thrift.Arbiter);
            }
        }
    }

    @Test
    public void testConfirmFixVolumeWithAAliveatPSA() throws TException, IOException, SQLException {
        ConfirmFixVolumeRequest_Thrift confirmFixVolumeRequest = new ConfirmFixVolumeRequest_Thrift();
        confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
        assertNotNull(volumeMetadata);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();

        Set<Long> storagePoolIdList = new HashSet<Long>();
        storagePoolIdList.add(volumeMetadata.getStoragePoolId());
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(volumeMetadata.getStoragePoolId());
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        storagePool.setArchivesInDataNode(archivesInDataNode);

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
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadataList.add(archiveMetadata);
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        for (Long i = 4L; i <= 5L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            archiveMetadataList.add(archiveMetadata);

            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
        when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);

        when(storageStore.get(TEST_ARBITER_ID)).thenReturn(instanceMetadataList.get(2));
        InstanceId instanceId = new InstanceId(TEST_ARBITER_ID);
        when(instanceStore.get(instanceId)).thenReturn(instanceList.get(2));

        for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        when(storageStore.list()).thenReturn(instanceMetadataList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

        Domain domain = mock(Domain.class);
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        Set<Long> lostDataNode = new HashSet<>();
        lostDataNode.add(TEST_SECONDARY1_ID);
        lostDataNode.add(TEST_PRIMARY_ID);
        confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
        boolean caughtLackNodeException = false;
        ConfirmFixVolumeResponse fixVolumeRsp = null;
        try {
            fixVolumeRsp = icImpl.confirmFixVolume(confirmFixVolumeRequest);
        } catch (LackDatanodeException_Thrift e) {
            caughtLackNodeException = true;
        }
        assertFalse(caughtLackNodeException);

        assertNotNull(fixVolumeRsp);
        assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
        Map<SegId_Thrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp.getCreateSegmentUnits();
        assertEquals(createSegmentUnitList.size(), 1);
        for (Map.Entry<SegId_Thrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList.entrySet()) {
            List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
            assertEquals(createSegmentUnitInfoList.size(), 1);
            for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
                assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 2);
                for (Map.Entry<InstanceIdAndEndPoint_Thrift, SegmentMembership_Thrift> entry1 : createSegmentUnitInfo
                        .getSegmentMembershipMap().entrySet()) {
                    Set<Long> arbiterId = entry1.getValue().getArbiters();
                    if (!arbiterId.contains(TEST_ARBITER_ID)) {
                        fail();
                    }
                    assertEquals(entry1.getValue().getPrimary(), entry1.getKey().getInstanceId());
                    assertEquals(entry1.getValue().getEpoch(), 2);
                    assertEquals(entry1.getValue().getGeneration(), 0);
                }
                assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRole_Thrift.Primary);
            }

        }
    }

    @Test
    public void testConfirmFixVolumeWithNoAliveatPSA() throws TException, IOException, SQLException {
        ConfirmFixVolumeRequest_Thrift confirmFixVolumeRequest = new ConfirmFixVolumeRequest_Thrift();
        confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
        assertNotNull(volumeMetadata);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
        volumeMetadatasList.add(volumeMetadata);

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();

        Set<Long> storagePoolIdList = new HashSet<Long>();
        storagePoolIdList.add(volumeMetadata.getStoragePoolId());
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(volumeMetadata.getStoragePoolId());
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        storagePool.setArchivesInDataNode(archivesInDataNode);

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
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadataList.add(archiveMetadata);
            archiveMetadata.setLogicalFreeSpace(1l);
            archiveMetadata.setStorageType(StorageType.SATA);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        for (Long i = 4L; i <= 5L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            archiveMetadataList.add(archiveMetadata);

            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
        when(volumeStore.listVolumesFromRoot(anyLong())).thenReturn(volumeMetadatasList);

        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
        when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
        when(storageStore.get(TEST_ARBITER_ID)).thenReturn(null);

        for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            InstanceId instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        when(storageStore.list()).thenReturn(instanceMetadataList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

        Domain domain = mock(Domain.class);
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        Set<Long> lostDataNode = new HashSet<>();
        lostDataNode.add(TEST_ARBITER_ID);
        lostDataNode.add(TEST_SECONDARY1_ID);
        lostDataNode.add(TEST_PRIMARY_ID);
        confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
        boolean caughtLackNodeException = false;
        ConfirmFixVolumeResponse fixVolumeRsp = null;
        try {
            fixVolumeRsp = icImpl.confirmFixVolume(confirmFixVolumeRequest);
        } catch (LackDatanodeException_Thrift e) {
            caughtLackNodeException = true;
        }
        assertFalse(caughtLackNodeException);

        assertNotNull(fixVolumeRsp);
        assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
        Map<SegId_Thrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp.getCreateSegmentUnits();
        assertEquals(createSegmentUnitList.size(), 1);
        for (Map.Entry<SegId_Thrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList.entrySet()) {
            List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
            assertEquals(createSegmentUnitInfoList.size(), 2);
            for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
                assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 1);
                for (Map.Entry<InstanceIdAndEndPoint_Thrift, SegmentMembership_Thrift> entry1 : createSegmentUnitInfo
                        .getSegmentMembershipMap().entrySet()) {
                    Set<Long> arbiterID = entry1.getValue().getArbiters();
                    if (entry1.getValue().getPrimary() == entry1.getKey().getInstanceId()) {
                        assertEquals(entry1.getValue().getEpoch(), 2);
                        assertEquals(entry1.getValue().getGeneration(), 0);
                        assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRole_Thrift.Primary);
                    } else if (arbiterID.contains(entry1.getKey().getInstanceId())) {
                        assertEquals(entry1.getValue().getEpoch(), 2);
                        assertEquals(entry1.getValue().getGeneration(), 0);
                        assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRole_Thrift.Arbiter);
                    } else {
                        fail();
                    }
                }
            }
        }
    }

    @Test
    public void testConfirmFixVolumeWithNotPAliveatPJA() throws TException, IOException, SQLException {
        ConfirmFixVolumeRequest_Thrift confirmFixVolumeRequest = new ConfirmFixVolumeRequest_Thrift();
        confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

        //Set<Long> datanodes = buildIdSet(5);
        InstanceMetadata datanode = mock(InstanceMetadata.class);

        VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
        assertNotNull(volumeMetadata);

        volumeMetadata.setVolumeSize(1);
        volumeMetadata.setSegmentSize(1);
        List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();

        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        List<Instance> instanceList = new ArrayList<>();

        Set<Long> storagePoolIdList = new HashSet<Long>();
        storagePoolIdList.add(volumeMetadata.getStoragePoolId());
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(volumeMetadata.getStoragePoolId());
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        storagePool.setArchivesInDataNode(archivesInDataNode);

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
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadataList.add(archiveMetadata);
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        for (Long i = 4L; i <= 5L; i++) {
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
            int groupID = new Long(i).intValue();
            Group group = new Group(groupID);
            Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.OK);
            assertNotNull(datanodeInstance);

            //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good Name");
            List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
            RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
            assertNotNull(archiveMetadata);

            archiveMetadata.setDeviceName("Good Name");
            archiveMetadata.setStatus(ArchiveStatus.GOOD);
            archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
            archiveMetadata.setLogicalFreeSpace(1L);
            archiveMetadata.setStorageType(StorageType.SATA);
            archiveMetadataList.add(archiveMetadata);

            dataNodeMetadata.setArchives(archiveMetadataList);
            dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
            dataNodeMetadata.setGroup(group);
            dataNodeMetadata.setDatanodeStatus(OK);
            dataNodeMetadata.setDatanodeType(NORMAL);
            archivesInDataNode.put(i, 0L);
            instanceMetadataList.add(dataNodeMetadata);
            instanceList.add(datanodeInstance);
        }

        SegmentMembership memship = volumeMetadata.getMembership(0);
        InstanceId id = new InstanceId(2L);
        SegmentMembership newMemship = memship.removeSecondary(id);

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

        when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(instanceMetadataList.get(1));
        when(storageStore.get(TEST_ARBITER_ID)).thenReturn(instanceMetadataList.get(2));
        InstanceId instanceId = new InstanceId(TEST_SECONDARY1_ID);
        when(instanceStore.get(instanceId)).thenReturn(instanceList.get(1));

        instanceId = new InstanceId(TEST_ARBITER_ID);
        when(instanceStore.get(instanceId)).thenReturn(instanceList.get(2));

        for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
            int listIndex = new Long(datanodeId).intValue() - 1;
            when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
            instanceId = new InstanceId(datanodeId);
            when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
        }

        when(storageStore.list()).thenReturn(instanceMetadataList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

        Domain domain = mock(Domain.class);
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
        confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

        Set<Long> lostDataNode = new HashSet<>();
        lostDataNode.add(TEST_PRIMARY_ID);
        confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
        boolean caughtLackNodeException = false;
        ConfirmFixVolumeResponse fixVolumeRsp = null;
        try {
            fixVolumeRsp = icImpl.confirmFixVolume(confirmFixVolumeRequest);
        } catch (LackDatanodeException_Thrift e) {
            caughtLackNodeException = true;
        }
        assertFalse(caughtLackNodeException);

        assertNotNull(fixVolumeRsp);
        assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
        Map<SegId_Thrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp.getCreateSegmentUnits();
        assertEquals(createSegmentUnitList.size(), 1);
        for (Map.Entry<SegId_Thrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList.entrySet()) {
            List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
            assertEquals(createSegmentUnitInfoList.size(), 1);
            for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
                assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 2);
                for (Map.Entry<InstanceIdAndEndPoint_Thrift, SegmentMembership_Thrift> entry1 : createSegmentUnitInfo
                        .getSegmentMembershipMap().entrySet()) {
                    Long primaryId = entry1.getValue().getPrimary();
                    if (primaryId.equals(TEST_PRIMARY_ID)) {
                        fail();
                    }
                    assertEquals(entry1.getValue().getEpoch(), 2);
                    assertEquals(entry1.getValue().getGeneration(), 0);
                }
                assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRole_Thrift.Primary);
            }
        }
    }
}
