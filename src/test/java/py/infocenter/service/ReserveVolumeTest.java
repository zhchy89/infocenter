package py.infocenter.service;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.icshare.InstanceMetadata.DatanodeType.NORMAL;
import static py.icshare.InstanceMetadata.DatanodeType.SIMPLE;

import java.io.*;
import java.util.*;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import py.archive.RawArchiveMetadata;
import py.archive.ArchiveStatus;
import py.archive.StorageType;
import py.common.RequestIdBuilder;
import py.common.client.RequestResponseHelper;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.icshare.AccountMetadata;
import py.icshare.AccountMetadata.AccountType;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePool;
import py.icshare.StoragePoolStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.rebalance.SegmentUnitsDistributionManager;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.rebalance.builder.SimulateInstanceBuilder;
import py.infocenter.rebalance.selector.SegmentUnitReserver;
import py.infocenter.rebalance.struct.SimpleDatanodeManager;
import py.infocenter.service.selection.RandomSelectionStrategy;
import py.infocenter.store.*;
import py.instance.Group;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.icshare.CreateVolumeRequest;
import py.thrift.infocenter.service.ReserveVolumeRequest;
import py.thrift.infocenter.service.ReserveVolumeResponse;
import py.thrift.share.*;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

/**
 * 
 * @author zjm
 * 
 */
public class ReserveVolumeTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(ReserveVolumeTest.class);

    @Mock
    private StorageStore storageStore;

    @Mock
    private VolumeStore volumeStore;

    @Mock
    private StoragePoolStore storagePoolStore;

    @Mock
    private DomainStore domainStore;

    private long segmentSize = 1L;

    private InformationCenterImpl icImpl;

    private RandomSelectionStrategy selectionStrategy = new RandomSelectionStrategy();

    private int arbiterGroupNumber = 0;

    private SegmentUnitsDistributionManager arbiterGroupSetSelector = new SegmentUnitsDistributionManagerImpl(
            segmentSize, volumeStore, storageStore, storagePoolStore);

    private SegmentUnitsDistributionManager arbiterGroupNotSetSelector = new SegmentUnitsDistributionManagerImpl(
            segmentSize, volumeStore, storageStore, storagePoolStore);



    @Before
    public void init() throws Exception {
        super.init();
        InfoCenterAppContext appContext = mock(InfoCenterAppContext.class);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        icImpl = new InformationCenterImpl();
        icImpl.setSelectionStrategy(selectionStrategy);
        icImpl.setStorageStore(storageStore);
        icImpl.setVolumeStore(volumeStore);
        icImpl.setSegmentSize(segmentSize);
        icImpl.setStoragePoolStore(storagePoolStore);
        icImpl.setAppContext(appContext);
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupSetSelector);
        icImpl.setSegmentWrappCount(10);
    }

    @Test
    public void testSucceedToReserveVolumeWithThreeInstance() throws Exception {
        AccountMetadata account = new AccountMetadata("test", "test", AccountType.Admin.name(), RequestIdBuilder.get());

        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(account.getAccountId());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(6 * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.REGULAR);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        for (long i = 0; i < volumeType.getNumMembers(); i++) {
            Group group = new Group();
            group.setGroupId((int) i);


            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i ));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 10; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                archive.setWeight(1);
                archives.add(archive);
                archivesInDataNode.put(i , Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            instanceList.add(instanceMetadata);
            domain.addDatanode(instanceMetadata.getInstanceId().getId());
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        ReserveVolumeResponse response = icImpl.reserveVolume(request);
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        Assert.assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        for (int segIndex : segIndex2Instances.keySet()) {
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);
            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            Assert.assertEquals(volumeType.getNumMembers(), arbiterInstanceList.size() + normalInstanceList.size());
            // TODO: verify different group
            Set<Long> instanceIdSet = new HashSet<>();
            for (long i = 0; i < volumeType.getNumMembers(); i++) {
                instanceIdSet.add(i );
            }

            for (InstanceIdAndEndPoint_Thrift instanceMetadata_Thrift : arbiterInstanceList) {
                Assert.assertTrue(instanceIdSet.contains(instanceMetadata_Thrift.getInstanceId()));
                instanceIdSet.remove(instanceMetadata_Thrift.getInstanceId());
            }
            for (InstanceIdAndEndPoint_Thrift instanceMetadata_Thrift : normalInstanceList) {
                Assert.assertTrue(instanceIdSet.contains(instanceMetadata_Thrift.getInstanceId()));
                instanceIdSet.remove(instanceMetadata_Thrift.getInstanceId());
            }
        }
    }

    @Test
    public void testReserveSmallVolumeWithThreeInstances() throws Exception {
        AccountMetadata account = new AccountMetadata("test", "test", AccountType.Admin.name(), RequestIdBuilder.get());

        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(account.getAccountId());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(5000 * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        for (long i = 0; i < volumeType.getNumMembers(); i++) {
            Group group = new Group();
            group.setGroupId((int)i);


            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i ));
            instanceMetadata.setGroup(group);
            instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
            instanceMetadata.setCapacity(5000 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 10; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(2500 * segmentSize);
                archive.setWeight(1);
                archives.add(archive);
                archivesInDataNode.put(i , Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            if (i == 0) {
                instanceMetadata.setDatanodeType(SIMPLE);
            } else {
                instanceMetadata.setDatanodeType(NORMAL);
            }
            arbiterGroupSetSelector.updateSimpleDatanodeInfo(instanceMetadata);

            instanceList.add(instanceMetadata);
            domain.addDatanode(instanceMetadata.getInstanceId().getId());
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        ReserveVolumeResponse response = icImpl.reserveVolume(request);
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        Assert.assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        for (int segIndex : segIndex2Instances.keySet()) {
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);
            assertEquals(1, arbiterInstanceList.size());
            InstanceId instanceId = new InstanceId(arbiterInstanceList.get(0).getInstanceId());
            Group group = instanceId2GroupMap.get(instanceId);
            assertEquals(arbiterGroupNumber, group.getGroupId());
            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            assertEquals(2, normalInstanceList.size());
            Assert.assertEquals(volumeType.getNumMembers(), arbiterInstanceList.size() + normalInstanceList.size());
            // TODO: verify different group
            Set<Long> instanceIdSet = new HashSet<>();
            for (long i = 0; i < volumeType.getNumMembers(); i++) {
                instanceIdSet.add(i );
            }

            for (InstanceIdAndEndPoint_Thrift instanceMetadata_Thrift : arbiterInstanceList) {
                Assert.assertTrue(instanceIdSet.contains(instanceMetadata_Thrift.getInstanceId()));
                instanceIdSet.remove(instanceMetadata_Thrift.getInstanceId());
            }
            for (InstanceIdAndEndPoint_Thrift instanceMetadata_Thrift : normalInstanceList) {
                Assert.assertTrue(instanceIdSet.contains(instanceMetadata_Thrift.getInstanceId()));
                instanceIdSet.remove(instanceMetadata_Thrift.getInstanceId());
            }
        }
    }

    /**
     * Test reserve volume with 9 groups each has 4 datanodes, arbiter group not set, expect arbiters are average distributed
     * among all the datanode.
     *
     * @throws Exception
     */
    @Test
    public void testReserveSmallVolumeWith36InstancesWithNoArbiterGroupSet() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupNotSetSelector);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000L;
        // allow distributed error ratio
        double sigma = 0.3;
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        long numberOfGroup = volumeType.getNumMembers() * 3;
        long datanodeCountInGroup = 4L;
        for (long i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId((int)i);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j) ));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(7500 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                for (int k = 0; k < 10; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(2500 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j) , Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());
            }
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        ReserveVolumeResponse response = icImpl.reserveVolume(request);
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();

        Set<Integer> arbiterAndNormalGroupIdSet = new HashSet<>();
        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterAndNormalGroupIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);
            assertEquals(2, arbiterInstanceList.size());
            int necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList){
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumArbiters()){
                    Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                    instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                    necessaryCount++;
                }

                arbiterAndNormalGroupIdSet.add(arbiterThrift.getGroupId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            assertEquals(4, normalInstanceList.size());
            necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumSecondaries() + 1){
                    Long normalCount = instanceId2NormalCount.get(normal);
                    instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);
                    necessaryCount++;
                }

                arbiterAndNormalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
            }

            //ensure all segment in different group
            assert(arbiterAndNormalGroupIdSet.size() == 6);
        }
        long totalArbiter = 0;
        long totalNormal = 0;
        long maxArbiterAllow = (long)((1+sigma) * (1 * numberOfSegment / (numberOfGroup * datanodeCountInGroup)));
        long minArbiterAllow = (long)((1-sigma) * (1 * numberOfSegment / (numberOfGroup * datanodeCountInGroup)));
        long maxNormalAllow = (long)((1+sigma) * (2 * numberOfSegment / (numberOfGroup * datanodeCountInGroup)));
        long minNormalAllow = (long)((1-sigma) * (2 * numberOfSegment / (numberOfGroup * datanodeCountInGroup)));
        logger.warn("{}", instanceId2ArbiterCount);
        logger.warn("arbiter allow range: {}--{}", minArbiterAllow, maxArbiterAllow);
        logger.warn("{}", instanceId2NormalCount);
        logger.warn("normal allow range: {}--{}", minNormalAllow, maxNormalAllow);
        for (Map.Entry<InstanceId, Long> instanceIdLongEntry : instanceId2ArbiterCount.entrySet()) {
            Long count = instanceIdLongEntry.getValue();
            assertNotNull(count);
            assertTrue(count < maxArbiterAllow);
            assertTrue(count > minArbiterAllow);
            totalArbiter += count;
        }
        for (Map.Entry<InstanceId, Long> instanceIdLongEntry : instanceId2NormalCount.entrySet()) {
            Long count = instanceIdLongEntry.getValue();
            assertNotNull(count);
            assertTrue(count < maxNormalAllow);
            assertTrue(count > minNormalAllow);
            totalNormal += count;
        }
        assertEquals(1 * numberOfSegment, totalArbiter);
        assertEquals(2 * numberOfSegment, totalNormal);
    }

    /**
     * Test reserve volume with 9 groups each has 4 datanodes, arbiter group set, expect arbiters are average distributed
     * among the datanodes in arbiter group, here is group 0.
     *
     * @throws Exception
     */
    @Test
    public void testReserveSmallVolumeWith36InstancesWithArbiterGroupSet() throws Exception {
        AccountMetadata account = new AccountMetadata("test", "test", AccountType.Admin.name(), RequestIdBuilder.get());

        //
        // numberOfSegment should not be too small
        long numberOfSegment = 5000L;
        // allow distributed error ratio
        double sigma = 0.2;
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(account.getAccountId());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        long numberOfGroup = volumeType.getNumMembers() * 3;
        long datanodeCountInGroup = 4L;

        Set<Long> srcSimpleDatanodeIdSet = new HashSet<>();
        for (long i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId((int)i);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j)));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(7500 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                for (int k = 0; k < 10; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(2500 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j), Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);

                if (i == 0 && j < 2) {
                    arbiterGroupNumber = (int) i;
                    instanceMetadata.setDatanodeType(SIMPLE);
                    srcSimpleDatanodeIdSet.add(instanceMetadata.getInstanceId().getId());
                } else {
                    instanceMetadata.setDatanodeType(NORMAL);
                }
                arbiterGroupSetSelector.updateSimpleDatanodeInfo(instanceMetadata);

                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());
            }
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        ReserveVolumeResponse response = icImpl.reserveVolume(request);
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();

        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        Set<Integer> allGroupIdSet = new HashSet<>();
        Set<Long> arbiterInstanceIdSet = new HashSet<>();


        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            normalGroupIdSet.clear();
            arbiterInstanceIdSet.clear();
            allGroupIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            //arbiter
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);
            assertEquals(2, arbiterInstanceList.size());
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : arbiterInstanceList){
                InstanceId instanceId = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                Long arbiterCount = instanceId2ArbiterCount.get(instanceId);
                instanceId2ArbiterCount.put(instanceId, null == arbiterCount ? 1 : arbiterCount + 1);

                arbiterGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
                arbiterInstanceIdSet.add(instanceIdAndEndPointThrift.getInstanceId());

                // arbiter segment unit priority selection was simple datanode to create
                if (instanceIdAndEndPointThrift.getGroupId() == arbiterGroupNumber){
                    assert(srcSimpleDatanodeIdSet.contains(instanceIdAndEndPointThrift.getInstanceId()));
                }
            }

            //normal
            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            assertEquals(4, normalInstanceList.size());
            int i = 0;
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                if (i < 2){
                    InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                    Long normalCount = instanceId2NormalCount.get(normal);
                    instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);
                }
                i++;

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
            }

            //all segment unit was created in different group
            allGroupIdSet.addAll(arbiterGroupIdSet);
            allGroupIdSet.addAll(normalGroupIdSet);

            assert(allGroupIdSet.size() == 6);
        }

//        long totalArbiter = 0;
        long totalNormal = 0;
        long maxArbiterAllow = (long)((1+sigma) * (numberOfSegment / datanodeCountInGroup));
        long minArbiterAllow = (long)((1-sigma) * (numberOfSegment / datanodeCountInGroup));
        long maxNormalAllow = (long)((1+sigma) * (2 * numberOfSegment / ((numberOfGroup - 1) * datanodeCountInGroup)));
        long minNormalAllow = (long)((1-sigma) * (2 * numberOfSegment / ((numberOfGroup - 1) * datanodeCountInGroup)));
        logger.warn("{}", instanceId2ArbiterCount);
        logger.warn("arbiter allow range: {}--{}", minArbiterAllow, maxArbiterAllow);
        logger.warn("{}", instanceId2NormalCount);
        logger.warn("normal allow range: {}--{}", minNormalAllow, maxNormalAllow);

//        for (Map.Entry<InstanceId, Long> instanceIdLongEntry : instanceId2ArbiterCount.entrySet()) {
//            InstanceId instanceId = instanceIdLongEntry.getKey();
//            assertTrue(arbiterGroupNumber == instanceId2GroupMap.get(instanceId).getGroupId());
//              if (instanceId2GroupMap.get(instanceId).getGroupId() != arbiterGroupNumber){
//                Long count = instanceIdLongEntry.getValue();
//                assertNotNull(count);
//                assertTrue(count < maxArbiterAllow);
//                assertTrue(count > minArbiterAllow);
//                totalArbiter += count;
//            }
//        }

        for (Map.Entry<InstanceId, Long> instanceIdLongEntry : instanceId2NormalCount.entrySet()) {
            InstanceId instanceId = instanceIdLongEntry.getKey();
            assertFalse(arbiterGroupNumber == instanceId2GroupMap.get(instanceId).getGroupId());
            Long count = instanceIdLongEntry.getValue();
            assertNotNull(count);
            assertTrue(count < maxNormalAllow);
            assertTrue(count > minNormalAllow);
            totalNormal += count;
        }
//        assertEquals(numberOfSegment, totalArbiter*2);
        assertEquals(2 * numberOfSegment, totalNormal);
    }

    @Test
    public void testReserveSmallVolumeWithThreeInstancesArbiterAverageDistributed() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupNotSetSelector);
        AccountMetadata account = new AccountMetadata("test", "test", AccountType.Admin.name(), RequestIdBuilder.get());

        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(account.getAccountId());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(5000 * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        for (long i = 0; i < volumeType.getNumMembers(); i++) {
            Group group = new Group();
            group.setGroupId((int)i);


            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i ));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(5000 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 10; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(2500 * segmentSize);
                archive.setWeight(1);
                archives.add(archive);
                archivesInDataNode.put(i , Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            instanceList.add(instanceMetadata);
            domain.addDatanode(instanceMetadata.getInstanceId().getId());
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        ReserveVolumeResponse response = icImpl.reserveVolume(request);
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        Assert.assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        Map<Long, Integer> arbiterInstance2Amount = new HashMap<>();
        arbiterInstance2Amount.put(0L , 0);
        arbiterInstance2Amount.put(1L , 0);
        arbiterInstance2Amount.put(2L , 0);
        for (int segIndex : segIndex2Instances.keySet()) {
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            for (InstanceIdAndEndPoint_Thrift instanceMetadata_Thrift : arbiterInstanceList) {
                Long id = instanceMetadata_Thrift.getInstanceId();
                arbiterInstance2Amount.put(id, arbiterInstance2Amount.get(id) + 1);
            }
        }
        assertTrue("number of arbiter in datanode 0 is " + arbiterInstance2Amount.get(0L) + ", is not distributed equally.",
                arbiterInstance2Amount.get(0L) < segIndex2Instances.size() / 2);
        assertTrue("number of arbiter in datanode 1 is " + arbiterInstance2Amount.get(1L ) + ", is not distributed equally.",
        arbiterInstance2Amount.get(1L ) < segIndex2Instances.size() / 2);
        assertTrue("number of arbiter in datanode 2 is " + arbiterInstance2Amount.get(2L ) + ", is not distributed equally.",
        arbiterInstance2Amount.get(2L ) < segIndex2Instances.size() / 2);
        assertEquals(segIndex2Instances.size(), arbiterInstance2Amount.get(0L) + arbiterInstance2Amount.get(1L ) +
                arbiterInstance2Amount.get(2L ));
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupSetSelector);
    }

    @Test
    public void testSucceedToReserveVolumeWithFiveInstance() throws Exception {
        AccountMetadata account = new AccountMetadata("test", "test", AccountType.Admin.name(), RequestIdBuilder.get());

        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(account.getAccountId());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(10 * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.REGULAR);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        for (long i = 0; i < 5; i++) {
            Group group = new Group();
            group.setGroupId((int)i);

            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i ));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);

            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 5; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(2 * segmentSize);
                archive.setWeight(1);
                archives.add(archive);
                archivesInDataNode.put(i , Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            domain.addDatanode(instanceMetadata.getInstanceId().getId());
            instanceList.add(instanceMetadata);
        }

        instanceList.get(0).setFreeSpace(segmentSize);

        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);
        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        ReserveVolumeResponse response = icImpl.reserveVolume(request);
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        Set<Long> instanceIdSet = new HashSet<>();
        Assert.assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        for (int segIndex : segIndex2Instances.keySet()) {
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);
            Assert.assertEquals(0, arbiterInstanceList.size());
            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            Assert.assertEquals(5, normalInstanceList.size());
            Assert.assertTrue(arbiterInstanceList.size() + normalInstanceList.size() >= volumeType.getNumMembers());
            for (InstanceIdAndEndPoint_Thrift instanceFromRemote : arbiterInstanceList) {
                instanceIdSet.add(instanceFromRemote.getInstanceId());
            }
            for (InstanceIdAndEndPoint_Thrift instanceFromRemote : normalInstanceList) {
                instanceIdSet.add(instanceFromRemote.getInstanceId());
            }
        }
        Assert.assertEquals(5, instanceIdSet.size());
    }

    @Test
    public void testReserveSmallVolumeWithFiveInstances() throws Exception {
        AccountMetadata account = new AccountMetadata("test", "test", AccountType.Admin.name(), RequestIdBuilder.get());

        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(account.getAccountId());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(6 * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        for (long i = 0; i < 5; i++) {
            Group group = new Group();
            group.setGroupId((int)i);


            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i ));
            instanceMetadata.setGroup(group);
            instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 10; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                archive.setWeight(1);
                archives.add(archive);
                archivesInDataNode.put(i , Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            if (i == 0) {
                instanceMetadata.setDatanodeType(SIMPLE);
            } else {
                instanceMetadata.setDatanodeType(NORMAL);
            }
            arbiterGroupSetSelector.updateSimpleDatanodeInfo(instanceMetadata);

            instanceList.add(instanceMetadata);
            domain.addDatanode(instanceMetadata.getInstanceId().getId());
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        ReserveVolumeResponse response = icImpl.reserveVolume(request);
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        Set<Long> instanceIdSet = new HashSet<>();
        Assert.assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        for (int segIndex : segIndex2Instances.keySet()) {
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);
            assertEquals(1, arbiterInstanceList.size());
            InstanceId instanceId = new InstanceId(arbiterInstanceList.get(0).getInstanceId());
            Group group = instanceId2GroupMap.get(instanceId);
            assertEquals(arbiterGroupNumber, group.getGroupId());
            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            assertEquals(4, normalInstanceList.size());
            Assert.assertTrue(arbiterInstanceList.size() + normalInstanceList.size() >= volumeType.getNumMembers());
            // TODO: verify different group
            for (InstanceIdAndEndPoint_Thrift instanceFromRemote : arbiterInstanceList) {
                instanceIdSet.add(instanceFromRemote.getInstanceId());
            }
            for (InstanceIdAndEndPoint_Thrift instanceFromRemote : normalInstanceList) {
                instanceIdSet.add(instanceFromRemote.getInstanceId());
            }

        }
        Assert.assertEquals(5, instanceIdSet.size());
    }

    @Test
    public void testFailedToReserveVolumeWithThreeInstances() throws Exception {
        AccountMetadata account = new AccountMetadata("test", "test", AccountType.Admin.name(), RequestIdBuilder.get());

        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(account.getAccountId());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(10 * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.REGULAR);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        for (int i = 0; i < volumeType.getNumMembers(); i++) {
            Group group = new Group();
            group.setGroupId(i);

            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < volumeType.getNumMembers(); k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                archive.setWeight(1);
                archives.add(archive);
                archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            domain.addDatanode(instanceMetadata.getInstanceId().getId());
            instanceList.add(instanceMetadata);
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());

        boolean cachedException = false;
        try {
            icImpl.reserveVolume(request);
        } catch (NotEnoughSpaceException_Thrift e) {
            cachedException = true;
        }
        Assert.assertTrue(cachedException);
    }

    @Test
    public void failedToReserveVolumeWithLittleGroups() throws Exception {
        AccountMetadata account = new AccountMetadata("test", "test", AccountType.Admin.name(), RequestIdBuilder.get());

        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(account.getAccountId());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(10 * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.REGULAR);
        request.setTags(new ArrayList<>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        for (int i = 0; i < volumeType.getNumMembers(); i++) {
            Group group = new Group();
            group.setGroupId(i);

            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 5; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(2 * segmentSize);
                archive.setWeight(1);
                archives.add(archive);
                archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            domain.addDatanode(instanceMetadata.getInstanceId().getId());
            instanceList.add(instanceMetadata);
        }

        // remove group 0, add the instance to group 1
        instanceList.get(0).getGroup().setGroupId(1);

        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);
        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        boolean exceptionCached = false;
        try {
            icImpl.reserveVolume(request);
        } catch (NotEnoughGroupException_Thrift e) {
            exceptionCached = true;
        }
        Assert.assertTrue(exceptionCached);
    }

    @Ignore
    @Test//(expected = ArbiterGroupNotFoundException_Thrift.class)
    public void failedToReserveVolumeWithNoDatanodeForArbiterOnly() throws Exception {
        AccountMetadata account = new AccountMetadata("test", "test", AccountType.Admin.name(), RequestIdBuilder.get());

        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(account.getAccountId());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(6 * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        for (int i = 1; i <= volumeType.getNumMembers(); i++) {
            Group group = new Group();
            group.setGroupId(i);


            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 10; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                archive.setWeight(1);
                archives.add(archive);
                archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            instanceList.add(instanceMetadata);
            domain.addDatanode(instanceMetadata.getInstanceId().getId());
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        icImpl.reserveVolume(request);
    }

    @Ignore
    @Test//(expected = ArbiterGroupNotFoundException_Thrift.class)
    public void failedToReserveVolumeWithDatanodeNotInStoragePool() throws Exception {
        AccountMetadata account = new AccountMetadata("test", "test", AccountType.Admin.name(), RequestIdBuilder.get());

        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(account.getAccountId());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(6 * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();

        Group groupNotInStoragePool = new Group();
        groupNotInStoragePool.setGroupId(0);
        InstanceMetadata instanceNotInStoragePool = new InstanceMetadata(new InstanceId(0));
        instanceNotInStoragePool.setGroup(groupNotInStoragePool);
        instanceNotInStoragePool.setDatanodeStatus(OK);
        instanceList.add(instanceNotInStoragePool);

        for (int i = 1; i <= volumeType.getNumMembers(); i++) {
            Group group = new Group();
            group.setGroupId(i);

            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 10; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                archive.setWeight(1);
                archives.add(archive);
                archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            instanceList.add(instanceMetadata);
            domain.addDatanode(instanceMetadata.getInstanceId().getId());
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        icImpl.reserveVolume(request);
    }

    public class MapTester {
        private int tester;

        public MapTester(int tester) {
            this.tester = tester;
        }

        public int getTester() {
            return tester;
        }

        public void setTester(int tester) {
            this.tester = tester;
        }
    }

    @Test
    public void testMap() {
        Map<Integer, MapTester> test = new HashMap<>();
        test.put(1, new MapTester(1));
        test.put(2, new MapTester(2));
        test.put(3, new MapTester(3));
        test.put(4, new MapTester(4));

        test.get(1).setTester(2);
        Assert.assertEquals(2, test.get(1).getTester());
    }

    @Test
    public void testIntegerToDouble() {
        int n = 1;
        int m = 4;
        double x = (double) n / m;
        double y = n / m;
        Assert.assertEquals(0.25, x);
        Assert.assertEquals(0.0, y);
    }

    @Test
    public void testReservVolume_PSA_6Group_2ArbiterGroup() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName("testReservVolume");
        request.setAccountId(123456);

        long volumeSize = 2000;
        request.setVolumeSize(volumeSize);
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setCacheType(CacheType_Thrift.MEMORY);
        request.setRequestType("CREATE_VOLUME");
        request.setVolumeId(1);
        request.setRootVolumeId(1);     //set current volume to be rootvolume

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        long segmentSize = 1;
        SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
                new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
                        storageStore, storagePoolStore);

        List<InstanceMetadata> ls = new ArrayList<>();

        Multimap<Long, Long> multiMap = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Long> simpleDatanodeInstanceId = new HashSet<>();
        Long domainId = RequestIdBuilder.get();
        int arbiterGroupIndex = 0;
        int datanodeCount = 12;
        for (int i = 0; i < datanodeCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
            instanceMetadata.setDomainId(domainId);
            instanceMetadata.setFreeSpace(10*datanodeCount);
            instanceMetadata.setCapacity(20*datanodeCount);
            instanceMetadata.setDatanodeStatus(OK);
            Group group = new Group((i%6)==0?6*10:(i%6)*10);
            instanceMetadata.setGroup(group);

            List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
            for (int archiveIndex = 0; archiveIndex < 1; archiveIndex++){
                RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                rawArchiveMetadata.setInstanceId(instanceId);
                rawArchiveMetadata.setArchiveId((long)archiveIndex);
                rawArchiveMetadata.setLogicalFreeSpace(volumeSize);
//                if (i == 1){
//                    rawArchiveMetadata.setLogicalFreeSpace(2);
//                }
                rawArchiveMetadata.setFreeFlexibleSegmentUnitCount(0);
                rawArchiveMetadata.setWeight(1);
                rawArchiveMetadataList.add(rawArchiveMetadata);
                multiMap.put(instanceId.getId(), (long)archiveIndex);
            }
            instanceMetadata.setArchives(rawArchiveMetadataList);

            if ((arbiterGroupIndex < 2) &&
                    ((i%6) == 2 || (i%6) == 4)){
                instanceMetadata.setDatanodeType(SIMPLE);
                simpleDatanodeInstanceId.add(instanceMetadata.getInstanceId().getId());
                arbiterGroupIdSet.add(group.getGroupId());
                arbiterGroupIndex++;
            } else {
                instanceMetadata.setDatanodeType(NORMAL);
            }
            ls.add(instanceMetadata);
            when(storageStore.get(i)).thenReturn(instanceMetadata);

            segmentUnitsDistributionManager.updateSimpleDatanodeInfo(instanceMetadata);
        }

        when(storageStore.list()).thenReturn(ls);

        icImpl.setStorageStore(storageStore);
        icImpl.setActualFreeSpace(800);

        Long storagePoolId = RequestIdBuilder.get();
        Set<Long> storagePoolIds = new HashSet<>();
        storagePoolIds.add(storagePoolId);
        request.setDomainId(domainId);
        request.setStoragePoolId(storagePoolId);

        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);
        when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);
        when(domain.isDeleting()).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(domain.getStoragePools()).thenReturn(storagePoolIds);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(storagePool.getArchivesInDataNode()).thenReturn(multiMap);

        icImpl.setVolumeStore(volumeStore);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> reservRet =
                segmentUnitsDistributionManager.reserveVolume(request.getVolumeSize(), RequestResponseHelper.convertVolumeType(request.getVolumeType()),
                        request.isSimpleConfiguration(), icImpl.getSegmentWrappCount(), request.getStoragePoolId());

        //verify result
        assert(reservRet.size() == volumeSize/segmentSize);
        for (int i = 0; i < reservRet.size(); i++){
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> segment = reservRet.get(i);

            // is segment count right?
            assert(segment.size() == 2);

            // is normal segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Normal).size() == 4);

            // is arbiter segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Arbiter).size() == 2);

            Set<Integer> normalSegmentUnitGroupSet = new HashSet<>();
            Set<Integer> arbiterSegmentUnitGroupSet = new HashSet<>();
            Set<Integer> arbiterInSimpleGroupSet = new HashSet<>();
            Set<Long> normalSegmentUnitInstanceSet = new HashSet<>();
            Set<Long> arbiterSegmentUnitInstanceSet = new HashSet<>();
            Set<Long> arbiterInSimpleInstanceSet = new HashSet<>();

            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Normal)) {
                normalSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                normalSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Arbiter)) {
                if (arbiterInSimpleGroupSet.size() < volumeType.getNumArbiters()){
                    arbiterInSimpleGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                }
                if (arbiterInSimpleInstanceSet.size() < volumeType.getNumArbiters()){
                    arbiterInSimpleInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
                }

                arbiterSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                arbiterSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }

            Set<Integer> retainSegmentUnitGroupSet = new HashSet<>();

//            // is normal segment unit not to be created in arbiterGroup?
//            retainSegmentUnitGroupSet.retainAll(normalSegmentUnitGroupSet);
//            assert(retainSegmentUnitGroupSet.size() == 0);

            // is normal segment unit not to be created at simple datanode?
            Set<Long> retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(normalSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == 0);

            // is all segment units's groupId different?
            Set<Integer> allSegmentUnitGroupSet = new HashSet<>();
            allSegmentUnitGroupSet.addAll(normalSegmentUnitGroupSet);
            allSegmentUnitGroupSet.addAll(arbiterSegmentUnitGroupSet);
            assert(allSegmentUnitGroupSet.size() == 6);

            // is arbiter segment unit selected arbiterGroup first to be created
            retainSegmentUnitGroupSet.clear();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(arbiterInSimpleGroupSet);
            assert(retainSegmentUnitGroupSet.size() == Math.min(arbiterGroupIdSet.size(),
                    arbiterInSimpleGroupSet.size()));

            // is arbiter segment unit selected simple datanode first to be created
            retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(arbiterInSimpleInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == Math.min(simpleDatanodeInstanceId.size(),
                    arbiterInSimpleInstanceSet.size()));
        }

        writeResult2File(volumeType, reservRet, datanodeCount);

        verifyResult(volumeType, reservRet, 5, 5, 5, 5);
    }

    @Test
    public void testReservVolume_PSA_6Group_2ArbiterGroupInDiffDomain() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName("testReservVolume");
        request.setAccountId(123456);

        long volumeSize = 500;
        request.setVolumeSize(volumeSize);
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setCacheType(CacheType_Thrift.MEMORY);
        request.setRequestType("CREATE_VOLUME");
        request.setVolumeId(1);
        request.setRootVolumeId(1);     //set current volume to be rootvolume

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
                new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
                        storageStore, storagePoolStore);

        List<InstanceMetadata> ls = new ArrayList<>();

        Multimap<Long, Long> multiMap = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Long> simpleDatanodeInstanceId = new HashSet<>();
        Long domainId = RequestIdBuilder.get();
        int arbiterGroupIndex = 0;
        int datanodeCount = 6;
        for (int i = 0; i < datanodeCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
            instanceMetadata.setDomainId(domainId);
            instanceMetadata.setFreeSpace(800);
            instanceMetadata.setCapacity(200);
            instanceMetadata.setDatanodeStatus(OK);
            Group group = new Group((i%6)==0?6*10:(i%6)*10);
            instanceMetadata.setGroup(group);

            List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
            RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
            rawArchiveMetadata.setArchiveId((long)i);
            rawArchiveMetadata.setLogicalFreeSpace(volumeSize);
            rawArchiveMetadata.setFreeFlexibleSegmentUnitCount(0);
            rawArchiveMetadata.setWeight(1);
            rawArchiveMetadataList.add(rawArchiveMetadata);
            instanceMetadata.setArchives(rawArchiveMetadataList);

            boolean isCurDomainInstance = true;
            if ((arbiterGroupIndex < 2) &&
                    ((i%6) == 2 || (i%6) == 4)){
                instanceMetadata.setDatanodeType(SIMPLE);
                simpleDatanodeInstanceId.add(instanceMetadata.getInstanceId().getId());
                arbiterGroupIdSet.add(group.getGroupId());

                if (arbiterGroupIndex == 0){
                    instanceMetadata.setDomainId(domainId + 1);
                }

                isCurDomainInstance = false;
                arbiterGroupIndex++;
            } else {
                instanceMetadata.setDatanodeType(NORMAL);
            }

            if (isCurDomainInstance){
                multiMap.put((long)i, (long)i);
            }
            ls.add(instanceMetadata);
            when(storageStore.get(i)).thenReturn(instanceMetadata);

            segmentUnitsDistributionManager.updateSimpleDatanodeInfo(instanceMetadata);
        }

        when(storageStore.list()).thenReturn(ls);

        icImpl.setStorageStore(storageStore);
        icImpl.setActualFreeSpace(800);

        Long storagePoolId = RequestIdBuilder.get();
        Set<Long> storagePoolIds = new HashSet<>();
        storagePoolIds.add(storagePoolId);
        request.setDomainId(domainId);
        request.setStoragePoolId(storagePoolId);

        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);
        when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);
        when(domain.isDeleting()).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(domain.getStoragePools()).thenReturn(storagePoolIds);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(storagePool.getArchivesInDataNode()).thenReturn(multiMap);

        icImpl.setVolumeStore(volumeStore);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> reservRet =
                segmentUnitsDistributionManager.reserveVolume(request.getVolumeSize(), RequestResponseHelper.convertVolumeType(request.getVolumeType()),
                        request.isSimpleConfiguration(), icImpl.getSegmentWrappCount(), request.getStoragePoolId());

        //verify result
        assert(reservRet.size() == volumeSize/segmentSize);
        for (int i = 0; i < reservRet.size(); i++){
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> segment = reservRet.get(i);

            // is segment count right?
            assert(segment.size() == 2);

            // is normal segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Normal).size() == 4);

            // is arbiter segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Arbiter).size() == 1);

            Set<Integer> normalSegmentUnitGroupSet = new HashSet<>();
            Set<Integer> arbiterSegmentUnitGroupSet = new HashSet<>();
            Set<Long> normalSegmentUnitInstanceSet = new HashSet<>();
            Set<Long> arbiterSegmentUnitInstanceSet = new HashSet<>();
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Normal)) {
                normalSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                normalSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Arbiter)) {
                arbiterSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                arbiterSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }

            // is normal segment unit not to be created in arbiterGroup?
            Set<Integer> retainSegmentUnitGroupSet = new HashSet<>();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(normalSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == 0);

            // is normal segment unit not to be created at simple datanode?
            Set<Long> retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(normalSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == 0);

            // is all segment units's groupId different?
            Set<Integer> allSegmentUnitGroupSet = new HashSet<>();
            allSegmentUnitGroupSet.addAll(normalSegmentUnitGroupSet);
            allSegmentUnitGroupSet.addAll(arbiterSegmentUnitGroupSet);
            assert(allSegmentUnitGroupSet.size() == 5);

            // is arbiter segment unit selected arbiterGroup first to be created
            retainSegmentUnitGroupSet.clear();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(arbiterSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == Math.min(arbiterGroupIdSet.size(),
                    arbiterSegmentUnitGroupSet.size()));

            // is arbiter segment unit selected simple datanode first to be created
            retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(arbiterSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == Math.min(simpleDatanodeInstanceId.size(),
                    arbiterSegmentUnitInstanceSet.size()));
        }

        writeResult2File(volumeType, reservRet, datanodeCount);

        verifyResult(volumeType, reservRet, 5, 5, 5, 5);

    }

    @Test
    public void testReservVolume_PSA_6Group_14Datanode_2ArbiterGroupInDiffDomain() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName("testReservVolume");
        request.setAccountId(123456);

        long volumeSize = 2000;
        request.setVolumeSize(volumeSize);
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setCacheType(CacheType_Thrift.MEMORY);
        request.setRequestType("CREATE_VOLUME");
        request.setVolumeId(1);
        request.setRootVolumeId(1);     //set current volume to be rootvolume

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
                new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
                        storageStore, storagePoolStore);

        List<InstanceMetadata> ls = new ArrayList<>();

        Multimap<Long, Long> multiMap = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Long> simpleDatanodeInstanceId = new HashSet<>();
        Set<Long> allSimpleDatanodeInstanceId = new HashSet<>();
        Long domainId = RequestIdBuilder.get();
        int arbiterGroupIndex = 0;
        int datanodeCount = 14;
        Set<Long> cannotCreateInstanceIdSet = new HashSet<>();
        for (int i = 0; i < datanodeCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
            instanceMetadata.setDomainId(domainId);
            instanceMetadata.setFreeSpace(800);
            instanceMetadata.setCapacity(200);
            instanceMetadata.setDatanodeStatus(OK);
            Group group = new Group((i%6)==0?6*10:(i%6)*10);
            instanceMetadata.setGroup(group);

            List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
            RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
            rawArchiveMetadata.setArchiveId((long)i);
            rawArchiveMetadata.setLogicalFreeSpace(volumeSize);
            rawArchiveMetadata.setFreeFlexibleSegmentUnitCount(0);
            rawArchiveMetadata.setWeight(1);
            rawArchiveMetadataList.add(rawArchiveMetadata);
            instanceMetadata.setArchives(rawArchiveMetadataList);

            boolean isCurDomainInstance = true;
            if ((arbiterGroupIndex < 2) &&
                    ((i%6) == 2 || (i%6) == 4)){
                instanceMetadata.setDatanodeType(SIMPLE);
                allSimpleDatanodeInstanceId.add(instanceMetadata.getInstanceId().getId());

                if (arbiterGroupIndex == 0){
                    instanceMetadata.setDomainId(domainId + 1);
                    cannotCreateInstanceIdSet.add(instanceMetadata.getInstanceId().getId());
                } else {
                    arbiterGroupIdSet.add(group.getGroupId());
                    simpleDatanodeInstanceId.add(instanceMetadata.getInstanceId().getId());
                }

                isCurDomainInstance = false;
                arbiterGroupIndex++;
            } else {
                instanceMetadata.setDatanodeType(NORMAL);
            }

            if (isCurDomainInstance){
                multiMap.put((long)i, (long)i);
            }
            ls.add(instanceMetadata);
            when(storageStore.get(i)).thenReturn(instanceMetadata);

            segmentUnitsDistributionManager.updateSimpleDatanodeInfo(instanceMetadata);
        }

        when(storageStore.list()).thenReturn(ls);

        icImpl.setStorageStore(storageStore);
        icImpl.setActualFreeSpace(800);

        Long storagePoolId = RequestIdBuilder.get();
        Set<Long> storagePoolIds = new HashSet<>();
        storagePoolIds.add(storagePoolId);
        request.setDomainId(domainId);
        request.setStoragePoolId(storagePoolId);

        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);
        when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);
        when(domain.isDeleting()).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(domain.getStoragePools()).thenReturn(storagePoolIds);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(storagePool.getArchivesInDataNode()).thenReturn(multiMap);

        icImpl.setVolumeStore(volumeStore);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> reservRet =
                segmentUnitsDistributionManager.reserveVolume(request.getVolumeSize(), RequestResponseHelper.convertVolumeType(request.getVolumeType()),
                        request.isSimpleConfiguration(), icImpl.getSegmentWrappCount(), request.getStoragePoolId());

        //verify result
        assert(reservRet.size() == volumeSize/segmentSize);
        for (int i = 0; i < reservRet.size(); i++){
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> segment = reservRet.get(i);

            // is segment count right?
            assert(segment.size() == 2);

            // is normal segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Normal).size() == 4);

            // is arbiter segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Arbiter).size() == 2);

            Set<Integer> normalSegmentUnitGroupSet = new HashSet<>();
            Set<Integer> arbiterSegmentUnitGroupSet = new HashSet<>();
            Set<Long> normalSegmentUnitInstanceSet = new HashSet<>();
            Set<Long> arbiterSegmentUnitInstanceSet = new HashSet<>();
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Normal)) {
                normalSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                normalSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
                assert(!cannotCreateInstanceIdSet.contains(instanceIdAndEndPoint_thrift.getInstanceId()));
            }
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Arbiter)) {
                arbiterSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                arbiterSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
                assert(!cannotCreateInstanceIdSet.contains(instanceIdAndEndPoint_thrift.getInstanceId()));
            }

            // is normal segment unit not to be created in arbiterGroup?
            Set<Integer> retainSegmentUnitGroupSet = new HashSet<>();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(normalSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == 0);

            // is normal segment unit not to be created at simple datanode?
            Set<Long> retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(allSimpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(normalSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == 0);

            // is all segment units's groupId different?
            Set<Integer> allSegmentUnitGroupSet = new HashSet<>();
            allSegmentUnitGroupSet.addAll(normalSegmentUnitGroupSet);
            allSegmentUnitGroupSet.addAll(arbiterSegmentUnitGroupSet);
            assert(allSegmentUnitGroupSet.size() == 6);

            // is arbiter segment unit selected arbiterGroup first to be created
            retainSegmentUnitGroupSet.clear();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(arbiterSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == Math.min(arbiterGroupIdSet.size(),
                    arbiterSegmentUnitGroupSet.size()));

            // is arbiter segment unit selected simple datanode first to be created
            retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(arbiterSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == Math.min(simpleDatanodeInstanceId.size(),
                    arbiterSegmentUnitInstanceSet.size()));
        }

        writeResult2File(volumeType, reservRet, datanodeCount);

        verifyResult(volumeType, reservRet, 5, 80, 0, 5);

    }

    @Test
    public void testReservVolume_PSA_4Group_1ArbiterGroup_4Datanode() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName("testReservVolume");
        request.setAccountId(123456);

        long volumeSize = 20000;
        request.setVolumeSize(volumeSize);
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setCacheType(CacheType_Thrift.MEMORY);
        request.setRequestType("CREATE_VOLUME");
        request.setVolumeId(1);
        request.setRootVolumeId(1);     //set current volume to be rootvolume

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
                new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
                        storageStore, storagePoolStore);

        List<InstanceMetadata> ls = new ArrayList<>();

        Long domainId = RequestIdBuilder.get();
        Multimap<Long, Long> multiMap = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Long> simpleDatanodeInstanceId = new HashSet<>();
        int arbiterGroupIndex = 0;
        int datanodeCount = 4;
        for (int i = 0; i < datanodeCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
            instanceMetadata.setDomainId(domainId);
            instanceMetadata.setFreeSpace(800);
            instanceMetadata.setCapacity(200);
            instanceMetadata.setDatanodeStatus(OK);
            Group group = new Group((i%6)==0?6*10:(i%6)*10);
            instanceMetadata.setGroup(group);

            List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
            RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
            rawArchiveMetadata.setArchiveId((long)i);
            rawArchiveMetadata.setLogicalFreeSpace(volumeSize);
            rawArchiveMetadata.setFreeFlexibleSegmentUnitCount(0);
            rawArchiveMetadata.setWeight(1);
            rawArchiveMetadataList.add(rawArchiveMetadata);
            instanceMetadata.setArchives(rawArchiveMetadataList);

            if ((arbiterGroupIndex < 1) &&
                    ((i%6) == 2)){
                instanceMetadata.setDatanodeType(SIMPLE);
                simpleDatanodeInstanceId.add(instanceMetadata.getInstanceId().getId());
                arbiterGroupIdSet.add(group.getGroupId());
                arbiterGroupIndex++;
            } else {
                instanceMetadata.setDatanodeType(NORMAL);
            }
            multiMap.put((long)i, (long)i);
            ls.add(instanceMetadata);
            when(storageStore.get(i)).thenReturn(instanceMetadata);

            segmentUnitsDistributionManager.updateSimpleDatanodeInfo(instanceMetadata);
        }

        when(storageStore.list()).thenReturn(ls);

        icImpl.setStorageStore(storageStore);
        icImpl.setActualFreeSpace(800);

        Long storagePoolId = RequestIdBuilder.get();
        Set<Long> storagePoolIds = new HashSet<>();
        storagePoolIds.add(storagePoolId);
        request.setDomainId(domainId);
        request.setStoragePoolId(storagePoolId);

        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);
        when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);
        when(domain.isDeleting()).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(domain.getStoragePools()).thenReturn(storagePoolIds);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(storagePool.getArchivesInDataNode()).thenReturn(multiMap);

        icImpl.setVolumeStore(volumeStore);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> reservRet =
                segmentUnitsDistributionManager.reserveVolume(request.getVolumeSize(), RequestResponseHelper.convertVolumeType(request.getVolumeType()),
                        request.isSimpleConfiguration(), icImpl.getSegmentWrappCount(), request.getStoragePoolId());

        //verify result
        assert(reservRet.size() == volumeSize/segmentSize);
        for (int i = 0; i < reservRet.size(); i++){
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> segment = reservRet.get(i);

            // is segment count right?
            assert(segment.size() == 2);

            // is normal segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Normal).size() == 3);

            // is arbiter segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Arbiter).size() == 1);

            Set<Integer> normalSegmentUnitGroupSet = new HashSet<>();
            Set<Integer> arbiterSegmentUnitGroupSet = new HashSet<>();
            Set<Long> normalSegmentUnitInstanceSet = new HashSet<>();
            Set<Long> arbiterSegmentUnitInstanceSet = new HashSet<>();
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Normal)) {
                normalSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                normalSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Arbiter)) {
                arbiterSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                arbiterSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }

            // is normal segment unit not to be created in arbiterGroup?
            Set<Integer> retainSegmentUnitGroupSet = new HashSet<>();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(normalSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == 0);

            // is normal segment unit not to be created at simple datanode?
            Set<Long> retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(normalSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == 0);

            // is all segment units's groupId different?
            Set<Integer> allSegmentUnitGroupSet = new HashSet<>();
            allSegmentUnitGroupSet.addAll(normalSegmentUnitGroupSet);
            allSegmentUnitGroupSet.addAll(arbiterSegmentUnitGroupSet);
            assert(allSegmentUnitGroupSet.size() == 4);

            // is arbiter segment unit selected arbiterGroup first to be created
            retainSegmentUnitGroupSet.clear();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(arbiterSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == Math.min(arbiterGroupIdSet.size(),
                    arbiterSegmentUnitGroupSet.size()));

            // is arbiter segment unit selected simple datanode first to be created
            retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(arbiterSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == Math.min(simpleDatanodeInstanceId.size(),
                    arbiterSegmentUnitInstanceSet.size()));
        }

        writeResult2File(volumeType, reservRet, datanodeCount);

        verifyResult(volumeType, reservRet, 5, 5, 0, 5);
    }

    @Test
    public void testReservVolume_PSA_4Group_1ArbiterGroup_8Datanode() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName("testReservVolume");
        request.setAccountId(123456);

        long volumeSize = 2000;
        request.setVolumeSize(volumeSize);
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setCacheType(CacheType_Thrift.MEMORY);
        request.setRequestType("CREATE_VOLUME");
        request.setVolumeId(1);
        request.setRootVolumeId(1);     //set current volume to be rootvolume

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        int groupCount = 4;
        SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
                new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
                        storageStore, storagePoolStore);

        List<InstanceMetadata> ls = new ArrayList<>();

        Long domainId = RequestIdBuilder.get();
        Multimap<Long, Long> multiMap = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Long> simpleDatanodeInstanceId = new HashSet<>();
        int arbiterGroupIndex = 0;
        int datanodeCount = 8;
        for (int i = 0; i < datanodeCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
            instanceMetadata.setDomainId(domainId);
            instanceMetadata.setFreeSpace(800);
            instanceMetadata.setCapacity(200);
            instanceMetadata.setDatanodeStatus(OK);
            Group group = new Group((i%groupCount)==0?groupCount*10:(i%groupCount)*10);
            instanceMetadata.setGroup(group);

            List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
            RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
            rawArchiveMetadata.setArchiveId((long)i);
            rawArchiveMetadata.setLogicalFreeSpace(volumeSize);
            rawArchiveMetadata.setFreeFlexibleSegmentUnitCount(0);
            rawArchiveMetadata.setWeight(1);
            rawArchiveMetadataList.add(rawArchiveMetadata);
            instanceMetadata.setArchives(rawArchiveMetadataList);

            if ((arbiterGroupIndex < 1) &&
                    ((i%groupCount) == 2)){
                instanceMetadata.setDatanodeType(SIMPLE);
                simpleDatanodeInstanceId.add(instanceMetadata.getInstanceId().getId());
                arbiterGroupIdSet.add(group.getGroupId());
                arbiterGroupIndex++;
            } else {
                instanceMetadata.setDatanodeType(NORMAL);
            }
            multiMap.put((long)i, (long)i);
            ls.add(instanceMetadata);
            when(storageStore.get(i)).thenReturn(instanceMetadata);

            segmentUnitsDistributionManager.updateSimpleDatanodeInfo(instanceMetadata);
        }

        when(storageStore.list()).thenReturn(ls);

        icImpl.setStorageStore(storageStore);
        icImpl.setActualFreeSpace(800);

        Long storagePoolId = RequestIdBuilder.get();
        Set<Long> storagePoolIds = new HashSet<>();
        storagePoolIds.add(storagePoolId);
        request.setDomainId(domainId);
        request.setStoragePoolId(storagePoolId);

        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);
        when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);
        when(domain.isDeleting()).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(domain.getStoragePools()).thenReturn(storagePoolIds);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(storagePool.getArchivesInDataNode()).thenReturn(multiMap);

        icImpl.setVolumeStore(volumeStore);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> reservRet =
                segmentUnitsDistributionManager.reserveVolume(request.getVolumeSize(), RequestResponseHelper.convertVolumeType(request.getVolumeType()),
                        request.isSimpleConfiguration(), icImpl.getSegmentWrappCount(), request.getStoragePoolId());

        //verify result
        assert(reservRet.size() == volumeSize/segmentSize);
        for (int i = 0; i < reservRet.size(); i++){
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> segment = reservRet.get(i);

            // is segment count right?
            assert(segment.size() == 2);

            // is normal segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Normal).size() == 3);

            // is arbiter segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Arbiter).size() == 1);

            Set<Integer> normalSegmentUnitGroupSet = new HashSet<>();
            Set<Integer> arbiterSegmentUnitGroupSet = new HashSet<>();
            Set<Long> normalSegmentUnitInstanceSet = new HashSet<>();
            Set<Long> arbiterSegmentUnitInstanceSet = new HashSet<>();
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Normal)) {
                normalSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                normalSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Arbiter)) {
                arbiterSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                arbiterSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }

            // is normal segment unit not to be created in arbiterGroup?
            Set<Integer> retainSegmentUnitGroupSet = new HashSet<>();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(normalSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == 0);

            // is normal segment unit not to be created at simple datanode?
            Set<Long> retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(normalSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == 0);

            // is all segment units's groupId different?
            Set<Integer> allSegmentUnitGroupSet = new HashSet<>();
            allSegmentUnitGroupSet.addAll(normalSegmentUnitGroupSet);
            allSegmentUnitGroupSet.addAll(arbiterSegmentUnitGroupSet);
            assert(allSegmentUnitGroupSet.size() == 4);

            // is arbiter segment unit selected arbiterGroup first to be created
            retainSegmentUnitGroupSet.clear();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(arbiterSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == Math.min(arbiterGroupIdSet.size(),
                    arbiterSegmentUnitGroupSet.size()));

            // is arbiter segment unit selected simple datanode first to be created
            retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(arbiterSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == Math.min(simpleDatanodeInstanceId.size(),
                    arbiterSegmentUnitInstanceSet.size()));
        }

        writeResult2File(volumeType, reservRet, datanodeCount);

        verifyResult(volumeType, reservRet, 5, 5, 0, 5);

    }

    @Test(expected = NotEnoughNormalGroupException_Thrift.class)
    public void testReservVolume_PSA_3Group_3Datanode_2ArbiterGroup() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName("testReservVolume");
        request.setAccountId(123456);

        long volumeSize = 2000;
        request.setVolumeSize(volumeSize);
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setCacheType(CacheType_Thrift.MEMORY);
        request.setRequestType("CREATE_VOLUME");
        request.setVolumeId(1);
        request.setRootVolumeId(1);     //set current volume to be rootvolume

        long segmentSize = 100;
        SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
                new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
                        storageStore, storagePoolStore);

        List<InstanceMetadata> ls = new ArrayList<>();

        Long domainId = RequestIdBuilder.get();
        Multimap<Long, Long> multiMap = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Long> simpleDatanodeInstanceId = new HashSet<>();
        int arbiterGroupIndex = 0;
        for (int i = 1; i <= 3; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
            instanceMetadata.setDomainId(domainId);
            instanceMetadata.setFreeSpace(800);
            instanceMetadata.setCapacity(volumeSize);
            instanceMetadata.setDatanodeStatus(OK);
            Group group = new Group((i%6)==0?6*10:(i%6)*10);
            instanceMetadata.setGroup(group);

            List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
            RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
            rawArchiveMetadata.setArchiveId((long) i);
            rawArchiveMetadata.setLogicalFreeSpace(volumeSize);
            rawArchiveMetadata.setFreeFlexibleSegmentUnitCount(0);
            rawArchiveMetadata.setWeight(1);
            rawArchiveMetadataList.add(rawArchiveMetadata);
            instanceMetadata.setArchives(rawArchiveMetadataList);

            if ((arbiterGroupIndex < 2) &&
                    ((i%6) == 2 || (i%6) == 3)){
                instanceMetadata.setDatanodeType(SIMPLE);
                arbiterGroupIdSet.add(group.getGroupId());
                simpleDatanodeInstanceId.add(instanceMetadata.getInstanceId().getId());
                arbiterGroupIndex++;
            } else {
                instanceMetadata.setDatanodeType(NORMAL);
            }
            multiMap.put((long) i, (long) i);
            ls.add(instanceMetadata);
            when(storageStore.get(i)).thenReturn(instanceMetadata);

            segmentUnitsDistributionManager.updateSimpleDatanodeInfo(instanceMetadata);
        }

        when(storageStore.list()).thenReturn(ls);

        icImpl.setStorageStore(storageStore);
        icImpl.setActualFreeSpace(800);

        Long storagePoolId = RequestIdBuilder.get();
        Set<Long> storagePoolIds = new HashSet<>();
        storagePoolIds.add(storagePoolId);
        request.setDomainId(domainId);
        request.setStoragePoolId(storagePoolId);

        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);
        when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);
        when(domain.isDeleting()).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(domain.getStoragePools()).thenReturn(storagePoolIds);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(storagePool.getArchivesInDataNode()).thenReturn(multiMap);

        icImpl.setVolumeStore(volumeStore);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> reservRet =
                segmentUnitsDistributionManager.reserveVolume(request.getVolumeSize(), RequestResponseHelper.convertVolumeType(request.getVolumeType()),
                        request.isSimpleConfiguration(), icImpl.getSegmentWrappCount(), request.getStoragePoolId());

        //verify result
        assert(reservRet.size() == volumeSize/segmentSize);
        for (int i = 0; i < reservRet.size(); i++){
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> segment = reservRet.get(i);

            // is segment count right?
            assert(segment.size() == 2);

            // is normal segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Normal).size() == 2);

            // is arbiter segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Arbiter).size() == 1);

            Set<Integer> normalSegmentUnitGroupSet = new HashSet<>();
            Set<Integer> arbiterSegmentUnitGroupSet = new HashSet<>();
            Set<Long> normalSegmentUnitInstanceSet = new HashSet<>();
            Set<Long> arbiterSegmentUnitInstanceSet = new HashSet<>();
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Normal)) {
                normalSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                normalSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Arbiter)) {
                arbiterSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                arbiterSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }

            // is normal segment unit not to be created in arbiterGroup?
            Set<Integer> retainSegmentUnitGroupSet = new HashSet<>();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(normalSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == 0);

            // is normal segment unit not to be created at simple datanode?
            Set<Long> retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(normalSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == 0);

            // is all segment units's groupId different?
            Set<Integer> allSegmentUnitGroupSet = new HashSet<>();
            allSegmentUnitGroupSet.addAll(normalSegmentUnitGroupSet);
            allSegmentUnitGroupSet.addAll(arbiterSegmentUnitGroupSet);
            assert(allSegmentUnitGroupSet.size() == 6);

            // is arbiter segment unit selected arbiterGroup first to be created
            retainSegmentUnitGroupSet.clear();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(arbiterSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == Math.min(arbiterGroupIdSet.size(),
                    arbiterSegmentUnitGroupSet.size()));

            // is arbiter segment unit selected simple datanode first to be created
            retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(arbiterSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == Math.min(simpleDatanodeInstanceId.size(),
                    arbiterSegmentUnitInstanceSet.size()));
        }
    }

    @Test
    public void testReservVolume_PSSAA_6Group_1ArbiterGroup() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName("testReservVolume");
        request.setAccountId(123456);

        long volumeSize = 2000;
        request.setVolumeSize(volumeSize);
        request.setVolumeType(VolumeType_Thrift.LARGE);
        request.setCacheType(CacheType_Thrift.MEMORY);
        request.setRequestType("CREATE_VOLUME");
        request.setVolumeId(1);
        request.setRootVolumeId(1);     //set current volume to be rootvolume

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
                new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
                        storageStore, storagePoolStore);

        List<InstanceMetadata> ls = new ArrayList<>();

        Multimap<Long, Long> multiMap = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Long> simpleDatanodeInstanceId = new HashSet<>();
        Long domainId = RequestIdBuilder.get();
        int arbiterGroupIndex = 0;
        int datanodeCount = 6;
        for (int i = 0; i < datanodeCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
            instanceMetadata.setDomainId(domainId);
            instanceMetadata.setFreeSpace(800);
            instanceMetadata.setCapacity(200);
            instanceMetadata.setDatanodeStatus(OK);
            Group group = new Group((i%6)==0?6*10:(i%6)*10);
            instanceMetadata.setGroup(group);

            List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
            RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
            rawArchiveMetadata.setArchiveId((long) i);
            rawArchiveMetadata.setLogicalFreeSpace(volumeSize);
            rawArchiveMetadata.setFreeFlexibleSegmentUnitCount(0);
            rawArchiveMetadata.setWeight(1);
            rawArchiveMetadataList.add(rawArchiveMetadata);
            instanceMetadata.setArchives(rawArchiveMetadataList);

            if ((arbiterGroupIndex < 1) && (i%6) == 2){
                instanceMetadata.setDatanodeType(SIMPLE);
                simpleDatanodeInstanceId.add(instanceMetadata.getInstanceId().getId());
                arbiterGroupIdSet.add(group.getGroupId());
                arbiterGroupIndex++;
            } else {
                instanceMetadata.setDatanodeType(NORMAL);
            }
            multiMap.put((long) i, (long) i);
            ls.add(instanceMetadata);
            when(storageStore.get(i)).thenReturn(instanceMetadata);

            segmentUnitsDistributionManager.updateSimpleDatanodeInfo(instanceMetadata);
        }

        when(storageStore.list()).thenReturn(ls);

        icImpl.setStorageStore(storageStore);
        icImpl.setActualFreeSpace(800);

        Long storagePoolId = RequestIdBuilder.get();
        Set<Long> storagePoolIds = new HashSet<>();
        storagePoolIds.add(storagePoolId);
        request.setDomainId(domainId);
        request.setStoragePoolId(storagePoolId);

        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);
        when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);
        when(domain.isDeleting()).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(domain.getStoragePools()).thenReturn(storagePoolIds);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(storagePool.getArchivesInDataNode()).thenReturn(multiMap);

        icImpl.setVolumeStore(volumeStore);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> reservRet =
                segmentUnitsDistributionManager.reserveVolume(request.getVolumeSize(), RequestResponseHelper.convertVolumeType(request.getVolumeType()),
                        request.isSimpleConfiguration(), icImpl.getSegmentWrappCount(), request.getStoragePoolId());

        //verify result
        assert(reservRet.size() == volumeSize/segmentSize);
        for (int i = 0; i < reservRet.size(); i++){
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> segment = reservRet.get(i);

            // is segment count right?
            assert(segment.size() == 2);

            // is normal segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Normal).size() == 4);

            // is arbiter segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Arbiter).size() == 2);

            Set<Integer> normalSegmentUnitGroupSet = new HashSet<>();
            Set<Integer> arbiterSegmentUnitGroupSet = new HashSet<>();
            Set<Long> normalSegmentUnitInstanceSet = new HashSet<>();
            Set<Long> arbiterSegmentUnitInstanceSet = new HashSet<>();
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Normal)) {
                normalSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                normalSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Arbiter)) {
                arbiterSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                arbiterSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }

            // is normal segment unit not to be created in arbiterGroup?
            Set<Integer> retainSegmentUnitGroupSet = new HashSet<>();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(normalSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == 0);

            // is normal segment unit not to be created at simple datanode?
            Set<Long> retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(normalSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == 0);

            // is all segment units's groupId different?
            Set<Integer> allSegmentUnitGroupSet = new HashSet<>();
            allSegmentUnitGroupSet.addAll(normalSegmentUnitGroupSet);
            allSegmentUnitGroupSet.addAll(arbiterSegmentUnitGroupSet);
            assert(allSegmentUnitGroupSet.size() == 6);

            // is arbiter segment unit selected arbiterGroup first to be created
            retainSegmentUnitGroupSet.clear();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(arbiterSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == Math.min(arbiterGroupIdSet.size(),
                    arbiterSegmentUnitGroupSet.size()));

            // is arbiter segment unit selected simple datanode first to be created
            retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(arbiterSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == Math.min(simpleDatanodeInstanceId.size(),
                    arbiterSegmentUnitInstanceSet.size()));
        }

        writeResult2File(volumeType, reservRet, datanodeCount);

        verifyResult(volumeType, reservRet, 5, 5, 1605, 5);
    }

    @Test
    public void testReservVolume_PSSAA_10Group_1ArbiterGroup() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName("testReservVolume");
        request.setAccountId(123456);

        long volumeSize = 2000;
        request.setVolumeSize(volumeSize);
        request.setVolumeType(VolumeType_Thrift.LARGE);
        request.setCacheType(CacheType_Thrift.MEMORY);
        request.setRequestType("CREATE_VOLUME");
        request.setVolumeId(1);
        request.setRootVolumeId(1);     //set current volume to be rootvolume
        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
                new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
                        storageStore, storagePoolStore);

        List<InstanceMetadata> ls = new ArrayList<>();

        int groupCount = 10;
        Multimap<Long, Long> multiMap = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Long> simpleDatanodeInstanceId = new HashSet<>();
        int arbiterGroupIndex = 0;
        int datanodeCount = 14;
        Long domainId = RequestIdBuilder.get();
        for (int i = 0; i < datanodeCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
            instanceMetadata.setDomainId(domainId);
            instanceMetadata.setFreeSpace(800);
            instanceMetadata.setCapacity(200);
            instanceMetadata.setDatanodeStatus(OK);
            Group group = new Group((i%groupCount)==0?groupCount*10:(i%groupCount)*10);
            instanceMetadata.setGroup(group);

            List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
            RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
            rawArchiveMetadata.setArchiveId((long) i);
            rawArchiveMetadata.setLogicalFreeSpace(volumeSize);
            rawArchiveMetadata.setFreeFlexibleSegmentUnitCount(0);
            rawArchiveMetadata.setWeight(1);
            rawArchiveMetadataList.add(rawArchiveMetadata);
            instanceMetadata.setArchives(rawArchiveMetadataList);

            if ((arbiterGroupIndex < 1) && (i%groupCount) == 8){
                instanceMetadata.setDatanodeType(SIMPLE);
                simpleDatanodeInstanceId.add(instanceMetadata.getInstanceId().getId());
                arbiterGroupIdSet.add(group.getGroupId());
                arbiterGroupIndex++;
            } else {
                instanceMetadata.setDatanodeType(NORMAL);
            }
            multiMap.put((long) i, (long) i);
            ls.add(instanceMetadata);
            when(storageStore.get(i)).thenReturn(instanceMetadata);

            segmentUnitsDistributionManager.updateSimpleDatanodeInfo(instanceMetadata);
        }

        when(storageStore.list()).thenReturn(ls);

        icImpl.setStorageStore(storageStore);
        icImpl.setActualFreeSpace(800);

        Long storagePoolId = RequestIdBuilder.get();
        Set<Long> storagePoolIds = new HashSet<>();
        storagePoolIds.add(storagePoolId);
        request.setDomainId(domainId);
        request.setStoragePoolId(storagePoolId);

        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);
        when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);
        when(domain.isDeleting()).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(domain.getStoragePools()).thenReturn(storagePoolIds);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(storagePool.getArchivesInDataNode()).thenReturn(multiMap);

        icImpl.setVolumeStore(volumeStore);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> reservRet =
                segmentUnitsDistributionManager.reserveVolume(request.getVolumeSize(), RequestResponseHelper.convertVolumeType(request.getVolumeType()),
                        request.isSimpleConfiguration(), icImpl.getSegmentWrappCount(), request.getStoragePoolId());

        //verify result
        assert(reservRet.size() == volumeSize/segmentSize);
        for (int i = 0; i < reservRet.size(); i++){
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> segment = reservRet.get(i);

            // is segment count right?
            assert(segment.size() == 2);

            // is normal segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Normal).size() == 5);

            // is arbiter segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Arbiter).size() == 3);

            Set<Integer> normalSegmentUnitGroupSet = new HashSet<>();
            Set<Integer> arbiterSegmentUnitGroupSet = new HashSet<>();
            Set<Long> normalSegmentUnitInstanceSet = new HashSet<>();
            Set<Long> arbiterSegmentUnitInstanceSet = new HashSet<>();
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Normal)) {
                normalSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                normalSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Arbiter)) {
                arbiterSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                arbiterSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }

            // is normal segment unit not to be created in arbiterGroup?
            Set<Integer> retainSegmentUnitGroupSet = new HashSet<>();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(normalSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == 0);

            // is normal segment unit not to be created at simple datanode?
            Set<Long> retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(normalSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == 0);

            // is all segment units's groupId different?
            Set<Integer> allSegmentUnitGroupSet = new HashSet<>();
            allSegmentUnitGroupSet.addAll(normalSegmentUnitGroupSet);
            allSegmentUnitGroupSet.addAll(arbiterSegmentUnitGroupSet);
            assert(allSegmentUnitGroupSet.size() == 8);

            // is arbiter segment unit selected arbiterGroup first to be created
            retainSegmentUnitGroupSet.clear();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(arbiterSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == Math.min(arbiterGroupIdSet.size(),
                    arbiterSegmentUnitGroupSet.size()));

            // is arbiter segment unit selected simple datanode first to be created
            retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(arbiterSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == Math.min(simpleDatanodeInstanceId.size(),
                    arbiterSegmentUnitInstanceSet.size()));
        }
        writeResult2File(volumeType, reservRet, datanodeCount);

        verifyResult(volumeType, reservRet, 5, 50, 1900, 5);
    }

    @Test
    public void testReservVolume_PSSAA_6Group_3ArbiterGroup() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName("testReservVolume");
        request.setAccountId(123456);

        long volumeSize = 2000;

        request.setVolumeSize(volumeSize);
        request.setVolumeType(VolumeType_Thrift.LARGE);
        request.setCacheType(CacheType_Thrift.MEMORY);
        request.setRequestType("CREATE_VOLUME");
        request.setVolumeId(1);
        request.setRootVolumeId(1);     //set current volume to be rootvolume

        SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
                new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
                        storageStore, storagePoolStore);

        List<InstanceMetadata> ls = new ArrayList<>();

        Multimap<Long, Long> multiMap = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Long> simpleDatanodeInstanceId = new HashSet<>();
        Long domainId = RequestIdBuilder.get();
        int arbiterGroupIndex = 0;
        int datanodeCount = 6;
        for (int i = 0; i < datanodeCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
            instanceMetadata.setDomainId(domainId);
            instanceMetadata.setFreeSpace(800);
            instanceMetadata.setCapacity(200);
            instanceMetadata.setDatanodeStatus(OK);
            Group group = new Group((i%6)==0?6*10:(i%6)*10);
            instanceMetadata.setGroup(group);

            List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
            RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
            rawArchiveMetadata.setArchiveId((long) i);
            rawArchiveMetadata.setLogicalFreeSpace(volumeSize);
            rawArchiveMetadata.setFreeFlexibleSegmentUnitCount(0);
            rawArchiveMetadata.setWeight(1);
            rawArchiveMetadataList.add(rawArchiveMetadata);
            instanceMetadata.setArchives(rawArchiveMetadataList);

            if ((arbiterGroupIndex < 4) &&
                    ((i%6) == 1 || (i%6) == 3 || (i%6) == 5)){
                instanceMetadata.setDatanodeType(SIMPLE);
                simpleDatanodeInstanceId.add(instanceMetadata.getInstanceId().getId());
                arbiterGroupIdSet.add(group.getGroupId());
                arbiterGroupIndex++;
            } else {
                instanceMetadata.setDatanodeType(NORMAL);
            }
            multiMap.put((long) i, (long) i);
            ls.add(instanceMetadata);
            when(storageStore.get(i)).thenReturn(instanceMetadata);

            segmentUnitsDistributionManager.updateSimpleDatanodeInfo(instanceMetadata);
        }

        when(storageStore.list()).thenReturn(ls);

        icImpl.setStorageStore(storageStore);
        icImpl.setActualFreeSpace(800);

        Long storagePoolId = RequestIdBuilder.get();
        Set<Long> storagePoolIds = new HashSet<>();
        storagePoolIds.add(storagePoolId);
        request.setDomainId(domainId);
        request.setStoragePoolId(storagePoolId);

        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);
        when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);
        when(domain.isDeleting()).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(domain.getStoragePools()).thenReturn(storagePoolIds);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(storagePool.getArchivesInDataNode()).thenReturn(multiMap);

        icImpl.setVolumeStore(volumeStore);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> reservRet =
                segmentUnitsDistributionManager.reserveVolume(request.getVolumeSize(), RequestResponseHelper.convertVolumeType(request.getVolumeType()),
                        request.isSimpleConfiguration(), icImpl.getSegmentWrappCount(), request.getStoragePoolId());

        //verify result
        assert(reservRet.size() == volumeSize/segmentSize);
        for (int i = 0; i < reservRet.size(); i++){
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> segment = reservRet.get(i);

            // is segment count right?
            assert(segment.size() == 2);

            // is normal segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Normal).size() == 3);

            // is arbiter segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Arbiter).size() == 3);

            Set<Integer> normalSegmentUnitGroupSet = new HashSet<>();
            Set<Integer> arbiterSegmentUnitGroupSet = new HashSet<>();
            Set<Long> normalSegmentUnitInstanceSet = new HashSet<>();
            Set<Long> arbiterSegmentUnitInstanceSet = new HashSet<>();
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Normal)) {
                normalSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                normalSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Arbiter)) {
                arbiterSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                arbiterSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }

            // is normal segment unit not to be created in arbiterGroup?
            Set<Integer> retainSegmentUnitGroupSet = new HashSet<>();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(normalSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == 0);

            // is normal segment unit not to be created at simple datanode?
            Set<Long> retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(normalSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == 0);

            // is all segment units's groupId different?
            Set<Integer> allSegmentUnitGroupSet = new HashSet<>();
            allSegmentUnitGroupSet.addAll(normalSegmentUnitGroupSet);
            allSegmentUnitGroupSet.addAll(arbiterSegmentUnitGroupSet);
            assert(allSegmentUnitGroupSet.size() == 6);

            // is arbiter segment unit selected arbiterGroup first to be created
            retainSegmentUnitGroupSet.clear();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(arbiterSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == Math.min(arbiterGroupIdSet.size(),
                    arbiterSegmentUnitGroupSet.size()));

            // is arbiter segment unit selected simple datanode first to be created
            retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(arbiterSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == Math.min(simpleDatanodeInstanceId.size(),
                    arbiterSegmentUnitInstanceSet.size()));
        }
        writeResult2File(RequestResponseHelper.convertVolumeType(request.getVolumeType()), reservRet, datanodeCount);

        verifyResult(RequestResponseHelper.convertVolumeType(request.getVolumeType()), reservRet,
                5, 5, 5, 5);
    }

    @Test
    public void testReservVolume_PSSAA_5Group_0ArbiterGroup() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName("testReservVolume");
        request.setAccountId(123456);

        long volumeSize = 2000;
        request.setVolumeSize(volumeSize);
        request.setVolumeType(VolumeType_Thrift.LARGE);
        request.setCacheType(CacheType_Thrift.MEMORY);
        request.setRequestType("CREATE_VOLUME");
        request.setVolumeId(1);
        request.setRootVolumeId(1);     //set current volume to be rootvolume

        SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
                new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
                        storageStore, storagePoolStore);

        List<InstanceMetadata> ls = new ArrayList<>();

        Multimap<Long, Long> multiMap = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Long> simpleDatanodeInstanceId = new HashSet<>();
//        int arbiterGroupIndex = 0;
        int datanodeCount = 5;
        Long domainId = RequestIdBuilder.get();
        for (int i = 0; i < datanodeCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
            instanceMetadata.setDomainId(domainId);
            instanceMetadata.setFreeSpace(800);
            instanceMetadata.setCapacity(200);
            instanceMetadata.setDatanodeStatus(OK);
            Group group = new Group((i%6)==0?6*10:(i%6)*10);
            instanceMetadata.setGroup(group);

            List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
            RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
            rawArchiveMetadata.setArchiveId((long) i);
            rawArchiveMetadata.setLogicalFreeSpace(volumeSize);
            rawArchiveMetadata.setFreeFlexibleSegmentUnitCount(0);
            rawArchiveMetadata.setWeight(1);
            rawArchiveMetadataList.add(rawArchiveMetadata);
            instanceMetadata.setArchives(rawArchiveMetadataList);

//            if ((arbiterGroupIndex < 0) && (i%6) == 2){
//                instanceMetadata.setDatanodeType(SIMPLE);
//                simpleDatanodeInstanceId.add(instanceMetadata.getInstanceId().getId());
//                arbiterGroupIdSet.add(group.getGroupId());
//                arbiterGroupIndex++;
//            } else {
                instanceMetadata.setDatanodeType(NORMAL);
//            }
            multiMap.put((long) i, (long) i);
            ls.add(instanceMetadata);
            when(storageStore.get(i)).thenReturn(instanceMetadata);

            segmentUnitsDistributionManager.updateSimpleDatanodeInfo(instanceMetadata);
        }

        when(storageStore.list()).thenReturn(ls);

        icImpl.setStorageStore(storageStore);
        icImpl.setActualFreeSpace(800);

        Long storagePoolId = RequestIdBuilder.get();
        Set<Long> storagePoolIds = new HashSet<>();
        storagePoolIds.add(storagePoolId);
        request.setDomainId(domainId);
        request.setStoragePoolId(storagePoolId);

        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);
        when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);
        when(domain.isDeleting()).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(domain.getStoragePools()).thenReturn(storagePoolIds);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(storagePool.getArchivesInDataNode()).thenReturn(multiMap);

        icImpl.setVolumeStore(volumeStore);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> reservRet =
                segmentUnitsDistributionManager.reserveVolume(request.getVolumeSize(), RequestResponseHelper.convertVolumeType(request.getVolumeType()),
                        request.isSimpleConfiguration(), icImpl.getSegmentWrappCount(), request.getStoragePoolId());

        //verify result
        assert(reservRet.size() == volumeSize/segmentSize);
        for (int i = 0; i < reservRet.size(); i++){
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> segment = reservRet.get(i);

            // is segment count right?
            assert(segment.size() == 2);

            // is normal segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Normal).size() == 3);

            // is arbiter segment unit count right?
            assert(segment.get(SegmentUnitType_Thrift.Arbiter).size() == 2);

            Set<Integer> normalSegmentUnitGroupSet = new HashSet<>();
            Set<Integer> arbiterSegmentUnitGroupSet = new HashSet<>();
            Set<Long> normalSegmentUnitInstanceSet = new HashSet<>();
            Set<Long> arbiterSegmentUnitInstanceSet = new HashSet<>();
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Normal)) {
                normalSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                normalSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : segment.get(SegmentUnitType_Thrift.Arbiter)) {
                arbiterSegmentUnitGroupSet.add(instanceIdAndEndPoint_thrift.getGroupId());
                arbiterSegmentUnitInstanceSet.add(instanceIdAndEndPoint_thrift.getInstanceId());
            }

            // is normal segment unit not to be created in arbiterGroup?
            Set<Integer> retainSegmentUnitGroupSet = new HashSet<>();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(normalSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == 0);

            // is normal segment unit not to be created at simple datanode?
            Set<Long> retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(normalSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == 0);


            // is all segment units's groupId different?
            Set<Integer> allSegmentUnitGroupSet = new HashSet<>();
            allSegmentUnitGroupSet.addAll(normalSegmentUnitGroupSet);
            allSegmentUnitGroupSet.addAll(arbiterSegmentUnitGroupSet);
            assert(allSegmentUnitGroupSet.size() == 5);

            // is arbiter segment unit selected arbiterGroup first to be created
            retainSegmentUnitGroupSet.clear();
            retainSegmentUnitGroupSet.addAll(arbiterGroupIdSet);
            retainSegmentUnitGroupSet.retainAll(arbiterSegmentUnitGroupSet);
            assert(retainSegmentUnitGroupSet.size() == Math.min(arbiterGroupIdSet.size(),
                    arbiterSegmentUnitGroupSet.size()));

            // is arbiter segment unit selected simple datanode first to be created
            retainSimpleDatanodeIdSet = new HashSet<>();
            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
            retainSimpleDatanodeIdSet.retainAll(arbiterSegmentUnitInstanceSet);
            assert(retainSimpleDatanodeIdSet.size() == Math.min(simpleDatanodeInstanceId.size(),
                    arbiterSegmentUnitInstanceSet.size()));
        }
        writeResult2File(RequestResponseHelper.convertVolumeType(request.getVolumeType()), reservRet, datanodeCount);

        verifyResult(RequestResponseHelper.convertVolumeType(request.getVolumeType()), reservRet,
                3, 3, 3, 3);

    }

    /**
     * Test reserve volume with 9 groups each has 4 datanodes, arbiter group not set, expect arbiters are average distributed
     * among all the datanode.
     *
     * @throws Exception
     */
    @Test
    public void testReserveVolume_PSA_36Instances_9Group_0SimpleDatanode() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupNotSetSelector);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000;
        // allow distributed error ratio
        double sigma = 0.3;
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setTags(new ArrayList<>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        //long numberOfGroup = volumeType.getNumMembers() * 3;
        long numberOfGroup = 9;
        long datanodeCountInGroup = 4L;
        for (long i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId((int)i);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j)));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(75000 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                int archiveCount = 10;
//                if (i*j < 3){
//                    archiveCount = 8;
//                }
                for (int k = 0; k < archiveCount; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(25000 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j), Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());
            }
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        ReserveVolumeResponse response = icImpl.reserveVolume(request);
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());

        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();
        Map<Long, Integer> primaryDatanodeMap = new HashMap<>();
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            normalGroupIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            // is arbiter segment unit count right?
            assertEquals(2, arbiterInstanceList.size());
            int necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList){
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumArbiters()){
                    Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                    instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                    necessaryCount++;
                }

                arbiterGroupIdSet.add(arbiterThrift.getGroupId());
            }

            Set<Long> normalInstanceIdSet = new HashSet<>();
            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            // is normal segment unit count right?
            assertEquals(4, normalInstanceList.size());
            necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumSecondaries()+1){
                    Long normalCount = instanceId2NormalCount.get(normal);
                    instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);
                    necessaryCount++;
                }

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
                normalInstanceIdSet.add(normal.getId());
            }
            Set<Long> secondaryIdSet = new HashSet<>();
            secondaryIdSet.add(normalInstanceList.get(1).getInstanceId());
            long primaryDatanodeId = normalInstanceList.get(0).getInstanceId();
            int oldCount = 0;
            if (primaryDatanodeMap.containsKey(primaryDatanodeId)){
                oldCount = primaryDatanodeMap.get(primaryDatanodeId);
            }
            primaryDatanodeMap.put(normalInstanceList.get(0).getInstanceId(), oldCount+1);

            //ensure all segment in different group
            normalGroupIdSet.addAll(arbiterGroupIdSet);
            assert(normalGroupIdSet.size() == arbiterInstanceList.size()+normalInstanceList.size());
        }

        writeResult2File(volumeType, segIndex2Instances, numberOfGroup*datanodeCountInGroup);

        verifyResult(volumeType, segIndex2Instances,2, 3, 3, 2);

        long totalArbiter = 0;
        long totalNormal = 0;
        long maxArbiterAllow = (long)((1+sigma) * (1 * numberOfSegment / (numberOfGroup * datanodeCountInGroup)));
        long minArbiterAllow = (long)((1-sigma) * (1 * numberOfSegment / (numberOfGroup * datanodeCountInGroup)));
        long maxNormalAllow = (long)((1+sigma) * (2 * numberOfSegment / (numberOfGroup * datanodeCountInGroup)));
        long minNormalAllow = (long)((1-sigma) * (2 * numberOfSegment / (numberOfGroup * datanodeCountInGroup)));
        logger.warn("{}", instanceId2ArbiterCount);
        logger.warn("arbiter allow range: {}--{}", minArbiterAllow, maxArbiterAllow);
        logger.warn("{}", instanceId2NormalCount);
        logger.warn("normal allow range: {}--{}", minNormalAllow, maxNormalAllow);
        for (Map.Entry<InstanceId, Long> instanceIdLongEntry : instanceId2ArbiterCount.entrySet()) {
            Long count = instanceIdLongEntry.getValue();
            assertNotNull(count);
            assertTrue(count < maxArbiterAllow);
            assertTrue(count > minArbiterAllow);
            totalArbiter += count;
        }
        for (Map.Entry<InstanceId, Long> instanceIdLongEntry : instanceId2NormalCount.entrySet()) {
            Long count = instanceIdLongEntry.getValue();
            assertNotNull(count);
            assertTrue(count < maxNormalAllow);
            assertTrue(count > minNormalAllow);
            totalNormal += count;
        }
        assertEquals(1 * numberOfSegment, totalArbiter);
        assertEquals(2 * numberOfSegment, totalNormal);
    }

    @Test
    public void testRandom(){
        File file = new File("/tmp/ReserveVolumeTest_testRandom.log");
        OutputStream outputStream = null;
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            outputStream = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        StringBuffer writeBuf = new StringBuffer();
        int count = 2000000;
        int i = 0;
        while (i++ < count){
            writeBuf.append("\t");
            writeBuf.append((int)(Math.random()*8));
            if (i%10 == 0){
                writeBuf.append("\r\n");
            }
        }

        try {
            outputStream.write(writeBuf.toString().getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }



        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

}

    @Test
    public void testReserveVolume_PSA_36Instances_9Group_1SimpleDatanode() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupSetSelector);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000L;
        // allow distributed error ratio
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        long numberOfGroup = 9;
        long datanodeCountInGroup = 4L;

        long simpleDatanodeIndex = new Random().nextInt((int)datanodeCountInGroup);
        long simpleGroupIdIndex = new Random().nextInt((int)numberOfGroup);
        Set<Integer> srcSimpleGroupIdSet = new HashSet<>();
        Set<Long> srcSimpleDatanodeIdSet = new HashSet<>();

        for (long i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId((int)i);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j) ));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(7500 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                for (int k = 0; k < 10; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(2500 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j) , Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());

                if (simpleGroupIdIndex == i && simpleDatanodeIndex == j){
                    arbiterGroupNumber = instanceMetadata.getGroup().getGroupId();
                    instanceMetadata.setDatanodeType(SIMPLE);
                    srcSimpleDatanodeIdSet.add(instanceMetadata.getInstanceId().getId());
                    srcSimpleGroupIdSet.add((int)i);
                } else {
                    instanceMetadata.setDatanodeType(NORMAL);
                }

                arbiterGroupSetSelector.updateSimpleDatanodeInfo(instanceMetadata);
            }
        }

        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);
        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());

        ReserveVolumeResponse response = icImpl.reserveVolume(request);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();

        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        Set<Long> arbiterInstanceIdSet = new HashSet<>();

        ObjectCounter<Long> simpleDatanodeCount = new TreeSetObjectCounter<>();
        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            normalGroupIdSet.clear();
            arbiterInstanceIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            // is arbiter segment unit count right?
            assertEquals(2, arbiterInstanceList.size());
            int necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList) {
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (srcSimpleDatanodeIdSet.contains(arbiter.getId())) {
                    simpleDatanodeCount.increment(arbiter.getId());

                    if (necessaryCount < volumeType.getNumArbiters()){
                        Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                        instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                        necessaryCount++;
                    }
                }

                arbiterGroupIdSet.add(arbiterThrift.getGroupId());
                arbiterInstanceIdSet.add(arbiter.getId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            // is normal segment unit count right?
            assertEquals(4, normalInstanceList.size());
            necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumSecondaries() + 1){
                    Long normalCount = instanceId2NormalCount.get(normal);
                    instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);
                    necessaryCount++;
                }

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
            }

            // is arbiter segment unit selected simple datanode first to be created
            Set<Long> retainIdSet = new HashSet<>();
            retainIdSet.addAll(arbiterInstanceIdSet);
            retainIdSet.retainAll(srcSimpleDatanodeIdSet);
            assert (retainIdSet.size() == Math.min(arbiterInstanceIdSet.size(),
                    srcSimpleDatanodeIdSet.size()));
            assert (arbiterInstanceIdSet.containsAll(srcSimpleDatanodeIdSet));

            //normal cannot create in arbiter group
            assert (!normalGroupIdSet.contains(arbiterGroupNumber));

            //ensure all segment in different group
            normalGroupIdSet.addAll(arbiterGroupIdSet);
            assert (normalGroupIdSet.size() == arbiterInstanceList.size() + normalInstanceList.size());
        }

        writeResult2File(volumeType, segIndex2Instances, numberOfGroup*datanodeCountInGroup);

        verifyResult(volumeType, segIndex2Instances,2, 3, 3, 2);

        long totalArbiter = 0;
        long totalNormal = 0;
        double sigma = 0.3;
        long maxArbiterAllow = (long)((1+sigma) * (1 * numberOfSegment / ((numberOfGroup-1) * datanodeCountInGroup)));
        long minArbiterAllow = (long)((1-sigma) * (1 * numberOfSegment / ((numberOfGroup-1) * datanodeCountInGroup)));
        long maxNormalAllow = (long)((1+sigma) * (2 * numberOfSegment / ((numberOfGroup-1) * datanodeCountInGroup)));
        long minNormalAllow = (long)((1-sigma) * (2 * numberOfSegment / ((numberOfGroup-1) * datanodeCountInGroup)));

        Iterator<Long> simpleDatanodeCountIt = simpleDatanodeCount.iterator();
        while (simpleDatanodeCountIt.hasNext()){
            long count = simpleDatanodeCount.get(simpleDatanodeCountIt.next());
            assert(count == numberOfSegment);
            totalArbiter += count;
        }
        for (Map.Entry<InstanceId, Long> instanceIdLongEntry : instanceId2ArbiterCount.entrySet()) {
            Long count = instanceIdLongEntry.getValue();
            assertNotNull(count);
            assertTrue(count == numberOfSegment);
//            assertTrue(count < maxArbiterAllow);
//            assertTrue(count > minArbiterAllow);
            totalArbiter += count;
        }
        for (Map.Entry<InstanceId, Long> instanceIdLongEntry : instanceId2NormalCount.entrySet()) {
            Long count = instanceIdLongEntry.getValue();
            assertNotNull(count);
            assertTrue(count < maxNormalAllow);
            assertTrue(count > minNormalAllow);
            totalNormal += count;
        }
        //assertEquals(2 * numberOfSegment, totalArbiter);
        assertEquals(2 * numberOfSegment, totalNormal);
    }

    @Test
    public void testReserveVolume_PSA_36Instances_9Group_2SimpleDatanode() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupSetSelector);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000L;
        // allow distributed error ratio
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        long numberOfGroup = 9;
        long datanodeCountInGroup = 4L;

        Set<Long> simpleDatanodeIndexSet = new HashSet<>();
        simpleDatanodeIndexSet.add(0L);
        while (simpleDatanodeIndexSet.size() < 1){
            simpleDatanodeIndexSet.add((long)(new Random().nextInt((int)datanodeCountInGroup)));
        }
        Set<Long> simpleGroupIdIndexSet = new HashSet<>();
        while (simpleGroupIdIndexSet.size() < 2){
            simpleGroupIdIndexSet.add(0L);
            simpleGroupIdIndexSet.add((long)(new Random().nextInt((int)numberOfGroup)));
        }
        Set<Integer> srcSimpleGroupIdSet = new HashSet<>();
        Set<Long> srcSimpleDatanodeIdSet = new HashSet<>();

        for (long i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId((int)i);

            for (long j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j) ));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(7500 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                for (int k = 0; k < 10; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(2500 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j) , Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());

                if (simpleGroupIdIndexSet.contains(i) && simpleDatanodeIndexSet.contains(j)){
                    arbiterGroupNumber = instanceMetadata.getGroup().getGroupId();
                    instanceMetadata.setDatanodeType(SIMPLE);
                    srcSimpleDatanodeIdSet.add(instanceMetadata.getInstanceId().getId());
                    srcSimpleGroupIdSet.add((int)i);
                } else {
                    instanceMetadata.setDatanodeType(NORMAL);
                }

                arbiterGroupSetSelector.updateSimpleDatanodeInfo(instanceMetadata);
            }
        }

        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);
        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());

        ReserveVolumeResponse response = icImpl.reserveVolume(request);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();

        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        List<Long> arbiterInstanceIdList = new LinkedList<>();

        ObjectCounter<Long> simpleDatanodeCount = new TreeSetObjectCounter<>();
        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            normalGroupIdSet.clear();
            arbiterInstanceIdList.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            // is arbiter segment unit count right?
            assertEquals(2, arbiterInstanceList.size());
            int necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList) {
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (srcSimpleDatanodeIdSet.contains(arbiter.getId())) {
                    simpleDatanodeCount.increment(arbiter.getId());

                    if (necessaryCount < volumeType.getNumArbiters()){
                        Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                        instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                        necessaryCount++;
                    }
                }

                arbiterGroupIdSet.add(arbiterThrift.getGroupId());
                arbiterInstanceIdList.add(arbiter.getId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            // is normal segment unit count right?
            assertEquals(4, normalInstanceList.size());
            necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumSecondaries() + 1){
                    Long normalCount = instanceId2NormalCount.get(normal);
                    instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);
                    necessaryCount++;
                }

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
            }

            // is arbiter segment unit selected simple datanode first to be created
            Set<Long> arbiterInSimpleGroupSet = new HashSet<>();
            for (int i = 0; i < volumeType.getNumArbiters(); i++){
                arbiterInSimpleGroupSet.add(arbiterInstanceIdList.get(i));
            }
            Set<Long> retainIdSet = new HashSet<>(arbiterInSimpleGroupSet);
            retainIdSet.retainAll(srcSimpleDatanodeIdSet);
            assert (retainIdSet.size() == Math.min(arbiterInSimpleGroupSet.size(), srcSimpleDatanodeIdSet.size()));

            //normal cannot create in arbiter group
            //assert (!normalGroupIdSet.contains(arbiterGroupNumber));

            //ensure all segment in different group
            normalGroupIdSet.addAll(arbiterGroupIdSet);
            assert (normalGroupIdSet.size() == arbiterInstanceList.size() + normalInstanceList.size());
        }

        writeResult2File(volumeType, segIndex2Instances, numberOfGroup*datanodeCountInGroup);

        verifyResult(volumeType, segIndex2Instances,2, 15, 3, 2);
    }

    @Test
    public void testReserveVolume_PSS_36Instances_9Group_1SimpleDatanode() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupSetSelector);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000L;
        // allow distributed error ratio
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.REGULAR);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        long numberOfGroup = volumeType.getNumMembers() * 3;
        long datanodeCountInGroup = 4L;

        long simpleDatanodeIndex = new Random().nextInt((int)datanodeCountInGroup);
        long simpleGroupIdIndex = new Random().nextInt((int)numberOfGroup);
        Set<Integer> srcSimpleGroupIdSet = new HashSet<>();
        Set<Long> srcSimpleDatanodeIdSet = new HashSet<>();

        for (long i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId((int)i);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j)));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(7500 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                for (int k = 0; k < 10; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(2500 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j), Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());

                if (simpleGroupIdIndex == i && simpleDatanodeIndex == j){
                    arbiterGroupNumber = instanceMetadata.getGroup().getGroupId();
                    instanceMetadata.setDatanodeType(SIMPLE);
                    srcSimpleDatanodeIdSet.add(instanceMetadata.getInstanceId().getId());
                    srcSimpleGroupIdSet.add((int)i);
                } else {
                    instanceMetadata.setDatanodeType(NORMAL);
                }

                arbiterGroupSetSelector.updateSimpleDatanodeInfo(instanceMetadata);
            }
        }

        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);
        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());

        ReserveVolumeResponse response = icImpl.reserveVolume(request);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();

        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        Set<Long> arbiterInstanceIdSet = new HashSet<>();

        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            normalGroupIdSet.clear();
            arbiterInstanceIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            // is arbiter segment unit count right?
            assertEquals(0, arbiterInstanceList.size());
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList){
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (!srcSimpleDatanodeIdSet.contains(arbiter.getId())){
                    Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                    instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                }

                arbiterGroupIdSet.add(arbiterThrift.getGroupId());
                arbiterInstanceIdSet.add(arbiter.getId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            // is normal segment unit count right?
            assertEquals(5, normalInstanceList.size());
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                Long normalCount = instanceId2NormalCount.get(normal);
                instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());

                assert(!srcSimpleDatanodeIdSet.contains(instanceIdAndEndPointThrift.getGroupId()));
            }

            //normal cannot create in arbiter group
            //assert(!normalGroupIdSet.contains(arbiterGroupNumber));

            //ensure all segment in different group
            normalGroupIdSet.addAll(arbiterGroupIdSet);
            assert(normalGroupIdSet.size() == arbiterInstanceList.size() + normalInstanceList.size());
        }

        writeResult2File(volumeType, segIndex2Instances, numberOfGroup*datanodeCountInGroup);
        verifyResult(volumeType, segIndex2Instances,3, 5, 3, 3);
    }

    @Test
    public void testReserveVolume_PSSAA_36Instances_9Group_0SimpleDatanode() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupNotSetSelector);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000L;
        // allow distributed error ratio
        double sigma = 0.3;
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.LARGE);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        long numberOfGroup = 9;
        long datanodeCountInGroup = 4L;
        for (long i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId((int)i);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(datanodeCountInGroup * i + j));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(7500 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                for (int k = 0; k < 10; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(2500 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j), Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());
            }
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        ReserveVolumeResponse response = icImpl.reserveVolume(request);
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();

        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            normalGroupIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            // is arbiter segment unit count right?
            assertEquals(3, arbiterInstanceList.size());
            int necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList){
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumArbiters()){
                    Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                    instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                    necessaryCount++;
                }

                arbiterGroupIdSet.add(arbiterThrift.getGroupId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            // is normal segment unit count right?
            assertEquals(5, normalInstanceList.size());
            necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumSecondaries()+1) {
                    Long normalCount = instanceId2NormalCount.get(normal);
                    instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);
                    necessaryCount++;
                }

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
            }

//            // is arbiter segment unit selected simple datanode first to be created
//            retainSimpleDatanodeIdSet = new HashSet<>();
//            retainSimpleDatanodeIdSet.addAll(simpleDatanodeInstanceId);
//            retainSimpleDatanodeIdSet.retainAll(arbiterSegmentUnitInstanceSet);
//            assert(retainSimpleDatanodeIdSet.size() == Math.min(simpleDatanodeInstanceId.size(),
//                    arbiterSegmentUnitInstanceSet.size()));

            //ensure all segment in different group
            normalGroupIdSet.addAll(arbiterGroupIdSet);
            assert(normalGroupIdSet.size() == arbiterInstanceList.size() + normalInstanceList.size());
        }

        writeResult2File(volumeType, segIndex2Instances, numberOfGroup*datanodeCountInGroup);
        verifyResult(volumeType, segIndex2Instances,3, 5, 3, 3);

        long totalArbiter = 0;
        long totalNormal = 0;
        long maxArbiterAllow = (long)((1+sigma) * (2 * numberOfSegment / (numberOfGroup * datanodeCountInGroup)));
        long minArbiterAllow = (long)((1-sigma) * (2 * numberOfSegment / (numberOfGroup * datanodeCountInGroup)));
        long maxNormalAllow = (long)((1+sigma) * (3 * numberOfSegment / (numberOfGroup * datanodeCountInGroup)));
        long minNormalAllow = (long)((1-sigma) * (3 * numberOfSegment / (numberOfGroup * datanodeCountInGroup)));
        logger.warn("{}", instanceId2ArbiterCount);
        logger.warn("arbiter allow range: {}--{}", minArbiterAllow, maxArbiterAllow);
        logger.warn("{}", instanceId2NormalCount);
        logger.warn("normal allow range: {}--{}", minNormalAllow, maxNormalAllow);
        for (Map.Entry<InstanceId, Long> instanceIdLongEntry : instanceId2ArbiterCount.entrySet()) {
            Long count = instanceIdLongEntry.getValue();
            assertNotNull(count);
            assertTrue(count < maxArbiterAllow);
            assertTrue(count > minArbiterAllow);
            totalArbiter += count;
        }
        for (Map.Entry<InstanceId, Long> instanceIdLongEntry : instanceId2NormalCount.entrySet()) {
            Long count = instanceIdLongEntry.getValue();
            assertNotNull(count);
            assertTrue(count < maxNormalAllow);
            assertTrue(count > minNormalAllow);
            totalNormal += count;
        }
        assertEquals(2 * numberOfSegment, totalArbiter);
        assertEquals(3 * numberOfSegment, totalNormal);
    }

    @Test
    public void testReserveVolume_PSSAA_36Instances_9Group_1SimpleDatanode() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupSetSelector);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000L;
        // allow distributed error ratio
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.LARGE);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        long numberOfGroup = 9;
        long datanodeCountInGroup = 4L;

        long simpleGroupIdIndex = new Random().nextInt((int)numberOfGroup);
        long simpleDatanodeIndex = new Random().nextInt((int)datanodeCountInGroup);
        Set<Integer> srcSimpleGroupIdSet = new HashSet<>();
        Set<Long> srcSimpleDatanodeIdSet = new HashSet<>();

        for (long i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId((int)i);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j) ));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(7500 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                for (int k = 0; k < 10; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(2500 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j) , Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());

                if (simpleGroupIdIndex == i && simpleDatanodeIndex == j){
                    arbiterGroupNumber = instanceMetadata.getGroup().getGroupId();
                    instanceMetadata.setDatanodeType(SIMPLE);
                    srcSimpleDatanodeIdSet.add(instanceMetadata.getInstanceId().getId());
                    srcSimpleGroupIdSet.add((int)i);
                } else {
                    instanceMetadata.setDatanodeType(NORMAL);
                }

                arbiterGroupSetSelector.updateSimpleDatanodeInfo(instanceMetadata);
            }
        }

        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);
        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());

        ReserveVolumeResponse response = icImpl.reserveVolume(request);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();

        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        Set<Long> arbiterInstanceIdSet = new HashSet<>();

        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            normalGroupIdSet.clear();
            arbiterInstanceIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            // is arbiter segment unit count right?
            assertEquals(3, arbiterInstanceList.size());
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList){
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (!srcSimpleDatanodeIdSet.contains(arbiter.getId())){
                    Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                    instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                }

                arbiterGroupIdSet.add(arbiterThrift.getGroupId());
                arbiterInstanceIdSet.add(arbiter.getId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            // is normal segment unit count right?
            assertEquals(5, normalInstanceList.size());
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                Long normalCount = instanceId2NormalCount.get(normal);
                instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
            }

            // is arbiter segment unit selected simple datanode first to be created
            Set<Long> retainIdSet = new HashSet<>();
            retainIdSet.addAll(arbiterInstanceIdSet);
            retainIdSet.retainAll(srcSimpleDatanodeIdSet);
            assert(retainIdSet.size() == Math.min(arbiterInstanceIdSet.size(),
                    srcSimpleDatanodeIdSet.size()));
            assert(arbiterInstanceIdSet.containsAll(srcSimpleDatanodeIdSet));

            //normal cannot create in arbiter group
            assert(!normalGroupIdSet.contains(arbiterGroupNumber));

            //ensure all segment in different group
            normalGroupIdSet.addAll(arbiterGroupIdSet);
            assert(normalGroupIdSet.size() == 8);
        }

        writeResult2File(volumeType, segIndex2Instances, numberOfGroup*datanodeCountInGroup);
        verifyResult(volumeType, segIndex2Instances,3, 5, 4850, 3);
    }

    @Test
    public void testReserveVolume_PSSAA_36Instances_9Group_2SimpleDatanodeIn1Group() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupSetSelector);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000L;
        // allow distributed error ratio
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.LARGE);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        long numberOfGroup = 9;
        long datanodeCountInGroup = 4L;

        Set<Integer> srcSimpleGroupIdSet = new HashSet<>();
        Set<Long> srcSimpleDatanodeIdSet = new HashSet<>();

        Random rand = new Random();
        while (srcSimpleGroupIdSet.size() < 1){
            srcSimpleGroupIdSet.add(rand.nextInt((int)numberOfGroup));
        }
        Random datanodeIndexRand = new Random();
        Set<Integer> randDatanodeIndexSet = new HashSet<>();
        while (randDatanodeIndexSet.size() < 2){
            randDatanodeIndexSet.add(datanodeIndexRand.nextInt((int)datanodeCountInGroup));
        }

        for (int i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId(i);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j) ));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(7500 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                for (int k = 0; k < 10; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(2500 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j) , Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());

                if (srcSimpleGroupIdSet.contains(i) &&
                        randDatanodeIndexSet.contains(j)){
                    arbiterGroupNumber = i;
                    instanceMetadata.setDatanodeType(SIMPLE);
                    srcSimpleDatanodeIdSet.add(instanceMetadata.getInstanceId().getId());
                } else {
                    instanceMetadata.setDatanodeType(NORMAL);
                }

                arbiterGroupSetSelector.updateSimpleDatanodeInfo(instanceMetadata);
            }
        }

        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);
        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());

        ReserveVolumeResponse response = icImpl.reserveVolume(request);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();

        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        Set<Long> arbiterInstanceIdSet = new HashSet<>();

        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            normalGroupIdSet.clear();
            arbiterInstanceIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            // is arbiter segment unit count right?
            assertEquals(3, arbiterInstanceList.size());
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList){
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (!srcSimpleDatanodeIdSet.contains(arbiter.getId())){
                    Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                    instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                }

                // arbiter segment unit priority selection is simple datanode
                if (arbiterThrift.getGroupId() == arbiterGroupNumber){
                    assert(srcSimpleDatanodeIdSet.contains(arbiter.getId()));
                }

                arbiterGroupIdSet.add(arbiterThrift.getGroupId());
                arbiterInstanceIdSet.add(arbiter.getId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            // is normal segment unit count right?
            assertEquals(5, normalInstanceList.size());
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                Long normalCount = instanceId2NormalCount.get(normal);
                instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
            }

            //arbiter priority create in arbiter group
            assert(arbiterGroupIdSet.contains(arbiterGroupNumber));
            //normal cannot create in arbiter group
            assert(!normalGroupIdSet.contains(arbiterGroupNumber));

            //ensure all segment in different group
            normalGroupIdSet.addAll(arbiterGroupIdSet);
            assert(normalGroupIdSet.size() == 8);
        }

        writeResult2File(volumeType, segIndex2Instances, numberOfGroup*datanodeCountInGroup);
        verifyResult(volumeType, segIndex2Instances,3, 5, 2350, 3);
    }

    @Test
    public void testReserveVolume_PSSAA_36Instances_9Group_2SimpleDatanodeIn2Group() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupSetSelector);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000L;
        // allow distributed error ratio
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.LARGE);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        long numberOfGroup = 9;
        long datanodeCountInGroup = 4L;

        Set<Integer> srcSimpleGroupIdSet = new HashSet<>();
        Set<Long> srcSimpleDatanodeIdSet = new HashSet<>();

        Random rand = new Random();
        while (srcSimpleGroupIdSet.size() < 2){
            srcSimpleGroupIdSet.add(rand.nextInt((int)numberOfGroup));
        }
        Random datanodeIndexRand = new Random();

        for (int i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId(i);

            long simpleDatanodeIndex = datanodeIndexRand.nextInt((int)datanodeCountInGroup);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j) ));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(7500 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                for (int k = 0; k < 10; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(2500 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j) , Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());

                if (srcSimpleGroupIdSet.contains(i) && (simpleDatanodeIndex == j)){
                    instanceMetadata.setDatanodeType(SIMPLE);
                    srcSimpleDatanodeIdSet.add(instanceMetadata.getInstanceId().getId());
                    srcSimpleGroupIdSet.add(instanceMetadata.getGroup().getGroupId());
                } else {
                    instanceMetadata.setDatanodeType(NORMAL);
                }

                arbiterGroupSetSelector.updateSimpleDatanodeInfo(instanceMetadata);
            }
        }

        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);
        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());

        ReserveVolumeResponse response = icImpl.reserveVolume(request);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();

        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        Set<Long> arbiterInstanceIdSet = new HashSet<>();

        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            normalGroupIdSet.clear();
            arbiterInstanceIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            // is arbiter segment unit count right?
            assertEquals(3, arbiterInstanceList.size());
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList){
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (!srcSimpleDatanodeIdSet.contains(arbiter.getId())){
                    Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                    instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                }

                // arbiter segment unit priority selection is simple datanode
                if (srcSimpleGroupIdSet.contains(arbiterThrift.getGroupId())){
                    assert(srcSimpleDatanodeIdSet.contains(arbiter.getId()));
                }

                arbiterGroupIdSet.add(arbiterThrift.getGroupId());
                arbiterInstanceIdSet.add(arbiter.getId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            // is normal segment unit count right?
            assertEquals(5, normalInstanceList.size());
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                Long normalCount = instanceId2NormalCount.get(normal);
                instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
                assert(!srcSimpleGroupIdSet.contains(instanceIdAndEndPointThrift.getGroupId()));
            }

            //arbiter priority create in arbiter group
            assert(arbiterGroupIdSet.containsAll(srcSimpleGroupIdSet));
//            //normal cannot create in arbiter group
//            assert(!normalGroupIdSet.contains(arbiterGroupNumber));

            //ensure all segment in different group
            normalGroupIdSet.addAll(arbiterGroupIdSet);
            assert(normalGroupIdSet.size() == 8);
        }
        writeResult2File(volumeType, segIndex2Instances, numberOfGroup*datanodeCountInGroup);
        verifyResult(volumeType, segIndex2Instances,3, 5, 3, 3);
    }

    @Test
    public void testReserveVolume_PSSAA_36Instances_9Group_5SimpleDatanodeIn5Group() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupSetSelector);
        setLogLevel(Level.DEBUG);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000L;
        // allow distributed error ratio
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.LARGE);
        request.setTags(new ArrayList<>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        long numberOfGroup = 9;
        long datanodeCountInGroup = 4L;

        Set<Integer> srcSimpleGroupIdSet = new HashSet<>();
        Set<Long> srcSimpleDatanodeIdSet = new HashSet<>();

        Random rand = new Random();
        while (srcSimpleGroupIdSet.size() < 5){
            srcSimpleGroupIdSet.add(rand.nextInt((int)numberOfGroup));
        }
        Random datanodeIndexRand = new Random();

        for (int i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId(i);

            int simpleDatanodeIndex = datanodeIndexRand.nextInt((int)datanodeCountInGroup);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j) ));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(7500 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                for (int k = 0; k < 10; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(2500 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j) , Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());

                if (srcSimpleGroupIdSet.contains(i) && simpleDatanodeIndex == j){
                    instanceMetadata.setDatanodeType(SIMPLE);
                    srcSimpleDatanodeIdSet.add(instanceMetadata.getInstanceId().getId());
                    srcSimpleGroupIdSet.add(instanceMetadata.getGroup().getGroupId());
                } else {
                    instanceMetadata.setDatanodeType(NORMAL);
                }

                arbiterGroupSetSelector.updateSimpleDatanodeInfo(instanceMetadata);
            }
        }

        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);
        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());

        ReserveVolumeResponse response = icImpl.reserveVolume(request);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();

        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> arbiterInSimpleGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        Set<Long> arbiterInstanceIdSet = new HashSet<>();

        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            arbiterInSimpleGroupIdSet.clear();
            normalGroupIdSet.clear();
            arbiterInstanceIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            // is arbiter segment unit count right?
            assertEquals(3, arbiterInstanceList.size());
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList){
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (!srcSimpleDatanodeIdSet.contains(arbiter.getId())){
                    Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                    instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                }

                // arbiter segment unit priority selection is simple datanode
                if (srcSimpleGroupIdSet.contains(arbiterThrift.getGroupId())){
                    assert(srcSimpleDatanodeIdSet.contains(arbiter.getId()));
                }

                if (arbiterInSimpleGroupIdSet.size() < volumeType.getNumArbiters()){
                    arbiterInSimpleGroupIdSet.add(arbiterThrift.getGroupId());
                }

                arbiterGroupIdSet.add(arbiterThrift.getGroupId());
                arbiterInstanceIdSet.add(arbiter.getId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            // is normal segment unit count right?
            assertEquals(5, normalInstanceList.size());
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                Long normalCount = instanceId2NormalCount.get(normal);
                instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
                //normal cannot create at simple datanode
                assert(!srcSimpleDatanodeIdSet.contains(instanceIdAndEndPointThrift.getInstanceId()));
            }

            //arbiter priority create in arbiter group
            assert(srcSimpleGroupIdSet.containsAll(arbiterInSimpleGroupIdSet));
//            //normal cannot create in arbiter group
//            assert(!normalGroupIdSet.contains(arbiterGroupNumber));

            //ensure all segment in different group
            normalGroupIdSet.addAll(arbiterGroupIdSet);
            assert(normalGroupIdSet.size() == arbiterInstanceList.size() + normalInstanceList.size());
        }
        writeResult2File(volumeType, segIndex2Instances, numberOfGroup*datanodeCountInGroup);
        verifyResult(volumeType, segIndex2Instances,3, 10, 3, 3);
    }

    @Test
    public void testUpdateSimpleDatanodeGroupIdSet() throws Exception {
        long segmentSize = 100;
        SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
                new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
                        storageStore, storagePoolStore);

        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Multimap<Integer, Long> groupId2InstanceIdMap = Multimaps.synchronizedSetMultimap(HashMultimap.create());
        int groupCount = 6;
        for (int i = 1; i <= 15; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
            Group group = new Group((i%groupCount)==0?groupCount*10:(i%groupCount)*10);
            instanceMetadata.setGroup(group);

            if ((i%groupCount) <= 5 && (i%groupCount) >= 3){
                arbiterGroupIdSet.add(group.getGroupId());
                instanceMetadata.setDatanodeType(SIMPLE);
                groupId2InstanceIdMap.put(group.getGroupId(), instanceMetadata.getInstanceId().getId());
            } else {
                instanceMetadata.setDatanodeType(NORMAL);
            }

            segmentUnitsDistributionManager.updateSimpleDatanodeInfo(instanceMetadata);
        }

        SimpleDatanodeManager simpleDatanodeManager = segmentUnitsDistributionManager.getSimpleDatanodeManager();
        Set<Integer> arbiterGroupIdSetRet = simpleDatanodeManager.getSimpleDatanodeGroupIdSet();

        assert(arbiterGroupIdSetRet.size() == arbiterGroupIdSet.size());
        assert(arbiterGroupIdSetRet.containsAll(arbiterGroupIdSet));
        for (int groupId : groupId2InstanceIdMap.keySet()) {
            Set<Long> instanceIdSet = simpleDatanodeManager.getSimpleDatanodeIdSetByGroupId(groupId);
            assert(instanceIdSet.size() == groupId2InstanceIdMap.get(groupId).size());
            assert(instanceIdSet.containsAll(groupId2InstanceIdMap.get(groupId)));
        }

        for (int i = 1; i <= 15; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
            Group group = new Group((i%groupCount)==0?groupCount*10:(i%groupCount)*10);
            instanceMetadata.setGroup(group);

            if ((i%groupCount) < 5 && (i%groupCount) >= 2){
                arbiterGroupIdSet.add(group.getGroupId());
                instanceMetadata.setDatanodeType(SIMPLE);
                groupId2InstanceIdMap.put(group.getGroupId(), instanceMetadata.getInstanceId().getId());
            } else {
                instanceMetadata.setDatanodeType(NORMAL);
                if (arbiterGroupIdSet.contains(group.getGroupId())) {
                    arbiterGroupIdSet.remove(group.getGroupId());
                }

                if (groupId2InstanceIdMap.containsEntry(group.getGroupId(),
                        instanceMetadata.getInstanceId().getId())) {
                    groupId2InstanceIdMap.remove(group.getGroupId(),
                            instanceMetadata.getInstanceId().getId());
                }
            }

            segmentUnitsDistributionManager.updateSimpleDatanodeInfo(instanceMetadata);
        }

        simpleDatanodeManager = segmentUnitsDistributionManager.getSimpleDatanodeManager();
        arbiterGroupIdSetRet = simpleDatanodeManager.getSimpleDatanodeGroupIdSet();

        assert(arbiterGroupIdSetRet.size() == arbiterGroupIdSet.size());
        assert(arbiterGroupIdSetRet.containsAll(arbiterGroupIdSet));
        for (int groupId : groupId2InstanceIdMap.keySet()) {
            Set<Long> instanceIdSet = simpleDatanodeManager.getSimpleDatanodeIdSetByGroupId(groupId);
            assert(instanceIdSet.size() == groupId2InstanceIdMap.get(groupId).size());
            assert(instanceIdSet.containsAll(groupId2InstanceIdMap.get(groupId)));
        }
    }

    @Test
    public void testReserveVolume_PSA_36Instances_9Group_0SimpleDatanode_diffWeight() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupNotSetSelector);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000;
        // allow distributed error ratio
        double sigma = 0.3;
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setTags(new ArrayList<>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        //long numberOfGroup = volumeType.getNumMembers() * 3;
        long numberOfGroup = 9;
        long datanodeCountInGroup = 4L;
        int minArchiveCount = 8;
        int maxArchiveCount = 10;
        Map<Long, Integer> instanceId2WeightMap = new HashMap<>();
        for (long i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId((int)i);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j)));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(75000 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                int archiveCount = maxArchiveCount;
                if (i <= 2 && i >= 1 && j == 0){
                    archiveCount = minArchiveCount;
                }
                instanceId2WeightMap.put(instanceMetadata.getInstanceId().getId(), archiveCount);
                for (int k = 0; k < archiveCount; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(25000 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j), Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());
            }
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        ReserveVolumeResponse response = icImpl.reserveVolume(request);
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());

        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();
        Map<Long, Integer> primaryDatanodeMap = new HashMap<>();
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            normalGroupIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            // is arbiter segment unit count right?
            assertEquals(2, arbiterInstanceList.size());
            int necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList){
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumArbiters()){
                    Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                    instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                    necessaryCount++;
                }

                arbiterGroupIdSet.add(arbiterThrift.getGroupId());
            }

            Set<Long> normalInstanceIdSet = new HashSet<>();
            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            // is normal segment unit count right?
            assertEquals(4, normalInstanceList.size());
            necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumSecondaries()+1){
                    Long normalCount = instanceId2NormalCount.get(normal);
                    instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);
                    necessaryCount++;
                }

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
                normalInstanceIdSet.add(normal.getId());
            }
            Set<Long> secondaryIdSet = new HashSet<>();
            secondaryIdSet.add(normalInstanceList.get(1).getInstanceId());
            long primaryDatanodeId = normalInstanceList.get(0).getInstanceId();
            int oldCount = 0;
            if (primaryDatanodeMap.containsKey(primaryDatanodeId)){
                oldCount = primaryDatanodeMap.get(primaryDatanodeId);
            }
            primaryDatanodeMap.put(normalInstanceList.get(0).getInstanceId(), oldCount+1);

            //ensure all segment in different group
            normalGroupIdSet.addAll(arbiterGroupIdSet);
            assert(normalGroupIdSet.size() == arbiterInstanceList.size()+normalInstanceList.size());
        }

        writeResult2File(volumeType, segIndex2Instances, numberOfGroup*datanodeCountInGroup);

        verifyResult(volumeType, segIndex2Instances, instanceId2WeightMap, 1, 2, 3, 1);
    }
    @Test
    public void testReserveVolume_PSA_8Instances_4Group_0SimpleDatanode_diffWeight() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupNotSetSelector);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000;
        // allow distributed error ratio
        double sigma = 0.3;
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setTags(new ArrayList<>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        //long numberOfGroup = volumeType.getNumMembers() * 3;
        long numberOfGroup =4;
        long datanodeCountInGroup = 3L;
        int minArchiveCount = 8;
        int maxArchiveCount = 10;
        Map<Long, Integer> instanceId2WeightMap = new HashMap<>();
        for (long i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId((int)i);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j)));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(75000 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                int archiveCount = maxArchiveCount;
                if (i <= 2 && i >= 1 && j == 0){
                    archiveCount = minArchiveCount;
                }
                instanceId2WeightMap.put(instanceMetadata.getInstanceId().getId(), archiveCount);
                for (int k = 0; k < archiveCount; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(25000 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j), Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());
            }
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        ReserveVolumeResponse response = icImpl.reserveVolume(request);
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());

        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();
        Map<Long, Integer> primaryDatanodeMap = new HashMap<>();
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            normalGroupIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            // is arbiter segment unit count right?
            assertEquals(1, arbiterInstanceList.size());
            int necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList){
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumArbiters()){
                    Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                    instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                    necessaryCount++;
                }

                arbiterGroupIdSet.add(arbiterThrift.getGroupId());
            }

            Set<Long> normalInstanceIdSet = new HashSet<>();
            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            // is normal segment unit count right?
            assertEquals(3, normalInstanceList.size());
            necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumSecondaries()+1){
                    Long normalCount = instanceId2NormalCount.get(normal);
                    instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);
                    necessaryCount++;
                }

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
                normalInstanceIdSet.add(normal.getId());
            }
            Set<Long> secondaryIdSet = new HashSet<>();
            secondaryIdSet.add(normalInstanceList.get(1).getInstanceId());
            long primaryDatanodeId = normalInstanceList.get(0).getInstanceId();
            int oldCount = 0;
            if (primaryDatanodeMap.containsKey(primaryDatanodeId)){
                oldCount = primaryDatanodeMap.get(primaryDatanodeId);
            }
            primaryDatanodeMap.put(normalInstanceList.get(0).getInstanceId(), oldCount+1);

            //ensure all segment in different group
            normalGroupIdSet.addAll(arbiterGroupIdSet);
            assert(normalGroupIdSet.size() == arbiterInstanceList.size()+normalInstanceList.size());
        }

        writeResult2File(volumeType, segIndex2Instances, numberOfGroup*datanodeCountInGroup);

        verifyResult(volumeType, segIndex2Instances, instanceId2WeightMap, 1, 2, 3, 1);
    }
    @Test
    public void testReserveVolume_PSA_8Instances_4Group_1SimpleDatanode() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupSetSelector);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000L;
        // allow distributed error ratio
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.SMALL);
        request.setTags(new ArrayList<Tag>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        long numberOfGroup = 4;
        long datanodeCountInGroup = 2L;

        long simpleDatanodeIndex = new Random().nextInt((int)datanodeCountInGroup);
        long simpleGroupIdIndex = new Random().nextInt((int)numberOfGroup);
        Set<Integer> srcSimpleGroupIdSet = new HashSet<>();
        Set<Long> srcSimpleDatanodeIdSet = new HashSet<>();
        int minArchiveCount = 8;
        int maxArchiveCount = 10;

        Map<Long, Integer> instanceId2WeightMap = new HashMap<>();
        for (long i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId((int)i);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j) ));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(7500 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                int archiveCount = maxArchiveCount;
                if (i <= 2 && i >= 1 && j == 0){
                    archiveCount = minArchiveCount;
                }
                instanceId2WeightMap.put(instanceMetadata.getInstanceId().getId(), archiveCount);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                for (int k = 0; k < archiveCount; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(2500 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j) , Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());

                if (simpleGroupIdIndex == i && simpleDatanodeIndex == j){
                    arbiterGroupNumber = instanceMetadata.getGroup().getGroupId();
                    instanceMetadata.setDatanodeType(SIMPLE);
                    srcSimpleDatanodeIdSet.add(instanceMetadata.getInstanceId().getId());
                    srcSimpleGroupIdSet.add((int)i);
                } else {
                    instanceMetadata.setDatanodeType(NORMAL);
                }

                arbiterGroupSetSelector.updateSimpleDatanodeInfo(instanceMetadata);
            }
        }

        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);
        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());

        ReserveVolumeResponse response = icImpl.reserveVolume(request);

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());
        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();

        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        Set<Long> arbiterInstanceIdSet = new HashSet<>();

        ObjectCounter<Long> simpleDatanodeCount = new TreeSetObjectCounter<>();
        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            normalGroupIdSet.clear();
            arbiterInstanceIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            // is arbiter segment unit count right?
            assertEquals(1, arbiterInstanceList.size());
            int necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList) {
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (srcSimpleDatanodeIdSet.contains(arbiter.getId())) {
                    simpleDatanodeCount.increment(arbiter.getId());

                    if (necessaryCount < volumeType.getNumArbiters()){
                        Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                        instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                        necessaryCount++;
                    }
                }

                arbiterGroupIdSet.add(arbiterThrift.getGroupId());
                arbiterInstanceIdSet.add(arbiter.getId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            // is normal segment unit count right?
            assertEquals(3, normalInstanceList.size());
            necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumSecondaries() + 1){
                    Long normalCount = instanceId2NormalCount.get(normal);
                    instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);
                    necessaryCount++;
                }

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
            }

            // is arbiter segment unit selected simple datanode first to be created
            Set<Long> retainIdSet = new HashSet<>();
            retainIdSet.addAll(arbiterInstanceIdSet);
            retainIdSet.retainAll(srcSimpleDatanodeIdSet);
            assert (retainIdSet.size() == Math.min(arbiterInstanceIdSet.size(),
                    srcSimpleDatanodeIdSet.size()));
            assert (arbiterInstanceIdSet.containsAll(srcSimpleDatanodeIdSet));

            //normal cannot create in arbiter group
            assert (!normalGroupIdSet.contains(arbiterGroupNumber));

            //ensure all segment in different group
            normalGroupIdSet.addAll(arbiterGroupIdSet);
            assert (normalGroupIdSet.size() == arbiterInstanceList.size() + normalInstanceList.size());
        }

        writeResult2File(volumeType, segIndex2Instances, numberOfGroup*datanodeCountInGroup);

        verifyResult(volumeType, segIndex2Instances, instanceId2WeightMap, 1, 2, 3, 1);
    }
    @Test
    public void testReserveVolume_PSS_8Instances_4Group_0SimpleDatanode_diffWeight() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupNotSetSelector);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000;
        // allow distributed error ratio
        double sigma = 0.3;
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.REGULAR);
        request.setTags(new ArrayList<>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        //long numberOfGroup = volumeType.getNumMembers() * 3;
        long numberOfGroup =4;
        long datanodeCountInGroup = 2L;
        int minArchiveCount = 8;
        int maxArchiveCount = 10;
        Map<Long, Integer> instanceId2WeightMap = new HashMap<>();
        for (long i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId((int)i);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j)));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(75000 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                int archiveCount = maxArchiveCount;
                if (i <= 2 && i >= 1 && j == 0){
                    archiveCount = minArchiveCount;
                }
                instanceId2WeightMap.put(instanceMetadata.getInstanceId().getId(), archiveCount);
                for (int k = 0; k < archiveCount; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(25000 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j), Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());
            }
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        ReserveVolumeResponse response = icImpl.reserveVolume(request);
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());

        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();
        Map<Long, Integer> primaryDatanodeMap = new HashMap<>();
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            normalGroupIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            // is arbiter segment unit count right?
            assertEquals(0, arbiterInstanceList.size());
            int necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList){
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumArbiters()){
                    Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                    instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                    necessaryCount++;
                }

                arbiterGroupIdSet.add(arbiterThrift.getGroupId());
            }

            Set<Long> normalInstanceIdSet = new HashSet<>();
            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            // is normal segment unit count right?
            assertEquals(4, normalInstanceList.size());
            necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumSecondaries()+1){
                    Long normalCount = instanceId2NormalCount.get(normal);
                    instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);
                    necessaryCount++;
                }

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
                normalInstanceIdSet.add(normal.getId());
            }
            Set<Long> secondaryIdSet = new HashSet<>();
            secondaryIdSet.add(normalInstanceList.get(1).getInstanceId());
            long primaryDatanodeId = normalInstanceList.get(0).getInstanceId();
            int oldCount = 0;
            if (primaryDatanodeMap.containsKey(primaryDatanodeId)){
                oldCount = primaryDatanodeMap.get(primaryDatanodeId);
            }
            primaryDatanodeMap.put(normalInstanceList.get(0).getInstanceId(), oldCount+1);

            //ensure all segment in different group
            normalGroupIdSet.addAll(arbiterGroupIdSet);
            assert(normalGroupIdSet.size() == arbiterInstanceList.size()+normalInstanceList.size());
        }

        writeResult2File(volumeType, segIndex2Instances, numberOfGroup*datanodeCountInGroup);

        verifyResult(volumeType, segIndex2Instances, instanceId2WeightMap, 1, 2, 3, 1);
    }
    @Test
    public void testReserveVolume_PSSAA_10Instances_5Group_0SimpleDatanode_diffWeight() throws Exception {
        icImpl.setSegmentUnitsDistributionManager(arbiterGroupNotSetSelector);

        // numberOfSegment should not be too small
        long numberOfSegment = 5000;
        // allow distributed error ratio
        double sigma = 0.3;
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(RequestIdBuilder.get());
        request.setVolumeSize(numberOfSegment * segmentSize);
        request.setSegmentSize(segmentSize);
        request.setName("test");
        request.setVolumeType(VolumeType_Thrift.LARGE);
        request.setTags(new ArrayList<>());

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.addStoragePool(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        Map<InstanceId, Group> instanceId2GroupMap = new HashMap<>();
        //long numberOfGroup = volumeType.getNumMembers() * 3;
        long numberOfGroup =5;
        long datanodeCountInGroup = 2L;
        int minArchiveCount = 8;
        int maxArchiveCount = 10;
        Map<Long, Integer> instanceId2WeightMap = new HashMap<>();
        for (long i = 0; i < numberOfGroup; i++) {
            Group group = new Group();
            group.setGroupId((int)i);

            for (int j = 0; j < datanodeCountInGroup; j++) {
                InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((datanodeCountInGroup * i + j)));
                instanceMetadata.setGroup(group);
                instanceId2GroupMap.put(instanceMetadata.getInstanceId(), group);
                instanceMetadata.setCapacity(75000 * segmentSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                int archiveCount = maxArchiveCount;
                if (i <= 2 && i >= 1 && j == 0){
                    archiveCount = minArchiveCount;
                }
                instanceId2WeightMap.put(instanceMetadata.getInstanceId().getId(), archiveCount);
                for (int k = 0; k < archiveCount; k++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)k);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(storagePoolId);
                    archive.setLogicalFreeSpace(25000 * segmentSize);
                    archive.setWeight(1);
                    archives.add(archive);
                    archivesInDataNode.put((datanodeCountInGroup * i + j), Long.valueOf(k));
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(domainId);
                instanceList.add(instanceMetadata);
                domain.addDatanode(instanceMetadata.getInstanceId().getId());
            }
        }
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instance : instanceList) {
            when(storageStore.get(instance.getInstanceId().getId())).thenReturn(instance);
        }
        when(domainStore.getDomain(anyLong())).thenReturn(domain);

        when(volumeStore.getVolume(anyLong())).thenReturn(new VolumeMetadata());
        ReserveVolumeResponse response = icImpl.reserveVolume(request);
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                response.getSegIndex2Instances();

        assertEquals((int) (request.getVolumeSize() / segmentSize), segIndex2Instances.size());

        Map<InstanceId, Long> instanceId2ArbiterCount = new HashMap<>();
        Map<InstanceId, Long> instanceId2NormalCount = new HashMap<>();
        Map<Long, Integer> primaryDatanodeMap = new HashMap<>();
        Set<Integer> arbiterGroupIdSet = new HashSet<>();
        Set<Integer> normalGroupIdSet = new HashSet<>();
        for (int segIndex : segIndex2Instances.keySet()) {
            arbiterGroupIdSet.clear();
            normalGroupIdSet.clear();

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            // is arbiter segment unit count right?
            assertEquals(2, arbiterInstanceList.size());
            int necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift arbiterThrift : arbiterInstanceList){
                InstanceId arbiter = new InstanceId(arbiterThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumArbiters()){
                    Long arbiterCount = instanceId2ArbiterCount.get(arbiter);
                    instanceId2ArbiterCount.put(arbiter, null == arbiterCount ? 1 : arbiterCount + 1);
                    necessaryCount++;
                }

                arbiterGroupIdSet.add(arbiterThrift.getGroupId());
            }

            Set<Long> normalInstanceIdSet = new HashSet<>();
            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);
            // is normal segment unit count right?
            assertEquals(3, normalInstanceList.size());
            necessaryCount = 0;
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : normalInstanceList) {
                InstanceId normal = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                if (necessaryCount < volumeType.getNumSecondaries()+1){
                    Long normalCount = instanceId2NormalCount.get(normal);
                    instanceId2NormalCount.put(normal, null == normalCount ? 1 : normalCount + 1);
                    necessaryCount++;
                }

                normalGroupIdSet.add(instanceIdAndEndPointThrift.getGroupId());
                normalInstanceIdSet.add(normal.getId());
            }
            Set<Long> secondaryIdSet = new HashSet<>();
            secondaryIdSet.add(normalInstanceList.get(1).getInstanceId());
            long primaryDatanodeId = normalInstanceList.get(0).getInstanceId();
            int oldCount = 0;
            if (primaryDatanodeMap.containsKey(primaryDatanodeId)){
                oldCount = primaryDatanodeMap.get(primaryDatanodeId);
            }
            primaryDatanodeMap.put(normalInstanceList.get(0).getInstanceId(), oldCount+1);

            //ensure all segment in different group
            normalGroupIdSet.addAll(arbiterGroupIdSet);
            assert(normalGroupIdSet.size() == arbiterInstanceList.size()+normalInstanceList.size());
        }

        writeResult2File(volumeType, segIndex2Instances, numberOfGroup*datanodeCountInGroup);

        verifyResult(volumeType, segIndex2Instances, instanceId2WeightMap, 1, 2, -1, 1);
    }

    public void parseResultFromFile() throws IOException {
        File readFile = new File("/tmp/ReserveVolumeTest_PS.log");
        BufferedReader reader= new BufferedReader(new FileReader(readFile));

        long datanodeCount = 0;

        /**
         * parse
         */
        ObjectCounter<Long> primaryId = new TreeSetObjectCounter<>();
        ObjectCounter<Long> secondaryId = new TreeSetObjectCounter<>();
        ObjectCounter<Long> arbiterId = new TreeSetObjectCounter<>();

        Map<Long, ObjectCounter<Long>> primary2SecondaryMap = new HashMap<>();
        LinkedList<Set<Long>> balanceSecondaryNode = new LinkedList<>();
        String lineBuf;
        while((lineBuf = reader.readLine()) != null) {
            String comb[] = lineBuf.split("\t");
            datanodeCount = comb.length - 2;
            Set<Long> sNode = new HashSet<>();
            long downNode = 3;
            boolean needBalance = false;

            ObjectCounter<Long> secondaryOfPrimaryCounterTemp = new TreeSetObjectCounter<>();
            long primaryIdTemp = 0;
            for (int index = 2; index < comb.length; index++){
                if (comb[index].equals("P")){
                    primaryIdTemp = (long)(index-2);
                    primaryId.increment(primaryIdTemp);
                    if (downNode == index-2){
                        needBalance = true;
                    }
                } else if (comb[index].equals("S")){
                    secondaryId.increment((long)(index-2));
                    sNode.add((long)(index-2));
                    secondaryOfPrimaryCounterTemp.increment((long)(index-2));
                } else if (comb[index].equals("A")){
                    arbiterId.increment((long)(index-2));
                }
            }

            if (needBalance){
                balanceSecondaryNode.add(sNode);
            }

            ObjectCounter<Long> secondaryOfPrimaryCounter = primary2SecondaryMap.computeIfAbsent(primaryIdTemp, value->new TreeSetObjectCounter<>());

            for (Long secondaryIdTemp : secondaryOfPrimaryCounterTemp.getAll()){
                secondaryOfPrimaryCounter.increment(secondaryIdTemp);
            }

        }

        /**
         * write node combination
         */
        File writeFile = new File("/tmp/ReserveVolumeTest_PS_result.log");
        OutputStream outputStream = null;
        if (!writeFile.exists()) {
            try {
                writeFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            outputStream = new FileOutputStream(writeFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String writeBuf = "";

        /**
         * primary
         */
        int pMax = 0;
        int pMin = 0xffffff;
        writeBuf += "P:\t\t";
        for (long i = 0; i < datanodeCount; i++){
            writeBuf += primaryId.get(i) + "\t";
            pMax = Math.max(pMax, (int)primaryId.get(i));
            if (primaryId.get(i) != 0){
                pMin = Math.min(pMin, (int)primaryId.get(i));
            }
        }
        if (pMin == 0xffffff){
            pMin = 0;
        }
        writeBuf += "\r\n";
        writeBuf += "P分配差额：\t"+pMax+"\t"+pMin+"\t"+(pMax-pMin)+"\r\n\n";


        /**
         * Secondary
         */
        pMax = 0;
        pMin = 0xffffff;
        writeBuf += "S:\t\t";
        for (long i = 0; i < datanodeCount; i++){
            writeBuf += secondaryId.get(i) + "\t";
            pMax = Math.max(pMax, (int)secondaryId.get(i));
            if (secondaryId.get(i) != 0){
                pMin = Math.min(pMin, (int)secondaryId.get(i));
            }
        }
        if (pMin == 0xffffff){
            pMin = 0;
        }
        writeBuf += "\r\n";
        writeBuf += "S分配差额：\t"+pMax+"\t"+pMin+"\t"+(pMax-pMin)+"\r\n\n";

        /**
         * arbiter
         */
        pMax = 0;
        pMin = 0xffffff;
        writeBuf += "A:\t\t";
        for (long i = 0; i < datanodeCount; i++){
            writeBuf += arbiterId.get(i) + "\t";
            pMax = Math.max(pMax, (int)arbiterId.get(i));
            if (arbiterId.get(i) != 0){
                pMin = Math.min(pMin, (int)arbiterId.get(i));
            }
        }
        if (pMin == 0xffffff){
            pMin = 0;
        }
        writeBuf += "\r\n";
        writeBuf += "A分配差额：\t"+pMax+"\t"+pMin+"\t"+(pMax-pMin)+"\r\n\n";

        /**
         * rebalance
         */
        ObjectCounter<Long> pBalanceCounter = new TreeSetObjectCounter<>();
        for (Set<Long> sNode : balanceSecondaryNode){
            for (Long i : sNode){
                pBalanceCounter.increment(i);
            }
        }

        for (long primaryIdTemp : primary2SecondaryMap.keySet()){
            ObjectCounter<Long> secondaryOfPrimaryCounter = primary2SecondaryMap.get(primaryIdTemp);
            pMax = 0;
            pMin = 0xffffff;
            writeBuf += "P("+primaryIdTemp+")再平衡:\t";

            for (long i = 0; i < datanodeCount; i++){
                writeBuf += secondaryOfPrimaryCounter.get(i) + "\t";
                pMax = Math.max(pMax, (int)secondaryOfPrimaryCounter.get(i));
                if (secondaryOfPrimaryCounter.get(i) != 0){
                    pMin = Math.min(pMin, (int)secondaryOfPrimaryCounter.get(i));
                }
            }
            if (pMin == 0xffffff){
                pMin = 0;
            }
            writeBuf += "\r\n";
            writeBuf += "P再平衡差额：\t"+pMax+"\t"+pMin+"\t"+(pMax-pMin)+"\r\n\n";
        }

        //write
        try {
            outputStream.write(writeBuf.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }

        //close
        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeResult2File(VolumeType volumeType, Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances,
                     long datanodeCount){
        File file = new File("/tmp/ReserveVolumeTest_PS.log");
        OutputStream outputStream = null;
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            outputStream = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        for (int segIndex : segIndex2Instances.keySet()) {
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            Set<Long> arbiterIdSet = new HashSet<>();
            for (int i = 0; i < volumeType.getNumArbiters(); i++) {
                arbiterIdSet.add(arbiterInstanceList.get(i).getInstanceId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);

            long primaryDatanodeId = normalInstanceList.get(0).getInstanceId();
            Set<Long> secondaryIdSet = new HashSet<>();
            for (int i = 1; i < volumeType.getNumSecondaries() + 1; i++) {
                secondaryIdSet.add(normalInstanceList.get(i).getInstanceId());
            }

            String writeBuf = segIndex + "\t";

            for (long i = 0; i < datanodeCount; i++) {
                if (primaryDatanodeId == i) {
                    writeBuf += "\tP";
                } else if (secondaryIdSet.contains(i)) {
                    writeBuf += "\tS";
                } else if (arbiterIdSet.contains(i)) {
                    writeBuf += "\tA";
                } else {
                    writeBuf += "\tO";
                }
            }

            writeBuf += "\r\n";

            try {
                outputStream.write(writeBuf.getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //close
        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            parseResultFromFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void verifyResult(VolumeType volumeType, Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances,
                             long pDiffCount, long sDiffCount, long aDiffCount, long pDownDiffCount){
        ObjectCounter<Long> arbiterIdCounter = new TreeSetObjectCounter<>();
        ObjectCounter<Long> primaryIdCounter = new TreeSetObjectCounter<>();
        ObjectCounter<Long> secondaryIdCounter = new TreeSetObjectCounter<>();

        Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();
        for (int segIndex : segIndex2Instances.keySet()) {

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            //count necessary arbiter
            for (int i = 0; i < volumeType.getNumArbiters(); i++) {
                arbiterIdCounter.increment(arbiterInstanceList.get(i).getInstanceId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);

            //count necessary primary
            long primaryId = normalInstanceList.get(0).getInstanceId();
            primaryIdCounter.increment(primaryId);

            ObjectCounter<Long> primarySecondaryCounter = primaryId2SecondaryIdCounterMap.get(primaryId);
            if (primarySecondaryCounter == null){
                primarySecondaryCounter = new TreeSetObjectCounter<>();
                primaryId2SecondaryIdCounterMap.put(primaryId, primarySecondaryCounter);
            }

            //count necessary secondary
            for (int i = 1; i < volumeType.getNumSecondaries() + 1; i++) {
                long secondaryId = normalInstanceList.get(i).getInstanceId();
                secondaryIdCounter.increment(secondaryId);

                primarySecondaryCounter.increment(secondaryId);
            }
        }

        long maxCount, minCount;
        /**
         * verify average distribution
         */
        //verify necessary primary max and min count
        maxCount = primaryIdCounter.maxValue();
        minCount = primaryIdCounter.minValue();
        if (maxCount - minCount > pDiffCount){
            logger.error("primary average distribute failed ! maxCount:{}, minCount:{}",maxCount, minCount);
        }
        assert(maxCount - minCount <= pDiffCount);

        //verify necessary secondary max and min count
        maxCount = secondaryIdCounter.maxValue();
        minCount = secondaryIdCounter.minValue();
        if (maxCount - minCount > sDiffCount){
            logger.error("secondary average distribute failed ! maxCount:{}, minCount:{}",maxCount, minCount);
        }
        assert(maxCount - minCount <= sDiffCount);

        //verify necessary arbiter max and min count
        if (arbiterIdCounter.size() > 0){
            maxCount = arbiterIdCounter.maxValue();
            minCount = arbiterIdCounter.minValue();
            if (maxCount - minCount > aDiffCount){
                logger.error("arbiter average distribute failed ! maxCount:{}, minCount:{}",maxCount, minCount);
            }
            assert(maxCount - minCount <= aDiffCount);
        }

        /**
         * verify rebalance when P down
         */
        for (Map.Entry<Long, ObjectCounter<Long>> entry : primaryId2SecondaryIdCounterMap.entrySet()){
            ObjectCounter<Long> secondaryCounterTemp = entry.getValue();
            maxCount = secondaryCounterTemp.maxValue();
            minCount = secondaryCounterTemp.minValue();

            if (maxCount - minCount > pDownDiffCount){
                logger.error("rebalance failed when P down! maxCount:{}, minCount:{}, entry:{}", maxCount, minCount, entry);
            }
            assert(maxCount - minCount <= pDownDiffCount);
        }
    }

    public void verifyResult(VolumeType volumeType, Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances,
                             Map<Long, Integer> instanceId2WeightMap, int pDiff, double sDiffPercent, long aDiffCount, double pDownDiffPercent){
        ObjectCounter<Long> arbiterIdCounter = new TreeSetObjectCounter<>();
        ObjectCounter<Long> primaryIdCounter = new TreeSetObjectCounter<>();
        ObjectCounter<Long> secondaryIdCounter = new TreeSetObjectCounter<>();

        Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();
        for (int segIndex : segIndex2Instances.keySet()) {

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            //count necessary arbiter
            for (int i = 0; i < volumeType.getNumArbiters(); i++) {
                arbiterIdCounter.increment(arbiterInstanceList.get(i).getInstanceId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);

            //count necessary primary
            long primaryId = normalInstanceList.get(0).getInstanceId();
            primaryIdCounter.increment(primaryId);

            ObjectCounter<Long> primarySecondaryCounter = primaryId2SecondaryIdCounterMap.get(primaryId);
            if (primarySecondaryCounter == null){
                primarySecondaryCounter = new TreeSetObjectCounter<>();
                primaryId2SecondaryIdCounterMap.put(primaryId, primarySecondaryCounter);
            }

            //count necessary secondary
            for (int i = 1; i < volumeType.getNumSecondaries() + 1; i++) {
                long secondaryId = normalInstanceList.get(i).getInstanceId();
                secondaryIdCounter.increment(secondaryId);

                primarySecondaryCounter.increment(secondaryId);
            }
        }

        long maxCount, minCount, min2Max, diffMin2Max, maxWeight;
        /**
         * verify average distribution
         */
        //verify necessary primary max and min count
        Iterator<Long> primaryIt = primaryIdCounter.iterator();
        maxCount = primaryIdCounter.maxValue();
        maxWeight = instanceId2WeightMap.get(primaryIdCounter.max());
        int maxPercent = (int)(maxCount/(double)maxWeight);
        while (primaryIt.hasNext()){
            long primaryId = primaryIt.next();
            long factCount = primaryIdCounter.get(primaryId);
            int expectWeight = instanceId2WeightMap.get(primaryId);
            int percent = (int)(factCount/(double)expectWeight);

            if (percent - maxPercent > pDiff || percent - maxPercent < -pDiff){
                logger.error("primary:{} average distribute failed ! factCount:{}, expectWeight:{}, maxCount:{}, maxWeight:{}",
                        primaryId, factCount, expectWeight, maxCount, maxWeight);
                assert(false);
            }
        }

        //verify necessary secondary max and min count
//        maxCount = secondaryIdCounter.maxValue();
//        minCount = secondaryIdCounter.minValue();
//        min2Max = (int)((double)minCount/(double)maxCount*100);
//        diffMin2Max = (int)(sDiffPercent*100);
//        if (min2Max - diffMin2Max > 5 || diffMin2Max - min2Max > 5) {
//            logger.error("secondary average distribute failed ! maxCount:{}, minCount:{}",maxCount, minCount);
//            assert(false);
//        }

        //verify necessary arbiter max and min count
        if (arbiterIdCounter.size() > 0 && aDiffCount != -1){
            maxCount = arbiterIdCounter.maxValue();
            minCount = arbiterIdCounter.minValue();
            if (maxCount - minCount > aDiffCount){
                logger.error("arbiter average distribute failed ! maxCount:{}, minCount:{}",maxCount, minCount);
                assert(false);
            }
        }

        /**
         * verify rebalance when P down
         */
        for (Map.Entry<Long, ObjectCounter<Long>> entry : primaryId2SecondaryIdCounterMap.entrySet()){
            ObjectCounter<Long> secondaryCounterTemp = entry.getValue();
            Iterator<Long> secondaryOfPrimaryIt = secondaryCounterTemp.iterator();
            maxCount = secondaryCounterTemp.maxValue();
            maxWeight = instanceId2WeightMap.get(secondaryCounterTemp.max());
            maxPercent = (int)(maxCount/(double)maxWeight);
            while (secondaryOfPrimaryIt.hasNext()){
                long insId = secondaryOfPrimaryIt.next();
                long factCount = secondaryCounterTemp.get(insId);
                int expectWeight = instanceId2WeightMap.get(insId);
                int percent = (int)(factCount/(double)expectWeight);

                if (percent - maxPercent > pDownDiffPercent || percent - maxPercent < -pDownDiffPercent){
                    logger.error("S:{} average distribute failed when P:{} down! average distribute failed ! factCount:{}, expectWeight:{}, maxCount:{}, maxWeight:{}",
                            insId, entry.getKey(), factCount, expectWeight, maxCount, maxWeight);
                    assert(false);
                }
            }
        }
    }
}