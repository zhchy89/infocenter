package py.infocenter.service;

import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.icshare.InstanceMetadata.DatanodeType.NORMAL;
import static py.icshare.InstanceMetadata.DatanodeType.SIMPLE;
import static py.volume.VolumeMetadata.VolumeInAction.NULL;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import py.archive.RawArchiveMetadata;
import py.archive.ArchiveStatus;
import py.archive.StorageType;
import py.archive.segment.*;
import py.common.RequestIdBuilder;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePool;
import py.icshare.StoragePoolStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.rebalance.SegmentUnitsDistributionManager;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.store.MemoryVolumeStoreImpl;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.instance.Group;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.membership.SegmentMembership;
import py.test.TestBase;
import py.thrift.icshare.CreateVolumeRequest;
import py.thrift.infocenter.service.ReserveSegUnitsRequest;
import py.thrift.infocenter.service.ReserveSegUnitsResponse;
import py.thrift.share.*;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

/**
 * A class includes some test for reserving segment units from information center.
 * 
 * @author zjm
 * 
 */
public class ReserveSegUnitsTest extends TestBase {

    @Mock
    private StorageStore storageStore;
    private VolumeStore volumeStore;
    @Mock
    private DomainStore domainStore;

    @Mock
    private StoragePoolStore storagePoolStore;

    private SegmentUnitsDistributionManager segmentUnitsDistributionManager;

    private long segmentSize = 1L;

    private InformationCenterImpl icImpl;

    @Before
    public void init() throws Exception {
        super.init();
        InfoCenterAppContext appContext = mock(InfoCenterAppContext.class);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        volumeStore = new MemoryVolumeStoreImpl();
        segmentUnitsDistributionManager = new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
                storageStore, storagePoolStore);

        icImpl = new InformationCenterImpl();
        icImpl.setStorageStore(storageStore);
        icImpl.setVolumeStore(volumeStore);
        icImpl.setDomainStore(domainStore);
        icImpl.setStoragePoolStore(storagePoolStore);
        icImpl.setAppContext(appContext);
        icImpl.setSegmentUnitsDistributionManager(segmentUnitsDistributionManager);
        icImpl.setSegmentWrappCount(10);
    }

    @Test
    public void testSucceedToReserveSegUnitsWithThreeInstance() throws Exception {
        Set<Long> excludedInstanceIds = new HashSet<>();
        excludedInstanceIds.add(0L);
        excludedInstanceIds.add(1L);

        ReserveSegUnitsRequest request = new ReserveSegUnitsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setExcludedInstanceIds(excludedInstanceIds);
        request.setSegmentSize(segmentSize);
        request.setVolumeId(RequestIdBuilder.get());
        request.setSegmentUnitType(SegmentUnitType_Thrift.Normal);
        request.setNumberOfSegUnits(1);

        Long volumeSize = 3L;
        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Group group = new Group();
            group.setGroupId(i);

            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            instanceMetadata.setDatanodeType(NORMAL);

            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 1; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(volumeSize * segmentSize);
                archives.add(archive);
                archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);

            instanceList.add(instanceMetadata);
        }
        // TestUtils.generateVolumeMetadata()
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.setStoragePools(storagePoolIdList);

        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        for (InstanceMetadata instanceMetadata : instanceList) {
            when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
        }

        createAVolume(3, VolumeType.SMALL, request.getVolumeId(), storagePoolId, domainId);

        ReserveSegUnitsResponse response = icImpl.reserveSegUnits(request);

        List<InstanceMetadata_Thrift> instancesFromRemote = response.getInstances();
        Assert.assertEquals(1, instancesFromRemote.size());
        Assert.assertEquals(2L, instancesFromRemote.get(0).getInstanceId());
    }

    @Test
    public void testFailedToReserveSegUnitsWithThreeInstance() throws Exception {
        Set<Long> excludedInstanceIds = new HashSet<>();
        excludedInstanceIds.add(0L);
        excludedInstanceIds.add(1L);

        ReserveSegUnitsRequest request = new ReserveSegUnitsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setExcludedInstanceIds(excludedInstanceIds);
        request.setSegmentSize(segmentSize);
        request.setNumberOfSegUnits(1);
        request.setSegmentUnitType(SegmentUnitType_Thrift.Normal);
        request.setNumberOfSegUnits(1);

        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Group group = new Group();
            group.setGroupId(i);

            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);

            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 2; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                // zero freeSize of last datanode that can be chosen for reserve segmentUnit
                if (i != 2) {
                    archive.setLogicalFreeSpace(3 * segmentSize);
                } else {
                    archive.setLogicalFreeSpace(0L);
                }
                archives.add(archive);
                archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);

            instanceList.add(instanceMetadata);
        }
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.setStoragePools(storagePoolIdList);

        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(storageStore.list()).thenReturn(instanceList);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

        for (InstanceMetadata instanceMetadata : instanceList) {
            when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
        }

        createAVolume(3, VolumeType.SMALL, request.getVolumeId(), storagePoolId, domainId);

        boolean caughtException = false;
        try {
            icImpl.reserveSegUnits(request);
        } catch (NotEnoughSpaceException_Thrift e) {
            caughtException = true;
        }

        Assert.assertTrue(caughtException);
    }

    @Test
    public void testSucceedToReserveSegUnitsWithFourInstanceWithDomain() throws Exception {
        Set<Long> excludedInstanceIds = new HashSet<>();
        excludedInstanceIds.add(0L);
        excludedInstanceIds.add(1L);

        ReserveSegUnitsRequest request = new ReserveSegUnitsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setExcludedInstanceIds(excludedInstanceIds);
        request.setSegmentSize(segmentSize);
        request.setVolumeId(RequestIdBuilder.get());
        request.setSegmentUnitType(SegmentUnitType_Thrift.Normal);
        request.setNumberOfSegUnits(1);

        List<InstanceMetadata> instanceList = new ArrayList<>();
        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        for (int i = 0; i < 4; i++) {
            Group group = new Group();
            group.setGroupId(i);

            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            instanceMetadata.setDatanodeType(NORMAL);

            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 2; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                archives.add(archive);
                if (i == 2) {
                    continue;
                }
                archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            instanceList.add(instanceMetadata);
        }
        // new Domain
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.setStoragePools(storagePoolIdList);

        // TestUtils.generateVolumeMetadata()
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        when(storageStore.list()).thenReturn(instanceList);
        for (InstanceMetadata instanceMetadata : instanceList) {
            when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
        }

        createAVolume(3, VolumeType.SMALL, request.getVolumeId(), storagePoolId, domainId);

        ReserveSegUnitsResponse response = icImpl.reserveSegUnits(request);

        List<InstanceMetadata_Thrift> instancesFromRemote = response.getInstances();
        Assert.assertEquals(1, instancesFromRemote.size());
        Assert.assertEquals(3L, instancesFromRemote.get(0).getInstanceId());
    }

    @Test
    public void testCreateVolumeRequest() {
        CreateVolumeRequest createVolumeRequest = new CreateVolumeRequest();
        Assert.assertTrue(createVolumeRequest.getDomainId() == 0);
    }

    @Test
    public void testReserveSegUnits_Secondary_PSA_4Datanode3Segment() throws Exception {
        Set<Long> excludedInstanceIds = new HashSet<>();
        excludedInstanceIds.add(0L);
        excludedInstanceIds.add(1L);

        ReserveSegUnitsRequest request = new ReserveSegUnitsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setExcludedInstanceIds(excludedInstanceIds);
        request.setSegmentSize(segmentSize);
        request.setVolumeId(RequestIdBuilder.get());
        request.setSegmentUnitType(SegmentUnitType_Thrift.Normal);
        request.setNumberOfSegUnits(1);

        List<InstanceMetadata> instanceList = new ArrayList<>();
        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        for (int i = 0; i < 4; i++) {
            Group group = new Group();
            group.setGroupId(i);

            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            instanceMetadata.setDatanodeType(NORMAL);

            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 2; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                archives.add(archive);
                archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            instanceList.add(instanceMetadata);
        }
        // new Domain
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.setStoragePools(storagePoolIdList);

        // TestUtils.generateVolumeMetadata()
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        when(storageStore.list()).thenReturn(instanceList);
        for (InstanceMetadata instanceMetadata : instanceList) {
            when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
        }

        int volumeSize = 3;
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceId>>> segmentIndex2SegmentMap = new HashMap<>();
        for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++){
            Map<SegmentUnitType_Thrift, List<InstanceId>>  segment = new HashMap<>();

            List<InstanceId> arbiterInstanceList = new ArrayList<>();
            if (segmentIndex == 2){
                arbiterInstanceList.add(instanceList.get(3).getInstanceId());
            } else {
                arbiterInstanceList.add(instanceList.get(2).getInstanceId());
            }
            segment.put(SegmentUnitType_Thrift.Arbiter, arbiterInstanceList);

            List<InstanceId> normalInstanceList = new ArrayList<>();
            normalInstanceList.add(instanceList.get(0).getInstanceId());
            if (segmentIndex == 2){
                normalInstanceList.add(instanceList.get(2).getInstanceId());
            } else {
                normalInstanceList.add(instanceList.get(3).getInstanceId());
            }
            segment.put(SegmentUnitType_Thrift.Normal, normalInstanceList);

            segmentIndex2SegmentMap.put(segmentIndex, segment);
        }

        createAVolume(volumeSize, VolumeType.SMALL, request.getVolumeId(), storagePoolId, domainId, segmentIndex2SegmentMap);

        ReserveSegUnitsResponse response = icImpl.reserveSegUnits(request);

        List<InstanceMetadata_Thrift> instancesFromRemote = response.getInstances();
        Assert.assertEquals(2, instancesFromRemote.size());
        Assert.assertEquals(2L, instancesFromRemote.get(0).getInstanceId());
    }

    @Test
    public void testReserveSegUnits_Secondary_PSA_5Datanode3Segment() throws Exception {
        Set<Long> excludedInstanceIds = new HashSet<>();
        excludedInstanceIds.add(0L);
        excludedInstanceIds.add(1L);

        ReserveSegUnitsRequest request = new ReserveSegUnitsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setExcludedInstanceIds(excludedInstanceIds);
        request.setSegmentSize(segmentSize);
        request.setVolumeId(RequestIdBuilder.get());
        request.setSegmentUnitType(SegmentUnitType_Thrift.Normal);
        request.setNumberOfSegUnits(1);

        List<InstanceMetadata> instanceList = new ArrayList<>();
        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        for (int i = 0; i < 5; i++) {
            Group group = new Group();
            group.setGroupId(i);

            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            instanceMetadata.setDatanodeType(NORMAL);

            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 2; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                archives.add(archive);
                archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            instanceList.add(instanceMetadata);
        }
        // new Domain
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.setStoragePools(storagePoolIdList);

        // TestUtils.generateVolumeMetadata()
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        when(storageStore.list()).thenReturn(instanceList);
        for (InstanceMetadata instanceMetadata : instanceList) {
            when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
        }

        int volumeSize = 3;
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceId>>> segmentIndex2SegmentMap = new HashMap<>();
        for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++){
            Map<SegmentUnitType_Thrift, List<InstanceId>>  segment = new HashMap<>();

            List<InstanceId> arbiterInstanceList = new ArrayList<>();
            if (segmentIndex == 2){
                arbiterInstanceList.add(instanceList.get(3).getInstanceId());
            } else {
                arbiterInstanceList.add(instanceList.get(2).getInstanceId());
            }
            segment.put(SegmentUnitType_Thrift.Arbiter, arbiterInstanceList);

            List<InstanceId> normalInstanceList = new ArrayList<>();
            normalInstanceList.add(instanceList.get(0).getInstanceId());
            if (segmentIndex == 2){
                normalInstanceList.add(instanceList.get(2).getInstanceId());
            } else {
                normalInstanceList.add(instanceList.get(3).getInstanceId());
            }
            segment.put(SegmentUnitType_Thrift.Normal, normalInstanceList);

            segmentIndex2SegmentMap.put(segmentIndex, segment);
        }

        createAVolume(volumeSize, VolumeType.SMALL, request.getVolumeId(), storagePoolId, domainId, segmentIndex2SegmentMap);

        ReserveSegUnitsResponse response = icImpl.reserveSegUnits(request);

        List<InstanceMetadata_Thrift> instancesFromRemote = response.getInstances();
        Assert.assertEquals(3, instancesFromRemote.size());
        Assert.assertEquals(4L, instancesFromRemote.get(0).getInstanceId());
    }

    @Test
    public void testReserveSegUnits_Secondary_PSA_5Datanode3Segment_1DatanodeNoArchive() throws Exception {
        Set<Long> excludedInstanceIds = new HashSet<>();
        excludedInstanceIds.add(0L);
        excludedInstanceIds.add(1L);

        ReserveSegUnitsRequest request = new ReserveSegUnitsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setExcludedInstanceIds(excludedInstanceIds);
        request.setSegmentSize(segmentSize);
        request.setVolumeId(RequestIdBuilder.get());
        request.setSegmentUnitType(SegmentUnitType_Thrift.Normal);
        request.setNumberOfSegUnits(1);

        List<InstanceMetadata> instanceList = new ArrayList<>();
        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        for (int i = 0; i < 5; i++) {
            Group group = new Group();
            group.setGroupId(i);

            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            instanceMetadata.setDatanodeType(NORMAL);

            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 2; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                if (i == 4){
                    archive.setLogicalFreeSpace(0);
                }
                archives.add(archive);
                archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            instanceList.add(instanceMetadata);
        }
        // new Domain
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.setStoragePools(storagePoolIdList);

        // TestUtils.generateVolumeMetadata()
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        when(storageStore.list()).thenReturn(instanceList);
        for (InstanceMetadata instanceMetadata : instanceList) {
            when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
        }

        int volumeSize = 3;
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceId>>> segmentIndex2SegmentMap = new HashMap<>();
        for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++){
            Map<SegmentUnitType_Thrift, List<InstanceId>>  segment = new HashMap<>();

            List<InstanceId> arbiterInstanceList = new ArrayList<>();
            if (segmentIndex == 2){
                arbiterInstanceList.add(instanceList.get(3).getInstanceId());
            } else {
                arbiterInstanceList.add(instanceList.get(2).getInstanceId());
            }
            segment.put(SegmentUnitType_Thrift.Arbiter, arbiterInstanceList);

            List<InstanceId> normalInstanceList = new ArrayList<>();
            normalInstanceList.add(instanceList.get(0).getInstanceId());
            if (segmentIndex == 2){
                normalInstanceList.add(instanceList.get(2).getInstanceId());
            } else {
                normalInstanceList.add(instanceList.get(3).getInstanceId());
            }
            segment.put(SegmentUnitType_Thrift.Normal, normalInstanceList);

            segmentIndex2SegmentMap.put(segmentIndex, segment);
        }

        createAVolume(volumeSize, VolumeType.SMALL, request.getVolumeId(), storagePoolId, domainId, segmentIndex2SegmentMap);

        ReserveSegUnitsResponse response = icImpl.reserveSegUnits(request);

        List<InstanceMetadata_Thrift> instancesFromRemote = response.getInstances();
        Assert.assertEquals(2, instancesFromRemote.size());
        Assert.assertEquals(2L, instancesFromRemote.get(0).getInstanceId());
    }

    @Test
    public void testReserveSegUnits_Arbiter_PSA_4Datanode3Segment() throws Exception {
        Set<Long> excludedInstanceIds = new HashSet<>();
        excludedInstanceIds.add(0L);
        excludedInstanceIds.add(1L);

        ReserveSegUnitsRequest request = new ReserveSegUnitsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setExcludedInstanceIds(excludedInstanceIds);
        request.setSegmentSize(segmentSize);
        request.setVolumeId(RequestIdBuilder.get());
        request.setSegmentUnitType(SegmentUnitType_Thrift.Arbiter);
        request.setNumberOfSegUnits(1);

        List<InstanceMetadata> instanceList = new ArrayList<>();
        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        for (int i = 0; i < 4; i++) {
            Group group = new Group();
            group.setGroupId(i);

            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            instanceMetadata.setDatanodeType(NORMAL);

            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 2; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                archives.add(archive);
                archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            instanceList.add(instanceMetadata);
        }
        // new Domain
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.setStoragePools(storagePoolIdList);

        // TestUtils.generateVolumeMetadata()
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        when(storageStore.list()).thenReturn(instanceList);
        for (InstanceMetadata instanceMetadata : instanceList) {
            when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
        }

        int volumeSize = 3;
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceId>>> segmentIndex2SegmentMap = new HashMap<>();
        for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++){
            Map<SegmentUnitType_Thrift, List<InstanceId>>  segment = new HashMap<>();

            List<InstanceId> arbiterInstanceList = new ArrayList<>();
            if (segmentIndex == 2){
                arbiterInstanceList.add(instanceList.get(3).getInstanceId());
            } else {
                arbiterInstanceList.add(instanceList.get(2).getInstanceId());
            }
            segment.put(SegmentUnitType_Thrift.Arbiter, arbiterInstanceList);

            List<InstanceId> normalInstanceList = new ArrayList<>();
            normalInstanceList.add(instanceList.get(0).getInstanceId());
            if (segmentIndex == 2){
                normalInstanceList.add(instanceList.get(2).getInstanceId());
            } else {
                normalInstanceList.add(instanceList.get(3).getInstanceId());
            }
            segment.put(SegmentUnitType_Thrift.Normal, normalInstanceList);

            segmentIndex2SegmentMap.put(segmentIndex, segment);
        }

        createAVolume(volumeSize, VolumeType.SMALL, request.getVolumeId(), storagePoolId, domainId, segmentIndex2SegmentMap);

        ReserveSegUnitsResponse response = icImpl.reserveSegUnits(request);

        List<InstanceMetadata_Thrift> instancesFromRemote = response.getInstances();
        Assert.assertEquals(2, instancesFromRemote.size());
        Assert.assertEquals(3L, instancesFromRemote.get(0).getInstanceId());
    }

    @Test
    public void testReserveSegUnits_Arbiter_PSA_5Datanode3Segment() throws Exception {
        Set<Long> excludedInstanceIds = new HashSet<>();
        excludedInstanceIds.add(0L);
        excludedInstanceIds.add(1L);

        ReserveSegUnitsRequest request = new ReserveSegUnitsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setExcludedInstanceIds(excludedInstanceIds);
        request.setSegmentSize(segmentSize);
        request.setVolumeId(RequestIdBuilder.get());
        request.setSegmentUnitType(SegmentUnitType_Thrift.Arbiter);
        request.setNumberOfSegUnits(1);

        List<InstanceMetadata> instanceList = new ArrayList<>();
        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        for (int i = 0; i < 5; i++) {
            Group group = new Group();
            group.setGroupId(i);

            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            instanceMetadata.setDatanodeType(NORMAL);

            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 2; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                archives.add(archive);
                archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            instanceList.add(instanceMetadata);
        }
        // new Domain
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.setStoragePools(storagePoolIdList);

        // TestUtils.generateVolumeMetadata()
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        when(storageStore.list()).thenReturn(instanceList);
        for (InstanceMetadata instanceMetadata : instanceList) {
            when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
        }

        int volumeSize = 3;
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceId>>> segmentIndex2SegmentMap = new HashMap<>();
        for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++){
            Map<SegmentUnitType_Thrift, List<InstanceId>>  segment = new HashMap<>();

            List<InstanceId> arbiterInstanceList = new ArrayList<>();
            if (segmentIndex == 2){
                arbiterInstanceList.add(instanceList.get(3).getInstanceId());
            } else {
                arbiterInstanceList.add(instanceList.get(2).getInstanceId());
            }
            segment.put(SegmentUnitType_Thrift.Arbiter, arbiterInstanceList);

            List<InstanceId> normalInstanceList = new ArrayList<>();
            normalInstanceList.add(instanceList.get(0).getInstanceId());
            if (segmentIndex == 2){
                normalInstanceList.add(instanceList.get(2).getInstanceId());
            } else {
                normalInstanceList.add(instanceList.get(3).getInstanceId());
            }
            segment.put(SegmentUnitType_Thrift.Normal, normalInstanceList);

            segmentIndex2SegmentMap.put(segmentIndex, segment);
        }

        createAVolume(volumeSize, VolumeType.SMALL, request.getVolumeId(), storagePoolId, domainId, segmentIndex2SegmentMap);

        ReserveSegUnitsResponse response = icImpl.reserveSegUnits(request);

        List<InstanceMetadata_Thrift> instancesFromRemote = response.getInstances();
        Assert.assertEquals(2, instancesFromRemote.size());
        Assert.assertEquals(4L, instancesFromRemote.get(0).getInstanceId());
    }

    @Test
    public void testReserveSegUnits_Arbiter_PSA_5Datanode3Segment_1Simple() throws Exception {
        Set<Long> excludedInstanceIds = new HashSet<>();
        excludedInstanceIds.add(0L);
        excludedInstanceIds.add(1L);

        ReserveSegUnitsRequest request = new ReserveSegUnitsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setExcludedInstanceIds(excludedInstanceIds);
        request.setSegmentSize(segmentSize);
        request.setVolumeId(RequestIdBuilder.get());
        request.setSegmentUnitType(SegmentUnitType_Thrift.Arbiter);
        request.setNumberOfSegUnits(1);

        List<InstanceMetadata> instanceList = new ArrayList<>();
        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        for (int i = 0; i < 5; i++) {
            Group group = new Group();
            group.setGroupId(i);

            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            instanceMetadata.setDatanodeType(NORMAL);
            if (i == 2){
                instanceMetadata.setDatanodeType(SIMPLE);
            }

            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 2; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                archives.add(archive);
                archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            instanceList.add(instanceMetadata);
        }
        // new Domain
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.setStoragePools(storagePoolIdList);

        // TestUtils.generateVolumeMetadata()
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        when(storageStore.list()).thenReturn(instanceList);
        for (InstanceMetadata instanceMetadata : instanceList) {
            when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
        }

        int volumeSize = 3;
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceId>>> segmentIndex2SegmentMap = new HashMap<>();
        for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++){
            Map<SegmentUnitType_Thrift, List<InstanceId>>  segment = new HashMap<>();

            List<InstanceId> arbiterInstanceList = new ArrayList<>();
            if (segmentIndex == 2){
                arbiterInstanceList.add(instanceList.get(3).getInstanceId());
            } else {
                arbiterInstanceList.add(instanceList.get(2).getInstanceId());
            }
            segment.put(SegmentUnitType_Thrift.Arbiter, arbiterInstanceList);

            List<InstanceId> normalInstanceList = new ArrayList<>();
            normalInstanceList.add(instanceList.get(0).getInstanceId());
            if (segmentIndex == 2){
                normalInstanceList.add(instanceList.get(2).getInstanceId());
            } else {
                normalInstanceList.add(instanceList.get(3).getInstanceId());
            }
            segment.put(SegmentUnitType_Thrift.Normal, normalInstanceList);

            segmentIndex2SegmentMap.put(segmentIndex, segment);
        }

        createAVolume(volumeSize, VolumeType.SMALL, request.getVolumeId(), storagePoolId, domainId, segmentIndex2SegmentMap);

        ReserveSegUnitsResponse response = icImpl.reserveSegUnits(request);

        List<InstanceMetadata_Thrift> instancesFromRemote = response.getInstances();
        Assert.assertEquals(2, instancesFromRemote.size());
        Assert.assertEquals(2L, instancesFromRemote.get(0).getInstanceId());
    }

    @Test
    public void testReserveSegUnits_Secondary_PSS_4Datanode3Segment() throws Exception {
        Set<Long> excludedInstanceIds = new HashSet<>();
        excludedInstanceIds.add(2L);
        excludedInstanceIds.add(1L);

        ReserveSegUnitsRequest request = new ReserveSegUnitsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setExcludedInstanceIds(excludedInstanceIds);
        request.setSegmentSize(segmentSize);
        request.setVolumeId(RequestIdBuilder.get());
        request.setSegmentUnitType(SegmentUnitType_Thrift.Normal);
        request.setNumberOfSegUnits(1);

        List<InstanceMetadata> instanceList = new ArrayList<>();
        Long domainId = 10010L;
        Long storagePoolId = 10086L;
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        StoragePool storagePool = new StoragePool();
        storagePool.setPoolId(storagePoolId);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        storagePool.setArchivesInDataNode(archivesInDataNode);
        for (int i = 1; i < 5; i++) {
            Group group = new Group();
            group.setGroupId(i);

            InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
            instanceMetadata.setGroup(group);
            instanceMetadata.setCapacity(10 * segmentSize);
            instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
            instanceMetadata.setDatanodeStatus(OK);
            instanceMetadata.setDatanodeType(NORMAL);

            List<RawArchiveMetadata> archives = new ArrayList<>();
            for (int k = 0; k < 2; k++) {
                RawArchiveMetadata archive = new RawArchiveMetadata();
                archive.setArchiveId((long)k);
                archive.setStatus(ArchiveStatus.GOOD);
                archive.setStorageType(StorageType.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                archives.add(archive);
                archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
            }
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(domainId);
            instanceList.add(instanceMetadata);
        }
        // new Domain
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.setStoragePools(storagePoolIdList);

        // TestUtils.generateVolumeMetadata()
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
        when(storageStore.list()).thenReturn(instanceList);
        for (InstanceMetadata instanceMetadata : instanceList) {
            when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
        }

        int volumeSize = 2;
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceId>>> segmentIndex2SegmentMap = new HashMap<>();
        for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++){
            Map<SegmentUnitType_Thrift, List<InstanceId>>  segment = new HashMap<>();

            segment.put(SegmentUnitType_Thrift.Arbiter, new ArrayList<>());

            List<InstanceId> normalInstanceList = new ArrayList<>();
            normalInstanceList.add(instanceList.get(0).getInstanceId());
            normalInstanceList.add(instanceList.get(2).getInstanceId());
            normalInstanceList.add(instanceList.get(3).getInstanceId());
            segment.put(SegmentUnitType_Thrift.Normal, normalInstanceList);

            segmentIndex2SegmentMap.put(segmentIndex, segment);
        }

        createAVolume(volumeSize, VolumeType.REGULAR, request.getVolumeId(), storagePoolId, domainId, segmentIndex2SegmentMap);

        ReserveSegUnitsResponse response = icImpl.reserveSegUnits(request);

        List<InstanceMetadata_Thrift> instancesFromRemote = response.getInstances();
        Assert.assertEquals(2, instancesFromRemote.size());

        Assert.assertEquals(3L, instancesFromRemote.get(0).getInstanceId());
    }

    private void createAVolume(long size, VolumeType volumeType, long volumeId, long poolId, long domainId,
                               Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceId>>> segmentIndex2SegmentMap) {
        ObjectCounter<InstanceId> diskIndexer = new TreeSetObjectCounter<>();
        VolumeMetadata volumeMetadata = new VolumeMetadata(volumeId, volumeId,
                size, segmentSize, volumeType, CacheType.MEMORY, domainId, poolId);
        for (Map.Entry<Integer, Map<SegmentUnitType_Thrift, List<InstanceId>>> indexMapEntry : segmentIndex2SegmentMap.entrySet()) {
            int segIndex = indexMapEntry.getKey();
            SegId segId = new SegId(volumeMetadata.getVolumeId(), segIndex);
            Map<SegmentUnitType_Thrift, List<InstanceId>> map = indexMapEntry.getValue();
            List<InstanceId> arbiterUnits = map.get(SegmentUnitType_Thrift.Arbiter);
            List<InstanceId> segUnits = map.get(SegmentUnitType_Thrift.Normal);

            // build membership
            SegmentMembership membership;
            InstanceId primary = new InstanceId(segUnits.get(0));
            List<InstanceId> secondaries = new ArrayList<>();
            for (int i = 1; i <= volumeType.getNumSecondaries(); i++) {
                secondaries.add(new InstanceId(segUnits.get(i)));
            }
            List<InstanceId> arbiters = new ArrayList<>();
            for (int i = 0; i < volumeType.getNumArbiters(); i++) {
                arbiters.add(new InstanceId(arbiterUnits.get(i)));
            }
            if (arbiters.isEmpty()) {
                membership = new SegmentMembership(primary, secondaries);
            } else {
                membership = new SegmentMembership(primary, secondaries, arbiters);
            }

            // build segment meta data
            SegmentMetadata segment = new SegmentMetadata(segId, segIndex);

            // build segment units
            for (InstanceId arbiter : membership.getArbiters()) {
                SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
                        SegmentUnitStatus.Arbiter, volumeType, SegmentUnitType.Arbiter, CacheType.MEMORY);
                InstanceId instanceId = new InstanceId(arbiter);
                InstanceMetadata instance = storageStore.get(instanceId.getId());
                diskIndexer.increment(instanceId);
                int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

                segmentUnitMetadata.setInstanceId(instanceId);
                segmentUnitMetadata.setArchiveId(instance.getArchives().get(diskIndex).getArchiveId());
                segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
            }

            Set<InstanceId> normalUnits = new HashSet<>(membership.getSecondaries());
            normalUnits.add(primary);
            for (InstanceId normalUnit : normalUnits) {
                SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
                        normalUnit.equals(primary) ? SegmentUnitStatus.Primary : SegmentUnitStatus.Secondary,
                        volumeType, SegmentUnitType.Normal, CacheType.MEMORY);
                InstanceId instanceId = new InstanceId(normalUnit);
                InstanceMetadata instance = storageStore.get(instanceId.getId());
                diskIndexer.increment(instanceId);
                int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

                segmentUnitMetadata.setInstanceId(instanceId);
                long archiveId = instance.getArchives().get(diskIndex).getArchiveId();
                segmentUnitMetadata.setArchiveId(archiveId);
                segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
            }

            volumeMetadata.addSegmentMetadata(segment, membership);
        }
        volumeMetadata.setVolumeStatus(VolumeStatus.Available);
        volumeMetadata.setInAction(NULL);
        volumeStore.saveVolume(volumeMetadata);
    }


    private void createAVolume(long size, VolumeType volumeType, long volumeId, long poolId, long domainId)
            throws NotEnoughGroupException_Thrift,
            NotEnoughSpaceException_Thrift, NotEnoughNormalGroupException_Thrift, TException {
        ObjectCounter<InstanceId> diskIndexer = new TreeSetObjectCounter<>();
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> result = segmentUnitsDistributionManager
                .reserveVolume(size, volumeType, false, Integer.MAX_VALUE, poolId);
        logger.warn("reserve result {}", result);
        VolumeMetadata volumeMetadata = new VolumeMetadata(volumeId, volumeId,
                size, segmentSize, volumeType, CacheType.MEMORY, domainId, poolId);
        for (Map.Entry<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> indexMapEntry : result
                .entrySet()) {
            int segIndex = indexMapEntry.getKey();
            SegId segId = new SegId(volumeMetadata.getVolumeId(), segIndex);
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> map = indexMapEntry.getValue();
            List<InstanceIdAndEndPoint_Thrift> arbiterUnits = map.get(SegmentUnitType_Thrift.Arbiter);
            List<InstanceIdAndEndPoint_Thrift> segUnits = map.get(SegmentUnitType_Thrift.Normal);

            // build membership
            SegmentMembership membership;
            InstanceId primary = new InstanceId(segUnits.get(0).getInstanceId());
            List<InstanceId> secondaries = new ArrayList<>();
            for (int i = 1; i <= volumeType.getNumSecondaries(); i++) {
                secondaries.add(new InstanceId(segUnits.get(i).getInstanceId()));
            }
            List<InstanceId> arbiters = new ArrayList<>();
            for (int i = 0; i < volumeType.getNumArbiters(); i++) {
                arbiters.add(new InstanceId(arbiterUnits.get(i).getInstanceId()));
            }
            if (arbiters.isEmpty()) {
                membership = new SegmentMembership(primary, secondaries);
            } else {
                membership = new SegmentMembership(primary, secondaries, arbiters);
            }

            // build segment meta data
            SegmentMetadata segment = new SegmentMetadata(segId, segIndex);

            // build segment units
            for (InstanceId arbiter : membership.getArbiters()) {
                SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
                        SegmentUnitStatus.Arbiter, volumeType, SegmentUnitType.Arbiter, CacheType.MEMORY);
                InstanceId instanceId = new InstanceId(arbiter);
                InstanceMetadata instance = storageStore.get(instanceId.getId());
                diskIndexer.increment(instanceId);
                int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

                segmentUnitMetadata.setInstanceId(instanceId);
                segmentUnitMetadata.setArchiveId(instance.getArchives().get(diskIndex).getArchiveId());
                segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
            }

            Set<InstanceId> normalUnits = new HashSet<>(membership.getSecondaries());
            normalUnits.add(primary);
            for (InstanceId normalUnit : normalUnits) {
                SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
                        normalUnit.equals(primary) ? SegmentUnitStatus.Primary : SegmentUnitStatus.Secondary,
                        volumeType, SegmentUnitType.Normal, CacheType.MEMORY);
                InstanceId instanceId = new InstanceId(normalUnit);
                InstanceMetadata instance = storageStore.get(instanceId.getId());
                diskIndexer.increment(instanceId);
                int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

                segmentUnitMetadata.setInstanceId(instanceId);
                long archiveId = instance.getArchives().get(diskIndex).getArchiveId();
                segmentUnitMetadata.setArchiveId(archiveId);
                segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
            }

            volumeMetadata.addSegmentMetadata(segment, membership);
        }
        volumeMetadata.setVolumeStatus(VolumeStatus.Available);
        volumeMetadata.setInAction(NULL);
        volumeStore.saveVolume(volumeMetadata);
    }

}
