package py.infocenter.service;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import py.archive.ArchiveStatus;
import py.archive.ArchiveType;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.common.RequestIdBuilder;
import py.common.client.RequestResponseHelper;
import py.icshare.*;
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.MigrationRuleStatus;
import py.icshare.qos.MigrationRuleStore;
import py.infocenter.DBManager.BackupDBManager;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.qos.ApplyMigrateRuleTest;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.store.ServerNodeStore;
import py.infocenter.test.utils.StorageMemStore;
import py.instance.Group;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.infocenter.service.ReportArchivesRequest;
import py.thrift.infocenter.service.ReportArchivesResponse;
import py.thrift.share.*;

/**
 * A class includes some test for reporting archives.
 * 
 * @author zjm
 * 
 */
public class ReportArchivesTest extends TestBase {
    private StorageMemStore storageMemStore = new StorageMemStore();

    @Mock
    private StoragePoolStore storagePoolStore;
    @Mock
    private ServerNodeStore serverNodeStore;
    @Mock
    private DomainStore domainStore;

    private InformationCenterImpl icImpl;
    private long segmentSize = 1L;
    private int groupCount = 5;

    @Mock
    private BackupDBManager backupDBManager;

    @Mock
    private MigrationRuleStore migrationRuleStore;

    @Mock
    private SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager;

    @Before
    public void init() throws Exception {
        super.init();
        InfoCenterAppContext appContext = mock(InfoCenterAppContext.class);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        MigrationRuleInformation migrationRuleInformation = ApplyMigrateRuleTest.buildMigrateRuleInformation(1L,
                MigrationRuleStatus.AVAILABLE);

        when(migrationRuleStore.get(anyLong())).thenReturn(migrationRuleInformation);
        icImpl = new InformationCenterImpl();
        icImpl.setStorageStore(storageMemStore);
        icImpl.setGroupCount(groupCount);
        icImpl.setDomainStore(domainStore);
        icImpl.setStoragePoolStore(storagePoolStore);
        icImpl.setBackupDBManager(backupDBManager);
        icImpl.setAppContext(appContext);
        icImpl.setMigrationRuleStore(migrationRuleStore);
        icImpl.setServerNodeStore(serverNodeStore);
        icImpl.setSegmentUnitsDistributionManager(segmentUnitsDistributionManager);
    }

    @Test
    public void testSucceedToReportArchives() throws Exception {
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);

        ReportDBResponse_Thrift reportDBResponse = mock(ReportDBResponse_Thrift.class);
        // for domain
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        // for storagePool
        List<StoragePool> storagePools = new ArrayList<>();
        storagePools.add(storagePool);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        for (long i = 0; i < 5; i++) {
            datanodeIds.add(i);
            for (long k = 0; k < 2; k++) {
                archivesInDataNode.put(i, k);
            }
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(domain.timePassedLongEnough(any(Long.class))).thenReturn(false);
        when(storagePool.timePassedLongEnough(any(Long.class))).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(backupDBManager.passedRecoveryTime()).thenReturn(true);
        when(backupDBManager.process(any(ReportDBRequest_Thrift.class))).thenReturn(reportDBResponse);
        when(migrationRuleStore.get(anyLong())).thenReturn(null);

        for (long i = 0; i < 5; i++) {
            ReportArchivesRequest request = new ReportArchivesRequest();
            request.setRequestId(RequestIdBuilder.get());
            InstanceMetadata_Thrift instanceToRemote = new InstanceMetadata_Thrift();

            List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
            for (long k = 0; k < 2; k++) {
                ArchiveMetadata_Thrift archive = new ArchiveMetadata_Thrift();
                archive.setDevName("test");
                archive.setArchiveId(k);
                archive.setStatus(ArchiveStatus_Thrift.GOOD);
                archive.setStoragetype(StorageType_Thrift.SATA);
                archive.setType(ArchiveType_Thrift.RAW_DISK);
                archive.setCreatedBy("test");
                archive.setUpdatedBy("test");
                archive.setCreatedTime(123L);
                archive.setUpdatedTime(123L);
                archive.setSerialNumber("456");
                archive.setSlotNo("789");

                archive.setLogicalSpace(3 * segmentSize);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                archives.add(archive);
            }
            instanceToRemote.setInstanceDomain(new InstanceDomain_Thrift(domainId));
            instanceToRemote.setArchiveMetadata(archives);
            instanceToRemote.setDatanodeStatus(DatanodeStatus_Thrift.OK);
            instanceToRemote.setDatanodeType(DatanodeType_Thrift.NORMAL);

            instanceToRemote.setCapacity(3 * segmentSize * 2);
            instanceToRemote.setFreeSpace(3 * segmentSize *2 );
            instanceToRemote.setLogicalCapacity(3 * segmentSize *2);

//            instanceToRemote.setCapacity(1L);
            instanceToRemote.setInstanceId(i);
            request.setInstance(instanceToRemote);
            ReportDBRequest_Thrift reportDBRequest = new ReportDBRequest_Thrift();
            request.setReportDBRequest(reportDBRequest);

            ReportArchivesResponse response = icImpl.reportArchives(request);
            Assert.assertEquals(i, response.getGroup().getGroupId());
            NextActionInfo_Thrift datandeNextActionInfo = response.getDatanodeNextAction();
            assertTrue(datandeNextActionInfo.getNextAction() == NextAction_Thrift.KEEP);
            for (Entry<Long, NextActionInfo_Thrift> entry : response.getArchiveIdMapNextAction().entrySet()) {
                NextActionInfo_Thrift archiveNextAction = entry.getValue();
                assertTrue(archiveNextAction.getNextAction() == NextAction_Thrift.KEEP);
            }
            Map<Long, PageMigrationSpeedInfo_Thrift> archiveIdMapMigrationSpeed = response.getArchiveIdMapMigrationSpeed();
            assertEquals(2, archiveIdMapMigrationSpeed.size());
            PageMigrationSpeedInfo_Thrift archive1MigrationSpeed = archiveIdMapMigrationSpeed.get(0L);
            assertEquals(RawArchiveMetadata.DEFAULT_MAX_MIGRATION_SPEED, archive1MigrationSpeed.getMaxMigrationSpeed());
            PageMigrationSpeedInfo_Thrift archive2MigrationSpeed = archiveIdMapMigrationSpeed.get(0L);
            assertEquals(RawArchiveMetadata.DEFAULT_MAX_MIGRATION_SPEED, archive2MigrationSpeed.getMaxMigrationSpeed());

            InstanceMetadata instance = RequestResponseHelper.buildInstanceFrom(instanceToRemote);
            logger.warn("-------------instance :{}", instance);
            assertEquals(instance.getCapacity(), 3 * segmentSize * 2);
            assertEquals(instance.getFreeSpace(), 3 * segmentSize * 2);
            assertEquals(instance.getLogicalCapacity(), 3 * segmentSize * 2);

            List<RawArchiveMetadata> rawArchiveMetadataList = instance.getArchives();
            for (RawArchiveMetadata rawArchiveMetadata : rawArchiveMetadataList){
                assertEquals(rawArchiveMetadata.getSerialNumber(), "456");
                assertEquals(rawArchiveMetadata.getDeviceName(), "test");
                assertEquals(rawArchiveMetadata.getStatus(), ArchiveStatus.GOOD);
                assertEquals(rawArchiveMetadata.getStorageType(), StorageType.SATA);
                assertEquals(rawArchiveMetadata.getArchiveType(), ArchiveType.RAW_DISK);
                assertEquals(rawArchiveMetadata.getCreatedTime(), 123);
                assertEquals(rawArchiveMetadata.getUpdatedBy(), "test");
                assertEquals(rawArchiveMetadata.getLogicalSpace(), 3 * segmentSize);
                assertEquals(rawArchiveMetadata.getSlotNo(), "789");
                assertEquals(rawArchiveMetadata.getLogicalFreeSpace(), 3 * segmentSize);
            }
            instance.setGroup(RequestResponseHelper.buildGroupFrom(response.getGroup()));
        }

        long[] capacityArray = { 3L, 2L, 5L, 1L, 4L, 10L, 3L, 6L, 7L, 9L };
        int[] groupArray = { 0, 1, 2, 3, 4, 3, 1, 0, 4, 1 };
        for (int i = 0; i < 10; i++) {
            ReportArchivesRequest request = new ReportArchivesRequest();
            request.setRequestId(RequestIdBuilder.get());
            InstanceMetadata_Thrift instanceToRemote = new InstanceMetadata_Thrift();
            instanceToRemote.setArchiveMetadata(new ArrayList<>());
            instanceToRemote.setCapacity(capacityArray[i]);
            instanceToRemote.setInstanceId((long) i);
            instanceToRemote.setInstanceDomain(new InstanceDomain_Thrift());
            instanceToRemote.setDatanodeType(DatanodeType_Thrift.NORMAL);
            request.setInstance(instanceToRemote);
            ReportDBRequest_Thrift reportDBRequest = new ReportDBRequest_Thrift();
            request.setReportDBRequest(reportDBRequest);

            ReportArchivesResponse response = icImpl.reportArchives(request);
            Assert.assertEquals(groupArray[i], response.getGroup().getGroupId());
            assertEquals(0, response.getArchiveIdMapMigrationSpeedSize());

            InstanceMetadata instance = RequestResponseHelper.buildInstanceFrom(instanceToRemote);
            instance.setGroup(RequestResponseHelper.buildGroupFrom(response.getGroup()));
        }
    }

    @Test
    public void nullNewGroupAndNullOldGroup() throws Exception {
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);

        // for domain
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        // for storagePool
        List<StoragePool> storagePools = new ArrayList<>();
        storagePools.add(storagePool);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        for (long i = 0; i < 5; i++) {
            datanodeIds.add(i);
            for (long k = 0; k < 2; k++) {
                archivesInDataNode.put(i, k);
            }
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);

        InstanceMetadata_Thrift newInstance = new InstanceMetadata_Thrift();
        newInstance.setInstanceId(0L);
        newInstance.setGroup(null);
        newInstance.setCapacity(1L);

        List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
        for (long k = 0; k < 2; k++) {
            ArchiveMetadata_Thrift archive = new ArchiveMetadata_Thrift();
            archive.setArchiveId(k);
            archive.setStatus(ArchiveStatus_Thrift.GOOD);
            archive.setStoragetype(StorageType_Thrift.SATA);
            archive.setStoragePoolId(storagePoolId);
            archive.setLogicalFreeSpace(3 * segmentSize);
            archive.setType(ArchiveType_Thrift.RAW_DISK);
            archives.add(archive);
        }
        newInstance.setInstanceDomain(new InstanceDomain_Thrift(domainId));
        newInstance.setArchiveMetadata(archives);
        newInstance.setDatanodeType(DatanodeType_Thrift.NORMAL);

        ReportArchivesRequest request = new ReportArchivesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setInstance(newInstance);

        ReportArchivesResponse response = icImpl.reportArchives(request);

        Assert.assertNotNull(response.getGroup());
    }

    @Test
    public void nullNewGroupAndNotNullOldGroup() throws Exception {
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);

        // for domain
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        // for storagePool
        List<StoragePool> storagePools = new ArrayList<>();
        storagePools.add(storagePool);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        for (long i = 0; i < 5; i++) {
            datanodeIds.add(i);
            for (long k = 0; k < 2; k++) {
                archivesInDataNode.put(i, k);
            }
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);

        InstanceMetadata_Thrift newInstance = new InstanceMetadata_Thrift();
        newInstance.setInstanceId(0L);
        newInstance.setGroup(null);
        newInstance.setCapacity(1L);

        List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
        for (long k = 0; k < 2; k++) {
            ArchiveMetadata_Thrift archive = new ArchiveMetadata_Thrift();
            archive.setArchiveId(k);
            archive.setStatus(ArchiveStatus_Thrift.GOOD);
            archive.setStoragetype(StorageType_Thrift.SATA);
            archive.setStoragePoolId(storagePoolId);
            archive.setLogicalFreeSpace(3 * segmentSize);
            archive.setType(ArchiveType_Thrift.RAW_DISK);
            archives.add(archive);
        }
        newInstance.setInstanceDomain(new InstanceDomain_Thrift(domainId));
        newInstance.setArchiveMetadata(archives);
        newInstance.setDatanodeType(DatanodeType_Thrift.NORMAL);

        InstanceMetadata oldInstance = new InstanceMetadata(new InstanceId(0L));
        oldInstance.setGroup(new Group(0));

        ReportArchivesRequest request = new ReportArchivesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setInstance(newInstance);

        storageMemStore.save(oldInstance);
        ReportArchivesResponse response = icImpl.reportArchives(request);

        Assert.assertEquals(0, response.getGroup().getGroupId());
    }

    @Test
    public void notNullNewGroupAndNullOldGroup() throws Exception {
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);

        // for domain
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        // for storagePool
        List<StoragePool> storagePools = new ArrayList<>();
        storagePools.add(storagePool);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        for (long i = 0; i < 5; i++) {
            datanodeIds.add(i);
            for (long k = 0; k < 2; k++) {
                archivesInDataNode.put(i, k);
            }
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);

        InstanceMetadata_Thrift newInstance = new InstanceMetadata_Thrift();
        newInstance.setInstanceId(0L);
        newInstance.setGroup(new Group_Thrift(0));
        newInstance.setCapacity(1L);

        List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
        for (long k = 0; k < 2; k++) {
            ArchiveMetadata_Thrift archive = new ArchiveMetadata_Thrift();
            archive.setArchiveId(k);
            archive.setStatus(ArchiveStatus_Thrift.GOOD);
            archive.setStoragetype(StorageType_Thrift.SATA);
            archive.setStoragePoolId(storagePoolId);
            archive.setLogicalFreeSpace(3 * segmentSize);
            archive.setType(ArchiveType_Thrift.RAW_DISK);
            archives.add(archive);
        }
        newInstance.setInstanceDomain(new InstanceDomain_Thrift(domainId));
        newInstance.setArchiveMetadata(archives);
        newInstance.setDatanodeType(DatanodeType_Thrift.NORMAL);

        ReportArchivesRequest request = new ReportArchivesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setInstance(newInstance);

        ReportArchivesResponse response = icImpl.reportArchives(request);

        Assert.assertEquals(0, response.getGroup().getGroupId());

        InstanceMetadata instanceFromStore = storageMemStore.get(newInstance.getInstanceId());
        Assert.assertEquals(0, instanceFromStore.getGroup().getGroupId());
    }

    @Test
    public void differentNotNullNewGroupAndNotNullOldGroup() throws Exception {
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);

        // for domain
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        // for storagePool
        List<StoragePool> storagePools = new ArrayList<>();
        storagePools.add(storagePool);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        for (long i = 0; i < 5; i++) {
            datanodeIds.add(i);
            for (long k = 0; k < 2; k++) {
                archivesInDataNode.put(i, k);
            }
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);

        InstanceMetadata_Thrift newInstance = new InstanceMetadata_Thrift();
        newInstance.setInstanceId(0L);
        newInstance.setGroup(new Group_Thrift(0));
        newInstance.setCapacity(1L);

        List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
        for (long k = 0; k < 2; k++) {
            ArchiveMetadata_Thrift archive = new ArchiveMetadata_Thrift();
            archive.setArchiveId(k);
            archive.setStatus(ArchiveStatus_Thrift.GOOD);
            archive.setStoragetype(StorageType_Thrift.SATA);
            archive.setStoragePoolId(storagePoolId);
            archive.setLogicalFreeSpace(3 * segmentSize);
            archive.setType(ArchiveType_Thrift.RAW_DISK);
            archives.add(archive);
        }
        newInstance.setInstanceDomain(new InstanceDomain_Thrift(domainId));
        newInstance.setArchiveMetadata(archives);
        newInstance.setDatanodeType(DatanodeType_Thrift.NORMAL);

        InstanceMetadata oldInstance = new InstanceMetadata(new InstanceId(0L));
        oldInstance.setGroup(new Group(1));

        ReportArchivesRequest request = new ReportArchivesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setInstance(newInstance);

        storageMemStore.save(oldInstance);

        ReportArchivesResponse response = icImpl.reportArchives(request);

        Assert.assertEquals(0, response.getGroup().getGroupId());
    }

    @Test
    public void sameNotNullNewGroupAndNotNullOldGroup() throws Exception {
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);

        // for domain
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        // for storagePool
        List<StoragePool> storagePools = new ArrayList<>();
        storagePools.add(storagePool);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        for (long i = 0; i < 5; i++) {
            datanodeIds.add(i);
            for (long k = 0; k < 2; k++) {
                archivesInDataNode.put(i, k);
            }
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);

        InstanceMetadata_Thrift newInstance = new InstanceMetadata_Thrift();
        newInstance.setInstanceId(0L);
        newInstance.setGroup(new Group_Thrift(0));
        newInstance.setCapacity(1L);

        List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
        for (long k = 0; k < 2; k++) {
            ArchiveMetadata_Thrift archive = new ArchiveMetadata_Thrift();
            archive.setArchiveId(k);
            archive.setStatus(ArchiveStatus_Thrift.GOOD);
            archive.setStoragetype(StorageType_Thrift.SATA);
            archive.setStoragePoolId(storagePoolId);
            archive.setLogicalFreeSpace(3 * segmentSize);
            archive.setType(ArchiveType_Thrift.RAW_DISK);
            archives.add(archive);
        }
        newInstance.setInstanceDomain(new InstanceDomain_Thrift(domainId));
        newInstance.setArchiveMetadata(archives);
        newInstance.setDatanodeType(DatanodeType_Thrift.NORMAL);

        InstanceMetadata oldInstance = new InstanceMetadata(new InstanceId(0L));
        oldInstance.setGroup(new Group(0));

        ReportArchivesRequest request = new ReportArchivesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setInstance(newInstance);

        storageMemStore.save(oldInstance);

        ReportArchivesResponse response = icImpl.reportArchives(request);

        Assert.assertEquals(0, response.getGroup().getGroupId());
    }

    /**
     * The case mocks datanodes reporting archives which has capacity 0. And we expect infocenter would assign these
     * archives to different groups but not a same one.
     */
    @Test
    public void reportArchivesWith0Capacity() throws Exception {

        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);

        // for domain
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        // for storagePool
        List<StoragePool> storagePools = new ArrayList<>();
        storagePools.add(storagePool);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        for (long i = 0; i < 5; i++) {
            datanodeIds.add(i);
            for (long k = 0; k < 2; k++) {
                archivesInDataNode.put(i, k);
            }
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);

        // create first instance with capacity 0
        InstanceMetadata_Thrift newInstance1 = new InstanceMetadata_Thrift();
        newInstance1.setInstanceId(0L);
        newInstance1.setCapacity(0L);
        newInstance1.setArchiveMetadata(new ArrayList<ArchiveMetadata_Thrift>());
        newInstance1.setInstanceDomain(new InstanceDomain_Thrift());
        newInstance1.setDatanodeType(DatanodeType_Thrift.NORMAL);

        // create second instance
        InstanceMetadata_Thrift newInstance2 = new InstanceMetadata_Thrift();
        newInstance2.setInstanceId(1L);
        newInstance2.setCapacity(0L);
        newInstance2.setArchiveMetadata(new ArrayList<ArchiveMetadata_Thrift>());
        newInstance2.setInstanceDomain(new InstanceDomain_Thrift());
        newInstance2.setDatanodeType(DatanodeType_Thrift.NORMAL);

        // report the first instance
        ReportArchivesRequest request = new ReportArchivesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setInstance(newInstance1);
        
        ReportArchivesResponse response = icImpl.reportArchives(request);
        Assert.assertEquals(0, response.getGroup().getGroupId());

        // after the first report, the first instance already exist in storage store
        newInstance1.setGroup(new Group_Thrift(0));

        // report the second instance
        request.setInstance(newInstance2);

        response = icImpl.reportArchives(request);
        // expect the second instance would not be assign to the next group from the group of first instance
        Assert.assertEquals(1, response.getGroup().getGroupId());
    }

    /**
     * As we know, datanodes run on different machines, they can report archives at the same time. This case mock that
     * and check if infocenter could handle this case.
     */
    @Test
    public void fiveDatanodeReportArchiveAtTheSameTime() throws Exception {
        // each instance belongs to different group, if the group is assigned, then set the group to true
        final boolean[] groupOccupied = new boolean[groupCount];
        final CountDownLatch mainLatch = new CountDownLatch(groupCount);
        final CountDownLatch threadLatch = new CountDownLatch(1);

        // datanode report archives at the same time
        for (int i = 0; i < groupCount; i++) {
            final long instanceId = i;
            groupOccupied[i] = false;

            Thread reportThread = new Thread() {
                @Override
                public void run() {

                    Long domainId = RequestIdBuilder.get();
                    Long storagePoolId = RequestIdBuilder.get();
                    Domain domain = mock(Domain.class);
                    StoragePool storagePool = mock(StoragePool.class);

                    // for domain
                    Set<Long> storagePoolIdList = new HashSet<>();
                    storagePoolIdList.add(storagePoolId);
                    List<Domain> domains = new ArrayList<>();
                    domains.add(domain);
                    Set<Long> datanodeIds = new HashSet<>();

                    // for storagePool
                    List<StoragePool> storagePools = new ArrayList<>();
                    storagePools.add(storagePool);
                    Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap
                            .<Long, Long> create());
                    for (long i = 0; i < 5; i++) {
                        datanodeIds.add(i);
                        for (long k = 0; k < 2; k++) {
                            archivesInDataNode.put(i, k);
                        }
                    }
                    try {
                        when(domainStore.listAllDomains()).thenReturn(domains);
                        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
                    } catch (Exception e) {
                        logger.error("caught not list domain or storage pool exception", e);
                    }
                    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
                    when(domain.getDataNodes()).thenReturn(datanodeIds);
                    when(domain.getDomainId()).thenReturn(domainId);
                    when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
                    when(storagePool.getPoolId()).thenReturn(storagePoolId);

                    InstanceMetadata_Thrift newInstance = new InstanceMetadata_Thrift();
                    newInstance.setInstanceId(instanceId);
                    newInstance.setCapacity(0L);
                    newInstance.setArchiveMetadata(new ArrayList<ArchiveMetadata_Thrift>());
                    newInstance.setInstanceDomain(new InstanceDomain_Thrift());
                    newInstance.setDatanodeType(DatanodeType_Thrift.NORMAL);
                    ReportArchivesRequest request = new ReportArchivesRequest();
                    request.setRequestId(RequestIdBuilder.get());
                    request.setInstance(newInstance);

                    try {
                        threadLatch.await();
                        ReportArchivesResponse response = icImpl.reportArchives(request);
                        groupOccupied[response.getGroup().getGroupId()] = true;
                    } catch (Exception e) {
                    } finally {
                        mainLatch.countDown();
                    }
                }
            };

            reportThread.start();
        }

        threadLatch.countDown();
        mainLatch.await();

        // check if all group is assigned
        for (int i = 0; i < groupCount; i++) {
            Assert.assertTrue(groupOccupied[i]);
        }
    }

    @Test
    public void testDataNodeKeepAction() throws Exception {
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);

        // for domain
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        for (long i = 0; i < 2; i++) {
            datanodeIds.add(i);
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(domain.isDeleting()).thenReturn(false);

        InstanceMetadata_Thrift newInstance = new InstanceMetadata_Thrift();
        newInstance.setInstanceId(0L);
        newInstance.setGroup(new Group_Thrift(0));
        newInstance.setCapacity(1L);

        List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
        newInstance.setInstanceDomain(new InstanceDomain_Thrift(domainId));
        newInstance.setArchiveMetadata(archives);
        newInstance.setDatanodeType(DatanodeType_Thrift.NORMAL);

        ReportArchivesRequest request = new ReportArchivesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setInstance(newInstance);

        ReportArchivesResponse response = icImpl.reportArchives(request);

        Assert.assertEquals(0, response.getGroup().getGroupId());
        NextActionInfo_Thrift datandeNextActionInfo = response.getDatanodeNextAction();
        assertTrue(datandeNextActionInfo.getNextAction() == NextAction_Thrift.KEEP);
    }

    @Test
    public void testDataNodeNewAllcAction() throws Exception {
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);

        // for domain
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        for (long i = 0; i < 2; i++) {
            datanodeIds.add(i);
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(domain.isDeleting()).thenReturn(false);
        when(domain.timePassedLongEnough(any(Long.class))).thenReturn(true);

        InstanceMetadata_Thrift newInstance = new InstanceMetadata_Thrift();
        newInstance.setInstanceId(0L);
        newInstance.setGroup(new Group_Thrift(0));
        newInstance.setCapacity(1L);

        List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
        newInstance.setArchiveMetadata(archives);
        newInstance.setInstanceDomain(null);
        newInstance.setDatanodeType(DatanodeType_Thrift.NORMAL);

        ReportArchivesRequest request = new ReportArchivesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setInstance(newInstance);

        ReportArchivesResponse response = icImpl.reportArchives(request);

        Assert.assertEquals(0, response.getGroup().getGroupId());
        NextActionInfo_Thrift datandeNextActionInfo = response.getDatanodeNextAction();
        assertTrue(datandeNextActionInfo.getNextAction() == NextAction_Thrift.NEWALLOC);
    }

    @Test
    public void testDataNodeFreeSelfAction() throws Exception {
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);

        // for domain
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        when(domainStore.listAllDomains()).thenReturn(domains);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(domain.isDeleting()).thenReturn(false);
        when(domain.timePassedLongEnough(any(Long.class))).thenReturn(true);
        when(backupDBManager.passedRecoveryTime()).thenReturn(true);

        InstanceMetadata_Thrift newInstance = new InstanceMetadata_Thrift();
        newInstance.setInstanceId(0L);
        newInstance.setGroup(new Group_Thrift(0));
        newInstance.setCapacity(1L);

        List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
        newInstance.setInstanceDomain(new InstanceDomain_Thrift(domainId));
        newInstance.setArchiveMetadata(archives);
        newInstance.setDatanodeType(DatanodeType_Thrift.NORMAL);

        ReportArchivesRequest request = new ReportArchivesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setInstance(newInstance);

        ReportArchivesResponse response = icImpl.reportArchives(request);

        Assert.assertEquals(0, response.getGroup().getGroupId());
        NextActionInfo_Thrift datandeNextActionInfo = response.getDatanodeNextAction();
        assertTrue(datandeNextActionInfo.getNextAction() == NextAction_Thrift.FREEMYSELF);
    }

    @Test
    public void testDataNodeChangeAction() throws Exception {
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);

        // for domain
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();
        for (long i = 0; i < 2; i++) {
            datanodeIds.add(i);
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId + 1);
        when(domain.isDeleting()).thenReturn(false);
        when(domain.timePassedLongEnough(any(Long.class))).thenReturn(true);

        InstanceMetadata_Thrift newInstance = new InstanceMetadata_Thrift();
        newInstance.setInstanceId(0L);
        newInstance.setGroup(new Group_Thrift(0));
        newInstance.setCapacity(1L);

        List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
        // diff with info center record
        newInstance.setInstanceDomain(new InstanceDomain_Thrift(domainId));
        newInstance.setArchiveMetadata(archives);
        newInstance.setDatanodeType(DatanodeType_Thrift.NORMAL);

        ReportArchivesRequest request = new ReportArchivesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setInstance(newInstance);

        ReportArchivesResponse response = icImpl.reportArchives(request);

        Assert.assertEquals(0, response.getGroup().getGroupId());
        NextActionInfo_Thrift datandeNextActionInfo = response.getDatanodeNextAction();
        assertTrue(datandeNextActionInfo.getNextAction() == NextAction_Thrift.CHANGE);
    }

    @Test
    public void testArchiveKeepAction() throws Exception {
        int archiveNumber = 5;
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);

        // for domain
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        // for storagePool
        List<StoragePool> storagePools = new ArrayList<>();
        storagePools.add(storagePool);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        for (long i = 0; i < 1; i++) {
            datanodeIds.add(i);
            for (long k = 0; k < archiveNumber; k++) {
                archivesInDataNode.put(i, k);
            }
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(domain.timePassedLongEnough(any(Long.class))).thenReturn(false);
        when(storagePool.timePassedLongEnough(any(Long.class))).thenReturn(false);
        when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(storagePool.isDeleting()).thenReturn(true);

        InstanceMetadata_Thrift newInstance = new InstanceMetadata_Thrift();
        newInstance.setInstanceId(0L);
        newInstance.setGroup(new Group_Thrift(0));
        newInstance.setCapacity(1L);

        List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
        for (int k = 0; k < archiveNumber; k++) {
            ArchiveMetadata_Thrift archive = new ArchiveMetadata_Thrift();
            archive.setArchiveId(k);
            archive.setStatus(ArchiveStatus_Thrift.GOOD);
            archive.setStoragetype(StorageType_Thrift.SATA);
            archive.setStoragePoolId(storagePoolId);
            archive.setLogicalFreeSpace(3 * segmentSize);
            archive.setType(ArchiveType_Thrift.RAW_DISK);
            archives.add(archive);
        }
        newInstance.setInstanceDomain(new InstanceDomain_Thrift(domainId));
        newInstance.setArchiveMetadata(archives);
        newInstance.setDatanodeType(DatanodeType_Thrift.NORMAL);

        ReportArchivesRequest request = new ReportArchivesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setInstance(newInstance);

        ReportArchivesResponse response = icImpl.reportArchives(request);

        Assert.assertEquals(0, response.getGroup().getGroupId());
        NextActionInfo_Thrift datandeNextActionInfo = response.getDatanodeNextAction();
        assertTrue(datandeNextActionInfo.getNextAction() == NextAction_Thrift.KEEP);
        for (Entry<Long, NextActionInfo_Thrift> entry : response.getArchiveIdMapNextAction().entrySet()) {
            NextActionInfo_Thrift archiveNextAction = entry.getValue();
            assertTrue(archiveNextAction.getNextAction() == NextAction_Thrift.KEEP);
        }
    }

    @Test
    public void testArchiveNewAllcAction() throws Exception {
        int archiveNumber = 5;
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);

        // for domain
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        // for storagePool
        List<StoragePool> storagePools = new ArrayList<>();
        storagePools.add(storagePool);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        for (long i = 0; i < 1; i++) {
            datanodeIds.add(i);
            for (long k = 0; k < archiveNumber; k++) {
                archivesInDataNode.put(i, k);
            }
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(domain.timePassedLongEnough(any(Long.class))).thenReturn(false);
        when(storagePool.timePassedLongEnough(any(Long.class))).thenReturn(true);
        when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(storagePool.isDeleting()).thenReturn(false);

        InstanceMetadata_Thrift newInstance = new InstanceMetadata_Thrift();
        newInstance.setInstanceId(0L);
        newInstance.setGroup(new Group_Thrift(0));
        newInstance.setCapacity(1L);

        List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
        for (int k = 0; k < archiveNumber; k++) {
            ArchiveMetadata_Thrift archive = new ArchiveMetadata_Thrift();
            archive.setArchiveId(k);
            archive.setStatus(ArchiveStatus_Thrift.GOOD);
            archive.setStoragetype(StorageType_Thrift.SATA);
            // archive.setStoragePoolId(storagePoolId);
            archive.setLogicalFreeSpace(3 * segmentSize);
            archive.setType(ArchiveType_Thrift.RAW_DISK);
            archives.add(archive);
        }
        newInstance.setInstanceDomain(new InstanceDomain_Thrift(domainId));
        newInstance.setArchiveMetadata(archives);
        newInstance.setDatanodeType(DatanodeType_Thrift.NORMAL);

        ReportArchivesRequest request = new ReportArchivesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setInstance(newInstance);

        ReportArchivesResponse response = icImpl.reportArchives(request);

        Assert.assertEquals(0, response.getGroup().getGroupId());
        NextActionInfo_Thrift datandeNextActionInfo = response.getDatanodeNextAction();
        assertTrue(datandeNextActionInfo.getNextAction() == NextAction_Thrift.KEEP);
        for (Entry<Long, NextActionInfo_Thrift> entry : response.getArchiveIdMapNextAction().entrySet()) {
            NextActionInfo_Thrift archiveNextAction = entry.getValue();
            assertTrue(archiveNextAction.getNextAction() == NextAction_Thrift.NEWALLOC);
        }
    }

    @Test
    public void testArchiveFreeSelfAction() throws Exception {
        int archiveNumber = 5;
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);

        // for domain
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        // for storagePool
        List<StoragePool> storagePools = new ArrayList<>();
        storagePools.add(storagePool);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        for (long i = 0; i < 1; i++) {
            datanodeIds.add(i);
            for (long k = 0; k < archiveNumber; k++) {
                archivesInDataNode.put(i, k + 10);
            }
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(domain.timePassedLongEnough(any(Long.class))).thenReturn(false);
        when(storagePool.timePassedLongEnough(any(Long.class))).thenReturn(true);
        when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(storagePool.isDeleting()).thenReturn(false);
        when(backupDBManager.passedRecoveryTime()).thenReturn(true);

        InstanceMetadata_Thrift newInstance = new InstanceMetadata_Thrift();
        newInstance.setInstanceId(0L);
        newInstance.setGroup(new Group_Thrift(0));
        newInstance.setCapacity(1L);

        List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
        for (int k = 0; k < archiveNumber; k++) {
            ArchiveMetadata_Thrift archive = new ArchiveMetadata_Thrift();
            archive.setArchiveId(k);
            archive.setStatus(ArchiveStatus_Thrift.GOOD);
            archive.setStoragetype(StorageType_Thrift.SATA);
            archive.setStoragePoolId(storagePoolId);
            archive.setLogicalFreeSpace(3 * segmentSize);
            archive.setType(ArchiveType_Thrift.RAW_DISK);
            archives.add(archive);
        }
        newInstance.setInstanceDomain(new InstanceDomain_Thrift(domainId));
        newInstance.setArchiveMetadata(archives);
        newInstance.setDatanodeType(DatanodeType_Thrift.NORMAL);

        ReportArchivesRequest request = new ReportArchivesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setInstance(newInstance);

        ReportArchivesResponse response = icImpl.reportArchives(request);

        Assert.assertEquals(0, response.getGroup().getGroupId());
        NextActionInfo_Thrift datandeNextActionInfo = response.getDatanodeNextAction();
        assertTrue(datandeNextActionInfo.getNextAction() == NextAction_Thrift.KEEP);
        for (Entry<Long, NextActionInfo_Thrift> entry : response.getArchiveIdMapNextAction().entrySet()) {
            NextActionInfo_Thrift archiveNextAction = entry.getValue();
            assertTrue(archiveNextAction.getNextAction() == NextAction_Thrift.FREEMYSELF);
        }
    }

    @Test
    public void testArchiveChangeAction() throws Exception {
        int archiveNumber = 5;
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);

        // for domain
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        // for storagePool
        List<StoragePool> storagePools = new ArrayList<>();
        storagePools.add(storagePool);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        for (long i = 0; i < 1; i++) {
            datanodeIds.add(i);
            for (long k = 0; k < archiveNumber; k++) {
                archivesInDataNode.put(i, k);
            }
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(domain.timePassedLongEnough(any(Long.class))).thenReturn(false);
        when(storagePool.timePassedLongEnough(any(Long.class))).thenReturn(true);
        when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
        when(storagePool.getPoolId()).thenReturn(storagePoolId + 10);
        when(storagePool.isDeleting()).thenReturn(false);

        InstanceMetadata_Thrift newInstance = new InstanceMetadata_Thrift();
        newInstance.setInstanceId(0L);
        newInstance.setGroup(new Group_Thrift(0));
        newInstance.setCapacity(1L);

        List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
        for (int k = 0; k < archiveNumber; k++) {
            ArchiveMetadata_Thrift archive = new ArchiveMetadata_Thrift();
            archive.setArchiveId(k);
            archive.setStatus(ArchiveStatus_Thrift.GOOD);
            archive.setStoragetype(StorageType_Thrift.SATA);
            archive.setStoragePoolId(storagePoolId);
            archive.setLogicalFreeSpace(3 * segmentSize);
            archive.setType(ArchiveType_Thrift.RAW_DISK);
            archives.add(archive);
        }
        newInstance.setInstanceDomain(new InstanceDomain_Thrift(domainId));
        newInstance.setArchiveMetadata(archives);
        newInstance.setDatanodeType(DatanodeType_Thrift.NORMAL);

        ReportArchivesRequest request = new ReportArchivesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setInstance(newInstance);

        ReportArchivesResponse response = icImpl.reportArchives(request);

        Assert.assertEquals(0, response.getGroup().getGroupId());
        NextActionInfo_Thrift datandeNextActionInfo = response.getDatanodeNextAction();
        assertTrue(datandeNextActionInfo.getNextAction() == NextAction_Thrift.KEEP);
        for (Entry<Long, NextActionInfo_Thrift> entry : response.getArchiveIdMapNextAction().entrySet()) {
            NextActionInfo_Thrift archiveNextAction = entry.getValue();
            assertTrue(archiveNextAction.getNextAction() == NextAction_Thrift.CHANGE);
        }
    }

    @Test
    public void testLongToDouble() {
        Long totalLogicSpace = 1024 * 1024 * 1024L;
        Long totalFreeSpace = 1000 * 1024 * 1024L;
        Double usedRatio = (((double)totalLogicSpace - totalFreeSpace) /  totalLogicSpace);
        assertTrue(usedRatio != 0);
    }

    @Test
    public void testSetSlot2Archives() throws Exception {
        storageMemStore.clearMemoryData();

        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);

        ReportDBResponse_Thrift reportDBResponse = mock(ReportDBResponse_Thrift.class);
        // for domain
        Set<Long> storagePoolIdList = new HashSet<>();
        storagePoolIdList.add(storagePoolId);
        List<Domain> domains = new ArrayList<>();
        domains.add(domain);
        Set<Long> datanodeIds = new HashSet<>();

        // for storagePool
        List<StoragePool> storagePools = new ArrayList<>();
        storagePools.add(storagePool);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long> create());
        for (long i = 0; i < 5; i++) {
            datanodeIds.add(i);
            for (long k = 0; k < 2; k++) {
                archivesInDataNode.put(i, k);
            }
        }
        when(domainStore.listAllDomains()).thenReturn(domains);
        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
        when(domain.getStoragePools()).thenReturn(storagePoolIdList);
        when(domain.getDataNodes()).thenReturn(datanodeIds);
        when(domain.getDomainId()).thenReturn(domainId);
        when(domain.timePassedLongEnough(any(Long.class))).thenReturn(false);
        when(storagePool.timePassedLongEnough(any(Long.class))).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        when(backupDBManager.passedRecoveryTime()).thenReturn(true);
        when(backupDBManager.process(any(ReportDBRequest_Thrift.class))).thenReturn(reportDBResponse);
        when(migrationRuleStore.get(anyLong())).thenReturn(null);

        ServerNode serverNode = new ServerNode();
        Set<DiskInfo> diskInfoSet = new HashSet<>();
        for (int i = 0; i < 3; i++){
            DiskInfo diskInfo = new DiskInfo();
            char disk = (char)((long)'a'+i);
            diskInfo.setSn("/dev/sd"+disk);
            diskInfo.setSlotNumber(String.valueOf(i));

            diskInfoSet.add(diskInfo);
        }
        serverNode.setDiskInfoSet(diskInfoSet);

        when(serverNodeStore.getServerNodeByIp(any())).thenReturn(serverNode);

        for (long i = 0; i < 5; i++) {
            ReportArchivesRequest request = new ReportArchivesRequest();
            request.setRequestId(RequestIdBuilder.get());
            InstanceMetadata_Thrift instanceToRemote = new InstanceMetadata_Thrift();

            List<ArchiveMetadata_Thrift> archives = new ArrayList<>();
            for (long k = 0; k < 2; k++) {
                ArchiveMetadata_Thrift archive = new ArchiveMetadata_Thrift();
                char disk = (char)((long)'a'+k);
                archive.setSerialNumber("/dev/sd"+disk);
                archive.setArchiveId(k);
                archive.setStatus(ArchiveStatus_Thrift.GOOD);
                archive.setStoragetype(StorageType_Thrift.SATA);
                archive.setStoragePoolId(storagePoolId);
                archive.setLogicalFreeSpace(3 * segmentSize);
                archive.setType(ArchiveType_Thrift.RAW_DISK);
                archives.add(archive);
            }
            instanceToRemote.setInstanceDomain(new InstanceDomain_Thrift(domainId));
            instanceToRemote.setArchiveMetadata(archives);
            instanceToRemote.setDatanodeStatus(DatanodeStatus_Thrift.OK);
            instanceToRemote.setEndpoint("10.0.2.79:1024");
            instanceToRemote.setDatanodeType(DatanodeType_Thrift.NORMAL);

            instanceToRemote.setCapacity(1L);
            instanceToRemote.setInstanceId(i);
            request.setInstance(instanceToRemote);
            ReportDBRequest_Thrift reportDBRequest = new ReportDBRequest_Thrift();
            request.setReportDBRequest(reportDBRequest);

            ReportArchivesResponse response = icImpl.reportArchives(request);
            Assert.assertEquals(i, response.getGroup().getGroupId());

            //verify slotNo
            InstanceMetadata insMetaDataRet = storageMemStore.get(instanceToRemote.getInstanceId());
            Assert.assertTrue(insMetaDataRet.getArchives().get(0).getSlotNo().equals("0"));
            Assert.assertTrue(insMetaDataRet.getArchives().get(1).getSlotNo().equals("1"));
//            Assert.assertTrue(insMetaDataRet.getArchives().get(0).getSlotNo().equals("1"));

            NextActionInfo_Thrift datandeNextActionInfo = response.getDatanodeNextAction();
            assertTrue(datandeNextActionInfo.getNextAction() == NextAction_Thrift.KEEP);
            for (Entry<Long, NextActionInfo_Thrift> entry : response.getArchiveIdMapNextAction().entrySet()) {
                NextActionInfo_Thrift archiveNextAction = entry.getValue();
                assertTrue(archiveNextAction.getNextAction() == NextAction_Thrift.KEEP);
            }
            Map<Long, PageMigrationSpeedInfo_Thrift> archiveIdMapMigrationSpeed = response.getArchiveIdMapMigrationSpeed();
            assertEquals(2, archiveIdMapMigrationSpeed.size());
            PageMigrationSpeedInfo_Thrift archive1MigrationSpeed = archiveIdMapMigrationSpeed.get(0L);
            assertEquals(RawArchiveMetadata.DEFAULT_MAX_MIGRATION_SPEED, archive1MigrationSpeed.getMaxMigrationSpeed());
            PageMigrationSpeedInfo_Thrift archive2MigrationSpeed = archiveIdMapMigrationSpeed.get(0L);
            assertEquals(RawArchiveMetadata.DEFAULT_MAX_MIGRATION_SPEED, archive2MigrationSpeed.getMaxMigrationSpeed());

            InstanceMetadata instance = RequestResponseHelper.buildInstanceFrom(instanceToRemote);
            instance.setGroup(RequestResponseHelper.buildGroupFrom(response.getGroup()));
        }

        long[] capacityArray = { 3L, 2L, 5L, 1L, 4L, 10L, 3L, 6L, 7L, 9L };
        int[] groupArray = { 0, 1, 2, 3, 4, 3, 1, 0, 4, 1 };
        for (int i = 0; i < 10; i++) {
            ReportArchivesRequest request = new ReportArchivesRequest();
            request.setRequestId(RequestIdBuilder.get());
            InstanceMetadata_Thrift instanceToRemote = new InstanceMetadata_Thrift();
            instanceToRemote.setArchiveMetadata(new ArrayList<>());
            instanceToRemote.setCapacity(capacityArray[i]);
            instanceToRemote.setInstanceId((long) i);
            instanceToRemote.setInstanceDomain(new InstanceDomain_Thrift());
            instanceToRemote.setDatanodeType(DatanodeType_Thrift.NORMAL);
            request.setInstance(instanceToRemote);
            ReportDBRequest_Thrift reportDBRequest = new ReportDBRequest_Thrift();
            request.setReportDBRequest(reportDBRequest);

            ReportArchivesResponse response = icImpl.reportArchives(request);
            Assert.assertEquals(groupArray[i], response.getGroup().getGroupId());
            assertEquals(0, response.getArchiveIdMapMigrationSpeedSize());

            InstanceMetadata instance = RequestResponseHelper.buildInstanceFrom(instanceToRemote);
            instance.setGroup(RequestResponseHelper.buildGroupFrom(response.getGroup()));
        }
    }

}
