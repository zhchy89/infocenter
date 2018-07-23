package py.infocenter.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.icshare.StoragePoolStrategy.Capacity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import junit.framework.Assert;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import py.app.context.AppContext;
import py.archive.ArchiveType;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.common.RequestIdBuilder;
import py.common.client.RequestResponseHelper;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePool;
import py.icshare.StoragePoolStore;
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.MigrationRuleStatus;
import py.icshare.qos.MigrationRuleStore;
import py.infocenter.DBManager.BackupDBManager;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.qos.ApplyMigrateRuleTest;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.test.TestUtils;
import py.thrift.share.*;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

/**
 * @author sxl
 */

public class ProcessStoragePoolTester extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(ProcessStoragePoolTester.class);

    public final String TESTING_STORAGE_POOL_NAME = "TestingStoragePool";
    public final long TESTING_STORAGE_POOL_ID = 1;
    public final long TESTING_DOMAIN_ID = 1;
    public final long TESTING_ARCHIVE_ID = 1;
    public final long TESTING_VOLUME_ID = 1;
    @Mock
    private StorageStore storageStore;
    @Mock
    private VolumeStore volumeStore;
    @Mock
    private DomainStore domainStore;
    @Mock
    private InfoCenterAppContext appContext;
    @Mock
    private StoragePoolStore storagePoolStore;
    @Mock
    private BackupDBManager backupDBManager;

    @Mock
    private MigrationRuleStore migrationRuleStore;

    private InformationCenterImpl icImpl;

    @Before
    public void init() throws Exception {
        super.init();
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        MigrationRuleInformation migrationRuleInformation = ApplyMigrateRuleTest.buildMigrateRuleInformation(1L, MigrationRuleStatus.AVAILABLE);
        when(migrationRuleStore.get(anyLong())).thenReturn(migrationRuleInformation);

        icImpl = new InformationCenterImpl();
        icImpl.setStorageStore(storageStore);
        icImpl.setVolumeStore(volumeStore);
        icImpl.setDomainStore(domainStore);
        icImpl.setAppContext(appContext);
        icImpl.setStoragePoolStore(storagePoolStore);
        icImpl.setBackupDBManager(backupDBManager);
        icImpl.setSegmentSize(1);
        icImpl.setMigrationRuleStore(migrationRuleStore);
    }

    @Test(expected = ServiceIsNotAvailable_Thrift.class)
    public void test_createStoragePool_serviceSuspend() throws Exception {
        CreateStoragePoolRequest_Thrift request = Mockito.mock(CreateStoragePoolRequest_Thrift.class);
        when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
        icImpl.createStoragePool(request);
        Assert.fail();
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void test_createStoragePool_invalidInput_storagePollNotSetted() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        CreateStoragePoolRequest_Thrift request1 = new CreateStoragePoolRequest_Thrift();
        request1.setRequestId(RequestIdBuilder.get());
        request1.setStoragePool(null);

        icImpl.createStoragePool(request1);
        Assert.fail();
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void test_createStoragePool_invalidInput_domainNotSetted() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        CreateStoragePoolRequest_Thrift request = new CreateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        StoragePool_Thrift tStoragePool = new StoragePool_Thrift();
        tStoragePool.setPoolId(RequestIdBuilder.get());
        tStoragePool.setPoolName(TestBase.getRandomString(6));
        tStoragePool.setDescription(TestBase.getRandomString(15));
        tStoragePool.setStrategy(StoragePoolStrategy_Thrift.Capacity);
        tStoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
        tStoragePool.setVolumeIds(TestUtils.buildIdSet(3));
        tStoragePool.setStatus(Status_Thrift.Available);
        tStoragePool.setLastUpdateTime(System.currentTimeMillis());
        request.setStoragePool(tStoragePool);

        icImpl.createStoragePool(request);
        Assert.fail();
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void test_createStoragePool_invalidInput_poolIdNotSetted() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        CreateStoragePoolRequest_Thrift request = new CreateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        StoragePool_Thrift tStoragePool = new StoragePool_Thrift();
        tStoragePool.setDomainId(RequestIdBuilder.get());
        tStoragePool.setPoolName(TestBase.getRandomString(6));
        tStoragePool.setDescription(TestBase.getRandomString(15));
        tStoragePool.setStrategy(StoragePoolStrategy_Thrift.Capacity);
        tStoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
        tStoragePool.setVolumeIds(TestUtils.buildIdSet(3));
        tStoragePool.setStatus(Status_Thrift.Available);
        tStoragePool.setLastUpdateTime(System.currentTimeMillis());
        request.setStoragePool(tStoragePool);

        icImpl.createStoragePool(request);
        Assert.fail();
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void test_createStoragePool_invalidInput_poolNameNotSetted() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        CreateStoragePoolRequest_Thrift request = new CreateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        StoragePool_Thrift tStoragePool = new StoragePool_Thrift();
        tStoragePool.setDomainId(RequestIdBuilder.get());
        tStoragePool.setPoolId(RequestIdBuilder.get());
        tStoragePool.setDescription(TestBase.getRandomString(15));
        tStoragePool.setStrategy(StoragePoolStrategy_Thrift.Capacity);
        tStoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
        tStoragePool.setVolumeIds(TestUtils.buildIdSet(3));
        tStoragePool.setStatus(Status_Thrift.Available);
        tStoragePool.setLastUpdateTime(System.currentTimeMillis());
        request.setStoragePool(tStoragePool);

        icImpl.createStoragePool(request);
        Assert.fail();
    }

    @Test(expected = DomainNotExistedException_Thrift.class)
    public void test_createStoragePool_domainNotExisted() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(null);

        CreateStoragePoolRequest_Thrift request = getCreatingRequest();
        icImpl.createStoragePool(request);
        Assert.fail();
    }

    @Test(expected = StoragePoolExistedException_Thrift.class)
    public void test_createStoragePool_poolAlreadyExisted_byId() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        Domain domain = new Domain();
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(new StoragePool());

        CreateStoragePoolRequest_Thrift request = getCreatingRequest();
        icImpl.createStoragePool(request);
        Assert.fail();
    }

    @Test(expected = StoragePoolNameExistedException_Thrift.class)
    public void test_createStoragePool_poolAlreadyExisted_byName_sameDomain() throws Exception {
        long domainId = 1L;
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
        List<StoragePool> allStoragePoolForTesting = new ArrayList<>();
        StoragePool storagePoolForTesting = RequestResponseHelper.buildStoragePoolFromThrift(getTestingStoragePool());
        storagePoolForTesting.setDomainId(domainId);
        storagePoolForTesting.setName(TESTING_STORAGE_POOL_NAME);
        allStoragePoolForTesting.add(storagePoolForTesting);
        when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePoolForTesting);

        //now create a storagePool
        StoragePool_Thrift tStoragePool = getTestingStoragePool();
        tStoragePool.setDomainId(domainId);
        CreateStoragePoolRequest_Thrift request = new CreateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        request.setStoragePool(tStoragePool);

        icImpl.createStoragePool(request);
        Assert.fail();
    }

    @Test
    public void test_createStoragePool_poolAlreadyExisted_byName_differentDomain() throws Exception {
        long existPoolDomainId = 1L;
        long toCreatePoolDomainId = 2L;

        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
        List<StoragePool> allStoragePoolForTesting = new ArrayList<>();
        StoragePool storagePoolForTesting = RequestResponseHelper.buildStoragePoolFromThrift(getTestingStoragePool());
        storagePoolForTesting.setDomainId(existPoolDomainId);
        storagePoolForTesting.setName(TESTING_STORAGE_POOL_NAME);
        allStoragePoolForTesting.add(storagePoolForTesting);
        when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePoolForTesting);

        //now create a storagePool
        StoragePool_Thrift tStoragePool = new StoragePool_Thrift();
        tStoragePool.setDomainId(toCreatePoolDomainId);
        tStoragePool.setPoolName(TESTING_STORAGE_POOL_NAME);
        tStoragePool.setPoolId(RequestIdBuilder.get());
        tStoragePool.setStrategy(StoragePoolStrategy_Thrift.Capacity);
        CreateStoragePoolRequest_Thrift request = new CreateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        request.setStoragePool(tStoragePool);

        try {
            icImpl.createStoragePool(request);
        } catch (Exception e) {
            logger.error("Caught an unexpected exception", e);
            Assert.fail();
        }

        Assert.assertEquals(1, storagePoolStore.listAllStoragePools().size());
    }

    @Test
    public void test_createStoragePool_archiveInDatanodeIsEmpty() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
        List<StoragePool> allStoragePoolForTesting = new ArrayList<>();
        StoragePool storagePoolForTesting = RequestResponseHelper.buildStoragePoolFromThrift(getTestingStoragePool());
        storagePoolForTesting.setName("NotExistedName");
        storagePoolForTesting.setArchivesInDataNode(TestUtils.buildMultiMap(3, 4));
        allStoragePoolForTesting.add(storagePoolForTesting);
        when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePoolForTesting);

        CreateStoragePoolRequest_Thrift request = getCreatingRequest();
        request.getStoragePool().setVolumeIds(new HashSet<Long>());
        request.getStoragePool().setArchivesInDatanode(new HashMap<Long, Set<Long>>());
        try {
            icImpl.createStoragePool(request);
        } catch (TException e) {
            logger.error("Caught an unexpected exception", e);
            Assert.fail();
        }

        Assert.assertEquals(1, storagePoolStore.listAllStoragePools().size());
    }

    @Test
    public void test_createStoragePool_archiveNotFound_notInStorageStore() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
        List<StoragePool> allStoragePoolForTesting = new ArrayList<>();
        StoragePool storagePoolForTesting = RequestResponseHelper.buildStoragePoolFromThrift(getTestingStoragePool());
        storagePoolForTesting.setName("NotExistedName");
        storagePoolForTesting.setArchivesInDataNode(TestUtils.buildMultiMap(3, 4));
        allStoragePoolForTesting.add(storagePoolForTesting);
        when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePoolForTesting);

        CreateStoragePoolRequest_Thrift request = getCreatingRequest();
        request.getStoragePool().setVolumeIds(new HashSet<Long>());
        when(storageStore.get(Mockito.anyLong())).thenReturn(null);
        try {
            icImpl.createStoragePool(request);
            Assert.fail();
        } catch (ArchiveNotFoundException_Thrift e) {
        } catch (TException e) {
            logger.error("Caught an unexpected exception", e);
            Assert.fail();
        }
        Mockito.verify(storageStore, Mockito.atLeastOnce()).get(Mockito.anyLong());
    }

    @Test
    public void test_createStoragePool_archiveNotFound_notInSpecifiedDomain() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        Domain domain = new Domain();
        domain.setDomainId(TESTING_DOMAIN_ID);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);

        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
        List<StoragePool> allStoragePoolForTesting = new ArrayList<>();
        StoragePool storagePoolForTesting = RequestResponseHelper.buildStoragePoolFromThrift(getTestingStoragePool());
        storagePoolForTesting.setName("NotExistedName");
        storagePoolForTesting.setArchivesInDataNode(TestUtils.buildMultiMap(3, 4));
        allStoragePoolForTesting.add(storagePoolForTesting);
        when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePoolForTesting);

        CreateStoragePoolRequest_Thrift request = getCreatingRequest();
        request.getStoragePool().setVolumeIds(new HashSet<Long>());
        InstanceMetadata datanode = new InstanceMetadata(new InstanceId(new Random().nextLong()));
        datanode.setDomainId(TESTING_DOMAIN_ID + 1);
        when(storageStore.get(Mockito.anyLong())).thenReturn(datanode);
        try {
            icImpl.createStoragePool(request);
            Assert.fail();
        } catch (ArchiveNotFoundException_Thrift e) {
        } catch (TException e) {
            logger.error("Caught an unexpected exception", e);
            Assert.fail();
        }

        Mockito.verify(storageStore, Mockito.atLeastOnce()).get(Mockito.anyLong());
    }

    @Test
    public void test_createStoragePool_archiveNotFound_noSpecifiedArchive() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        Domain domain = new Domain();
        domain.setDomainId(TESTING_DOMAIN_ID);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);

        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
        List<StoragePool> allStoragePoolForTesting = new ArrayList<>();
        StoragePool storagePoolForTesting = RequestResponseHelper.buildStoragePoolFromThrift(getTestingStoragePool());
        storagePoolForTesting.setName("NotExistedName");
        storagePoolForTesting.setArchivesInDataNode(TestUtils.buildMultiMap(3, 4));
        allStoragePoolForTesting.add(storagePoolForTesting);
        when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePoolForTesting);

        CreateStoragePoolRequest_Thrift request = getCreatingRequest();
        request.getStoragePool().setVolumeIds(new HashSet<Long>());
        InstanceMetadata datanode = new InstanceMetadata(new InstanceId(new Random().nextLong()));
        datanode.setDomainId(TESTING_DOMAIN_ID);
        List<RawArchiveMetadata> archives = new ArrayList<>();
        RawArchiveMetadata archiveMetadata = new RawArchiveMetadata();
        archiveMetadata.setFree();
        archiveMetadata.setArchiveId(TESTING_ARCHIVE_ID);
        archives.add(archiveMetadata);
        datanode.setArchives(archives);
        when(storageStore.get(Mockito.anyLong())).thenReturn(datanode);
        try {
            icImpl.createStoragePool(request);
            Assert.fail();
        } catch (ArchiveNotFoundException_Thrift e) {
        } catch (TException e) {
            logger.error("Caught an unexpected exception", e);
            Assert.fail();
        }

        Mockito.verify(storageStore, Mockito.atLeastOnce()).get(Mockito.anyLong());
    }

    @Test
    public void test_createStoragePool_archiveBeenFound_archiveNotFree() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        Domain domain = new Domain();
        domain.setDomainId(TESTING_DOMAIN_ID);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);

        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
        List<StoragePool> allStoragePoolForTesting = new ArrayList<>();
        StoragePool storagePoolForTesting = RequestResponseHelper.buildStoragePoolFromThrift(getTestingStoragePool());
        storagePoolForTesting.setName("NotExistedName");

        Multimap<Long, Long> datanodeToArchiveMap = Multimaps
                .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        datanodeToArchiveMap.put(new Random().nextLong(), TESTING_ARCHIVE_ID);
        allStoragePoolForTesting.add(storagePoolForTesting);
        when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePoolForTesting);

        CreateStoragePoolRequest_Thrift request = getCreatingRequest();
        request.getStoragePool().setVolumeIds(new HashSet<Long>());
        InstanceMetadata datanode = new InstanceMetadata(new InstanceId(new Random().nextLong()));
        datanode.setDomainId(TESTING_DOMAIN_ID);
        List<RawArchiveMetadata> archives = new ArrayList<>();
        RawArchiveMetadata archiveMetadata = new RawArchiveMetadata();
        archiveMetadata.setArchiveId(TESTING_ARCHIVE_ID);
        archives.add(archiveMetadata);
        datanode.setArchives(archives);
        when(storageStore.get(Mockito.anyLong())).thenReturn(datanode);
        try {
            icImpl.createStoragePool(request);
            Assert.fail();
        } catch (ArchiveNotFoundException_Thrift e) {
        } catch (TException e) {
            logger.error("Caught an unexpected exception", e);
            Assert.fail();
        }

        Mockito.verify(storageStore, Mockito.atLeastOnce()).get(Mockito.anyLong());
    }

    @Test(expected = ServiceHavingBeenShutdown_Thrift.class)
    public void test_updateStoragePool_serviceHasBeenShutdown() throws Exception {
        UpdateStoragePoolRequest_Thrift request = new UpdateStoragePoolRequest_Thrift();
        icImpl.shutdownForTest();
        icImpl.updateStoragePool(request);
        Assert.fail();
    }

    @Test(expected = ServiceIsNotAvailable_Thrift.class)
    public void test_updateStoragePool_serviceSuspended() throws Exception {
        UpdateStoragePoolRequest_Thrift request = Mockito.mock(UpdateStoragePoolRequest_Thrift.class);
        when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
        icImpl.updateStoragePool(request);
        Assert.fail();
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void test_updateStoragePool_invalidInput_storagePoolNotSet() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        UpdateStoragePoolRequest_Thrift request = new UpdateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        request.setStoragePool(null);

        icImpl.updateStoragePool(request);
        Assert.fail();
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void test_updateStoragePool_invalidInput_domainIdNotSet() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        UpdateStoragePoolRequest_Thrift request = new UpdateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        StoragePool_Thrift tStoragePool = new StoragePool_Thrift();
        tStoragePool.setPoolId(RequestIdBuilder.get());
        tStoragePool.setPoolName(TestBase.getRandomString(6));
        tStoragePool.setDescription(TestBase.getRandomString(15));
        tStoragePool.setStrategy(StoragePoolStrategy_Thrift.Capacity);
        tStoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
        tStoragePool.setVolumeIds(TestUtils.buildIdSet(3));
        request.setStoragePool(tStoragePool);

        icImpl.updateStoragePool(request);
        Assert.fail();
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void test_updateStoragePool_invalidInput_poolIdNotSet() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        UpdateStoragePoolRequest_Thrift request = new UpdateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        StoragePool_Thrift tStoragePool = new StoragePool_Thrift();
        tStoragePool.setPoolId(RequestIdBuilder.get());
        tStoragePool.setDomainId(TESTING_DOMAIN_ID);
        // tStoragePool.setPoolName(TestBase.getRandomString(6));
        tStoragePool.setDescription(TestBase.getRandomString(15));
        tStoragePool.setStrategy(StoragePoolStrategy_Thrift.Capacity);
        tStoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
        tStoragePool.setVolumeIds(TestUtils.buildIdSet(3));
        request.setStoragePool(tStoragePool);

        icImpl.updateStoragePool(request);
        Assert.fail();
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void test_updateStoragePool_invalidInput_poolNameNotSet() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        UpdateStoragePoolRequest_Thrift request = new UpdateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        StoragePool_Thrift tStoragePool = new StoragePool_Thrift();
        tStoragePool.setPoolId(RequestIdBuilder.get());
        tStoragePool.setDomainId(TESTING_DOMAIN_ID);
        // tStoragePool.setPoolName(TestBase.getRandomString(6));
        tStoragePool.setDescription(TestBase.getRandomString(15));
        tStoragePool.setStrategy(StoragePoolStrategy_Thrift.Capacity);
        tStoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
        tStoragePool.setVolumeIds(TestUtils.buildIdSet(3));
        request.setStoragePool(tStoragePool);

        icImpl.updateStoragePool(request);
        Assert.fail();
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void test_updateStoragePool_invalidInput_strategyNotSet() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        UpdateStoragePoolRequest_Thrift request = new UpdateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        StoragePool_Thrift tStoragePool = new StoragePool_Thrift();
        tStoragePool.setPoolId(RequestIdBuilder.get());
        tStoragePool.setDomainId(TESTING_DOMAIN_ID);
        tStoragePool.setPoolName(TestBase.getRandomString(6));
        tStoragePool.setDescription(TestBase.getRandomString(15));
        // tStoragePool.setStrategy(StoragePoolStrategy_Thrift.Capacity);
        tStoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
        tStoragePool.setVolumeIds(TestUtils.buildIdSet(3));
        request.setStoragePool(tStoragePool);

        icImpl.updateStoragePool(request);
        Assert.fail();
    }

    @Test(expected = DomainNotExistedException_Thrift.class)
    public void test_updateStoragePool_domainNotExisted() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(null);

        UpdateStoragePoolRequest_Thrift request = getUpdatingRequest();
        icImpl.updateStoragePool(request);
        Assert.fail();
    }

    @Test(expected = StoragePoolNotExistedException_Thrift.class)
    public void test_updateStoragePool_storagePoolNotExisted_byId() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);

        UpdateStoragePoolRequest_Thrift request = getUpdatingRequest();
        icImpl.updateStoragePool(request);
        Assert.fail();
    }

    @Test(expected = StoragePoolNameExistedException_Thrift.class)
    public void test_updateStoragePool_storagePoolNameExisted_sameDomain() throws Exception {
        long domainId = 1L;

        StoragePool_Thrift tStoragePool = getTestingStoragePool();
        tStoragePool.setDomainId(domainId);
        UpdateStoragePoolRequest_Thrift request = new UpdateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        request.setStoragePool(tStoragePool);

        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
        StoragePool storagePoolExist = new StoragePool();
        storagePoolExist.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
        storagePoolExist.setPoolId(request.getStoragePool().getPoolId() + 1);

        List<StoragePool> allStoragePool = new ArrayList<>();
        StoragePool storagePool = new StoragePool();
        storagePool.setDomainId(domainId);
        storagePool.setName(request.getStoragePool().getPoolName());
        allStoragePool.add(storagePool);

        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);
        when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePool);

        icImpl.updateStoragePool(request);
        Assert.fail();
    }

    @Test
    public void test_updateStoragePool_storagePoolNameExisted_differentDomain() throws Exception {
        long existPoolDomainId = 1L;
        long toUpdatePoolDomainId = 2L;

        StoragePool_Thrift tStoragePool = new StoragePool_Thrift();
        tStoragePool.setDomainId(toUpdatePoolDomainId);
        tStoragePool.setPoolName(TESTING_STORAGE_POOL_NAME);
        tStoragePool.setPoolId(RequestIdBuilder.get());
        tStoragePool.setStrategy(StoragePoolStrategy_Thrift.Capacity);
        UpdateStoragePoolRequest_Thrift request = new UpdateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        request.setStoragePool(tStoragePool);

        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
        StoragePool storagePoolExist = new StoragePool();
        storagePoolExist.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
        storagePoolExist.setPoolId(request.getStoragePool().getPoolId() + 1);

        List<StoragePool> allStoragePool = new ArrayList<>();
        StoragePool storagePool = new StoragePool();
        storagePool.setDomainId(existPoolDomainId);
        storagePool.setName(request.getStoragePool().getPoolName());
        allStoragePool.add(storagePool);

        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);
        when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePool);

        try {
            icImpl.updateStoragePool(request);
        } catch (Exception e) {
            logger.error("Caught an unexpected exception", e);
            Assert.fail();
        }
    }

    @Test(expected = ArchiveNotFoundException_Thrift.class)
    public void test_updateStoragePool_instanceNotExisted() throws Exception {
        long domainId = 1L;

        StoragePool_Thrift tStoragePool = getTestingStoragePool();
        tStoragePool.setDomainId(domainId);
        UpdateStoragePoolRequest_Thrift request = new UpdateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        request.setStoragePool(tStoragePool);

        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
        StoragePool storagePoolExist = new StoragePool();
        storagePoolExist.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
        storagePoolExist.setDomainId(domainId);
        storagePoolExist.setPoolId(request.getStoragePool().getPoolId() + 1);

        List<StoragePool> allStoragePool = new ArrayList<>();
        StoragePool storagePool = new StoragePool();
        storagePool.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
        storagePool.setDomainId(domainId);
        allStoragePool.add(storagePool);

        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);
        when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePool);
        when(storageStore.get(Mockito.anyLong())).thenReturn(null);

        icImpl.updateStoragePool(request);
        Assert.fail();
    }

    @Test(expected = ArchiveNotFoundException_Thrift.class)
    public void test_updateStoragePool_instanceNotInSpecifiedDomain() throws Exception {
        long domainId = 1L;

        StoragePool_Thrift tStoragePool = getTestingStoragePool();
        tStoragePool.setDomainId(domainId);
        UpdateStoragePoolRequest_Thrift request = new UpdateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        request.setStoragePool(tStoragePool);

        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        StoragePool storagePoolExist = new StoragePool();
        storagePoolExist.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
        storagePoolExist.setDomainId(domainId);
        storagePoolExist.setPoolId(request.getStoragePool().getPoolId() + 1);

        List<StoragePool> allStoragePool = new ArrayList<>();
        StoragePool storagePool = new StoragePool();
        storagePool.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
        storagePool.setDomainId(domainId);
        allStoragePool.add(storagePool);

        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);
        when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePool);
        InstanceMetadata datanode = new InstanceMetadata(new InstanceId(new Random().nextLong()));
        datanode.setDomainId(TESTING_DOMAIN_ID);
        when(storageStore.get(Mockito.anyLong())).thenReturn(datanode);

        Domain domain = new Domain();
        domain.setDomainId(TESTING_DOMAIN_ID + 1);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);

        icImpl.updateStoragePool(request);
        Assert.fail();
    }

    @Test(expected = ArchiveNotFoundException_Thrift.class)
    public void test_updateStoragePool_archiveNotFoundById() throws Exception {
        long domainId = 1L;

        StoragePool_Thrift tStoragePool = getTestingStoragePool();
        tStoragePool.setDomainId(domainId);
        UpdateStoragePoolRequest_Thrift request = new UpdateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        request.setStoragePool(tStoragePool);

        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        StoragePool storagePoolExist = new StoragePool();
        storagePoolExist.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
        storagePoolExist.setPoolId(request.getStoragePool().getPoolId() + 1);
        storagePoolExist.setDomainId(domainId);

        List<StoragePool> allStoragePool = new ArrayList<>();
        StoragePool storagePool = new StoragePool();
        storagePool.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
        storagePool.setDomainId(domainId);
        allStoragePool.add(storagePool);

        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);
        when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePool);
        InstanceMetadata datanode = Mockito.mock(InstanceMetadata.class);

        datanode.setDomainId(TESTING_DOMAIN_ID);
        when(storageStore.get(Mockito.anyLong())).thenReturn(datanode);

        Domain domain = Mockito.mock(Domain.class);
        domain.setDomainId(TESTING_DOMAIN_ID + 1);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);

        when(datanode.getArchiveById(Mockito.anyLong())).thenReturn(null);
        icImpl.updateStoragePool(request);
        Assert.fail();
    }

    @Test(expected = ArchiveNotFreeToUseException_Thrift.class)
    public void test_updateStoragePool_archiveNotFreeToUse() throws Exception {
        long domainId = 1L;

        StoragePool_Thrift tStoragePool = getTestingStoragePool();
        tStoragePool.setDomainId(domainId);
        UpdateStoragePoolRequest_Thrift request = new UpdateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        request.setStoragePool(tStoragePool);

        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        StoragePool storagePoolExist = new StoragePool();
        storagePoolExist.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
        storagePoolExist.setPoolId(request.getStoragePool().getPoolId() + 1);
        storagePoolExist.setDomainId(domainId);

        List<StoragePool> allStoragePool = new ArrayList<>();
        StoragePool storagePool = new StoragePool();
        storagePool.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
        storagePool.setDomainId(domainId);
        allStoragePool.add(storagePool);

        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);
        when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePool);
        InstanceMetadata datanode = Mockito.mock(InstanceMetadata.class);

        datanode.setDomainId(TESTING_DOMAIN_ID);
        when(storageStore.get(Mockito.anyLong())).thenReturn(datanode);

        Domain domain = Mockito.mock(Domain.class);
        domain.setDomainId(TESTING_DOMAIN_ID + 1);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);

        RawArchiveMetadata archive = Mockito.mock(RawArchiveMetadata.class);
        archive.setFree();
        when(datanode.getArchiveById(Mockito.anyLong())).thenReturn(archive);
        icImpl.updateStoragePool(request);
        Assert.fail();
    }

    @Test(expected = ServiceHavingBeenShutdown_Thrift.class)
    public void test_deleteStoragePool_serviceBeenShutdown() throws Exception {
        DeleteStoragePoolRequest_Thrift request = new DeleteStoragePoolRequest_Thrift();
        icImpl.shutdownForTest();
        icImpl.deleteStoragePool(request);
        Assert.fail();
    }

    @Test(expected = ServiceIsNotAvailable_Thrift.class)
    public void test_deleteStoragePool_serviceSuspended() throws Exception {
        DeleteStoragePoolRequest_Thrift request = Mockito.mock(DeleteStoragePoolRequest_Thrift.class);
        when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
        icImpl.deleteStoragePool(request);
        Assert.fail();
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void test_deleteStoragePool_invalidInput_storagePoolNotSet() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        DeleteStoragePoolRequest_Thrift request = new DeleteStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        request.setDomainId(this.TESTING_DOMAIN_ID);

        icImpl.deleteStoragePool(request);
        Assert.fail();
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void test_deleteStoragePool_invalidInput_domainNotSet() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        DeleteStoragePoolRequest_Thrift request = new DeleteStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        request.setStoragePoolId(this.TESTING_STORAGE_POOL_ID);

        icImpl.deleteStoragePool(request);
        Assert.fail();
    }

    @Test(expected = DomainNotExistedException_Thrift.class)
    public void test_deleteStoragePool_domainNotExisted() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(null);

        DeleteStoragePoolRequest_Thrift request = new DeleteStoragePoolRequest_Thrift();
        request.setDomainId(this.TESTING_DOMAIN_ID);
        request.setStoragePoolId(this.TESTING_STORAGE_POOL_ID);
        icImpl.deleteStoragePool(request);
        Assert.fail();
    }

    @Test(expected = StoragePoolNotExistedException_Thrift.class)
    public void test_deleteStoragePool_storagePoolNotExisted() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());

        DeleteStoragePoolRequest_Thrift request = new DeleteStoragePoolRequest_Thrift();
        request.setDomainId(this.TESTING_DOMAIN_ID);
        request.setStoragePoolId(this.TESTING_STORAGE_POOL_ID);

        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
        icImpl.deleteStoragePool(request);
        Assert.fail();
    }

    @Test(expected = StillHaveVolumeException_Thrift.class)
    public void test_deleteStoragePool_storagePoolStillContainsVolume() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());

        DeleteStoragePoolRequest_Thrift request = new DeleteStoragePoolRequest_Thrift();
        request.setDomainId(this.TESTING_DOMAIN_ID);
        request.setStoragePoolId(this.TESTING_STORAGE_POOL_ID);

        StoragePool storagePoolExist = new StoragePool();
        Set<Long> volumeIds = new HashSet<>();
        volumeIds.add(this.TESTING_VOLUME_ID);
        storagePoolExist.setVolumeIds(volumeIds);
        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);

        VolumeMetadata volume = new VolumeMetadata();
        volume.setVolumeStatus(VolumeStatus.Available);
        when(volumeStore.getVolume(Mockito.anyLong())).thenReturn(volume);
        icImpl.deleteStoragePool(request);
        Assert.fail();
    }

    @Test
    public void test_deleteStoragePool_noException() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());

        DeleteStoragePoolRequest_Thrift request = new DeleteStoragePoolRequest_Thrift();
        request.setDomainId(this.TESTING_DOMAIN_ID);
        request.setStoragePoolId(this.TESTING_STORAGE_POOL_ID);

        StoragePool storagePoolExist = new StoragePool();
        Set<Long> volumeIds = new HashSet<>();
        volumeIds.add(this.TESTING_VOLUME_ID);
        storagePoolExist.setVolumeIds(volumeIds);
        when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);

        when(volumeStore.getVolume(Mockito.anyLong())).thenReturn(null);

        storagePoolExist.setDomainId(this.TESTING_DOMAIN_ID);
        storagePoolExist.setPoolId(this.TESTING_STORAGE_POOL_ID);
        storagePoolStore.saveStoragePool(storagePoolExist);
        icImpl.deleteStoragePool(request);
    }

    @Test(expected = ServiceHavingBeenShutdown_Thrift.class)
    public void test_listStoragePools_serviceBeeningShutdown() throws Exception {
        ListStoragePoolRequest_Thrift request = new ListStoragePoolRequest_Thrift();
        icImpl.shutdownForTest();
        icImpl.listStoragePools(request);
        Assert.fail();
    }

    @Test(expected = ServiceIsNotAvailable_Thrift.class)
    public void test_listStoragePools_serviceSuspend() throws Exception {
        ListStoragePoolRequest_Thrift request = Mockito.mock(ListStoragePoolRequest_Thrift.class);
        when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
        icImpl.listStoragePools(request);
        Assert.fail();
    }

    @Test()
    public void test_listStoragePools_invalidInput() throws Exception {
        ListStoragePoolRequest_Thrift request = Mockito.mock(ListStoragePoolRequest_Thrift.class);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        icImpl.listStoragePools(request);
    }

    @Test
    public void test_listStoragePools_withoutException() throws Exception {
        ListStoragePoolRequest_Thrift request = new ListStoragePoolRequest_Thrift();
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        request.setDomainId(this.TESTING_DOMAIN_ID);
        icImpl.listStoragePools(request);
    }

    @Test
    public void testListStoragePoolsWithMigrationInfo() throws Exception {
        ListStoragePoolRequest_Thrift request = new ListStoragePoolRequest_Thrift();
        List<Long> listStoragePoolIds = new ArrayList<>();
        listStoragePoolIds.add(1L);
        request.setStoragePoolIds(listStoragePoolIds);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        request.setDomainId(this.TESTING_DOMAIN_ID);
        List<StoragePool> storagePools = new ArrayList<>();
        StoragePool storagePool = new StoragePool(1L, 1L, "Pool", Capacity, "Pool");
        storagePools.add(storagePool);
        when(storagePoolStore.listStoragePools(any(ArrayList.class))).thenReturn(storagePools);


        // prepare archive
        RawArchiveMetadata archive1InDatanode1 = new RawArchiveMetadata();
        archive1InDatanode1.setArchiveId(1L);
        archive1InDatanode1.setArchiveType(ArchiveType.RAW_DISK);
        archive1InDatanode1.setStorageType(StorageType.SATA);
        archive1InDatanode1.setMigrationSpeed(10);
        archive1InDatanode1.addTotalPageToMigrate(100);
        archive1InDatanode1.addAlreadyMigratedPage(50);
        RawArchiveMetadata archive2InDatanode2 = new RawArchiveMetadata();
        archive2InDatanode2.setArchiveId(2L);
        archive2InDatanode2.setArchiveType(ArchiveType.RAW_DISK);
        archive2InDatanode2.setStorageType(StorageType.SATA);
        archive2InDatanode2.setMigrationSpeed(10);
        archive2InDatanode2.addTotalPageToMigrate(100);
        archive2InDatanode2.addAlreadyMigratedPage(50);


        storagePool.addArchiveInDatanode(1L, 1L);
        storagePool.addArchiveInDatanode(2L, 2L);
        InstanceMetadata datanode1 = new InstanceMetadata(new InstanceId(1L));
        datanode1.setDatanodeStatus(OK);
        datanode1.getArchives().add(archive1InDatanode1);
        InstanceMetadata datanode2 = new InstanceMetadata(new InstanceId(2L));
        datanode2.setDatanodeStatus(OK);
        datanode2.getArchives().add(archive2InDatanode2);
        when(storageStore.get(1L)).thenReturn(datanode1);
        when(storageStore.get(2L)).thenReturn(datanode2);
        ListStoragePoolResponse_Thrift listStoragePoolResponseThrift = icImpl.listStoragePools(request);
        assertEquals(1L, listStoragePoolResponseThrift.getStoragePoolDisplaysSize());
        OneStoragePoolDisplay_Thrift oneStoragePoolDisplayThrift = listStoragePoolResponseThrift.getStoragePoolDisplays().get(0);
        StoragePool_Thrift storagePoolThrift = oneStoragePoolDisplayThrift.getStoragePoolThrift();
        assertEquals(10, storagePoolThrift.getMigrationSpeed());
        assertTrue(50 == storagePoolThrift.getMigrationRatio());

    }

    @Test(expected = ServiceHavingBeenShutdown_Thrift.class)
    public void test_removeArchiveFromStoragePool_serviceBeeningShutdown() throws Exception {
        RemoveDatanodeFromDomainRequest request = new RemoveDatanodeFromDomainRequest();
        icImpl.shutdownForTest();
        icImpl.removeDatanodeFromDomain(request);
        Assert.fail();
    }

    @Test(expected = ServiceIsNotAvailable_Thrift.class)
    public void test_removeArchiveFromStoragePool_serviceSuspend() throws Exception {
        RemoveDatanodeFromDomainRequest request = new RemoveDatanodeFromDomainRequest();
        when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
        icImpl.removeDatanodeFromDomain(request);
        Assert.fail();
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void test_removeArchiveFromStoragePool_invalidInput() throws Exception {
        RemoveDatanodeFromDomainRequest request = new RemoveDatanodeFromDomainRequest();
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        icImpl.removeDatanodeFromDomain(request);
        Assert.fail();
    }

    @Test(expected = DomainNotExistedException_Thrift.class)
    public void test_removeArchiveFromStoragePool_domainNotExisted() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(null);

        RemoveDatanodeFromDomainRequest request = new RemoveDatanodeFromDomainRequest();
        request.setDomainId(this.TESTING_DOMAIN_ID);
        request.setDatanodeInstanceId(new Random().nextLong());
        icImpl.removeDatanodeFromDomain(request);
        Assert.fail();
    }

    @Test(expected = DatanodeNotFoundException_Thrift.class)
    public void test_removeArchiveFromStoragePool_datanodeNotExisted() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
        when(storageStore.get(Mockito.anyLong())).thenReturn(null);

        RemoveDatanodeFromDomainRequest request = new RemoveDatanodeFromDomainRequest();
        request.setDomainId(this.TESTING_DOMAIN_ID);
        request.setDatanodeInstanceId(new Random().nextLong());
        icImpl.removeDatanodeFromDomain(request);
        Assert.fail();
    }

    @Test
    public void test_removeArchiveFromStoragePool_withoutException() throws Exception {
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
        InstanceMetadata datanode = new InstanceMetadata(new InstanceId(new Random().nextLong()));
        when(storageStore.get(Mockito.anyLong())).thenReturn(datanode);

        RemoveDatanodeFromDomainRequest request = new RemoveDatanodeFromDomainRequest();
        request.setDomainId(this.TESTING_DOMAIN_ID);
        request.setDatanodeInstanceId(new Random().nextLong());
        icImpl.removeDatanodeFromDomain(request);
    }

    private CreateStoragePoolRequest_Thrift getCreatingRequest() {
        CreateStoragePoolRequest_Thrift request = new CreateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        StoragePool_Thrift tStoragePool = getTestingStoragePool();
        request.setStoragePool(tStoragePool);

        return request;
    }

    private UpdateStoragePoolRequest_Thrift getUpdatingRequest() {
        UpdateStoragePoolRequest_Thrift request = new UpdateStoragePoolRequest_Thrift();
        request.setRequestId(RequestIdBuilder.get());
        StoragePool_Thrift tStoragePool = getTestingStoragePool();
        request.setStoragePool(tStoragePool);

        return request;
    }

    private StoragePool_Thrift getTestingStoragePool() {
        StoragePool_Thrift tStoragePool = new StoragePool_Thrift();
        tStoragePool.setDomainId(RequestIdBuilder.get());
        tStoragePool.setPoolId(RequestIdBuilder.get());
        tStoragePool.setPoolName(TESTING_STORAGE_POOL_NAME);
        tStoragePool.setDescription(TestBase.getRandomString(15));
        tStoragePool.setStrategy(StoragePoolStrategy_Thrift.Capacity);
        tStoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
        tStoragePool.setVolumeIds(TestUtils.buildIdSet(3));
        tStoragePool.setStatus(Status_Thrift.Available);
        tStoragePool.setLastUpdateTime(System.currentTimeMillis());
        logger.debug("TStoragePool is {}", tStoragePool);
        return tStoragePool;
    }
}