package py.infocenter.service;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;
import static py.icshare.InstanceMetadata.DatanodeType.SIMPLE;
import static py.volume.VolumeMetadata.VolumeInAction.NULL;

import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.hibernate.SessionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import py.archive.RawArchiveMetadata;
import py.archive.ArchiveStatus;
import py.archive.StorageType;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.client.RequestResponseHelper;
import py.common.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.icshare.*;
import py.icshare.authorization.*;
import py.icshare.exception.AccessDeniedException;
import py.icshare.iscsiAccessRule.IscsiAccessRuleInformation;
import py.icshare.iscsiAccessRule.IscsiRuleRelationshipInformation;
import py.icshare.qos.*;
import py.infocenter.DBManager.BackupDBManager;
import py.infocenter.DBManager.BackupDBManagerImpl;
import py.infocenter.store.*;
import py.instance.*;
import py.license.LicenseDBStore;
import py.license.LicenseJson;
import py.license.LicenseStorage;
import py.test.TestBase;
import py.test.TestUtils;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.share.*;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

@Configuration
@ImportResource({"classpath:spring-config/hibernate.xml"})
class InformationCenterAppConfigTest {
    @Autowired
    private SessionFactory sessionFactory;

    @Bean
    public VolumeStore dbVolumeStore() {
        DBVolumeStoreImpl volumeStoreImpl = new DBVolumeStoreImpl();
        volumeStoreImpl.setSessionFactory(sessionFactory);
        volumeStoreImpl.setSegmentStore(dbSegmentStore());
        return volumeStoreImpl;
    }

    @Bean
    public StorageDBStore dbStorageStore() {
        StorageStoreImpl storageStoreImpl = new StorageStoreImpl();
        storageStoreImpl.setSessionFactory(sessionFactory);
        return storageStoreImpl;
    }

    @Bean
    public StorageStore dbStorageStore1() {
        StorageStoreImpl storageStoreImpl = new StorageStoreImpl();
        storageStoreImpl.setSessionFactory(sessionFactory);
        storageStoreImpl.setArchiveStore(dbArchiveStore());
        return storageStoreImpl;
    }

    @Bean
    public ArchiveStore dbArchiveStore() {
        ArchiveStoreImpl archiveStoreImpl = new ArchiveStoreImpl();
        archiveStoreImpl.setSessionFactory(sessionFactory);
        return archiveStoreImpl;
    }

    @Bean
    public DriverDBStore dbDriverStore() {
        DriverStoreImpl driverStore = new DriverStoreImpl();
        driverStore.setSessionFactory(sessionFactory);
        return driverStore;
    }

    @Bean
    public DriverStore driverStore() {
        DriverStoreImpl driverStore = new DriverStoreImpl();
        driverStore.setSessionFactory(sessionFactory);
        return driverStore;
    }
//
//    @Bean
//    public AccessRuleStore dbAccessRuleStore() {
//        AccessRuleStoreImpl accessRuleStore = new AccessRuleStoreImpl();
//        accessRuleStore.setSessionFactory(sessionFactory);
//        return accessRuleStore;
//    }

    @Bean
    public SegmentStore dbSegmentStore() {
        SegmentStoreImpl segmentStore = new SegmentStoreImpl();
        segmentStore.setSessionFactory(sessionFactory);
        return segmentStore;
    }

    @Bean
    public VolumeRuleRelationshipStore dbVolumeRuleRelationshipStore() {
        VolumeRuleRelationshipStoreImpl volumeRuleRelationshipStore = new VolumeRuleRelationshipStoreImpl();
        volumeRuleRelationshipStore.setSessionFactory(sessionFactory);
        return volumeRuleRelationshipStore;
    }

    @Bean
    public AccessRuleStore dbAccessRuleStore() {
        AccessRuleStoreImpl accessRuleStore = new AccessRuleStoreImpl();
        accessRuleStore.setSessionFactory(sessionFactory);
        return accessRuleStore;
    }

    @Bean
    public IscsiRuleRelationshipStore dbIscsiRuleRelationshipStore() {
        IscsiRuleRelationshipStoreImpl iscsiRuleRelationshipStore = new IscsiRuleRelationshipStoreImpl();
        iscsiRuleRelationshipStore.setSessionFactory(sessionFactory);
        return iscsiRuleRelationshipStore;
    }

    @Bean
    public IscsiAccessRuleStore dbIscsiAccessRuleStore() {
        IscsiAccessRuleStoreImpl iscsiAccessRuleStore = new IscsiAccessRuleStoreImpl();
        iscsiAccessRuleStore.setSessionFactory(sessionFactory);
        return iscsiAccessRuleStore;
    }

    @Bean
    public IOLimitationStore ioLimitationStore() {
        IOLimitationStoreImpl ioLimitationStore = new IOLimitationStoreImpl();
        ioLimitationStore.setSessionFactory(sessionFactory);
        return ioLimitationStore;
    }

    @Bean
    public IOLimitationRelationshipStore ioLimitationRelationshipStore() {
        IOLimitationRelationshipStoreImpl ioLimitationRelationshipStore = new IOLimitationRelationshipStoreImpl();
        ioLimitationRelationshipStore.setSessionFactory(sessionFactory);
        return ioLimitationRelationshipStore;
    }

    @Bean
    public MigrationRuleStore migrationRuleStore() {
        MigrationRuleStoreImpl migrationSpeedRuleStore = new MigrationRuleStoreImpl();
        migrationSpeedRuleStore.setSessionFactory(sessionFactory);
        return migrationSpeedRuleStore;
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public DomainDBStore dbDomainStore() {
        DomainStoreImpl domainStore = new DomainStoreImpl();
        domainStore.setSessionFactory(sessionFactory);
        return domainStore;
    }

    @Bean
    public DomainStore domainStore() {
        DomainStoreImpl domainStore = new DomainStoreImpl();
        domainStore.setSessionFactory(sessionFactory);
        return domainStore;
    }

    @Bean
    public StoragePoolDBStore dbStoragePoolStore() {
        StoragePoolStoreImpl storagePoolStore = new StoragePoolStoreImpl();
        storagePoolStore.setSessionFactory(sessionFactory);
        return storagePoolStore;
    }

    @Bean
    public StoragePoolStore storagePoolStore() {
        StoragePoolStoreImpl storagePoolStore = new StoragePoolStoreImpl();
        storagePoolStore.setSessionFactory(sessionFactory);
        return storagePoolStore;
    }

    @Bean
    public CapacityRecordStore dbCapacityRecordStore() {
        CapacityRecordStoreImpl capacityRecordStore = new CapacityRecordStoreImpl();
        capacityRecordStore.setSessionFactory(sessionFactory);
        return capacityRecordStore;
    }

    @Bean
    public CapacityRecordStore capacityRecordStore() {
        CapacityRecordStoreImpl capacityRecordStore = new CapacityRecordStoreImpl();
        capacityRecordStore.setSessionFactory(sessionFactory);
        return capacityRecordStore;
    }

    @Bean
    CloneRelationshipsDBStore cloneRelationshipsStore() {
        CloneRelationshipsStoreImpl cloneRelationshipsStore = new CloneRelationshipsStoreImpl();
        cloneRelationshipsStore.setSessionFactory(sessionFactory);
        return cloneRelationshipsStore;
    }

    @Bean
    LicenseStorage licenseDBStore() {
        LicenseStorage licenseDBStore = new LicenseDBStore(sessionFactory);
        return licenseDBStore;
    }

    @Bean
    public AccountStore inMemAccountStore() {
        InMemoryAccountStoreImpl inMemoryAccountStore = new InMemoryAccountStoreImpl();
        inMemoryAccountStore.setAccountStore(dbAccountStore());
        return inMemoryAccountStore;
    }

    @Bean
    public AccountStore dbAccountStore() {
        AccountStoreDBImpl dbAccountStore = new AccountStoreDBImpl();
        dbAccountStore.setSessionFactory(sessionFactory);
        return dbAccountStore;
    }

    @Bean
    public RoleStore roleStore() {
        RoleDBStoreImpl roleDBStore = new RoleDBStoreImpl();
        roleDBStore.setSessionFactory(sessionFactory);
        return roleDBStore;
    }

    @Bean
    public APIStore apiDBStore() {
        APIDBStoreImpl apiDBStore = new APIDBStoreImpl();
        apiDBStore.setSessionFactory(sessionFactory);
        return apiDBStore;
    }

    @Bean
    public ResourceStore resourceDBStore() {
        ResourceDBStoreImpl resourceDBStore = new ResourceDBStoreImpl();
        resourceDBStore.setSessionFactory(sessionFactory);
        return resourceDBStore;

    }

    @Bean
    ServerNodeStore serverNodeStore() {
        ServerNodeStoreImpl serverNodeStore = new ServerNodeStoreImpl();
        serverNodeStore.setSessionFactory(sessionFactory);
        return serverNodeStore;
    }

    @Bean
    DiskInfoStore diskInfoStore() {
        DiskInfoStoreImpl diskInfoStore = new DiskInfoStoreImpl();
        diskInfoStore.setSessionFactory(sessionFactory);
        return diskInfoStore;
    }

}

public class DatabaseStoreTest extends TestBase {
    private static final int amountOfAccounts = 1000;

    StorageDBStore storageDBStore = null;
    StorageStore storageStore = null;
    DriverDBStore driverDBStore = null;
    DriverStore driverStore = null;
    ArchiveStore archiveStore = null;
    SegmentStore segmentStore = null;
    VolumeStore volumeStore = null;
    VolumeRuleRelationshipStore volumeRuleRelationshipStore = null;
    AccessRuleStore accessRuleStore = null;
    IscsiRuleRelationshipStore iscsiRuleRelationshipStore = null;
    IscsiAccessRuleStore iscsiAccessRuleStore = null;
    DomainDBStore domainDBStore = null;
    DomainStore domainStore = null;
    StoragePoolDBStore storagePoolDBStore = null;
    StoragePoolStore storagePoolStore = null;
    CapacityRecordDBStore capacityRecordDBStore = null;
    CapacityRecordStore capacityRecordStore = null;
    CloneRelationshipsDBStore cloneRelationshipsStore = null;
    LicenseStorage licenseDBStore = null;
    AccountStore accountStore = null;
    RoleStore roleStore = null;
    APIStore apiStore = null;
    ResourceStore resourceStore = null;
    ServerNodeStore serverNodeStore = null;
    DiskInfoStore diskInfoStore = null;
    IOLimitationStore ioLimitationStore = null;
    MigrationRuleStore migrationRuleStore = null;



    public void init() throws Exception {
        super.init();
        @SuppressWarnings("resource")
        ApplicationContext ctx = new AnnotationConfigApplicationContext(InformationCenterAppConfigTest.class);
        storageDBStore = (StorageDBStore) ctx.getBean("dbStorageStore");
        storageStore = (StorageStore) ctx.getBean("dbStorageStore1");
        driverDBStore = (DriverDBStore) ctx.getBean("dbDriverStore");
        driverStore = (DriverStore) ctx.getBean("driverStore");
        archiveStore = (ArchiveStore) ctx.getBean("dbArchiveStore");
        segmentStore = (SegmentStore) ctx.getBean("dbSegmentStore");
        volumeRuleRelationshipStore = (VolumeRuleRelationshipStore) ctx.getBean("dbVolumeRuleRelationshipStore");
        accessRuleStore = (AccessRuleStore) ctx.getBean("dbAccessRuleStore");
        iscsiRuleRelationshipStore = (IscsiRuleRelationshipStore) ctx.getBean("dbIscsiRuleRelationshipStore");
        iscsiAccessRuleStore = (IscsiAccessRuleStore) ctx.getBean("dbIscsiAccessRuleStore");
        volumeStore = (VolumeStore) ctx.getBean("dbVolumeStore");
        domainDBStore = (DomainDBStore) ctx.getBean("dbDomainStore");
        domainStore = (DomainStore) ctx.getBean("domainStore");
        storagePoolDBStore = (StoragePoolDBStore) ctx.getBean("dbStoragePoolStore");
        storagePoolStore = (StoragePoolStore) ctx.getBean("storagePoolStore");
        capacityRecordDBStore = (CapacityRecordDBStore) ctx.getBean("dbCapacityRecordStore");
        capacityRecordStore = (CapacityRecordStore) ctx.getBean("capacityRecordStore");
        cloneRelationshipsStore = (CloneRelationshipsDBStore) ctx.getBean("cloneRelationshipsStore");
        licenseDBStore = (LicenseStorage) ctx.getBean("licenseDBStore");
        accountStore = (InMemoryAccountStoreImpl) ctx.getBean("inMemAccountStore");
        roleStore = (RoleStore) ctx.getBean("roleStore");
        apiStore = (APIStore) ctx.getBean("apiDBStore");
        resourceStore = (ResourceStore) ctx.getBean("resourceDBStore");
        serverNodeStore = (ServerNodeStore) ctx.getBean("serverNodeStore");
        diskInfoStore = (DiskInfoStore) ctx.getBean("diskInfoStore");
        ioLimitationStore = (IOLimitationStore) ctx.getBean("ioLimitationStore");
        migrationRuleStore = (MigrationRuleStore)ctx.getBean("migrationRuleStore");

    }

    @Before
    public void beforeTest() throws Exception {
        List<StoragePool> storagePools = storagePoolStore.listAllStoragePools();
        if (storagePools != null && !storagePools.isEmpty()) {
            storagePools.forEach(storage -> storagePoolStore.deleteStoragePool(storage.getPoolId()));
        }
        assertTrue(storagePoolStore.listAllStoragePools().isEmpty());

        List<StorageInformation> storages = storageDBStore.listFromDB();
        if (storages != null && !storages.isEmpty()) {
            storages.forEach(storage -> storageDBStore.deleteFromDB(storage.getInstanceId()));
        }
        storageStore.clearMemoryData();
        assertTrue(storageDBStore.listFromDB().isEmpty());

        List<Domain> domains = domainStore.listAllDomains();
        if (domains != null && !domains.isEmpty()) {
            domains.forEach(domain -> domainStore.deleteDomain(domain.getDomainId()));
        }
        domainStore.clearMemoryMap();
        assertTrue(domainStore.listAllDomains().isEmpty());

        List<DriverInformation> drivers = driverDBStore.listFromDB();
        if (drivers != null && !drivers.isEmpty()) {
            drivers.forEach(driver -> driverDBStore.deleteFromDB(driver.getDriverKeyInfo().getVolumeId()));
        }
        assertTrue(driverDBStore.listFromDB().isEmpty());

        driverStore.clearMemoryData();
        assertTrue(driverStore.list().isEmpty());

        List<VolumeRuleRelationshipInformation> volumeRuleRelationshipInformationList =
                volumeRuleRelationshipStore.list();
        if (null != volumeRuleRelationshipInformationList && !volumeRuleRelationshipInformationList.isEmpty()) {
            volumeRuleRelationshipInformationList.forEach(volumeRule -> volumeRuleRelationshipStore.deleteByRuleIdandVolumeID(
                    volumeRule.getVolumeId(), volumeRule.getRuleId()
            ));
        }
        assertTrue(volumeRuleRelationshipStore.list().isEmpty());

        List<AccessRuleInformation> accessRuleInformationList = accessRuleStore.list();
        if (null != accessRuleInformationList && !accessRuleInformationList.isEmpty()) {
            accessRuleInformationList.forEach(accessRuleInformation -> accessRuleStore.delete(accessRuleInformation.getRuleId()));
        }
        assertTrue(accessRuleStore.list().isEmpty());


        List<IscsiRuleRelationshipInformation> iscsiRuleRelationshipInformationList =
                iscsiRuleRelationshipStore.list();
        if (null != iscsiRuleRelationshipInformationList && !iscsiRuleRelationshipInformationList.isEmpty()) {
            iscsiRuleRelationshipInformationList.forEach(iscsiRule -> iscsiRuleRelationshipStore.deleteByRuleIdandDriverKey(
                    new DriverKey_Thrift(iscsiRule.getDriverContainerId(), iscsiRule.getVolumeId(), iscsiRule.getSnapshotId(),
                            DriverType_Thrift.valueOf(iscsiRule.getDriverType())), iscsiRule.getRuleId()
            ));
        }
        assertTrue(iscsiRuleRelationshipStore.list().isEmpty());

        List<IscsiAccessRuleInformation> iscsiAccessRuleInformationList = iscsiAccessRuleStore.list();
        if (null != iscsiAccessRuleInformationList && !iscsiAccessRuleInformationList.isEmpty()) {
            iscsiAccessRuleInformationList.forEach(iscsiAccessRuleInformation -> iscsiAccessRuleStore.delete(iscsiAccessRuleInformation.getRuleId()));
        }
        assertTrue(iscsiAccessRuleStore.list().isEmpty());

        List<CloneRelationshipInformation> cloneRelationshipInformationList = cloneRelationshipsStore.loadFromDB();
        for (CloneRelationshipInformation cloneRelationshipInformation : cloneRelationshipInformationList) {
            cloneRelationshipsStore.deleteFromDB(cloneRelationshipInformation.getDestVolumeId());
        }
        assertTrue(cloneRelationshipsStore.loadFromDB().isEmpty());

        licenseDBStore.deleteLicenseCryptogramInformation();
        assertNull(licenseDBStore.getLicenseCryptogram());

        accountStore.deleteAllAccounts();
        assertTrue(accountStore.listAccounts().isEmpty());

        roleStore.cleanRoles();
        assertTrue(roleStore.listRoles().isEmpty());

        apiStore.cleanAPIs();
        assertTrue(apiStore.listAPIs().isEmpty());

        resourceStore.cleanResources();
        assertTrue(resourceStore.listResources().isEmpty());

        serverNodeStore.clearDB();
        assertTrue(serverNodeStore.listAllServerNodes().isEmpty());

        diskInfoStore.clearDB();
        assertTrue(diskInfoStore.listDiskInfos().isEmpty());

        List<IOLimitationInformation> ioLimitationList = ioLimitationStore.list();
        if (null != ioLimitationList && !ioLimitationList.isEmpty()) {
            ioLimitationList.forEach(ioLimitation -> ioLimitationStore.delete(ioLimitation.getRuleId()));
        }
        assertTrue(ioLimitationStore.list().isEmpty());

        List<MigrationRuleInformation> migrationSpeedInformationList = migrationRuleStore.list();
        if (null != migrationSpeedInformationList && !migrationSpeedInformationList.isEmpty()) {
            migrationSpeedInformationList.forEach(migrationSpeedInformation -> migrationRuleStore.delete(migrationSpeedInformation.getRuleId()));
        }
        assertTrue(migrationRuleStore.list().isEmpty());

    }

    @Test
    public void testDriverTable() {

        DriverInformation driverInformation;

        // write a record
        long driverContainerId = RequestIdBuilder.get();
        long volumeId1 = RequestIdBuilder.get();
        driverInformation = new DriverInformation(driverContainerId, volumeId1, 0, DriverType.ISCSI);
        driverInformation.setDriverStatus(DriverStatus.START.name());
        driverDBStore.saveToDB(driverInformation);

        // write a next record
        long driverContainerId1 = RequestIdBuilder.get();
        driverInformation = new DriverInformation(driverContainerId1, volumeId1, 0, DriverType.NBD);
        driverInformation.setDriverStatus(DriverStatus.START.name());
        driverDBStore.saveToDB(driverInformation);

        long volumeId2 = RequestIdBuilder.get();
        driverInformation = new DriverInformation(driverContainerId, volumeId2, 0, DriverType.ISCSI);
        driverInformation.setDriverStatus(DriverStatus.START.name());
        driverDBStore.saveToDB(driverInformation);

        assertEquals(2, driverDBStore.getByVolumeIdFromDB(volumeId1).size());
        assertEquals(1, driverDBStore.getByVolumeIdFromDB(volumeId2).size());

        driverInformation.setDriverStatus(DriverStatus.LAUNCHING.name());
        driverDBStore.updateToDB(driverInformation);
        driverDBStore.updateStatusToDB(volumeId1, DriverType.ISCSI, 0, DriverStatus.LAUNCHING.name());
        driverDBStore.updateStatusToDB(volumeId1, DriverType.NBD, 0, DriverStatus.LAUNCHING.name());
        List<DriverInformation> listDrivers = driverDBStore.listFromDB();
        assertEquals(3, listDrivers.size());

        for (DriverInformation driver : listDrivers) {
            logger.warn("current driver: {}", driver);
            assertTrue(driver.getDriverStatus().equals(DriverStatus.LAUNCHING.name()));
        }

        // delete a record by ip+port
        assertEquals(2, driverDBStore.getByVolumeIdFromDB(volumeId1).size());
        driverDBStore.deleteFromDB(volumeId1, DriverType.NBD, 0);
        assertEquals(1, driverDBStore.getByVolumeIdFromDB(volumeId1).size());

        driverDBStore.deleteFromDB(volumeId1);
        assertEquals(1, driverDBStore.listFromDB().size());

        driverDBStore.deleteFromDB(volumeId2);
        assertTrue(driverDBStore.listFromDB().isEmpty());
    }


    @Test
    public void testDriverLimitationStore() {
        try {
            long volumeId = 1111;
            long accountId = 1862755152385798555L;
            long driverContainerId = RequestIdBuilder.get();
            DriverType driverType = DriverType.NBD;
            int snapshotId = 0;

            long limitId1 = 1;
            long limitId2 = 2;

            DriverMetadata driverMetadata = new DriverMetadata();
            driverMetadata.setVolumeId(volumeId);
            driverMetadata.setDriverContainerId(driverContainerId);
            driverMetadata.setSnapshotId(snapshotId);
            driverMetadata.setDriverType(driverType);
            driverMetadata.setAccountId(accountId);
            driverMetadata.setDriverStatus(DriverStatus.LAUNCHED);

            IOLimitation limit1 = new IOLimitation(limitId1, "rule",100, 10,
                    1000, 100, LocalTime.now().plusSeconds(5),
                    LocalTime.now().plusSeconds(15));


            IOLimitation limit2 = new IOLimitation(limitId2, "rule",200, 20,
                    2000, 200, LocalTime.now().plusSeconds(20),
                    LocalTime.now().plusSeconds(25));

            List<IOLimitation> limitList = new ArrayList<>();
            limitList.add(limit1);
            limitList.add(limit2);
            driverMetadata.setDynamicIOLimitationId(100);
            driverMetadata.setStaticIOLimitationId(101);

            driverStore.save(driverMetadata);

            driverStore.updateIOLimit(driverContainerId, volumeId, driverType, snapshotId, limit1);
            driverStore.deleteIOLimit(driverContainerId, volumeId, driverType, snapshotId, limitId2);
            DriverMetadata driverMetadata1 = driverStore.get(driverContainerId, volumeId, driverType, snapshotId);
//            boolean found = false;
//            for (IOLimitation limit : driverMetadata1.getIoLimitations()) {
//                logger.debug("limit : {}", limit);
//                if (limit.getId() == limit1.getId()) {
//                    found = true;
//                }
//            }
//            org.junit.Assert.assertEquals(1, driverMetadata1.getIoLimitations().size());
//            if (!found) {
//                Validate.isTrue(false);
//            }

        } catch (Exception e) {
            logger.error("exception catch", e);
            Validate.isTrue(false);
        }
    }

    @Test
    public void testStorageDBTable() {

        StorageInformation storageMetadata;
        long instanceId1 = RequestIdBuilder.get();
        storageMetadata = new StorageInformation(instanceId1, 100, 50, 50, new Date(), 1L);

        storageMetadata.setSsdCacheSize(100000);
        storageMetadata.setSsdCacheStatus(1);
        storageMetadata.setTagKey("corperate");
        storageMetadata.setTagValue("pengyunnetwork");
        storageDBStore.saveToDB(storageMetadata);

        long instanceId2 = RequestIdBuilder.get();
        storageMetadata = new StorageInformation(instanceId2, 100, 50, 50, new Date(), 2L);
        storageMetadata.setSsdCacheStatus(1);
        storageDBStore.saveToDB(storageMetadata);

        long instanceId3 = RequestIdBuilder.get();
        storageMetadata = new StorageInformation(instanceId3, 100, 50, 50, new Date(), 3L);
        storageDBStore.saveToDB(storageMetadata);

        assertTrue(storageDBStore.getByInstanceIdFromDB(0) == null);
        assertTrue(storageDBStore.getByInstanceIdFromDB(instanceId1) != null);

        storageMetadata.setSsdCacheStatus(1);
        storageDBStore.saveToDB(storageMetadata);

        List<StorageInformation> storages = storageDBStore.listFromDB();
        assertTrue(storages.size() == 3);
        for (StorageInformation storage : storages) {
            assertTrue(storage.getSsdCacheStatus() == 1);
        }

        storageDBStore.deleteFromDB(instanceId1);
        assertTrue(storageDBStore.listFromDB().size() == 2);

        storageDBStore.deleteFromDB(instanceId2);
        assertTrue(storageDBStore.listFromDB().size() == 1);

        storageDBStore.deleteFromDB(instanceId3);
        assertTrue(storageDBStore.listFromDB().size() == 0);
    }

    // add by wzy @2014-10-08 13:45:53
    @Test
    public void testStorageTable() {

        InstanceMetadata instanceMetadata;
        long instanceId1 = RequestIdBuilder.get();
        instanceMetadata = new InstanceMetadata(new InstanceId(instanceId1));

        instanceMetadata.setCapacity(100000);
        instanceMetadata.setFreeSpace(1);
        instanceMetadata.setEndpoint("py123");
        instanceMetadata.setLastUpdated(199L);
        instanceMetadata.setDatanodeType(SIMPLE);

        // add archive list to instanceMetadata
        List<RawArchiveMetadata> archivesList = new ArrayList<>();
        for (long i = 0; i < 3; i++) {
            RawArchiveMetadata amdata = new RawArchiveMetadata();
            amdata.setArchiveId(i);
            amdata.setInstanceId(new InstanceId(instanceId1));
            amdata.setStatus(ArchiveStatus.GOOD);
            archivesList.add(amdata);
        }

        instanceMetadata.setArchives(archivesList);
        storageStore.save(instanceMetadata);

        List<InstanceMetadata> storages = storageStore.list();
        assertTrue(storages.size() == 1);
        storageStore.delete(instanceId1);

        assertTrue(storageStore.size() == 0);
    }

    @Test
    public void testArchiveTable() {
        long instanceId = 20000000;
        long archiveId = 10000000;
        ArchiveInformation archiveInformation;
        // no more archives in a instance
        archiveInformation = new ArchiveInformation(archiveId + 1, instanceId, StorageType.SATA, ArchiveStatus.GOOD,
                10000, 1L);
        archiveStore.save(archiveInformation);
        archiveInformation = new ArchiveInformation(archiveId + 2, instanceId, StorageType.SATA, ArchiveStatus.GOOD,
                10000, 2L);
        archiveStore.save(archiveInformation);
        archiveInformation = new ArchiveInformation(archiveId + 3, instanceId, StorageType.SATA, ArchiveStatus.GOOD,
                10000, 3L);
        archiveStore.save(archiveInformation);

        archiveInformation = new ArchiveInformation(archiveId + 4, instanceId + 1, StorageType.SAS, ArchiveStatus.GOOD,
                10000, 4L);
        archiveStore.save(archiveInformation);
        archiveInformation = new ArchiveInformation(archiveId + 5, instanceId + 1, StorageType.SAS, ArchiveStatus.GOOD,
                10000, 5L);
        archiveStore.save(archiveInformation);

        assertTrue(archiveStore.get(archiveId + 1).getInstanceId() == instanceId);
        assertTrue(archiveStore.get(archiveId + 4).getInstanceId() == instanceId + 1);

        assertTrue(archiveStore.getByInstanceId(instanceId + 1).size() == 2);
        assertTrue(archiveStore.getByInstanceId(instanceId).size() == 3);

        assertTrue(archiveStore.list().size() == 5);

        assertTrue(archiveStore.get(archiveId + 1).getInstanceId() == instanceId);
        assertTrue(archiveStore.get(archiveId + 7) == null);

        assertTrue(archiveStore.deleteByInstanceId(instanceId) == 3);
        assertTrue(archiveStore.list().size() == 2);

        assertTrue(archiveStore.delete(archiveId + 4) == 1);
        assertTrue(archiveStore.list().size() == 1);

        assertTrue(archiveStore.delete(archiveId + 5) == 1);
        assertTrue(archiveStore.list().size() == 0);
    }

    @Test
    public void testAccessRuleStore() {

        AccessRuleInformation accessRuleInformation;
        long ruleId = 10000000;

        accessRuleInformation = new AccessRuleInformation(ruleId, "10.0.1.101", AccessPermissionType.READ.getValue());
        accessRuleStore.save(accessRuleInformation);
        accessRuleInformation = new AccessRuleInformation(ruleId + 1, "10.0.1.102",
                AccessPermissionType.WRITE.getValue());
        accessRuleStore.save(accessRuleInformation);
        accessRuleInformation = new AccessRuleInformation(ruleId + 2, "10.0.1.103",
                AccessPermissionType.WRITE.getValue());
        accessRuleStore.save(accessRuleInformation);
        accessRuleInformation = new AccessRuleInformation(ruleId + 3, "10.0.1.104",
                AccessPermissionType.WRITE.getValue());
        accessRuleStore.save(accessRuleInformation);

        assertTrue(accessRuleStore.list().size() == 4);
        assertTrue(accessRuleStore.get(ruleId + 2).getIpAddress().equals("10.0.1.103"));
        accessRuleStore.delete(ruleId + 3);
        assertTrue(accessRuleStore.list().size() == 3);

        accessRuleInformation.setRuleId(ruleId);
        accessRuleInformation.setIpAddress("10.0.1.101");
        accessRuleInformation.permission(AccessPermissionType.WRITE);
        accessRuleStore.update(accessRuleInformation);

        for (AccessRuleInformation rule : accessRuleStore.list()) {
            assertTrue(rule.getPermission() == AccessPermissionType.WRITE.getValue());
        }

        accessRuleStore.delete(ruleId);
        accessRuleStore.delete(ruleId + 1);
        accessRuleStore.delete(ruleId + 2);
        assertTrue(accessRuleStore.list().size() == 0);

    }

    @Test
    public void testVolumeRuleRelationshipStore() {
        long ruleId = 10000000;
        long volumeId = 2000000;
        long relationshipId = 3000000;
        VolumeRuleRelationshipInformation relationship;
        relationship = new VolumeRuleRelationshipInformation(relationshipId, volumeId, ruleId);
        volumeRuleRelationshipStore.save(relationship);

        relationship = new VolumeRuleRelationshipInformation(relationshipId + 2, volumeId, ruleId + 1);
        volumeRuleRelationshipStore.save(relationship);

        relationship = new VolumeRuleRelationshipInformation(relationshipId + 3, volumeId + 1, ruleId + 3);
        volumeRuleRelationshipStore.save(relationship);

        relationship = new VolumeRuleRelationshipInformation(relationshipId + 4, volumeId + 2, ruleId + 1);
        volumeRuleRelationshipStore.save(relationship);

        assertTrue(volumeRuleRelationshipStore.list().size() == 4);
        assertTrue(volumeRuleRelationshipStore.getByRuleId(ruleId + 1).size() == 2);
        assertTrue(volumeRuleRelationshipStore.getByRuleId(ruleId + 1).size() == 2);
        assertTrue(volumeRuleRelationshipStore.getByVolumeId(volumeId).size() == 2);
        assertTrue(volumeRuleRelationshipStore.deleteByRuleId(ruleId + 1) == 2);
        assertTrue(volumeRuleRelationshipStore.deleteByVolumeId(volumeId) == 1);
        assertTrue(volumeRuleRelationshipStore.deleteByRuleId(ruleId + 3) == 1);
    }


    @Test
    public void testIscsiAccessRuleStore() {

        IscsiAccessRuleInformation iscsiAccessRuleInformation = null;
        long ruleId = 10000000;

        iscsiAccessRuleInformation = new IscsiAccessRuleInformation(ruleId, "rule1","10.0.1.101", "root", "root",
                "root", "root",AccessPermissionType.READ.getValue());
        iscsiAccessRuleStore.save(iscsiAccessRuleInformation);
        iscsiAccessRuleInformation = new IscsiAccessRuleInformation(ruleId + 1, "rule2","10.0.1.102", "root", "root",
                "root", "root",AccessPermissionType.WRITE.getValue());
        iscsiAccessRuleStore.save(iscsiAccessRuleInformation);
        iscsiAccessRuleInformation = new IscsiAccessRuleInformation(ruleId + 2, "rule3","10.0.1.103", "root", "root",
                "root", "root",AccessPermissionType.WRITE.getValue());
        iscsiAccessRuleStore.save(iscsiAccessRuleInformation);
        iscsiAccessRuleInformation = new IscsiAccessRuleInformation(ruleId + 3, "rule4","10.0.1.104", "root", "root",
                "root", "root",AccessPermissionType.WRITE.getValue());
        iscsiAccessRuleStore.save(iscsiAccessRuleInformation);

        assertTrue(iscsiAccessRuleStore.list().size() == 4);
        assertTrue(iscsiAccessRuleStore.get(ruleId + 2).getInitiatorName().equals("10.0.1.103"));
        iscsiAccessRuleStore.delete(ruleId + 3);
        assertTrue(iscsiAccessRuleStore.list().size() == 3);

        iscsiAccessRuleInformation.setRuleId(ruleId);
        iscsiAccessRuleInformation.setInitiatorName("10.0.1.101");
        iscsiAccessRuleInformation.permission(AccessPermissionType.WRITE);
        iscsiAccessRuleStore.update(iscsiAccessRuleInformation);

        for (IscsiAccessRuleInformation rule : iscsiAccessRuleStore.list()) {
            assertTrue(rule.getPermission() == AccessPermissionType.WRITE.getValue());
        }

        iscsiAccessRuleStore.delete(ruleId);
        iscsiAccessRuleStore.delete(ruleId + 1);
        iscsiAccessRuleStore.delete(ruleId + 2);
        assertTrue(iscsiAccessRuleStore.list().size() == 0);

    }

    @Test
    public void testIscsiRuleRelationshipStore() {
        long ruleId = 10000000;
        long volumeId = 2000000;
        DriverKey_Thrift driverKey = new DriverKey_Thrift(0, 0, 0, DriverType_Thrift.ISCSI.ISCSI);

        long relationshipId = 3000000;
        IscsiRuleRelationshipInformation relationship;
        relationship = new IscsiRuleRelationshipInformation(relationshipId, 0, 0, 0, "ISCSI", ruleId);
        iscsiRuleRelationshipStore.save(relationship);

        relationship = new IscsiRuleRelationshipInformation(relationshipId + 2, 0, 0, 0, "ISCSI", ruleId + 1);
        iscsiRuleRelationshipStore.save(relationship);

        relationship = new IscsiRuleRelationshipInformation(relationshipId + 3, 2, 0, 0, "ISCSI", ruleId + 3);
        iscsiRuleRelationshipStore.save(relationship);

        relationship = new IscsiRuleRelationshipInformation(relationshipId + 4, 3, 0, 0, "ISCSI", ruleId + 1);
        iscsiRuleRelationshipStore.save(relationship);

        assertTrue(iscsiRuleRelationshipStore.list().size() == 4);
        assertTrue(iscsiRuleRelationshipStore.getByRuleId(ruleId + 1).size() == 2);
        assertTrue(iscsiRuleRelationshipStore.getByRuleId(ruleId + 1).size() == 2);
        assertTrue(iscsiRuleRelationshipStore.getByDriverKey(driverKey).size() == 2);
        assertTrue(iscsiRuleRelationshipStore.deleteByRuleId(ruleId + 1) == 2);
        assertTrue(iscsiRuleRelationshipStore.deleteByDriverKey(driverKey) == 1);
        assertTrue(iscsiRuleRelationshipStore.deleteByRuleId(ruleId + 3) == 1);
    }


    @Test
    public void testIOLimitationStore() {
        IOLimitationInformation ioLimitationInformation;
        long ruleId = 10000000;

        LocalTime startTime0 = LocalTime.now().plusSeconds(0);
        LocalTime endTime0 = LocalTime.now().plusSeconds(4);
        List<IOLimitationEntry> entries = new ArrayList<>();
        IOLimitationEntry entry1 = new IOLimitationEntry(0,100, 10, 1000, 100, startTime0, endTime0);
        entries.add(entry1);
        IOLimitation ioLimitation = new IOLimitation();
        ioLimitation.setId(ruleId);
        ioLimitation.setLimitType(IOLimitation.LimitType.Dynamic);
        ioLimitation.setEntries(entries);

        ioLimitationInformation = ioLimitation.toIOLimitationInformation(ioLimitationStore);
        ioLimitationStore.save(ioLimitationInformation);
        IOLimitationInformation ioLimitationInformationDB = ioLimitationStore.get(ruleId);
        assertNotNull(ioLimitationInformationDB);
        IOLimitation ioLimitationDB = new IOLimitation(ioLimitationInformationDB);
        assertEquals(1, ioLimitationDB.getEntries().size());
        IOLimitationEntry entryDB = ioLimitationDB.getEntries().get(0);
        assertEquals(100, entryDB.getUpperLimitedIOPS());
        IOLimitation_Thrift ioLimitationThrift = RequestResponseHelper.buildThriftIOLimitationFrom(ioLimitationDB);
        assertNotNull(ioLimitationThrift);
        assertEquals(1, ioLimitationThrift.getEntriesSize());
        IOLimitationEntry_Thrift ioLimitationEntryThrift = ioLimitationThrift.getEntries().get(0);
        assertEquals(100, ioLimitationEntryThrift.getUpperLimitedIOPS());
    }

    @Test
    public void testMigrationSpeedRuleStore() {

        MigrationRuleInformation migrationSpeedInformation;
        long ruleId = 10000000;

        migrationSpeedInformation = new MigrationRuleInformation(ruleId, "rule", 22, MigrationRuleStatus.AVAILABLE.toString());
        migrationRuleStore.save(migrationSpeedInformation);
        migrationSpeedInformation = new MigrationRuleInformation(ruleId+1, "rule1", 22, MigrationRuleStatus.AVAILABLE.toString());
        migrationRuleStore.save(migrationSpeedInformation);
        migrationSpeedInformation = new MigrationRuleInformation(ruleId+2, "rule2", 22, MigrationRuleStatus.AVAILABLE.toString());
        migrationRuleStore.save(migrationSpeedInformation);
        migrationSpeedInformation = new MigrationRuleInformation(ruleId+3, "rule3", 22, MigrationRuleStatus.AVAILABLE.toString());
        migrationRuleStore.save(migrationSpeedInformation);

        assertEquals(4, migrationRuleStore.list().size());
        assertTrue(migrationRuleStore.get(ruleId + 2).getMigrationRuleName().equals("rule2"));
        migrationRuleStore.delete(ruleId + 3);
        assertEquals(3, migrationRuleStore.list().size());

        migrationRuleStore.delete(ruleId);
        migrationRuleStore.delete(ruleId + 1);
        migrationRuleStore.delete(ruleId + 2);
        assertEquals(0, migrationRuleStore.list().size());

    }


    @Test
    public void testVolumeStore() throws AccessDeniedException {
        VolumeMetadata volumeMetadata = new VolumeMetadata(20000, 20001, 20002, 20003, null, null, 0L, 0L);

        volumeMetadata.setVolumeId(37002);
        volumeMetadata.setRootVolumeId(1003);
        volumeMetadata.setChildVolumeId(null);
        volumeMetadata.setVolumeSize(1005);
        volumeMetadata.setExtendingSize(1006);
        volumeMetadata.setName("stdname");
        volumeMetadata.setVolumeType(VolumeType.REGULAR);
        volumeMetadata.setCacheType(CacheType.MEMORY);
        volumeMetadata.setVolumeStatus(VolumeStatus.Available);
        volumeMetadata.setAccountId(SUPERADMIN_ACCOUNT_ID);
        volumeMetadata.setSegmentSize(1008);
        volumeMetadata.setDeadTime(0L);
        volumeMetadata.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
        volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
        volumeMetadata.setInAction(NULL);
        SegId segId = new SegId(37002, 11);
        SegmentMetadata segmentMetadata = new SegmentMetadata(segId, segId.getIndex());

        // segmentMetadata.getSegmentUnitMetadataTable().put(key, value);

        volumeMetadata.addSegmentMetadata(segmentMetadata, TestUtils.generateMembership());

        volumeStore.saveVolume(volumeMetadata);
        VolumeMetadata volumeMetadata1 = volumeStore.getVolume(37002L);
        assertNotNull(volumeMetadata1);
    }

    @Test
    public void testVolumeStoreCreatedTimeAndVolumeSource() throws AccessDeniedException {
        VolumeMetadata volumeMetadata = new VolumeMetadata(20000, 20001, 20002,
                20003, null, null, 0L, 0L);

        volumeMetadata.setVolumeId(37002);
        volumeMetadata.setRootVolumeId(1003);
        volumeMetadata.setChildVolumeId(null);
        volumeMetadata.setVolumeSize(1005);
        volumeMetadata.setExtendingSize(1006);
        volumeMetadata.setName("stdname");
        volumeMetadata.setVolumeType(VolumeType.REGULAR);
        volumeMetadata.setCacheType(CacheType.MEMORY);
        volumeMetadata.setVolumeStatus(VolumeStatus.Available);
        volumeMetadata.setAccountId(SUPERADMIN_ACCOUNT_ID);
        volumeMetadata.setSegmentSize(1008);
        volumeMetadata.setDeadTime(0L);
        volumeMetadata.setVolumeCreatedTime(new Date());
        volumeMetadata.setPageWrappCount(128);
        volumeMetadata.setSegmentWrappCount(10);
        volumeMetadata.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
        volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
        volumeMetadata.setInAction(NULL);
        SegId segId = new SegId(37002, 11);
        SegmentMetadata segmentMetadata = new SegmentMetadata(segId, segId.getIndex());


        volumeMetadata.addSegmentMetadata(segmentMetadata, TestUtils.generateMembership());

        volumeStore.saveVolume(volumeMetadata);
        VolumeMetadata volumeMetadata1;
        volumeMetadata1 = volumeStore.getVolume(37002L);
        Date now = new Date();
        assertTrue("CreatedTime isn't close enough to the testTime!", now.getTime() - volumeMetadata1.getVolumeCreatedTime().getTime() < 1000 * 60);
        assertEquals(volumeMetadata1.getVolumeSource(), VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    }

    @Test
    public void testDomainDBStore() throws SQLException, IOException {
        Long domainId = 0L;
        Set<Long> dataNodes = new HashSet<>();
        for (long key = 0; key < 3; key++) {
            dataNodes.add(key);
        }

        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.setDomainName("testForDomainDB");
        domain.setDomainDescription(null);
        domain.setDataNodes(dataNodes);
        domain.setStoragePools(dataNodes);


        domainDBStore.saveDomainToDB(domain);
        Domain domainFromDB = domainDBStore.getDomainFromDB(domainId);
        Set<Long> dataNodesInDB = domainFromDB.getDataNodes();
        assertTrue(dataNodesInDB.size() == 3);
        int sameCount = 0;
        for (Long datanodeFromDB : dataNodesInDB) {
            for (Long datanodeOri : dataNodes) {
                if (datanodeOri.equals(datanodeFromDB)) {
                    sameCount++;
                }
            }
        }

        assertTrue(sameCount == 3);
        assertTrue(dataNodes.size() == 3);
        // remove one element
        Long deleteValue = 0L;
        dataNodes.remove(deleteValue);
        assertTrue(dataNodes.size() == 2);

        domainDBStore.saveDomainToDB(domain);
        domainFromDB = domainDBStore.getDomainFromDB(domainId);
        dataNodesInDB = domainFromDB.getDataNodes();
        assertTrue(dataNodesInDB.size() == 2);
        sameCount = 0;
        for (Long datanodeFromDB : dataNodesInDB) {
            for (Long datanodeOri : dataNodes) {
                if (datanodeOri.equals(datanodeFromDB)) {
                    sameCount++;
                }
            }
        }
        assertTrue(sameCount == 2);

        domainDBStore.deleteDomainFromDB(domainId);
        domainFromDB = domainDBStore.getDomainFromDB(domainId);
        assertNull(domainFromDB);
    }

    @Test
    public void testDomainStore() throws SQLException, IOException {
        Long domainId = 0L;
        Set<Long> dataNodes = new HashSet<>();
        for (int key = 0; key < 3; key++) {
            dataNodes.add((long) key);
        }
        Domain domain = new Domain();
        domain.setDomainId(domainId);
        domain.setDomainName("testForDomainMem");
        domain.setDomainDescription(null);
        domain.setDataNodes(dataNodes);

        domainStore.saveDomain(domain);
        Domain domainFromDB = domainStore.getDomain(domainId);
        Set<Long> dataNodesInMem = domainFromDB.getDataNodes();
        assertTrue(dataNodesInMem.size() == 3);
        int sameCount = 0;
        for (Long datanodeFromDB : dataNodesInMem) {
            for (Long datanodeOri : dataNodes) {
                if (datanodeOri.equals(datanodeFromDB)) {
                    sameCount++;
                }
            }
        }
        assertTrue(sameCount == 3);

        assertTrue(dataNodes.size() == 3);
        // remove one element
        Long deleteValue = 0L;
        dataNodes.remove(deleteValue);
        assertTrue(dataNodes.size() == 2);

        domainStore.saveDomain(domain);
        domainFromDB = domainStore.getDomain(domainId);
        dataNodesInMem = domainFromDB.getDataNodes();
        assertTrue(dataNodesInMem.size() == 2);
        sameCount = 0;
        for (Long datanodeFromDB : dataNodesInMem) {
            for (Long datanodeOri : dataNodes) {
                if (datanodeOri.equals(datanodeFromDB)) {
                    sameCount++;
                }
            }
        }
        assertTrue(sameCount == 2);

        domainStore.deleteDomain(domainId);
        domainFromDB = domainStore.getDomain(domainId);
        assertTrue(domainFromDB == null);
    }

    @Test  //nyj
    public void testCloneRelationshipsStore() {
        Long srcVolumeId = 0L;
        Long cloneVolumeId1 = 1L;
        Long cloneVolumeId2 = 2L;
        Long cloneVolumeId3 = 3L;

        CloneRelationshipInformation cloneRelationshipInformation1 = new CloneRelationshipInformation();
        CloneRelationshipInformation cloneRelationshipInformation2 = new CloneRelationshipInformation();
        CloneRelationshipInformation cloneRelationshipInformation3 = new CloneRelationshipInformation();

        cloneRelationshipInformation1.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation1.setDestVolumeId(cloneVolumeId1);

        cloneRelationshipInformation2.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation2.setDestVolumeId(cloneVolumeId2);

        cloneRelationshipInformation3.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation3.setDestVolumeId(cloneVolumeId3);

        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation1);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation2);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation3);

        assertNotNull(cloneRelationshipsStore.getFromDB(cloneVolumeId1));
        assertNotNull(cloneRelationshipsStore.getFromDB(cloneVolumeId2));
        assertNotNull(cloneRelationshipsStore.getFromDB(cloneVolumeId3));

        assertEquals(3, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(3, cloneRelationshipsStore.loadFromDB().size());

        cloneRelationshipsStore.deleteFromDB(cloneVolumeId1);
        cloneRelationshipsStore.deleteFromDB(cloneVolumeId2);
        cloneRelationshipsStore.deleteFromDB(cloneVolumeId3);
        assertTrue(cloneRelationshipsStore.loadFromDB().isEmpty());
    }

    @Test   //nyj
    public void testDomainStoreCleanMemAndPutNewDomainBeforeList() throws SQLException, IOException {
        Long domainId1 = 1L;
        Long domainId2 = 2L;
        Long domainId3 = 3L;
        Long domainId4 = 4L;
        Domain domain1 = new Domain(domainId1, "testdomain1", null, new HashSet<>(), new HashSet<>());
        Domain domain2 = new Domain(domainId2, "testdomain2", null, new HashSet<>(), new HashSet<>());
        Domain domain3 = new Domain(domainId3, "testdomain3", null, new HashSet<>(), new HashSet<>());
        Domain domain4 = new Domain(domainId4, "testdomain4", null, new HashSet<>(), new HashSet<>());
        //put1,2,3
        domainStore.saveDomain(domain1);
        domainStore.saveDomain(domain2);
        domainStore.saveDomain(domain3);

        List<Domain> domainList = domainStore.listAllDomains();
        assertTrue(domainList.size() == 3);

        //clear1,2,3
        domainStore.clearMemoryMap();
        //list
        domainList = domainStore.listAllDomains();
        assertTrue(domainList.size() == 3);

        //clear1,2,3
        domainStore.clearMemoryMap();
        //put4
        domainStore.saveDomain(domain4);
        //list
        domainList = domainStore.listAllDomains();
        assertTrue(domainList.size() == 1);
        Assert.assertEquals(domain4.getDomainId(), domainList.get(0).getDomainId());

        // (put4)  clear4
        domainStore.clearMemoryMap();
        //list
        domainList = domainStore.listAllDomains();
        assertTrue(domainList.size() == 4);
    }

    @Test
    public void testDomainStoreMemClean() throws SQLException, IOException {
        Long domainId1 = 0L;
        Long domainId2 = 1L;
        Long domainId3 = 2L;
        long instanceIndex = 0;
        Set<Long> dataNodes = new HashSet<>();

        instanceIndex++;
        dataNodes.add(instanceIndex);

        Domain domain0 = new Domain(domainId1, "testForDomainMem1", null, dataNodes, new HashSet<>());
        Domain domain1 = new Domain(domainId2, "testForDomainMem2", null, dataNodes, new HashSet<>());
        Domain domain2 = new Domain(domainId3, "testForDomainMem3", null, dataNodes, new HashSet<>());
        domainStore.saveDomain(domain0);
        domainStore.saveDomain(domain1);
        domainStore.saveDomain(domain2);
        List<Domain> domainList;
        domainList = domainStore.listAllDomains();
        assertTrue(domainList.size() == 3);
        assertTrue(domainList.contains(domain0));
        assertTrue(domainList.contains(domain1));
        assertTrue(domainList.contains(domain2));

        List<Long> domainIds = new ArrayList<>();
        domainIds.add(0L);
        domainList = domainStore.listDomains(domainIds);
        assertTrue(domainList.size() == 1);
        assertTrue(domainList.contains(domain0));

        // clear memory map
        domainStore.clearMemoryMap();
        domainList = domainStore.listAllDomains();
        assertTrue(domainList.size() == 3);
        int sameCount = 0;
        for (Domain domain : domainList) {
            if (domain.equals(domain0)) {
                sameCount++;
            } else if (domain.equals(domain1)) {
                sameCount++;
            } else if (domain.equals(domain2)) {
                sameCount++;
            }
        }
        assertTrue(sameCount == 3);
        // clear memory map
        domainStore.clearMemoryMap();
        domainList = domainStore.listDomains(domainIds);
        assertTrue(domainList.size() == 1);
        assertTrue(domainList.contains(domain0));
    }


    @Test
    public void testStoragePoolDBStore() throws InterruptedException, SQLException, IOException {
        StoragePool originalStoragePool = TestUtils.buildStoragePool();
        storagePoolDBStore.saveStoragePoolToDB(originalStoragePool);

        StoragePool fromDBPool = storagePoolDBStore.getStoragePoolFromDB(originalStoragePool.getPoolId());
        assertEquals(originalStoragePool, fromDBPool);

        Long datanodeId = (Long) originalStoragePool.getArchivesInDataNode().keySet().toArray()[0];
        // test delete one archive id
        assertNotNull(datanodeId);
        Long deleteArchiveId = (Long) originalStoragePool.getArchivesInDataNode().get(datanodeId).toArray()[0];

        originalStoragePool.removeArchiveFromDatanode(datanodeId, deleteArchiveId);

        assertTrue(!originalStoragePool.getArchivesInDataNode().containsEntry(datanodeId, deleteArchiveId));
        storagePoolDBStore.saveStoragePoolToDB(originalStoragePool);

        fromDBPool = storagePoolDBStore.getStoragePoolFromDB(originalStoragePool.getPoolId());
        assertTrue(fromDBPool.equals(originalStoragePool));

        // test add one archive id
        Long newArchiveId = RequestIdBuilder.get();
        originalStoragePool.addArchiveInDatanode(datanodeId, newArchiveId);
        assertTrue(originalStoragePool.getArchivesInDataNode().containsEntry(datanodeId, newArchiveId));
        storagePoolDBStore.saveStoragePoolToDB(originalStoragePool);

        fromDBPool = storagePoolDBStore.getStoragePoolFromDB(originalStoragePool.getPoolId());
        assertTrue(fromDBPool.equals(originalStoragePool));

        // test remove volumeId
        Long deleteVolumeId = (Long) originalStoragePool.getVolumeIds().toArray()[0];
        originalStoragePool.removeVolumeId(deleteVolumeId);

        assertTrue(!originalStoragePool.getVolumeIds().contains(deleteVolumeId));
        storagePoolDBStore.saveStoragePoolToDB(originalStoragePool);

        fromDBPool = storagePoolDBStore.getStoragePoolFromDB(originalStoragePool.getPoolId());
        assertTrue(fromDBPool.equals(originalStoragePool));
        // test add volumeId
        Long newVolumeId = RequestIdBuilder.get();
        originalStoragePool.addVolumeId(newVolumeId);

        assertTrue(originalStoragePool.getVolumeIds().contains(newVolumeId));
        storagePoolDBStore.saveStoragePoolToDB(originalStoragePool);

        fromDBPool = storagePoolDBStore.getStoragePoolFromDB(originalStoragePool.getPoolId());
        assertTrue(fromDBPool.equals(originalStoragePool));
        storagePoolDBStore.deleteStoragePoolFromDB(fromDBPool.getPoolId());
        fromDBPool = storagePoolDBStore.getStoragePoolFromDB(originalStoragePool.getPoolId());
        assertTrue(fromDBPool == null);
    }

    @Test
    public void testStoragePoolStore() throws SQLException, IOException {
        StoragePool originalStoragePool = TestUtils.buildStoragePool();
        storagePoolStore.saveStoragePool(originalStoragePool);

        StoragePool fromDBPool = storagePoolStore.getStoragePool(originalStoragePool.getPoolId());
        assertTrue(fromDBPool.equals(originalStoragePool));

        Long datanodeId = (Long) originalStoragePool.getArchivesInDataNode().keySet().toArray()[0];
        // test delete one archive id
        assertNotNull(datanodeId);
        Long deleteArchiveId = (Long) originalStoragePool.getArchivesInDataNode().get(datanodeId).toArray()[0];

        originalStoragePool.removeArchiveFromDatanode(datanodeId, deleteArchiveId);

        assertTrue(!originalStoragePool.getArchivesInDataNode().containsEntry(datanodeId, deleteArchiveId));
        storagePoolStore.saveStoragePool(originalStoragePool);

        fromDBPool = storagePoolStore.getStoragePool(originalStoragePool.getPoolId());
        assertTrue(fromDBPool.equals(originalStoragePool));

        // test add one archive id
        Long newArchiveId = RequestIdBuilder.get();
        originalStoragePool.addArchiveInDatanode(datanodeId, newArchiveId);
        assertTrue(originalStoragePool.getArchivesInDataNode().containsEntry(datanodeId, newArchiveId));
        storagePoolStore.saveStoragePool(originalStoragePool);

        fromDBPool = storagePoolStore.getStoragePool(originalStoragePool.getPoolId());
        assertTrue(fromDBPool.equals(originalStoragePool));

        // test remove volumeId
        Long deleteVolumeId = (Long) originalStoragePool.getVolumeIds().toArray()[0];
        originalStoragePool.removeVolumeId(deleteVolumeId);

        assertTrue(!originalStoragePool.getVolumeIds().contains(deleteVolumeId));
        storagePoolStore.saveStoragePool(originalStoragePool);

        fromDBPool = storagePoolStore.getStoragePool(originalStoragePool.getPoolId());
        assertTrue(fromDBPool.equals(originalStoragePool));
        // test add volumeId
        Long newVolumeId = RequestIdBuilder.get();
        originalStoragePool.addVolumeId(newVolumeId);

        assertTrue(originalStoragePool.getVolumeIds().contains(newVolumeId));
        storagePoolStore.saveStoragePool(originalStoragePool);

        fromDBPool = storagePoolStore.getStoragePool(originalStoragePool.getPoolId());
        assertTrue(fromDBPool.equals(originalStoragePool));
    }

    @Test
    public void testListStoragePool() throws SQLException, IOException {
        Long domainId = RequestIdBuilder.get();
        StoragePool storagePool1 = TestUtils.buildStoragePool();
        storagePool1.setDomainId(domainId);
        StoragePool storagePool2 = TestUtils.buildStoragePool();
        storagePool2.setDomainId(domainId);
        StoragePool storagePool3 = TestUtils.buildStoragePool();

        storagePoolStore.saveStoragePool(storagePool1);
        storagePoolStore.saveStoragePool(storagePool2);
        storagePoolStore.saveStoragePool(storagePool3);

        List<StoragePool> storagePools = storagePoolStore.listStoragePools(domainId);

        assertTrue(storagePools.size() == 2);
        for (StoragePool storagePool : storagePools) {
            if (!storagePool.equals(storagePool1) && !storagePool.equals(storagePool2)) {
                assertTrue(false);
            }
        }
    }

    @Test
    public void testCleanStoragePoolStoreMemory() throws SQLException, IOException {
        StoragePool storagePool1 = TestUtils.buildStoragePool();
        StoragePool storagePool2 = TestUtils.buildStoragePool();
        StoragePool storagePool3 = TestUtils.buildStoragePool();
        storagePoolStore.saveStoragePool(storagePool1);
        storagePoolStore.saveStoragePool(storagePool2);
        storagePoolStore.saveStoragePool(storagePool3);

        List<StoragePool> allStoragePools = storagePoolStore.listAllStoragePools();
        assertEquals(3, allStoragePools.size());

        List<Long> listPoolIds = new ArrayList<>();
        listPoolIds.add(storagePool1.getPoolId());
        listPoolIds.add(storagePool2.getPoolId());
        List<StoragePool> someStoragePools = storagePoolStore.listStoragePools(listPoolIds);
        assertEquals(2, someStoragePools.size());

        storagePoolStore.clearMemoryMap();
        allStoragePools = storagePoolStore.listAllStoragePools();
        assertEquals(3, allStoragePools.size());
    }

    @Test
    public void testCapacityRecordDBStore() throws Exception {
        CapacityRecord capacityRecord1 = TestUtils.buildCapacityRecord();

        // save 1 to db
        capacityRecordDBStore.saveToDB(capacityRecord1);
        CapacityRecord loadRecordFromDB1 = capacityRecordDBStore.loadFromDB();
        for (Entry<String, TotalAndUsedCapacity> entry : capacityRecord1.getRecordMap().entrySet()) {
            assertEquals(entry.getValue(), loadRecordFromDB1.getRecordMap().get(entry.getKey()));
        }
        CapacityRecord capacityRecord2 = TestUtils.buildCapacityRecord();

        // save 2 to db
        capacityRecordDBStore.saveToDB(capacityRecord2);
        CapacityRecord loadRecordFromDB2 = capacityRecordDBStore.loadFromDB();
        for (Entry<String, TotalAndUsedCapacity> entry : capacityRecord2.getRecordMap().entrySet()) {
            assertEquals(entry.getValue(), loadRecordFromDB2.getRecordMap().get(entry.getKey()));
        }
    }

    @Test
    public void testCapacityRecordStore() throws Exception {
        CapacityRecord capacityRecord1 = TestUtils.buildCapacityRecord();
        capacityRecordStore.saveCapacityRecord(capacityRecord1);

        CapacityRecord recordFrom1 = capacityRecordStore.getCapacityRecord();
        assertEquals(capacityRecord1, recordFrom1);

        recordFrom1.getRecordMap().clear();
        TotalAndUsedCapacity capacityInfo = new TotalAndUsedCapacity(RequestIdBuilder.get(), RequestIdBuilder.get());
        recordFrom1.getRecordMap().put(TestBase.getRandomString(8), capacityInfo);
        CapacityRecord recordFrom2 = capacityRecordStore.getCapacityRecord();
        assertEquals(capacityRecord1, recordFrom2);
    }

    @Test
    public void testCapacityRecordRemoveEarliestRecord() throws InterruptedException {
        CapacityRecord capacityRecord1 = TestUtils.buildCapacityRecord();
        capacityRecord1.getRecordMap().clear();
        TotalAndUsedCapacity capacityInfo = new TotalAndUsedCapacity(RequestIdBuilder.get(), RequestIdBuilder.get());

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        Date nowDate = new Date();
        String firstKey = dateFormat.format(nowDate);
        capacityRecord1.addRecord(firstKey, capacityInfo);

        Thread.sleep(1000);
        nowDate = new Date();
        String secondKey = dateFormat.format(nowDate);
        capacityRecord1.addRecord(secondKey, capacityInfo);

        Thread.sleep(1000);
        nowDate = new Date();
        String thirdKey = dateFormat.format(nowDate);
        capacityRecord1.addRecord(thirdKey, capacityInfo);

        Thread.sleep(1000);
        nowDate = new Date();
        String fourthKey = dateFormat.format(nowDate);
        capacityRecord1.addRecord(fourthKey, capacityInfo);

        assertTrue(capacityRecord1.recordCount() == 4);

        capacityRecord1.removeEarliestRecord();
        assertTrue(capacityRecord1.recordCount() == 3);
        assertTrue(!capacityRecord1.getRecordMap().containsKey(firstKey));

        capacityRecord1.removeEarliestRecord();
        assertTrue(capacityRecord1.recordCount() == 2);
        assertTrue(!capacityRecord1.getRecordMap().containsKey(secondKey));

        capacityRecord1.removeEarliestRecord();
        assertTrue(capacityRecord1.recordCount() == 1);
        assertTrue(!capacityRecord1.getRecordMap().containsKey(thirdKey));

        capacityRecord1.removeEarliestRecord();
        assertTrue(capacityRecord1.recordCount() == 0);
    }

    @Test
    public void testBuildJson() {
        String dtoAsString = null;
        ObjectMapper mapper = new ObjectMapper();
        try {
            mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
            dtoAsString = mapper.writeValueAsString(new TotalAndUsedCapacity());
        } catch (JsonProcessingException e) {
            assertTrue(false);
        }
        assertTrue(dtoAsString != null);
    }

    @Test
    public void testBackupDBManagerFirstRound() throws Exception {
        // init config for backup DB manager
        long roundTimeInterval = 3000; // ms
        int maxBackupCount = 3;
        BackupDBManager backupDBManager = new BackupDBManagerImpl(roundTimeInterval, maxBackupCount,
                volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
                capacityRecordStore, cloneRelationshipsStore, licenseDBStore, accountStore,
                apiStore, roleStore, resourceStore, null, iscsiRuleRelationshipStore,
                iscsiAccessRuleStore,ioLimitationStore, migrationRuleStore);

        GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
                GenericThriftClientFactory.class);
        ((BackupDBManagerImpl) backupDBManager).setDataNodeClientFactory(dataNodeClientFactory);
        DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
        when(dataNodeClientFactory.generateSyncClient(any(EndPoint.class), any(Long.class), any(Integer.class)))
                .thenReturn(dataNodeClient);

        // test first round
        long sequenceId1 = 10;
        EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
        ReportDBRequest_Thrift reportRequest1 = TestUtils.buildReportDBRequest(1, endPoint1, sequenceId1);

        long sequenceId2 = 9;
        EndPoint endPoint2 = new EndPoint("10.0.1.2", 1234);
        ReportDBRequest_Thrift reportRequest2 = TestUtils.buildReportDBRequest(2, endPoint2, sequenceId2);

        long sequenceId3 = 11;
        EndPoint endPoint3 = new EndPoint("10.0.1.3", 1234);
        ReportDBRequest_Thrift reportRequest3 = TestUtils.buildReportDBRequest(3, endPoint3, sequenceId3);

        GetDBInfoResponse_Thrift getDBInfoResponse = new GetDBInfoResponse_Thrift();
        getDBInfoResponse.setRequestId(RequestIdBuilder.get());
        getDBInfoResponse.setDbInfo(reportRequest3);
        when(dataNodeClient.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse);

        backupDBManager.process(reportRequest1);
        assertEquals(0, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        assertEquals(sequenceId1, ((BackupDBManagerImpl) backupDBManager).getRoundSequenceId().longValue());

        backupDBManager.process(reportRequest2);
        assertEquals(0, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        assertEquals(sequenceId1, ((BackupDBManagerImpl) backupDBManager).getRoundSequenceId().longValue());

        backupDBManager.process(reportRequest3);
        assertEquals(0, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        assertEquals(sequenceId3, ((BackupDBManagerImpl) backupDBManager).getRoundSequenceId().longValue());

        // test in first round, same endpoint show up many times, but record won't save any one
        backupDBManager.process(reportRequest1);
        assertEquals(0, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        backupDBManager.process(reportRequest2);
        assertEquals(0, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        backupDBManager.process(reportRequest3);
        assertEquals(0, ((BackupDBManagerImpl) backupDBManager).getRecordCount());

        // test after first round, biggest sequence id report, but not accept
        Thread.sleep(roundTimeInterval);
        assertEquals(0, ((BackupDBManagerImpl) backupDBManager).getRecordCount());

        long sequenceId4 = 14;
        EndPoint endPoint4 = new EndPoint("10.0.1.4", 1234);
        ReportDBRequest_Thrift reportRequest4 = TestUtils.buildReportDBRequest(4, endPoint4, sequenceId4);

        /*
         * in this process, manager will save request3 to database
         */
        ReportDBResponse_Thrift reportResponseThrift = backupDBManager.process(reportRequest4);

        long afterFirstRoundSequenceId = sequenceId3 + 1;
        assertEquals(afterFirstRoundSequenceId,
                ((BackupDBManagerImpl) backupDBManager).getRoundSequenceId().longValue());
        assertEquals(afterFirstRoundSequenceId, reportResponseThrift.getSequenceId());
        assertEquals(1, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        // test save to db and load from db
        TestUtils.compareReportRequestAndReportResponse(reportRequest3, reportResponseThrift);

        reportResponseThrift = backupDBManager.process(reportRequest1);
        assertEquals(2, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        assertEquals(afterFirstRoundSequenceId, reportResponseThrift.getSequenceId());
        TestUtils.compareReportRequestAndReportResponse(reportRequest3, reportResponseThrift);

        reportResponseThrift = backupDBManager.process(reportRequest1);
        assertEquals(2, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        assertEquals(afterFirstRoundSequenceId, reportResponseThrift.getSequenceId());
        assertFalse(reportResponseThrift.isSetDomainThriftList());
        assertFalse(reportResponseThrift.isSetStoragePoolThriftList());
        assertFalse(reportResponseThrift.isSetVolume2RuleThriftList());
        assertFalse(reportResponseThrift.isSetAccessRuleThriftList());
        assertFalse(reportResponseThrift.isSetCapacityRecordThriftList());
        assertFalse(reportResponseThrift.isSetAccountMetadataBackupThriftList());
        assertFalse(reportResponseThrift.isSetRoleThriftList());

        reportResponseThrift = backupDBManager.process(reportRequest2);
        assertEquals(3, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        assertEquals(afterFirstRoundSequenceId, reportResponseThrift.getSequenceId());
        TestUtils.compareReportRequestAndReportResponse(reportRequest3, reportResponseThrift);

        /*
         * cause max save count is 3, if has been saved 3 datanodes, more datanode come, but manager will not response
         * database info
         */
        reportResponseThrift = backupDBManager.process(reportRequest3);
        assertEquals(3, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        assertEquals(afterFirstRoundSequenceId, reportResponseThrift.getSequenceId());
        assertFalse(reportResponseThrift.isSetDomainThriftList());
        assertFalse(reportResponseThrift.isSetStoragePoolThriftList());
        assertFalse(reportResponseThrift.isSetVolume2RuleThriftList());
        assertFalse(reportResponseThrift.isSetAccessRuleThriftList());
        assertFalse(reportResponseThrift.isSetCapacityRecordThriftList());
        assertFalse(reportResponseThrift.isSetAccountMetadataBackupThriftList());
        assertFalse(reportResponseThrift.isSetRoleThriftList());
    }

    @Test
    public void testBackupDBManagerAfterFirstRound1() throws Exception {
        // init config for backup DB manager
        long roundTimeInterval = 1000; // ms
        int maxBackupCount = 1;
        BackupDBManager backupDBManager = new BackupDBManagerImpl(roundTimeInterval, maxBackupCount,
                volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
                capacityRecordStore, cloneRelationshipsStore, licenseDBStore, accountStore,
                apiStore, roleStore, resourceStore, null, iscsiRuleRelationshipStore,
                iscsiAccessRuleStore,ioLimitationStore, migrationRuleStore);

        GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
                GenericThriftClientFactory.class);
        ((BackupDBManagerImpl) backupDBManager).setDataNodeClientFactory(dataNodeClientFactory);
        DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
        when(dataNodeClientFactory.generateSyncClient(any(EndPoint.class), any(Long.class), any(Integer.class)))
                .thenReturn(dataNodeClient);

        // process first round
        long sequenceId1 = 10;
        EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
        ReportDBRequest_Thrift reportRequest1 = TestUtils.buildReportDBRequest(1, endPoint1, sequenceId1);

        GetDBInfoResponse_Thrift getDBInfoResponse = new GetDBInfoResponse_Thrift();
        getDBInfoResponse.setRequestId(RequestIdBuilder.get());
        getDBInfoResponse.setDbInfo(reportRequest1);
        when(dataNodeClient.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse);

        backupDBManager.process(reportRequest1);

        assertEquals(0, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        assertEquals(sequenceId1, ((BackupDBManagerImpl) backupDBManager).getRoundSequenceId().longValue());
        Thread.sleep(roundTimeInterval);
        // do loop process
        int loopCount = 100;
        int groupCount = 5;
        long currentSequenceId = sequenceId1;
        for (int i = 0; i < loopCount; i++) {
            logger.debug("== loop:{} times ==", i);
            int groupId0 = i % groupCount;
            if (RandomUtils.nextBoolean()) {
                Thread.sleep(roundTimeInterval);
                ReportDBRequest_Thrift reportRequest = TestUtils.buildReportDBRequest(groupId0, endPoint1, sequenceId1);
                ReportDBResponse_Thrift reportResponseThrift = backupDBManager.process(reportRequest);
                assertTrue(reportResponseThrift.getSequenceId() > currentSequenceId);
                currentSequenceId = reportResponseThrift.getSequenceId();
                assertTrue(((BackupDBManagerImpl) backupDBManager).getRecordCount() <= maxBackupCount);
                TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);
            }
        }
    }

    @Test
    public void testBackupDBManagerAfterFirstRound2() throws Exception {
        // init config for backup DB manager
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.DEBUG);
        long roundTimeInterval = 1000; // ms
        int maxBackupCount = 3;
        BackupDBManager backupDBManager = new BackupDBManagerImpl(roundTimeInterval, maxBackupCount,
                volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
                capacityRecordStore, cloneRelationshipsStore, licenseDBStore, accountStore,
                        apiStore, roleStore, resourceStore, null, iscsiRuleRelationshipStore,
                iscsiAccessRuleStore,ioLimitationStore, migrationRuleStore);

        GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
                GenericThriftClientFactory.class);
        ((BackupDBManagerImpl) backupDBManager).setDataNodeClientFactory(dataNodeClientFactory);
        DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
        when(dataNodeClientFactory.generateSyncClient(any(EndPoint.class), any(Long.class), any(Integer.class)))
                .thenReturn(dataNodeClient);

        // process first round
        long sequenceId1 = 10;
        EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
        ReportDBRequest_Thrift reportRequest1 = TestUtils.buildReportDBRequest(1, endPoint1, sequenceId1);

        GetDBInfoResponse_Thrift getDBInfoResponse = new GetDBInfoResponse_Thrift();
        getDBInfoResponse.setRequestId(RequestIdBuilder.get());
        getDBInfoResponse.setDbInfo(reportRequest1);
        when(dataNodeClient.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse);

        backupDBManager.process(reportRequest1);

        assertEquals(0, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        assertEquals(sequenceId1, ((BackupDBManagerImpl) backupDBManager).getRoundSequenceId().longValue());
        Thread.sleep(roundTimeInterval);
        // do loop process
        int loopCount = 100;
        int groupCount = 5;
        long currentSequenceId = sequenceId1;
        for (int i = 0; i < loopCount; i++) {
            logger.debug("== loop:{} times ==", i);
            int groupId0 = i % groupCount;
            if (RandomUtils.nextBoolean()) {
                ReportDBRequest_Thrift reportRequest = TestUtils.buildReportDBRequest(groupId0, endPoint1, sequenceId1);
                ReportDBResponse_Thrift reportResponseThrift = backupDBManager.process(reportRequest);
                assertTrue(reportResponseThrift.getSequenceId() >= currentSequenceId);
                currentSequenceId = reportResponseThrift.getSequenceId();
                if (!reportResponseThrift.isSetDomainThriftList()) {
                    if (!((BackupDBManagerImpl) backupDBManager).getRoundRecordMap().containsKey(new Group(groupId0))) {
                        logger.debug("map:{}, current group:{}",
                                ((BackupDBManagerImpl) backupDBManager).getRoundRecordMap(), groupId0);
                        assertEquals(maxBackupCount, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
                    }
                } else {
                    TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);
                }
            }
            int groupId1 = (i + 1) % groupCount;
            if (RandomUtils.nextBoolean()) {
                ReportDBRequest_Thrift reportRequest = TestUtils.buildReportDBRequest(groupId1, endPoint1, sequenceId1);
                ReportDBResponse_Thrift reportResponseThrift = backupDBManager.process(reportRequest);
                assertTrue(reportResponseThrift.getSequenceId() >= currentSequenceId);
                currentSequenceId = reportResponseThrift.getSequenceId();
                if (!reportResponseThrift.isSetDomainThriftList()) {
                    if (!((BackupDBManagerImpl) backupDBManager).getRoundRecordMap().containsKey(new Group(groupId1))) {
                        logger.debug("map:{}, current group:{}",
                                ((BackupDBManagerImpl) backupDBManager).getRoundRecordMap(), groupId1);
                        assertEquals(maxBackupCount, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
                    }
                } else {
                    TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);
                }
            }
            int groupId2 = (i + 2) % groupCount;
            if (RandomUtils.nextBoolean()) {
                ReportDBRequest_Thrift reportRequest = TestUtils.buildReportDBRequest(groupId2, endPoint1, sequenceId1);
                ReportDBResponse_Thrift reportResponseThrift = backupDBManager.process(reportRequest);
                assertTrue(reportResponseThrift.getSequenceId() >= currentSequenceId);
                currentSequenceId = reportResponseThrift.getSequenceId();
                if (!reportResponseThrift.isSetDomainThriftList()) {
                    if (!((BackupDBManagerImpl) backupDBManager).getRoundRecordMap().containsKey(new Group(groupId2))) {
                        logger.debug("map:{}, current group:{}",
                                ((BackupDBManagerImpl) backupDBManager).getRoundRecordMap(), groupId2);
                        assertEquals(maxBackupCount, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
                    }
                } else {
                    TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);
                }
            }
            int groupId3 = (i + 3) % groupCount;
            if (RandomUtils.nextBoolean()) {
                ReportDBRequest_Thrift reportRequest = TestUtils.buildReportDBRequest(groupId3, endPoint1, sequenceId1);
                ReportDBResponse_Thrift reportResponseThrift = backupDBManager.process(reportRequest);
                assertTrue(reportResponseThrift.getSequenceId() >= currentSequenceId);
                currentSequenceId = reportResponseThrift.getSequenceId();
                if (!reportResponseThrift.isSetDomainThriftList()) {
                    if (!((BackupDBManagerImpl) backupDBManager).getRoundRecordMap().containsKey(new Group(groupId3))) {
                        logger.debug("map:{}, current group:{}",
                                ((BackupDBManagerImpl) backupDBManager).getRoundRecordMap(), groupId3);
                        assertEquals(maxBackupCount, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
                    }
                } else {
                    TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);
                }
            }
            int groupId4 = (i + 4) % groupCount;
            if (RandomUtils.nextBoolean()) {
                Thread.sleep(roundTimeInterval);
                ReportDBRequest_Thrift reportRequest = TestUtils.buildReportDBRequest(groupId4, endPoint1, sequenceId1);
                ReportDBResponse_Thrift reportResponseThrift = backupDBManager.process(reportRequest);
                assertTrue(reportResponseThrift.getSequenceId() >= currentSequenceId);
                currentSequenceId = reportResponseThrift.getSequenceId();
                if (!reportResponseThrift.isSetDomainThriftList()) {
                    if (!((BackupDBManagerImpl) backupDBManager).getRoundRecordMap().containsKey(new Group(groupId4))) {
                        logger.debug("map:{}, current group:{}",
                                ((BackupDBManagerImpl) backupDBManager).getRoundRecordMap(), groupId4);
                        assertEquals(maxBackupCount, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
                    }
                } else {
                    TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);
                }
            }
        }
    }

    @Test(timeout = 180000)
    public void testMultiThreadReportDBRequest() throws Exception {
        // init config for backup DB manager
        long roundTimeInterval = 1000; // ms
        int maxBackupCount = 3;
        LicenseStorage licenseStorage = mock(LicenseDBStore.class);
        when(licenseStorage.getLicense()).thenReturn(new LicenseJson());
        BackupDBManager backupDBManager = new BackupDBManagerImpl(roundTimeInterval, maxBackupCount,
                volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
        capacityRecordStore, cloneRelationshipsStore, licenseDBStore, accountStore,
                apiStore, roleStore, resourceStore,null, iscsiRuleRelationshipStore,
                iscsiAccessRuleStore,ioLimitationStore, migrationRuleStore);

        GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
                GenericThriftClientFactory.class);
        ((BackupDBManagerImpl) backupDBManager).setDataNodeClientFactory(dataNodeClientFactory);
        DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
        when(dataNodeClientFactory.generateSyncClient(any(EndPoint.class), any(Long.class), any(Integer.class)))
                .thenReturn(dataNodeClient);

        long sequenceId1 = 10;
        EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
        ReportDBRequest_Thrift reportRequest1 = TestUtils.buildReportDBRequest(1, endPoint1, sequenceId1);

        GetDBInfoResponse_Thrift getDBInfoResponse = new GetDBInfoResponse_Thrift();
        getDBInfoResponse.setRequestId(RequestIdBuilder.get());
        getDBInfoResponse.setDbInfo(reportRequest1);
        when(dataNodeClient.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse);

        backupDBManager.process(reportRequest1);

        assertEquals(0, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        assertEquals(sequenceId1, ((BackupDBManagerImpl) backupDBManager).getRoundSequenceId().longValue());
        Thread.sleep(roundTimeInterval);

        int threadCount = 100;
        int eachThreadRunLoop = 100;
        final CountDownLatch allThreadLatch = new CountDownLatch(threadCount);
        AtomicBoolean meetError = new AtomicBoolean(false);
        for (int i = 0; i < threadCount; i++) {
            final int groupId = i;
            Thread thread = new Thread() {
                long sequenceId = 0;
                long currentSequenceId = 0;
                Group group = new Group(groupId % 5);
                EndPoint endPoint = new EndPoint("10.0.1.1", 1234);
                ReportDBResponse_Thrift saveReportResponse = null;
                int runTimes = 0;

                @Override
                public void run() {
                    while (true) {

                        ReportDBRequest_Thrift reportRequest = TestUtils.buildReportDBRequest(group.getGroupId(),
                                endPoint, sequenceId);
                        ReportDBResponse_Thrift reportResponseThrift = backupDBManager.process(reportRequest);
                        assertTrue(reportResponseThrift.getSequenceId() >= currentSequenceId);
                        currentSequenceId = reportResponseThrift.getSequenceId();
                        if (!reportResponseThrift.isSetDomainThriftList()) {
                            if (!((BackupDBManagerImpl) backupDBManager).getRoundRecordMap().containsKey(group)) {
                                logger.debug("map:{}, current group:{}",
                                        ((BackupDBManagerImpl) backupDBManager).getRoundRecordMap(), group);
                                if (maxBackupCount != ((BackupDBManagerImpl) backupDBManager).getRecordCount()) {
                                    logger.debug("meetError set true! maxBackupCount:{}; backupDBManager.getRecordCount:{}", maxBackupCount,
                                            ((BackupDBManagerImpl) backupDBManager).getRecordCount());
                                    meetError.set(true);
                                }
                            }
                        } else {
                            if (saveReportResponse == null) {
                                saveReportResponse = reportResponseThrift;
                            }
                            if (!TestUtils.compareTwoReportDBResponse(saveReportResponse, reportResponseThrift)) {
                                logger.debug("meetError set true! saveReportResponse:{}; reportResponseThrift:{}", saveReportResponse,
                                        reportResponseThrift);
                                meetError.set(true);
                            }
                        }
                        try {
                            logger.warn("== run times:{}==", runTimes);
                            sequenceId++;
                            runTimes++;
                            if (runTimes == eachThreadRunLoop) {
                                allThreadLatch.countDown();
                            }
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            logger.error("failed to sleep", e);
                        }
                    }
                }
            };
            thread.start();
        }
        allThreadLatch.await();
        assertTrue(!meetError.get());
    }

    @Test
    public void testActiveBackupDatabaseAfterFirstRoundActiveBackupBeforeReport() throws Exception {
        // init config for backup DB manager
        long roundTimeInterval = 3000; // ms
        int maxBackupCount = 3;
        InstanceStore instanceStore = mock(InstanceStore.class);
        BackupDBManager backupDBManager = new BackupDBManagerImpl(roundTimeInterval, maxBackupCount,
                volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
                capacityRecordStore, cloneRelationshipsStore, licenseDBStore, accountStore,
                apiStore, roleStore, resourceStore, instanceStore,iscsiRuleRelationshipStore,
                iscsiAccessRuleStore,ioLimitationStore, migrationRuleStore);
        GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
                GenericThriftClientFactory.class);
        ((BackupDBManagerImpl) backupDBManager).setDataNodeClientFactory(dataNodeClientFactory);
        DataNodeService.Iface dataNodeClient1 = mock(DataNodeService.Iface.class);
        DataNodeService.Iface dataNodeClient2 = mock(DataNodeService.Iface.class);
        DataNodeService.Iface dataNodeClient3 = mock(DataNodeService.Iface.class);

        Instance datanode1 = mock(Instance.class);
        Instance datanode2 = mock(Instance.class);
        Instance datanode3 = mock(Instance.class);
        EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
        EndPoint endPoint2 = new EndPoint("10.0.1.2", 1234);
        EndPoint endPoint3 = new EndPoint("10.0.1.3", 1234);
        Group group1 = new Group(1);
        Group group2 = new Group(2);
        Group group3 = new Group(3);
        when(datanode1.getGroup()).thenReturn(group1);
        when(datanode2.getGroup()).thenReturn(group2);
        when(datanode3.getGroup()).thenReturn(group3);
        Set<Instance> okDatanodes = new HashSet<>();
        okDatanodes.add(datanode1);
        okDatanodes.add(datanode2);
        okDatanodes.add(datanode3);
        when(instanceStore.getAll(PyService.DATANODE.getServiceName(), InstanceStatus.OK)).thenReturn(okDatanodes);

        when(dataNodeClientFactory.generateSyncClient(eq(endPoint1), any(Long.class), any(Integer.class)))
                .thenReturn(dataNodeClient1);
        when(dataNodeClientFactory.generateSyncClient(eq(endPoint2), any(Long.class), any(Integer.class)))
                .thenReturn(dataNodeClient2);
        when(dataNodeClientFactory.generateSyncClient(eq(endPoint3), any(Long.class), any(Integer.class)))
                .thenReturn(dataNodeClient3);

        when(datanode1.getEndPoint()).thenReturn(endPoint1);
        when(datanode2.getEndPoint()).thenReturn(endPoint2);
        when(datanode3.getEndPoint()).thenReturn(endPoint3);

        // test first round
        long sequenceId1 = 10;
        ReportDBRequest_Thrift reportRequest1 = TestUtils.buildReportDBRequest(1, endPoint1, sequenceId1);

        long sequenceId2 = 9;
        ReportDBRequest_Thrift reportRequest2 = TestUtils.buildReportDBRequest(2, endPoint2, sequenceId2);

        long sequenceId3 = 11;
        ReportDBRequest_Thrift reportRequest3 = TestUtils.buildReportDBRequest(3, endPoint3, sequenceId3);


        GetDBInfoResponse_Thrift getDBInfoResponse1 = new GetDBInfoResponse_Thrift();
        getDBInfoResponse1.setRequestId(RequestIdBuilder.get());
        getDBInfoResponse1.setDbInfo(reportRequest1);
        when(dataNodeClient1.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse1);

        GetDBInfoResponse_Thrift getDBInfoResponse2 = new GetDBInfoResponse_Thrift();
        getDBInfoResponse2.setRequestId(RequestIdBuilder.get());
        getDBInfoResponse2.setDbInfo(reportRequest2);
        when(dataNodeClient2.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse2);

        GetDBInfoResponse_Thrift getDBInfoResponse3 = new GetDBInfoResponse_Thrift();
        getDBInfoResponse3.setRequestId(RequestIdBuilder.get());
        getDBInfoResponse3.setDbInfo(reportRequest3);
        when(dataNodeClient3.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse3);

        logger.warn("Process reportRequest1.");
        backupDBManager.process(reportRequest1);

        backupDBManager.backupDatabase();

        logger.warn("Process reportRequest2.");
        ReportDBResponse_Thrift reportResponseThrift = backupDBManager.process(reportRequest2);

        assertEquals(sequenceId1, reportResponseThrift.getSequenceId());
        logger.warn("Process reportRequest3.");
        backupDBManager.process(reportRequest3);

        // test after first round, biggest sequence id report, but not accept
        Thread.sleep(roundTimeInterval + 1);
        assertEquals(0, ((BackupDBManagerImpl) backupDBManager).getRecordCount());

        backupDBManager.backupDatabase();

        // now all datanode will have reportRequest3 database info
        when(dataNodeClient1.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse3);
        when(dataNodeClient2.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse3);
        when(dataNodeClient3.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse3);

        long sequenceId4 = 14;
        EndPoint endPoint4 = new EndPoint("10.0.1.4", 1234);
        ReportDBRequest_Thrift reportRequest4 = TestUtils.buildReportDBRequest(4, endPoint4, sequenceId4);

        /*
         * in this process, manager will save request3 to database
         */
        logger.warn("Process reportRequest4.");
        reportResponseThrift = backupDBManager.process(reportRequest4);

        long backupRoundRefreshAmount = 2;

        long afterFirstRoundSequenceId = sequenceId3 + backupRoundRefreshAmount;
        assertEquals(afterFirstRoundSequenceId,
                ((BackupDBManagerImpl) backupDBManager).getRoundSequenceId().longValue());
        assertEquals(afterFirstRoundSequenceId, reportResponseThrift.getSequenceId());
        assertEquals(1, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        assertEquals(6, ((BackupDBManagerImpl) backupDBManager).getLastRecordCount());
        // test save to db and load from db
        TestUtils.compareReportRequestAndReportResponse(reportRequest3, reportResponseThrift);
    }

    @Test
    public void testActiveBackupDatabaseAfterFirstRoundReportBeforeActiveBackup() throws Exception {
        // init config for backup DB manager
        long roundTimeInterval = 3000; // ms
        int maxBackupCount = 3;
        InstanceStore instanceStore = mock(InstanceStore.class);
        BackupDBManager backupDBManager = new BackupDBManagerImpl(roundTimeInterval, maxBackupCount,
                volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
                capacityRecordStore, cloneRelationshipsStore, licenseDBStore, accountStore,
                apiStore, roleStore, resourceStore, instanceStore,iscsiRuleRelationshipStore,
                iscsiAccessRuleStore,ioLimitationStore, migrationRuleStore);

        GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
                GenericThriftClientFactory.class);
        ((BackupDBManagerImpl) backupDBManager).setDataNodeClientFactory(dataNodeClientFactory);
        DataNodeService.Iface dataNodeClient1 = mock(DataNodeService.Iface.class);
        DataNodeService.Iface dataNodeClient2 = mock(DataNodeService.Iface.class);
        DataNodeService.Iface dataNodeClient3 = mock(DataNodeService.Iface.class);

        Instance datanode1 = mock(Instance.class);
        Instance datanode2 = mock(Instance.class);
        Instance datanode3 = mock(Instance.class);
        EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
        EndPoint endPoint2 = new EndPoint("10.0.1.2", 1234);
        EndPoint endPoint3 = new EndPoint("10.0.1.3", 1234);
        Group group1 = new Group(1);
        Group group2 = new Group(2);
        Group group3 = new Group(3);
        when(datanode1.getGroup()).thenReturn(group1);
        when(datanode2.getGroup()).thenReturn(group2);
        when(datanode3.getGroup()).thenReturn(group3);
        Set<Instance> okDatanodes = new HashSet<>();
        okDatanodes.add(datanode1);
        okDatanodes.add(datanode2);
        okDatanodes.add(datanode3);
        when(instanceStore.getAll(PyService.DATANODE.getServiceName(), InstanceStatus.OK)).thenReturn(okDatanodes);

        when(dataNodeClientFactory.generateSyncClient(eq(endPoint1), any(Long.class), any(Integer.class)))
                .thenReturn(dataNodeClient1);
        when(dataNodeClientFactory.generateSyncClient(eq(endPoint2), any(Long.class), any(Integer.class)))
                .thenReturn(dataNodeClient2);
        when(dataNodeClientFactory.generateSyncClient(eq(endPoint3), any(Long.class), any(Integer.class)))
                .thenReturn(dataNodeClient3);

        when(datanode1.getEndPoint()).thenReturn(endPoint1);
        when(datanode2.getEndPoint()).thenReturn(endPoint2);
        when(datanode3.getEndPoint()).thenReturn(endPoint3);

        // test first round
        long sequenceId1 = 10;
        ReportDBRequest_Thrift reportRequest1 = TestUtils.buildReportDBRequest(1, endPoint1, sequenceId1);

        long sequenceId2 = 9;
        ReportDBRequest_Thrift reportRequest2 = TestUtils.buildReportDBRequest(2, endPoint2, sequenceId2);

        long sequenceId3 = 11;
        ReportDBRequest_Thrift reportRequest3 = TestUtils.buildReportDBRequest(3, endPoint3, sequenceId3);


        GetDBInfoResponse_Thrift getDBInfoResponse1 = new GetDBInfoResponse_Thrift();
        getDBInfoResponse1.setRequestId(RequestIdBuilder.get());
        getDBInfoResponse1.setDbInfo(reportRequest1);
        when(dataNodeClient1.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse1);

        GetDBInfoResponse_Thrift getDBInfoResponse2 = new GetDBInfoResponse_Thrift();
        getDBInfoResponse2.setRequestId(RequestIdBuilder.get());
        getDBInfoResponse2.setDbInfo(reportRequest2);
        when(dataNodeClient2.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse2);

        GetDBInfoResponse_Thrift getDBInfoResponse3 = new GetDBInfoResponse_Thrift();
        getDBInfoResponse3.setRequestId(RequestIdBuilder.get());
        getDBInfoResponse3.setDbInfo(reportRequest3);
        when(dataNodeClient3.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse3);

        backupDBManager.process(reportRequest1);

        backupDBManager.backupDatabase();

        ReportDBResponse_Thrift reportResponseThrift = backupDBManager.process(reportRequest2);

        assertEquals(sequenceId1, reportResponseThrift.getSequenceId());
        backupDBManager.process(reportRequest3);

        // test after first round, biggest sequence id report, but not accept
        Thread.sleep(roundTimeInterval);
        assertEquals(0, ((BackupDBManagerImpl) backupDBManager).getRecordCount());

        long sequenceId4 = 14;
        EndPoint endPoint4 = new EndPoint("10.0.1.4", 1234);
        ReportDBRequest_Thrift reportRequest4 = TestUtils.buildReportDBRequest(4, endPoint4, sequenceId4);

        /*
         * in this process, manager will save request3 to database
         */
        reportResponseThrift = backupDBManager.process(reportRequest4);

        long reportRoundRefreshAmount = 1;

        long afterFirstRoundSequenceId = sequenceId3 + reportRoundRefreshAmount;
        assertEquals(afterFirstRoundSequenceId,
                ((BackupDBManagerImpl) backupDBManager).getRoundSequenceId().longValue());
        assertEquals(afterFirstRoundSequenceId, reportResponseThrift.getSequenceId());
        assertEquals(1, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        assertEquals(3, ((BackupDBManagerImpl) backupDBManager).getLastRecordCount());
        // test save to db and load from db
        TestUtils.compareReportRequestAndReportResponse(reportRequest3, reportResponseThrift);

        backupDBManager.backupDatabase();

        long backupRoundRefreshAmount = 2;
        afterFirstRoundSequenceId += backupRoundRefreshAmount;
        assertEquals(afterFirstRoundSequenceId,
                ((BackupDBManagerImpl) backupDBManager).getRoundSequenceId().longValue());
        assertEquals(7, ((BackupDBManagerImpl) backupDBManager).getLastRecordCount());
    }

    @Test
    public void testUpdateSomeDBWhenReportDBRequest() throws Exception {
        // init config for backup DB manager
        long roundTimeInterval = 1000; // ms
        int maxBackupCount = 3;
        BackupDBManager backupDBManager = new BackupDBManagerImpl(roundTimeInterval, maxBackupCount,
                volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
        capacityRecordStore, cloneRelationshipsStore, licenseDBStore, accountStore,
                apiStore, roleStore, resourceStore,null, iscsiRuleRelationshipStore,
                iscsiAccessRuleStore,ioLimitationStore, migrationRuleStore);


        GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
                GenericThriftClientFactory.class);
        ((BackupDBManagerImpl) backupDBManager).setDataNodeClientFactory(dataNodeClientFactory);
        DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
        when(dataNodeClientFactory.generateSyncClient(any(EndPoint.class), any(Long.class), any(Integer.class)))
                .thenReturn(dataNodeClient);

        long sequenceId1 = 10;
        EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
        ReportDBRequest_Thrift reportRequest1 = TestUtils.buildReportDBRequest(1, endPoint1, sequenceId1);

        GetDBInfoResponse_Thrift getDBInfoResponse = new GetDBInfoResponse_Thrift();
        getDBInfoResponse.setRequestId(RequestIdBuilder.get());
        getDBInfoResponse.setDbInfo(reportRequest1);
        when(dataNodeClient.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse);

        backupDBManager.process(reportRequest1);

        assertEquals(0, ((BackupDBManagerImpl) backupDBManager).getRecordCount());
        assertEquals(sequenceId1, ((BackupDBManagerImpl) backupDBManager).getRoundSequenceId().longValue());
        Thread.sleep(roundTimeInterval + 100);

        ReportDBRequest_Thrift reportRequest = TestUtils.buildReportDBRequest(2, endPoint1, sequenceId1);
        ReportDBResponse_Thrift reportResponseThrift = backupDBManager.process(reportRequest);
        assertEquals(sequenceId1 + 1, reportResponseThrift.getSequenceId());
        TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);

        // clear all domains in memory and database
        for (Domain domain : domainStore.listAllDomains()) {
            domainStore.deleteDomain(domain.getDomainId());
        }
        assertEquals(0, domainStore.listAllDomains().size());
        // save new domains to database
        int newDomainCount = 5;
        List<Domain_Thrift> newDomainThriftList = new ArrayList<>();
        for (int i = 0; i < newDomainCount; i++) {
            Domain newDomain = TestUtils.buildDomain();
            newDomainThriftList.add(RequestResponseHelper.buildDomainThriftFrom(newDomain));
            domainStore.saveDomain(newDomain);
        }
        assertEquals(newDomainCount, domainStore.listAllDomains().size());
        assertEquals(newDomainCount, newDomainThriftList.size());
        // update these new domains to report request for compare
        reportRequest1.setDomainThriftList(newDomainThriftList);
        {
        }

        // report DB request again
        reportRequest = TestUtils.buildReportDBRequest(3, endPoint1, sequenceId1);
        reportResponseThrift = backupDBManager.process(reportRequest);
        assertEquals(reportResponseThrift.getSequenceId(), sequenceId1 + 1);
        assertTrue(((BackupDBManagerImpl) backupDBManager).getRecordCount() == 2);
        TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);
    }

    @Test
    public void testBackupDBManagerPassedRecoveryTime() throws Exception {
        for (int i = 0; i < 20; i++) {
            // init config for backup DB manager
            long roundTimeInterval = 500; // ms
            int maxBackupCount = 3;
            BackupDBManager backupDBManager = new BackupDBManagerImpl(roundTimeInterval, maxBackupCount,
                    volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
            capacityRecordStore, cloneRelationshipsStore, licenseDBStore, accountStore,
                    apiStore, roleStore, resourceStore, null,iscsiRuleRelationshipStore, iscsiAccessRuleStore,
                    ioLimitationStore, migrationRuleStore)
            ;
            long sequenceId1 = 10;
            EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
            ReportDBRequest_Thrift reportRequest1 = TestUtils.buildReportDBRequest(1, endPoint1, sequenceId1);

            GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
                    GenericThriftClientFactory.class);
            ((BackupDBManagerImpl) backupDBManager).setDataNodeClientFactory(dataNodeClientFactory);
            DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
            when(dataNodeClientFactory.generateSyncClient(any(EndPoint.class), any(Long.class), any(Integer.class)))
                    .thenReturn(dataNodeClient);

            GetDBInfoResponse_Thrift getDBInfoResponse = new GetDBInfoResponse_Thrift();
            getDBInfoResponse.setRequestId(RequestIdBuilder.get());
            getDBInfoResponse.setDbInfo(reportRequest1);
            when(dataNodeClient.getDBInfo(any(GetDBInfoRequest_Thrift.class))).thenReturn(getDBInfoResponse);

            logger.debug("loop time:{}", i);
            assertTrue(!backupDBManager.passedRecoveryTime());

            Thread.sleep(800);

            // round 0 over, round = 0
            backupDBManager.process(reportRequest1);

            assertTrue(!backupDBManager.passedRecoveryTime());

            Thread.sleep(800);
            // round 0 over, round = 1
            backupDBManager.process(reportRequest1);

            assertTrue(!backupDBManager.passedRecoveryTime());

            Thread.sleep(800);
            // round 1 over, round = 2
            backupDBManager.process(reportRequest1);

            assertTrue(backupDBManager.passedRecoveryTime());
        }
    }

    @Test
    public void testSaveOrUpdateServerNode() {
        ServerNode serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
                "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:DB:C3:48", "10.0.0.1", "manageIp", "storeIp",
                "rackNo", "slotNo");

        DiskInfo diskInfo = generateDiskInfo();
        Set<DiskInfo> diskInfoSet = new HashSet<>();
        diskInfoSet.add(diskInfo);
        serverNode.setDiskInfoSet(diskInfoSet);

        serverNodeStore.saveOrUpdateServerNode(serverNode);

        List<ServerNode> serverNodeList = serverNodeStore.listAllServerNodes();
        System.out.println(serverNodeList.get(0));
        assertEquals(1, serverNodeList.size());

        diskInfo = generateDiskInfo();
        diskInfoSet.clear();
        diskInfoSet.add(diskInfo);
        serverNode.setDiskInfoSet(diskInfoSet);
        serverNodeStore.saveOrUpdateServerNode(serverNode);
        serverNodeList = serverNodeStore.listAllServerNodes();
        System.out.println(serverNodeList.get(0));
        assertEquals(1, serverNodeList.get(0).getDiskInfoSet().size());
    }

    @Test
    public void testDeleteServerNodes() {
        ServerNode serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
                "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:DB:C3:48", "10.0.0.1", "manageIp", "storeIp",
                "rackNo", "slotNo");
        DiskInfo diskInfo = generateDiskInfo();
        Set<DiskInfo> diskInfoSet = new HashSet<>();
        diskInfoSet.add(diskInfo);
        serverNode.setDiskInfoSet(diskInfoSet);
        serverNodeStore.saveOrUpdateServerNode(serverNode);

        assertEquals(1, serverNodeStore.getCountTotle());
        serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
                "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:DB:C3:48", "10.0.0.1", "manageIp", "storeIp",
                "rackNo", "slotNo");
        serverNodeStore.saveOrUpdateServerNode(serverNode);
        assertEquals(2, serverNodeStore.getCountTotle());

        List<ServerNode> serverNodeList = serverNodeStore.listAllServerNodes();
        List<String> ids = new ArrayList<>();
        for (ServerNode serverNode1 : serverNodeList) {
            ids.add(serverNode1.getId());
        }
        serverNodeStore.deleteServerNodes(ids);
        assertEquals(0, serverNodeStore.getCountTotle());
    }

    @Test
    public void testUpdateServerNode() {
        ServerNode serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
                "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:DB:C3:48", "10.0.0.1", "manageIp", "storeIp",
                "rackNo", "slotNo");
        serverNodeStore.saveOrUpdateServerNode(serverNode);
        assertEquals(1, serverNodeStore.getCountTotle());
        serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
                "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:DB:C3:48", "10.0.0.1", "manageIp", "storeIp",
                "rackNo", "slotNo");
        serverNodeStore.saveOrUpdateServerNode(serverNode);
        assertEquals(2, serverNodeStore.getCountTotle());

        List<ServerNode> serverNodeList = serverNodeStore.listAllServerNodes();

        String updatedInfo = "updatedCpuInfo";
        ServerNode serverNode1 = serverNodeList.get(0);
        serverNode1.setCpuInfo(updatedInfo);
        serverNodeStore.updateServerNode(serverNode1);

        List<ServerNode> serverNodeList1 = serverNodeStore.listAllServerNodes();
        int count = 0;
        for (ServerNode serverNode2 : serverNodeList1) {
            if (serverNode2.getId().equals(serverNode1.getId())) {
                assertEquals(serverNode2.getCpuInfo(), updatedInfo);
            } else {
                count++;
            }
        }
        assertEquals(1, count);
    }

    @Test
    public void testListServerNodes() {
        ServerNode serverNode1 = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
                "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:DB:C3:48", "10.0.0.1", "manageIp", "storeIp",
                "rack-001", "001");
        ServerNode serverNode2 = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
                "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:DB:C3:48", "10.0.0.1", "manageIp", "storeIp",
                "rack-001", "002");
        ServerNode serverNode3 = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
                "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:DB:C3:48", "10.0.0.1", "manageIp", "storeIp",
                "rack-001", "003");
        ServerNode serverNode4 = generateServerNode("Intel® Core™ i7-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
                "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:DB:C3:48", "10.0.0.1", "manageIp", "storeIp",
                "rack-002", "001");
        ServerNode serverNode5 = generateServerNode("Intel® Core™ i7-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
                "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:DB:C3:48", "10.0.0.1", "manageIp", "storeIp",
                "rack-002", "002");
        ServerNode serverNode6 = generateServerNode("Intel® Pentium™  CPU @ 3.40GHz × 4", "1.2 tB", "31.3 GiB",
                "HP DL360G6 E5506 PROMO 9018AP Server", "xx:1B:0D:DB:C3:48", "10.0.0.2", "manageIpTest", "storeIpTest",
                "rack-002", "003");

        DiskInfo diskInfo = generateDiskInfo();
        Set<DiskInfo> diskInfoSet = new HashSet<>();
        diskInfoSet.add(diskInfo);
        serverNode1.setDiskInfoSet(diskInfoSet);

        serverNodeStore.saveOrUpdateServerNode(serverNode1);
        serverNodeStore.saveOrUpdateServerNode(serverNode2);
        serverNodeStore.saveOrUpdateServerNode(serverNode3);
        serverNodeStore.saveOrUpdateServerNode(serverNode4);
        serverNodeStore.saveOrUpdateServerNode(serverNode5);
        serverNodeStore.saveOrUpdateServerNode(serverNode6);

        assertEquals(6, serverNodeStore.getCountTotle());

        List<ServerNode> serverNodeList = serverNodeStore
                .listServerNodes(0, 10, null, null, null, null, null, null, null, null, null, null, null, "rack-001", null);
        assertEquals(3, serverNodeList.size());

        serverNodeList = serverNodeStore
                .listServerNodes(0, 10, null, null, null, null, null, null, null, null, null, null, null, null, "002");
        assertEquals(2, serverNodeList.size());

        serverNodeList = serverNodeStore
                .listServerNodes(0, 10, null, null, null, null, null, null, null, null, null, null, null, "rack-001", "002");
        assertEquals(1, serverNodeList.size());

        serverNodeList = serverNodeStore
                .listServerNodes(0, 10, null, null, null, null, "core", null, null, null, null, null, null, null, null);
        assertEquals(5, serverNodeList.size());

        serverNodeList = serverNodeStore
                .listServerNodes(0, 10, null, null, null, null, null, null, "1.2 T", null, null, null, null, null, null);
        assertEquals(1, serverNodeList.size());

        serverNodeList = serverNodeStore
                .listServerNodes(0, 10, null, null, null, null, null, null, "1.2 T", "Xx", null, null, null, null, null);
        assertEquals(1, serverNodeList.size());

        serverNodeList = serverNodeStore
                .listServerNodes(0, 10, null, null, null, null, null, null, null, null, null, "0.0.2", "storeIptest", null, null);
        assertEquals(1, serverNodeList.size());

    }

    @Test
    public void testGetServerNodeByIp() {
        ServerNode serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
                "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:DB:C3:48", "10.0.0.1", "manageIp", "storeIp",
                "rackNo", "slotNo");
        serverNode.setNetworkCardInfoName("10.0.2.231##172.16.10.231");
        serverNodeStore.saveOrUpdateServerNode(serverNode);

        ServerNode serverNodeByIp = serverNodeStore.getServerNodeByIp("10.0.2.231");

        assertEquals(serverNode.getId(), serverNodeByIp.getId());
    }

    @Test
    public void testListDiskInfo() {

        ServerNode serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
                "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:DB:C3:48", "10.0.0.1", "manageIp", "storeIp",
                "rackNo", "slotNo");

        DiskInfo diskInfo = generateDiskInfo();
        diskInfo.setServerNode(serverNode);
        Set<DiskInfo> diskInfoSet = new HashSet<>();
        diskInfoSet.add(diskInfo);
        serverNode.setDiskInfoSet(diskInfoSet);

        serverNodeStore.saveOrUpdateServerNode(serverNode);

        assertEquals(1, diskInfoStore.listDiskInfos().size());

        DiskInfo diskInfoFromDb = diskInfoStore.listDiskInfoById(diskInfo.getId());

        assertEquals(diskInfo.toString(), diskInfoFromDb.toString());
    }

    @Test
    public void testUpdateDiskInfo(){
        ServerNode serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
                "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:DB:C3:48", "10.0.0.1", "manageIp", "storeIp",
                "rackNo", "slotNo");

        DiskInfo diskInfo = generateDiskInfo();
        diskInfo.setServerNode(serverNode);
        Set<DiskInfo> diskInfoSet = new HashSet<>();
        diskInfoSet.add(diskInfo);
        serverNode.setDiskInfoSet(diskInfoSet);

        serverNodeStore.saveOrUpdateServerNode(serverNode);

        assertEquals(1, diskInfoStore.listDiskInfos().size());

        assertEquals(null, diskInfoStore.listDiskInfoById(diskInfo.getId()).getSwith());

        diskInfoStore.updateDiskInfoLightStatusById(diskInfo.getId(), DiskInfoLightStatus.ON.toString());

        assertEquals(DiskInfoLightStatus.ON.toString(), diskInfoStore.listDiskInfoById(diskInfo.getId()).getSwith());
    }

    private ServerNode generateServerNode(String cpuInfo, String diskInfo, String memoryInfo, String modelInfo,
                                          String networkCardInfo, String gatewayIp, String manageIp, String storeIp, String rackNo, String slotNo) {
        ServerNode serverNode = new ServerNode();
        serverNode.setId(UUID.randomUUID().toString());
        serverNode.setCpuInfo(cpuInfo);
        serverNode.setDiskInfo(diskInfo);
        serverNode.setMemoryInfo(memoryInfo);
        serverNode.setModelInfo(modelInfo);
        serverNode.setNetworkCardInfo(networkCardInfo);
        serverNode.setGatewayIp(gatewayIp);
        serverNode.setManageIp(manageIp);
        serverNode.setStoreIp(storeIp);
        serverNode.setRackNo(rackNo);
        serverNode.setSlotNo(slotNo);
        return serverNode;
    }

    private DiskInfo generateDiskInfo() {
        DiskInfo diskInfo = new DiskInfo();
        diskInfo.setId(String.valueOf(RequestIdBuilder.get()));
        diskInfo.setSn(String.valueOf(RequestIdBuilder.get()));
        diskInfo.setName("sda");
        diskInfo.setVendor("vendor");
        diskInfo.setSsdOrHdd("hdd");
        diskInfo.setRate(65535);
        diskInfo.setModel("model");
        diskInfo.setSerialNumber("0101019230");

        diskInfo.setReadErrorRate(1);
        diskInfo.setReallocatedSector(1);
        diskInfo.setSpinRetryCount(1);
        diskInfo.setEndtoEndError(1);
        diskInfo.setCommandTimeout(1);
        diskInfo.setReallocationEvent_Count(1);
        diskInfo.setCurrentPendingSector(1);
        diskInfo.setOfflineUncorrectable(1);
        diskInfo.setSoftReadErrorRate(1);

        return diskInfo;
    }

    @After
    public void cleanUp() {
    }
}
