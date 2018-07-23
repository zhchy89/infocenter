package py.infocenter.service;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.icshare.InstanceMetadata.DatanodeType.NORMAL;
import static py.icshare.InstanceMetadata.DatanodeType.SIMPLE;
import static py.volume.VolumeMetadata.VolumeInAction.NULL;
import static py.volume.VolumeMetadata.VolumeSourceType.CREATE_VOLUME;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.thrift.TException;
import org.glassfish.grizzly.utils.ArraySet;
import org.hibernate.SessionFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.fasterxml.jackson.core.JsonProcessingException;

import junit.framework.Assert;
import py.app.context.AppContext;
import py.archive.ArchiveStatus;
import py.archive.ArchiveType;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.archive.segment.*;
import py.common.Constants;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.client.RequestResponseHelper;
import py.common.struct.EndPoint;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.infocenter.InfoCenterAppContext;
import py.icshare.*;
import py.infocenter.common.InfoCenterConstants;
import py.infocenter.service.selection.BalancedDriverContainerSelectionStrategy;
import py.infocenter.service.selection.DriverContainerSelectionStrategy;
import py.infocenter.service.selection.InstanceSelectionStrategy;
import py.infocenter.store.*;
import py.infocenter.test.utils.TestBeans;
import py.infocenter.worker.StorageStoreSweeper;
import py.infocenter.worker.VolumeSweeper;
import py.instance.*;
import py.membership.SegmentMembership;
import py.monitorserver.common.UserDefineName;
import py.querylog.EventDataUtil.EventDataWorker;
import py.test.TestBase;
import py.test.TestUtils;
import py.thrift.icshare.*;
import py.thrift.infocenter.service.*;
import py.thrift.share.*;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

public class InformationCenterImplTest extends TestBase {
    @Autowired
    private SessionFactory sessionFactory;

    private InformationCenterImpl informationCenter;

    @Mock
    private DBVolumeStoreImpl dbVolumeStore;

    @Mock
    private SegmentUnitTimeoutStore timeoutStore;

    private VolumeStatusTransitionStore statusStore = new VolumeStatusTransitionStoreImpl();

    @Mock
    private InstanceStore instanceStore;

    @Mock
    private AppContext appContext;

    @Mock
    private StorageStore storageStore;

    private TwoLevelVolumeStoreImpl volumeStore;
    @Mock
    private TwoLevelVolumeStoreImpl volumeStore1;

    @Mock
    private TwoLevelVolumeStoreImpl volumeStore2;

    @Mock
    private InstanceSelectionStrategy instanceSelectionStrategy;

    @Mock
    private OrphanVolumeStore orphanVolumeStore;

    @Mock
    private DomainStore domainStore;

    @Mock
    private StoragePoolStore storagePoolStore;

    @Mock
    private InstanceMaintenanceDBStore instanceMaintenanceDBStore;

    private CloneRelationshipsDBStore cloneRelationshipsStore;

    private DriverStore driverStore;
    private AccessRuleStore accessRuleStore;
    private VolumeRuleRelationshipStore volumeRuleRelationshipStore;

    long segmentSize = 100;

    long deadVolumeToRemove = 15552000;

    VolumeSweeper volumeSweeper = null;

    private StorageStoreSweeper storageStoreSweeper;

    @Before
    public void setup() {
        InfoCenterAppContext appContext = mock(InfoCenterAppContext.class);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        ApplicationContext ctx = new AnnotationConfigApplicationContext(TestBeans.class);

        informationCenter = new InformationCenterImpl();
        informationCenter.setInstanceStore(instanceStore);
        volumeStore = new TwoLevelVolumeStoreImpl(new MemoryVolumeStoreImpl(), dbVolumeStore);
        informationCenter.setVolumeStore(volumeStore);
        driverStore = ctx.getBean(DriverStore.class);
        accessRuleStore = ctx.getBean(AccessRuleStore.class);
        volumeRuleRelationshipStore = ctx.getBean(VolumeRuleRelationshipStore.class);
        cloneRelationshipsStore = ctx.getBean(CloneRelationshipsDBStore.class);

        informationCenter.setDriverStore(driverStore);
        informationCenter.setSegmentSize(100);
        informationCenter.setDeadVolumeToRemove(deadVolumeToRemove);
        informationCenter.setSegmentUnitTimeoutStore(timeoutStore);
        informationCenter.setVolumeStatusStore(statusStore);
        informationCenter.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore);
        informationCenter.setAppContext(appContext);
        informationCenter.setOrphanVolumes(orphanVolumeStore);
        informationCenter.setDomainStore(domainStore);
        informationCenter.setStoragePoolStore(storagePoolStore);
        informationCenter.setAccessRuleStore(accessRuleStore);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);
        informationCenter.setStorageStore(storageStore);
        informationCenter.setPageWrappCount(128);
        informationCenter.setSegmentWrappCount(10);

        volumeSweeper = new VolumeSweeper();
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);
        volumeSweeper.setAppContext(appContext);
        volumeSweeper.setVolumeStore(volumeStore);
        volumeSweeper.setCloneRelationshipsStore(cloneRelationshipsStore);

        storageStoreSweeper = new StorageStoreSweeper();
        storageStoreSweeper.setAppContext(appContext);
        storageStoreSweeper.setStoragePoolStore(storagePoolStore);
        storageStoreSweeper.setInstanceMetadataStore(storageStore);
        storageStoreSweeper.setDomainStore(domainStore);
        storageStoreSweeper.setVolumeStore(volumeStore);
        storageStoreSweeper.setInstanceMaintenanceDBStore(instanceMaintenanceDBStore);

        List<CloneRelationshipInformation> cloneRelationshipInformationList = cloneRelationshipsStore.loadFromDB();
        if (null != cloneRelationshipInformationList && !cloneRelationshipInformationList.isEmpty()) {
            cloneRelationshipInformationList.forEach(cloneRelationshipInformation -> cloneRelationshipsStore
                    .deleteFromDB(cloneRelationshipInformation.getDestVolumeId()));
        }

        DriverContainerSelectionStrategy balancedDriverContainerSelectionStrategy = new BalancedDriverContainerSelectionStrategy();
        informationCenter.setDriverContainerSelectionStrategy(balancedDriverContainerSelectionStrategy);
    }

    @Test
    public void testReportSegmentUnitsMetadata() throws Exception {
        // report the first volume
        VolumeMetadata volumeMetadata = generateVolumeMetadata(1, null, 6, 3, 1);
        volumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);
        ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volumeMetadata,
                SegmentUnitStatus.Secondary);

        when(dbVolumeStore.getVolume(anyLong())).thenReturn(null);
        informationCenter.reportSegmentUnitsMetadata(request);
        // check volume
        VolumeMetadata volumeFromStore = volumeStore.getVolume(1L);
        assertEquals(1, volumeFromStore.getVersion());
        assertEquals(null, volumeFromStore.getChildVolumeId());
        assertEquals(0, volumeFromStore.getPositionOfFirstSegmentInLogicVolume());
        assertTrue(volumeFromStore.isUpdatedToDataNode());
        assertTrue(volumeFromStore.isPersistedToDatabase());

        // now the volume store has the first volume, update volume
        volumeMetadata = generateVolumeMetadata(1, null, 6, 3, 2);
        volumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);
        request = generateSegUnitsMetadataRequest(volumeMetadata, SegmentUnitStatus.Secondary);
        informationCenter.reportSegmentUnitsMetadata(request);

        // check volume
        volumeFromStore = volumeStore.getVolume(1L);
        assertEquals(2, volumeFromStore.getVersion());
        assertEquals(null, volumeFromStore.getChildVolumeId());
        assertEquals(0, volumeFromStore.getPositionOfFirstSegmentInLogicVolume());
        assertTrue(volumeFromStore.isUpdatedToDataNode());
        assertTrue(volumeFromStore.isPersistedToDatabase());

        // report new volume with child volume
        long childVolumeId = 10;
        int positionOfFirstSegmentInLogicVolume = 10;

        volumeMetadata = generateVolumeMetadata(2, childVolumeId, 6, 3, 1);
        volumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);
        request = generateSegUnitsMetadataRequest(volumeMetadata, SegmentUnitStatus.Secondary);
        informationCenter.reportSegmentUnitsMetadata(request);

        // check volume
        volumeFromStore = volumeStore.getVolume(2L);
        assertEquals(1, volumeFromStore.getVersion());
        assertEquals(volumeFromStore.getChildVolumeId(), new Long(childVolumeId));
        assertEquals(0, volumeFromStore.getPositionOfFirstSegmentInLogicVolume());
        assertTrue(volumeFromStore.isUpdatedToDataNode());
        assertTrue(volumeFromStore.isPersistedToDatabase());

        // report child volume
        volumeMetadata = generateVolumeMetadata(childVolumeId, null, 6, 3, 1);
        volumeMetadata.setPositionOfFirstSegmentInLogicVolume(positionOfFirstSegmentInLogicVolume);
        request = generateSegUnitsMetadataRequest(volumeMetadata, SegmentUnitStatus.Secondary);
        informationCenter.reportSegmentUnitsMetadata(request);

        // check volume
        volumeFromStore = volumeStore.getVolume(childVolumeId);
        assertEquals(1, volumeFromStore.getVersion());
        assertEquals(null, volumeFromStore.getChildVolumeId());
        assertEquals(positionOfFirstSegmentInLogicVolume, volumeFromStore.getPositionOfFirstSegmentInLogicVolume());
        assertTrue(volumeFromStore.isUpdatedToDataNode());
        assertTrue(volumeFromStore.isPersistedToDatabase());
    }

    @Test
    public void testReportSegmentUnitMetadata_normalProcess() throws Exception {
        VolumeMetadata volume = generateVolumeMetadata(1, null, 3, 3, 1);
        volumeStore.saveVolume(volume);

        ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Primary,
                1, 0);
        informationCenter.reportSegmentUnitsMetadata(request);

        request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 2, 0);
        informationCenter.reportSegmentUnitsMetadata(request);

        request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 3, 0);
        informationCenter.reportSegmentUnitsMetadata(request);

        assertEquals(volume.getSegments().size(), 1);
        volume.updateStatus(false,false);

        assertEquals(volume.getVolumeStatus(), VolumeStatus.Available);
    }

    /**
     * volume not exist, but after process first segment unit, it will produce a new volume;
     *
     * @throws Exception
     */
    @Test
    public void testReportSegmentUnitMetadata_volumeNotExist() throws Exception {

        VolumeMetadata volume = generateVolumeMetadata(1, null, segmentSize, segmentSize, 1);
        VolumeMetadata saveVolume = volumeStore.getVolume(volume.getVolumeId());
        assertNull(saveVolume);

        // report a segment unit, it will create a new volume in info center
        ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Primary,
                1, 0);
        informationCenter.reportSegmentUnitsMetadata(request);
        saveVolume = volumeStore.getVolume(volume.getVolumeId());
        assertNotNull(saveVolume);
        saveVolume.updateStatus(false,false);

        // continue to report other two secondary segment units
        request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 2, 0);
        informationCenter.reportSegmentUnitsMetadata(request);
        request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 3, 0);
        informationCenter.reportSegmentUnitsMetadata(request);

        saveVolume.updateStatus(false,false);
        assertEquals(saveVolume.getSegments().size(), 1);
        assertEquals(saveVolume.getVolumeStatus(), VolumeStatus.Available);
    }

    /**
     * volume exists with status dead but segment unit is not dead, info center will save segment unit to notice data
     * node to delete it
     *
     * @throws Exception
     */
    @Test
    public void testReportSegmentUnitMetadata_volumeDead_segmentnotdead() throws Exception {
        VolumeMetadata volume = generateVolumeMetadata(1, null, segmentSize, segmentSize, 1);
        volume.setVolumeStatus(VolumeStatus.Dead);
        volumeStore.saveVolume(volume);

        ReportSegmentUnitsMetadataResponse response1;
        ReportSegmentUnitsMetadataResponse response2;
        ReportSegmentUnitsMetadataResponse response3;

        ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Primary,
                1, 0);
        response1 = informationCenter.reportSegmentUnitsMetadata(request);

        request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 2, 0);
        response2 = informationCenter.reportSegmentUnitsMetadata(request);

        request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 3, 0);
        response3 = informationCenter.reportSegmentUnitsMetadata(request);

        // for primary, info center not notify data node to delete
        assertEquals(response1.getConflicts().size(), 0);
        assertEquals(response2.getConflicts().size(), 1);
        assertEquals(response3.getConflicts().size(), 1);
    }

    /**
     * volume exists with status is dead. segment unit is also deleted or deleting. nothing to do
     *
     * @throws Exception
     */
    @Test
    public void testReportSegmentUnitMetadata_volumeDead_segmentdead() throws Exception {
        VolumeMetadata volume = generateVolumeMetadata(1, null, segmentSize, segmentSize, 1);
        volume.setVolumeStatus(VolumeStatus.Dead);
        volumeStore.saveVolume(volume);

        ReportSegmentUnitsMetadataResponse response1;
        ReportSegmentUnitsMetadataResponse response2;
        ReportSegmentUnitsMetadataResponse response3;

        ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Deleted,
                1, 0);
        response1 = informationCenter.reportSegmentUnitsMetadata(request);
        request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Deleting, 2, 0);
        response2 = informationCenter.reportSegmentUnitsMetadata(request);
        request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Deleting, 3, 0);
        response3 = informationCenter.reportSegmentUnitsMetadata(request);

        assertEquals(response1.getConflicts().size(), 0);
        assertEquals(response2.getConflicts().size(), 0);
        assertEquals(response3.getConflicts().size(), 0);
    }

    /**
     * test the last updated time beyond 6 months and the segment is not the first. Then info center create a fake
     * volume
     *
     * @throws Exception
     */
    @Test
    public void testReportSegmentUnitMetadata_notfirst_createAnFakeVolume() throws Exception {
        long beforeTime = System.currentTimeMillis() / 1000 - (deadVolumeToRemove + 1);

        VolumeMetadata volume = generateVolumeMetadata(1, null, 2 * segmentSize, segmentSize, 1);
        ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary,
                1, 1);
        SegmentUnitMetadata_Thrift segUnit = request.getSegUnitsMetadata().get(0);
        segUnit.setLastUpdated(beforeTime);

        informationCenter.reportSegmentUnitsMetadata(request);
        VolumeMetadata fakeVolume = volumeStore.getVolume(volume.getVolumeId());
        assertEquals(fakeVolume.getVolumeStatus(), VolumeStatus.Unavailable);
        assertTrue(fakeVolume.getName().contains(InfoCenterConstants.name));

        // when the first segment come, it id will be changed;
        request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Primary, 1, 0);
        informationCenter.reportSegmentUnitsMetadata(request);
        fakeVolume = volumeStore.getVolume(volume.getVolumeId());
        assertEquals(fakeVolume.getName(), volume.getName());
    }

    /**
     * test the last updated time beyond 6 months and the segment is the first. Not to create a fake volume
     *
     * @throws Exception
     */
    @Test
    public void testReportSegmentUnitMetadata_firstSegment_notcreatefakevolume() throws Exception {
        long beforeTime = System.currentTimeMillis() / 1000 - (deadVolumeToRemove + 1);
        VolumeMetadata volume = generateVolumeMetadata(1, null, 2 * segmentSize, segmentSize, 1);
        ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Primary,
                1, 0);
        SegmentUnitMetadata_Thrift segUnit = request.getSegUnitsMetadata().get(0);
        segUnit.setLastUpdated(beforeTime);

        informationCenter.reportSegmentUnitsMetadata(request);
        VolumeMetadata saveVolume = volumeStore.getVolume(volume.getVolumeId());
        assertEquals(saveVolume.getVolumeStatus(), VolumeStatus.ToBeCreated);
        assertEquals(saveVolume.getName(), volume.getName());

        saveVolume.updateStatus(false,false);
        assertEquals(saveVolume.getVolumeStatus(), VolumeStatus.Creating);
    }

    /**
     * test the normal situation and the segment unit data is changed;
     *
     * @throws Exception
     */
    @Test
    public void testReportSegmentUnitMetadata_segunit_datachange() throws Exception {
        VolumeMetadata volume = generateVolumeMetadata(1, null, segmentSize, segmentSize, 1);
        ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary,
                2, 0);
        informationCenter.reportSegmentUnitsMetadata(request);
        request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Primary, 1, 0);
        informationCenter.reportSegmentUnitsMetadata(request);
        request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 3, 0);
        informationCenter.reportSegmentUnitsMetadata(request);

        VolumeMetadata saveVolume = volumeStore.getVolume(volume.getVolumeId());
        assertEquals(saveVolume.getName(), volume.getName());

        saveVolume.updateStatus(false,false);
        assertEquals(saveVolume.getVolumeStatus(), VolumeStatus.Available);
        request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Primary, 1, 0);
        SegmentUnitMetadata_Thrift segUnit = request.getSegUnitsMetadata().get(0);
        segUnit.getMembership().setEpoch(2);
        informationCenter.reportSegmentUnitsMetadata(request);

        SegmentMetadata segment = saveVolume.getSegmentByIndex(0);
        SegmentUnitMetadata segmentUnit = segment.getSegmentUnitMetadata(new InstanceId(1));
        assertEquals(segmentUnit.getMembership().getSegmentVersion().getEpoch(), 2);
    }

    @Test
    public void testReportSegmentUnit_normalStatus_withinfocenter_sweeper() throws Exception {
        VolumeMetadata volume = generateVolumeMetadata(1, null, segmentSize, segmentSize, 1);
        volume.setVolumeSource(CREATE_VOLUME);
        VolumeMetadata saveVolume = volumeStore.getVolume(volume.getVolumeId());
        assertNull(saveVolume);

        // report a segment unit, it will create a new volume in info center
        ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Primary,
                1, 0);
        informationCenter.reportSegmentUnitsMetadata(request);
        saveVolume = volumeStore.getVolume(volume.getVolumeId());
        assertNotNull(saveVolume);

        volumeSweeper.doWork();
        saveVolume = volumeStore.getVolume(volume.getVolumeId());
        assertEquals(VolumeStatus.Creating, saveVolume.getVolumeStatus());

        // continue to report other two secondary segment units
        request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 2, 0);
        informationCenter.reportSegmentUnitsMetadata(request);
        request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 3, 0);
        informationCenter.reportSegmentUnitsMetadata(request);

        volumeSweeper.doWork();
        saveVolume = volumeStore.getVolume(volume.getVolumeId());
        assertEquals(1, saveVolume.getSegments().size());
        assertEquals(VolumeStatus.Available, saveVolume.getVolumeStatus());
    }

    @Test
    public void testReportDriverMetadata() throws Exception {
        VolumeMetadata testVolume = new VolumeMetadata();
        testVolume.setVolumeId(1L);
        testVolume.setName("test_volume");
        volumeStore.saveVolume(testVolume);
        // report first driver
        Long driverContainerId = RequestIdBuilder.get();
        DriverMetadata_Thrift driverMetadata_Thrift = new DriverMetadata_Thrift();
        driverMetadata_Thrift.setVolumeId(1L);
        driverMetadata_Thrift.setDriverType(DriverType_Thrift.NBD);
        driverMetadata_Thrift.setDriverStatus(DriverStatus_Thrift.LAUNCHED);
        driverMetadata_Thrift.setHostName("hostname1");
        driverMetadata_Thrift.setPort(1);
        driverMetadata_Thrift.setDriverContainerId(driverContainerId);
        driverMetadata_Thrift.setDynamicIOLimitationId(100);
        driverMetadata_Thrift.setStaticIOLimitationId(101);
        driverMetadata_Thrift.setPortalType(PortalType_Thrift.IPV4);
        ReportDriverMetadataRequest request = new ReportDriverMetadataRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.addToDrivers(driverMetadata_Thrift);
        informationCenter.reportDriverMetadata(request);

        DriverMetadata driverMetadata1 = driverStore.get(driverContainerId, 1L, DriverType.NBD, 0);
        logger.warn("{}", driverMetadata1);
        long lastReportTime1 = driverMetadata1.getLastReportTime();

        // report second driver
        request.setRequestId(RequestIdBuilder.get());
        informationCenter.reportDriverMetadata(request);

        DriverMetadata driverMetadata2 = driverStore.get(driverContainerId, 1L, DriverType.NBD, 0);
        long lastReportTime2 = driverMetadata2.getLastReportTime();

        // check driver
        assertTrue(lastReportTime2 > lastReportTime1);
        assertTrue(driverMetadata1.getHostName().equals(driverMetadata2.getHostName()));
        assertTrue(driverMetadata1.getVolumeId() == driverMetadata2.getVolumeId());
    }

    /**
     * This is a test for driver container allocation. In the test, total 2 driver container instance are available.
     * First allocation and second allocation are successful. But third allocation would not allocate any driver
     * container due to the two driver container is used up.
     *
     * @throws Exception
     */
    @Test
    public void testAllocDriverContainer() throws Exception {
        VolumeMetadata volume = generateVolumeMetadata(1, null, 6, 3, 1);
        volumeStore.saveVolume(volume);

        long driverContainerId = RequestIdBuilder.get();
        Instance instance1 = new Instance(new InstanceId(driverContainerId), null, PyService.DRIVERCONTAINER.name(),
                InstanceStatus.OK);
        instance1.putEndPointByServiceName(PortType.CONTROL, new EndPoint("0.0.0.1", 1));
        instance1.putEndPointByServiceName(PortType.IO, new EndPoint("0.0.0.1", 2));

        Set<Instance> instances = new HashSet<>();
        instances.add(instance1);

        when(instanceStore.getAll(any(String.class), any(InstanceStatus.class))).thenReturn(instances);

        // get driver container from information center
        AllocDriverContainerRequest request = new AllocDriverContainerRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(1L);
        request.setDriverAmount(1);
        request.setDriverType(DriverType_Thrift.NBD);
        request.setSnapshotId(0);
        AllocDriverContainerResponse response = informationCenter.allocDriverContainer(request);

        // check first driver container allocation
        Assert.assertEquals(1, response.getCandidatesSize());

        driverStore.delete(request.getVolumeId());
        DriverMetadata driverMetadata = new DriverMetadata();
        driverMetadata.setHostName("0.0.0.1");
        driverMetadata.setVolumeId(request.getVolumeId());
        driverMetadata.setDriverType(DriverType.NBD);
        driverMetadata.setSnapshotId(0);
        driverMetadata.setDriverContainerId(driverContainerId);
        driverMetadata.setDynamicIOLimitationId(100);
        driverMetadata.setStaticIOLimitationId(101);
        driverStore.save(driverMetadata);

        // check two NBD cannot on same driver container allocation
        informationCenter.allocDriverContainer(request);

        // check FSD and NBD can on the same driver container
        request.setDriverType(DriverType_Thrift.FSD);
        response = informationCenter.allocDriverContainer(request);
        Assert.assertEquals(1, response.getCandidatesSize());

        DriverMetadata driverMetadata1 = new DriverMetadata();
        driverMetadata1.setHostName("0.0.0.1");
        driverMetadata1.setVolumeId(request.getVolumeId());
        driverMetadata1.setDriverType(DriverType.FSD);
        driverMetadata1.setSnapshotId(0);
        driverMetadata1.setDynamicIOLimitationId(100);
        driverMetadata1.setStaticIOLimitationId(101);
        driverStore.save(driverMetadata1);

        // check ISCSI can not compatible with FSD or NBD
        request.setDriverType(DriverType_Thrift.ISCSI);
        informationCenter.allocDriverContainer(request);

        driverStore.delete(request.getVolumeId());

        // check first have ISCSI
        response = informationCenter.allocDriverContainer(request);
        Assert.assertEquals(1, response.getCandidatesSize());

        driverMetadata1.setHostName("0.0.0.1");
        driverMetadata1.setVolumeId(request.getVolumeId());
        driverMetadata1.setDriverType(DriverType.ISCSI);
        driverMetadata1.setSnapshotId(0);
        driverMetadata1.setDriverContainerId(driverContainerId);
        driverStore.save(driverMetadata1);

        // check new FSD or NBD cannot get the container already has a ISCSI
        request.setDriverType(DriverType_Thrift.FSD);
        informationCenter.allocDriverContainer(request);

        driverStore.delete(request.getVolumeId());
    }

    /**
     * Test case steps:
     * 1. Prepare 1 volume and 1 driver container;
     * 2. Launch first driver with 0 snapshot id, success;
     * 3. Launch second driver with 1 snapshot id, success;
     * 4. Launch third driver with 0 snapshot id again, catch too many driver exception.
     *
     * @throws Exception
     */

    @Test
    public void testAllocDriverContainerWithSnapshotId() throws Exception {
        VolumeMetadata volume = generateVolumeMetadata(1, null, 6, 3, 1);
        volumeStore.saveVolume(volume);

        long driverContainerId = RequestIdBuilder.get();
        Instance instance = new Instance(new InstanceId(driverContainerId), null, PyService.DRIVERCONTAINER.name(),
                InstanceStatus.OK);
        instance.putEndPointByServiceName(PortType.CONTROL, new EndPoint("0.0.0.1", 1));
        instance.putEndPointByServiceName(PortType.IO, new EndPoint("0.0.0.1", 2));

        Set<Instance> instances = new HashSet<>();
        instances.add(instance);

        when(instanceStore.getAll(any(String.class), any(InstanceStatus.class))).thenReturn(instances);

        // get driver container from information center
        AllocDriverContainerRequest request = new AllocDriverContainerRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(1L);
        request.setDriverAmount(1);
        request.setDriverType(DriverType_Thrift.NBD);
        request.setSnapshotId(0);
        AllocDriverContainerResponse response = informationCenter.allocDriverContainer(request);

        // check first driver container allocation
        Assert.assertEquals(1, response.getCandidatesSize());

        driverStore.delete(request.getVolumeId());
        DriverMetadata driverMetadata = new DriverMetadata();
        driverMetadata.setHostName("0.0.0.1");
        driverMetadata.setVolumeId(request.getVolumeId());
        driverMetadata.setDriverType(DriverType.NBD);
        driverMetadata.setSnapshotId(0);
        driverMetadata.setDriverStatus(DriverStatus.LAUNCHED);
        driverMetadata.setDriverContainerId(driverContainerId);
        driverMetadata.setDynamicIOLimitationId(100);
        driverMetadata.setStaticIOLimitationId(101);
        driverStore.save(driverMetadata);

        // check launch another NBD driver with different snapshot id success
        request.setSnapshotId(1);

        response = informationCenter.allocDriverContainer(request);

        // check second driver container allocation
        Assert.assertEquals(1, response.getCandidatesSize());

        // Last check launch third NBD driver with same snapshot id with first driver, excepted TooManyDriversException_Thrift
        request.setSnapshotId(0);
        informationCenter.allocDriverContainer(request);
    }

    private ReportSegmentUnitsMetadataRequest generateSegUnitsMetadataRequest(VolumeMetadata volumeMetadata,
            SegmentUnitStatus status) throws JsonProcessingException {
        ReportSegmentUnitsMetadataRequest request = new ReportSegmentUnitsMetadataRequest();
        // report units from instance 1 which is a secondary
        request.setInstanceId(1);
        List<SegmentUnitMetadata_Thrift> segUnitsMetadata = new ArrayList<>();
        SegmentUnitMetadata segUnitMetadata = TestUtils
                .generateSegmentUnitMetadata(new SegId(volumeMetadata.getVolumeId(), 0), status);

        segUnitMetadata.setVolumeMetadataJson(volumeMetadata.getVersion() + ":" + volumeMetadata.toJsonString());
        SegmentUnitMetadata_Thrift segUnitMetadataThrift = RequestResponseHelper
                .buildThriftSegUnitMetadataFrom(segUnitMetadata, true);
        segUnitsMetadata.add(segUnitMetadataThrift);
        request.setSegUnitsMetadata(segUnitsMetadata);
        return request;
    }

    private ReportSegmentUnitsMetadataRequest generateSegUnitsMetadataRequest(VolumeMetadata volumeMetadata,
            SegmentUnitStatus status, long instanceId, int segmentIndex) throws JsonProcessingException {
        ReportSegmentUnitsMetadataRequest request = new ReportSegmentUnitsMetadataRequest();
        request.setInstanceId(instanceId);

        List<SegmentUnitMetadata_Thrift> segUnitsMetadata = new ArrayList<>();
        SegmentUnitMetadata segUnitMetadata = TestUtils
                .generateSegmentUnitMetadata(new SegId(volumeMetadata.getVolumeId(), segmentIndex), status);
        segUnitMetadata.setVolumeMetadataJson(volumeMetadata.getVersion() + ":" + volumeMetadata.toJsonString());
        SegmentUnitMetadata_Thrift segUnitMetadataThrift = RequestResponseHelper
                .buildThriftSegUnitMetadataFrom(segUnitMetadata, true);
        segUnitsMetadata.add(segUnitMetadataThrift);
        request.setSegUnitsMetadata(segUnitsMetadata);
        return request;
    }

    @Test(expected = VolumeNotFoundException_Thrift.class)
    public void testDeleteVolumeNotFoundVolume() throws Exception {
        DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
        deleteVolumeRequest.setAccountId(111);
        deleteVolumeRequest.setVolumeId(111);

        informationCenter.deleteVolume(deleteVolumeRequest);
    }

    @Test(expected = VolumeIsCloningException_Thrift.class)
    public void testDeleteVolumeInCloning() throws Exception {
        long srcVolumeId = 0l;
        long destVolumeId = 1l;
        VolumeMetadata volume = generateVolumeMetadata(srcVolumeId, null, 6, 3, 1);
        volumeStore.saveVolume(volume);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(destVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);

        DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
        deleteVolumeRequest.setRequestId(10L);
        deleteVolumeRequest.setVolumeId(srcVolumeId);
        informationCenter.deleteVolume(deleteVolumeRequest);
    }

    @Test(expected = VolumeInExtendingException_Thrift.class)
    public void testDeleteVolumeInExtending() throws Exception {
        VolumeMetadata volume = generateVolumeMetadata(1, null, 6, 3, 1);
        volumeStore.saveVolume(volume);
        volume.setExtendingSize(100);

        DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
        deleteVolumeRequest.setRequestId(111);
        deleteVolumeRequest.setVolumeId(1);
        informationCenter.deleteVolume(deleteVolumeRequest);
    }

    @Test
    public void testDeleteVolumeNormal() throws Exception {
        // report the first volume
        long volumeId = 1;
        VolumeMetadata volume = generateVolumeMetadata(volumeId, null, 6, 3, 1);
        volumeStore.saveVolume(volume);
        assertEquals(VolumeStatus.ToBeCreated, volume.getVolumeStatus());

        DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
        deleteVolumeRequest.setRequestId(111);
        deleteVolumeRequest.setVolumeId(volumeId);

        DriverStore driverStoreMock = mock(DriverStore.class);
        List<DriverMetadata> driverMetadataList = new ArrayList<>();
        when(driverStoreMock.get(anyLong())).thenReturn(driverMetadataList);
        informationCenter.setDriverStore(driverStoreMock);

        DeleteVolumeResponse response = informationCenter.deleteVolume(deleteVolumeRequest);
        assertEquals(response.getRequestId(), 111);
        assertEquals(VolumeStatus.Deleting, volume.getVolumeStatus());
    }

    @Test(expected = VolumeBeingDeletedException_Thrift.class)
    public void testDeleteVolumeInDeletingStatus() throws Exception {
        VolumeMetadata volume = generateVolumeMetadata(1, null, 6, 3, 1);
        volume.setVolumeStatus(VolumeStatus.Deleting);
        volumeStore.saveVolume(volume);

        DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
        deleteVolumeRequest.setRequestId(111);
        deleteVolumeRequest.setVolumeId(1);

        informationCenter.deleteVolume(deleteVolumeRequest);
        fail("This test case not throw exception");
    }

    @Test(expected = VolumeBeingDeletedException_Thrift.class)
    public void testDeleteVolumeInDeletedStatus() throws Exception {
        VolumeMetadata volume = generateVolumeMetadata(1, null, 6, 3, 1);
        volume.setVolumeStatus(VolumeStatus.Deleting);
        volumeStore.saveVolume(volume);

        DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
        deleteVolumeRequest.setRequestId(111);
        deleteVolumeRequest.setVolumeId(1);

        informationCenter.deleteVolume(deleteVolumeRequest);
        fail("This test case not throw exception");
    }

    @Test(expected = ServiceHavingBeenShutdown_Thrift.class)
    public void testDeleteVolumeInfoCenterShutdown() throws Exception {
        VolumeMetadata volume = generateVolumeMetadata(1, null, 6, 3, 1);
        volumeStore.saveVolume(volume);

        informationCenter.shutdownForTest();
        ;
        DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
        deleteVolumeRequest.setRequestId(111);
        deleteVolumeRequest.setVolumeId(1);

        informationCenter.deleteVolume(deleteVolumeRequest);
        fail("This test case not throw exception");
    }

    private VolumeMetadata generateVolumeMetadata(long volumeId, Long childVolumeId, long volumeSize, long segmentSize,
            int version) {
        VolumeMetadata volumeMetadata = new VolumeMetadata(volumeId, volumeId, volumeSize, segmentSize,
                VolumeType.REGULAR, CacheType.MEMORY, 0L, 0L);
        volumeMetadata.setChildVolumeId(childVolumeId);
        volumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);
        volumeMetadata.setName("testvolume");
        volumeMetadata.setVolumeSource(CREATE_VOLUME);
        while (version-- > 0) {
            volumeMetadata.incVersion();
        }

        return volumeMetadata;
    }

    private VolumeMetadata generateVolumeMetadata2(long rootVolumeId, Long volumeId, Long childVolumeId,
            String volumeName, long volumeSize, long segmentSize, int version, List<SegId> oriSegIdList) {
        VolumeMetadata volumeMetadata = new VolumeMetadata(rootVolumeId, volumeId, volumeSize, segmentSize,
                VolumeType.REGULAR, CacheType.MEMORY, 0L, 0L);
        volumeMetadata.setChildVolumeId(childVolumeId);
        volumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);
        volumeMetadata.setName(volumeName);
        volumeMetadata.setVolumeSource(CREATE_VOLUME);
        for (int i = 0; i < volumeSize / segmentSize; i++) {
            SegId segId = new SegId(volumeId, i);
            oriSegIdList.add(segId);
            SegmentMetadata segmentMetadata = TestUtils.generateSegmentMetadata(segId);
            volumeMetadata.addSegmentMetadata(segmentMetadata, TestUtils.generateMembership(VolumeType.REGULAR));
        }
        while (version-- > 0) {
            volumeMetadata.incVersion();
        }

        return volumeMetadata;
    }

    /**
     * nyj
     * we already have a srcVolume with only one segment.
     * then create the cloneVolume,its segmentStatus is Available.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeOnlyRootVolume1() throws Exception {
        long srcVolumeId = RequestIdBuilder.get();
        long rootVolumeId = RequestIdBuilder.get();
        long volumeSize = 6L;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Available);
        rootVolume.setSimpleConfiguration(true);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        rootVolume.setInAction(NULL);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata srcVolume = new VolumeMetadata();
        srcVolume.setVolumeId(srcVolumeId);
        srcVolume.setInAction(NULL);

        VolumeStatusTransitionStoreImpl volumeStatusTransitionStore = new VolumeStatusTransitionStoreImpl();
        volumeStatusTransitionStore.addVolumeToStore(rootVolume);
        volumeStore.saveVolume(rootVolume);
        volumeStore.saveVolume(srcVolume);
        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertTrue(cloneRelationshipsStore.loadFromDB().isEmpty());
    }

    /**
     * nyj
     * we already have a srcVolume with only one segment.
     * then create the cloneVolume,its segmentStatus is Unavailable.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeOnlyRootVolume2() throws Exception {
        long srcVolumeId = RequestIdBuilder.get();
        long rootVolumeId = RequestIdBuilder.get();
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Unavailable);
        rootVolume.setSimpleConfiguration(true);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        rootVolume.setInAction(NULL);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata srcVolume = new VolumeMetadata();
        srcVolume.setVolumeId(srcVolumeId);
        srcVolume.setInAction(NULL);

        VolumeStatusTransitionStoreImpl volumeStatusTransitionStore = new VolumeStatusTransitionStoreImpl();
        volumeStatusTransitionStore.addVolumeToStore(rootVolume);
        volumeStore.saveVolume(rootVolume);
        volumeStore.saveVolume(srcVolume);
        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertTrue(cloneRelationshipsStore.loadFromDB().isEmpty());
    }

    /**
     * nyj
     * we already have a srcVolume with only one segment.
     * then create the cloneVolume,its segmentStatus is Creating.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeOnlyRootVolume3() throws Exception {
        long srcVolumeId = RequestIdBuilder.get();
        long rootVolumeId = RequestIdBuilder.get();
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Creating);
        rootVolume.setSimpleConfiguration(false);
        //        rootVolume.setNotCreateAllSegmentAtBeginning(true);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        statusStore.addVolumeToStore(rootVolume);

        VolumeStatusTransitionStoreImpl volumeStatusTransitionStore = new VolumeStatusTransitionStoreImpl();
        volumeStatusTransitionStore.addVolumeToStore(rootVolume);
        volumeStore.saveVolume(rootVolume);
        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());
    }

    /**
     * nyj
     * we already have a srcVolume with only one segment.
     * then create the cloneVolume,its segmentStatus is ToBeCreated.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeOnlyRootVolume4() throws Exception {
        long srcVolumeId = RequestIdBuilder.get();
        long rootVolumeId = RequestIdBuilder.get();
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.ToBeCreated);
        rootVolume.setSimpleConfiguration(true);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        statusStore.addVolumeToStore(rootVolume);

        VolumeStatusTransitionStoreImpl volumeStatusTransitionStore = new VolumeStatusTransitionStoreImpl();
        volumeStatusTransitionStore.addVolumeToStore(rootVolume);
        volumeStore.saveVolume(rootVolume);
        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,its segmentsStatus are all Available.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeIsAllAvailable() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Available);
        rootVolume.setSimpleConfiguration(true);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        rootVolume.setInAction(NULL);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.Available);
        childVolume.setSimpleConfiguration(true);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        rootVolume.setInAction(NULL);
        volumeStore.saveVolume(childVolume);

        VolumeMetadata srcVolume = new VolumeMetadata();
        srcVolume.setVolumeId(srcVolumeId);
        srcVolume.setInAction(NULL);

        volumeStore.saveVolume(srcVolume);
        statusStore.addVolumeToStore(childVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertTrue(cloneRelationshipsStore.loadFromDB().isEmpty());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,its segmentsStatus are all Unavailable.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeIsAllUnavailable() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Unavailable);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        rootVolume.setInAction(NULL);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.Unavailable);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        rootVolume.setInAction(NULL);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        VolumeMetadata srcVolume = new VolumeMetadata();
        srcVolume.setVolumeId(srcVolumeId);
        srcVolume.setInAction(NULL);

        volumeStore.saveVolume(srcVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertTrue(cloneRelationshipsStore.loadFromDB().isEmpty());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,its segmentsStatus are all Creating.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeIsAllCreating() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Creating);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.Creating);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,its segmentsStatus are all ToBeCreated.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeIsAllToBeCreated() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.ToBeCreated);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.ToBeCreated);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,rootVolumeStatus is Available,childVolumeStatus is Unavailable.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeRootAvailableChildUnavailable() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Available);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        rootVolume.setInAction(NULL);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.Unavailable);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        rootVolume.setInAction(NULL);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        VolumeMetadata srcVolume = new VolumeMetadata();
        srcVolume.setVolumeId(srcVolumeId);
        srcVolume.setInAction(NULL);

        volumeStore.saveVolume(srcVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertTrue(cloneRelationshipsStore.loadFromDB().isEmpty());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,rootVolumeStatus is Available,childVolumeStatus is Creating.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeRootAvailableChildCreating() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Available);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.Creating);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,rootVolumeStatus is Available,childVolumeStatus is ToBeCreated.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeRootAvailableChildToBeCreated() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Available);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.ToBeCreated);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,rootVolumeStatus is Unavailable,childVolumeStatus is Available.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeRootUnavailableChildAvailable() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Unavailable);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        rootVolume.setInAction(NULL);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.Available);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        childVolume.setInAction(NULL);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        VolumeMetadata srcVolume = new VolumeMetadata();
        srcVolume.setVolumeId(srcVolumeId);
        srcVolume.setInAction(NULL);

        volumeStore.saveVolume(srcVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertTrue(cloneRelationshipsStore.loadFromDB().isEmpty());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,rootVolumeStatus is Unavailable,childVolumeStatus is Creating.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeRootUnavailableChildCreating() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Unavailable);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.Creating);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,rootVolumeStatus is Unavailable,childVolumeStatus is ToBeCreated.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeRootUnavailableChildToBeCreated() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Unavailable);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.ToBeCreated);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,rootVolumeStatus is Creating,childVolumeStatus is Available.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeRootCreatingChildAvailable() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Creating);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.Available);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,rootVolumeStatus is Creating,childVolumeStatus is Unavailable.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeRootCreatingChildUnavailable() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Creating);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.Unavailable);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,rootVolumeStatus is Creating,childVolumeStatus is ToBeCreated.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeRootCreatingChildToBeCreated() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.Creating);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.ToBeCreated);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,rootVolumeStatus is ToBeCreated,childVolumeStatus is Available.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeRootToBeCreatedChildAvailable() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.ToBeCreated);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.Available);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,rootVolumeStatus is ToBeCreated,childVolumeStatus is Unavailable.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeRootToBeCreatedChildUnavailable() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.ToBeCreated);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.Unavailable);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());
    }

    /**
     * nyj
     * we already have a extended srcVolume with two segments.
     * then create the cloneVolume,rootVolumeStatus is ToBeCreated,childVolumeStatus is Creating.
     * delete the cloneRelationshipInformation from CloneRelationshipsStore.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeRootToBeCreatedChildCreating() throws Exception {
        long srcVolumeId = 0l;
        long rootVolumeId = 2l;
        long childVolumeId = 1l;
        long volumeSize = 6l;
        appContext.setStatus(InstanceStatus.OK);
        volumeSweeper.setAppContext(appContext);
        informationCenter.setVolumeStatusStore(statusStore);
        volumeSweeper.setVolumeStatusTransitionStore(statusStore);

        VolumeMetadata rootVolume = new VolumeMetadata();
        rootVolume.setVolumeId(rootVolumeId);
        rootVolume.setRootVolumeId(rootVolumeId);
        rootVolume.setVolumeSize(volumeSize);
        rootVolume.setVolumeStatus(VolumeStatus.ToBeCreated);
        rootVolume.setSimpleConfiguration(false);
        rootVolume.setChildVolumeId(childVolumeId);
        rootVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        volumeStore.saveVolume(rootVolume);
        statusStore.addVolumeToStore(rootVolume);

        VolumeMetadata childVolume = new VolumeMetadata();
        childVolume.setVolumeId(childVolumeId);
        childVolume.setRootVolumeId(rootVolumeId);
        childVolume.setVolumeSize(volumeSize);
        childVolume.setVolumeStatus(VolumeStatus.Creating);
        childVolume.setSimpleConfiguration(false);
        childVolume.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);

        volumeStore.saveVolume(childVolume);
        statusStore.addVolumeToStore(childVolume);

        informationCenter.setVolumeStore(volumeStore);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(rootVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsStore);

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        volumeSweeper.doWork();

        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(rootVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());
    }

    @Test(expected = ServiceHavingBeenShutdown_Thrift.class)
    public void testReserveVolume_Shutdown() throws Exception {
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        informationCenter.shutdownForTest();
        ;
        informationCenter.reserveVolume(request);
        fail("This test case not throw exception");
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void testReserveVolume_getValidAccount() throws Exception {
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setAccountId(1234567);

        informationCenter.reserveVolume(request);
        fail("This test case not throw exception");
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void testReserveVolume_expectedSize_not_big_segmentSize() throws Exception {
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setAccountId(123456);

        informationCenter.reserveVolume(request);
        fail("This test case not throw exception");
    }

    @Test(expected = VolumeNotFoundException_Thrift.class)
    public void testReserveVolume_VolumeNotFoundException_Thrift() throws Exception {
        ReserveVolumeRequest request = new ReserveVolumeRequest();
        request.setAccountId(123456);

        request.setVolumeSize(101);

        request.setVolumeType(VolumeType_Thrift.REGULAR);

        List<InstanceMetadata> ls = new ArrayList<>();

        InstanceId instanceId = new InstanceId(1);
        InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
        instanceMetadata.setFreeSpace(400);
        instanceMetadata.setCapacity(500);
        instanceMetadata.setDatanodeStatus(OK);

        ls.add(instanceMetadata);

        when(storageStore.list()).thenReturn(ls);
        informationCenter.setStorageStore(storageStore);

        informationCenter.setUserMaxCapacityByte(501);

        request.setVolumeId(1);
        VolumeMetadata volumeMetadata = new VolumeMetadata();

        when(volumeStore1.getVolume(request.getVolumeId())).thenReturn(null);
        informationCenter.setVolumeStore(volumeStore1);

        informationCenter.reserveVolume(request);
        fail("This test case not throw exception");
    }

    @Test(expected = ServiceHavingBeenShutdown_Thrift.class)
    public void testCreateVolume_Shutdown() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        informationCenter.shutdownForTest();
        ;
        informationCenter.CreateVolume(request);
        fail("This test case not throw exception");
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void testCreateVolume_getname_error_1() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName(null);
        informationCenter.CreateVolume(request);
        fail("This test case not throw exception");
    }

    @Test(expected = InvalidInputException_Thrift.class)
    public void testCreateVolume_getname_error_2() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName("asdfalksdjfaskjdfiwjefjsdfijawoeifjsdfjkoasidjgojejfasdijfowiejfoj");
        informationCenter.CreateVolume(request);
        fail("This test case not throw exception");
    }

    @Test(expected = VolumeExistingException_Thrift.class)
    public void testCreateVolume_volumeMetadata_is_not_null() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName("wjhsuper");
        request.setAccountId(123456);

        request.setVolumeSize(101);
        request.setVolumeType(VolumeType_Thrift.LARGE);
        request.setCacheType(CacheType_Thrift.MEMORY);
        List<InstanceMetadata> ls = new ArrayList<InstanceMetadata>();

        InstanceId instanceId = new InstanceId(1);
        InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
        instanceMetadata.setFreeSpace(800);
        instanceMetadata.setCapacity(200);
        instanceMetadata.setDatanodeStatus(OK);

        ls.add(instanceMetadata);

        when(storageStore.list()).thenReturn(ls);
        informationCenter.setStorageStore(storageStore);

        informationCenter.setActualFreeSpace(800);

        request.setVolumeId(1);
        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Set<Long> storagePoolIds = new HashSet<Long>();
        storagePoolIds.add(storagePoolId);
        request.setDomainId(domainId);
        request.setStoragePoolId(storagePoolId);
        VolumeMetadata volumeMetadata = new VolumeMetadata();

        when(volumeStore1.getVolume(request.getVolumeId())).thenReturn(volumeMetadata);
        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);
        when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);
        // TODO:
        when(domain.isDeleting()).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(domain.getStoragePools()).thenReturn(storagePoolIds);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);

        informationCenter.setVolumeStore(volumeStore1);

        informationCenter.CreateVolume(request);
        fail("This test case not throw exception");
    }

    @Test(expected = RootVolumeNotFoundException_Thrift.class)
    public void testCreateVolume_root_is_null() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName("wjhsuper");
        request.setAccountId(123456);
        request.setRequestType("CREATE_VOLUME");

        request.setVolumeSize(101);
        request.setVolumeType(VolumeType_Thrift.LARGE);
        request.setCacheType(CacheType_Thrift.MEMORY);
        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Set<Long> storagePoolIds = new HashSet<>();
        storagePoolIds.add(storagePoolId);
        request.setDomainId(domainId);
        request.setStoragePoolId(storagePoolId);
        List<InstanceMetadata> ls = new ArrayList<>();

        InstanceId instanceId = new InstanceId(1);
        InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
        instanceMetadata.setFreeSpace(800);
        instanceMetadata.setCapacity(200);
        instanceMetadata.setDatanodeStatus(OK);

        ls.add(instanceMetadata);

        when(storageStore.list()).thenReturn(ls);
        informationCenter.setStorageStore(storageStore);

        informationCenter.setUserMaxCapacityByte(201);

        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);
        when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);
        when(domain.isDeleting()).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(domain.getStoragePools()).thenReturn(storagePoolIds);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);

        request.setVolumeId(1);

        when(volumeStore1.getVolume(request.getVolumeId())).thenReturn(null);
        informationCenter.setVolumeStore(volumeStore1);

        informationCenter.CreateVolume(request);
        fail("This test case not throw exception");
    }

    @Test(expected = NotEnoughSpaceException_Thrift.class)
    public void testCreateVolume_NotEnoughSpaceException_Thrift_1() throws Exception {
        CreateVolumeRequest request = new CreateVolumeRequest();
        request.setName("wjhsuper");
        request.setAccountId(123456);

        request.setVolumeSize(101);
        request.setVolumeType(VolumeType_Thrift.LARGE);
        request.setCacheType(CacheType_Thrift.MEMORY);
        Long domainId = RequestIdBuilder.get();
        Long storagePoolId = RequestIdBuilder.get();
        Set<Long> storagePoolIds = new HashSet<>();
        storagePoolIds.add(storagePoolId);
        request.setDomainId(domainId);
        request.setStoragePoolId(storagePoolId);
        List<InstanceMetadata> ls = new ArrayList<>();

        InstanceId instanceId = new InstanceId(1);
        InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
        instanceMetadata.setFreeSpace(150);
        instanceMetadata.setCapacity(200);
        instanceMetadata.setDatanodeStatus(OK);

        ls.add(instanceMetadata);

        when(storageStore.list()).thenReturn(ls);
        informationCenter.setStorageStore(storageStore);

        informationCenter.setUserMaxCapacityByte(201);

        request.setVolumeId(0);

        Domain domain = mock(Domain.class);
        StoragePool storagePool = mock(StoragePool.class);
        when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);
        when(domain.isDeleting()).thenReturn(false);
        when(storagePool.isDeleting()).thenReturn(false);
        when(domain.getStoragePools()).thenReturn(storagePoolIds);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);

        when(volumeStore1.getVolume(request.getVolumeId())).thenReturn(null);
        informationCenter.setVolumeStore(volumeStore1);

        request.setRootVolumeId(1);
        VolumeMetadata volumeMetadata1 = new VolumeMetadata();

        when(volumeStore2.getVolume(request.getRootVolumeId())).thenReturn(volumeMetadata1);
        informationCenter.setVolumeStore(volumeStore2);
        // volumeStore2.updateStatus(request.getRootVolumeId(), "");

        informationCenter.CreateVolume(request);
        fail("This test case not throw exception");
    }

    @Test(expected = VolumeIsCloningException_Thrift.class)
    public void testExtendVolume_In_Cloning() throws Exception {
        long srcVolumeId = RequestIdBuilder.get();
        long destVolumeId = RequestIdBuilder.get();
        long extendVolumeId = RequestIdBuilder.get();
        VolumeMetadata volume = generateVolumeMetadata(srcVolumeId, null, 6, 3, 1);
        volumeStore.saveVolume(volume);

        CloneRelationshipInformation cloneRelationshipInformation = new CloneRelationshipInformation();
        cloneRelationshipInformation.setScrVolumeId(srcVolumeId);
        cloneRelationshipInformation.setDestVolumeId(destVolumeId);
        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);

        CreateVolumeRequest extendVolumeRequest = new CreateVolumeRequest();
        extendVolumeRequest.setRootVolumeId(srcVolumeId);
        extendVolumeRequest.setName("test_clone");
        extendVolumeRequest.setAccountId(111L);
        extendVolumeRequest.setLeastSegmentUnitCount(2);
        extendVolumeRequest.setNotCreateAllSegmentAtBegining(true);
        extendVolumeRequest.setVolumeSize(segmentSize);
        extendVolumeRequest.setVolumeType(VolumeType_Thrift.SMALL);
        extendVolumeRequest.setCacheType(CacheType_Thrift.MEMORY);
        extendVolumeRequest.setVolumeId(extendVolumeId);
        extendVolumeRequest.setRequestType("EXTEND_VOLUME");

        Long domainId = RequestIdBuilder.get();
        extendVolumeRequest.setDomainId(domainId);
        Long storagePoolId = RequestIdBuilder.get();
        Set<Long> storagePoolIds = new HashSet<>();
        storagePoolIds.add(storagePoolId);
        extendVolumeRequest.setStoragePoolId(storagePoolId);
        Domain domain = mock(Domain.class);
        when(domain.isDeleting()).thenReturn(false);
        StoragePool storagePool = mock(StoragePool.class);
        when(storagePool.isDeleting()).thenReturn(false);
        when(domainStore.getDomain(extendVolumeRequest.getDomainId())).thenReturn(domain);
        when(storagePoolStore.getStoragePool(extendVolumeRequest.getStoragePoolId())).thenReturn(storagePool);
        when(domain.getStoragePools()).thenReturn(storagePoolIds);
        when(storagePool.getPoolId()).thenReturn(storagePoolId);
        AccountMetadata accountMetadata = new AccountMetadata();
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(1L));
        instanceMetadata.setFreeSpace(1000L);
        instanceMetadata.setDatanodeStatus(OK);
        instanceMetadataList.add(instanceMetadata);
        when(storageStore.list()).thenReturn(instanceMetadataList);

        informationCenter.CreateVolume(extendVolumeRequest);
    }

    @Test
    public void testInformCloneRelationship() throws Exception {
        long srcVolumeId = 0L;
        long destVolumeId1 = 1L;
        long destVolumeId2 = 2L;
        InformCloneRelationshipRequest informCloneRelationshipRequest = new InformCloneRelationshipRequest();
        VolumeCloneRelationship_Thrift volumeCloneRelationship_thrift1 = new VolumeCloneRelationship_Thrift();
        VolumeCloneRelationship_Thrift volumeCloneRelationship_thrift2 = new VolumeCloneRelationship_Thrift();
        volumeCloneRelationship_thrift1.setSrcVolumeId(srcVolumeId);
        volumeCloneRelationship_thrift1.setDestVolumeId(destVolumeId1);
        volumeCloneRelationship_thrift2.setSrcVolumeId(srcVolumeId);
        volumeCloneRelationship_thrift2.setDestVolumeId(destVolumeId2);

        informCloneRelationshipRequest.setCloneRelationship(volumeCloneRelationship_thrift1);
        informationCenter.informCloneRelationship(informCloneRelationshipRequest);
        assertEquals(1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getScrVolumeId());
        assertEquals(destVolumeId1, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(0).getDestVolumeId());

        informCloneRelationshipRequest.setCloneRelationship(volumeCloneRelationship_thrift2);
        informationCenter.informCloneRelationship(informCloneRelationshipRequest);
        assertEquals(2, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).size());
        assertEquals(srcVolumeId, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(1).getScrVolumeId());
        assertEquals(destVolumeId2, cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).get(1).getDestVolumeId());
    }

    @Test
    public void testDateConvert() throws ParseException {
        Date nowDate = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        String dateString = dateFormat.format(nowDate);
        Date newDate = dateFormat.parse(dateString);
        assertTrue(newDate != null);
        assertEquals(nowDate.toString(), newDate.toString());
    }

    /**
     * Two volumes: one status is dead and another status is not dead.
     * Can get volume which status is not dead.
     * Cannot get volume which status is dead.
     */
    @Test
    public void testGetVolumeFilterDead() {
        VolumeMetadata volumeMetadata = generateVolumeMetadata(1, null, 6, 3, 1);
        volumeMetadata.setVolumeStatus(VolumeStatus.Dead);
        volumeMetadata.setAccountId(123456);
        volumeMetadata.setInAction(NULL);
        VolumeMetadata volumeMetadata1 = generateVolumeMetadata(2, null, 6, 3, 1);
        volumeMetadata1.setAccountId(123456);
        volumeMetadata1.setInAction(NULL);
        volumeStore.saveVolume(volumeMetadata);
        volumeStore.saveVolume(volumeMetadata1);
        informationCenter.setVolumeStore(volumeStore);

        GetVolumeRequest request = new GetVolumeRequest(RequestIdBuilder.get(), 1, 123456, null, true);
        request.setContainDeadVolume(false);
        // cannot get volume which status is dead
        try {
            GetVolumeResponse response = informationCenter.getVolume(request);
            assertFalse(response.isSetVolumeMetadata());
        } catch (Exception e) {
            logger.error("Cannot get volume!", e);
            fail();
        }

        // no problem to get volume which status is not dead
        try {
            request.setVolumeId(2);
            GetVolumeResponse response = informationCenter.getVolume(request);
            assertTrue(response.isSetVolumeMetadata());
        } catch (Exception e) {
            logger.error("Cannot get volume!", e);
            fail();
        }

    }

    @Test
    public void testGetTwiceExtendVolume() {
        int segmentSize = 1;
        int rootVolumeSize = 10;
        int childVolumeSize1 = 5;
        int childVolumeSize2 = 12;

        Long rootVolumeId = RequestIdBuilder.get();
        Long childVolumeId1 = RequestIdBuilder.get();
        Long childVolumeId2 = RequestIdBuilder.get();

        List<SegId> oriSegIdList = new ArrayList<>();

        VolumeMetadata rootVolumeMetadata = generateVolumeMetadata2(rootVolumeId, rootVolumeId, childVolumeId1,
                "rootVolume", rootVolumeSize, segmentSize, 1, oriSegIdList);
        rootVolumeMetadata.setAccountId(Constants.SUPERADMIN_ACCOUNT_ID);
        rootVolumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);

        VolumeMetadata volumeMetadata1 = generateVolumeMetadata2(rootVolumeId, childVolumeId1, childVolumeId2,
                "childVolumeId1", childVolumeSize1, segmentSize, 1, oriSegIdList);
        volumeMetadata1.setAccountId(Constants.SUPERADMIN_ACCOUNT_ID);
        volumeMetadata1.setPositionOfFirstSegmentInLogicVolume(rootVolumeSize);

        VolumeMetadata volumeMetadata2 = generateVolumeMetadata2(rootVolumeId, childVolumeId2, null, "childVolumeId2",
                childVolumeSize2, segmentSize, 1, oriSegIdList);
        volumeMetadata2.setAccountId(Constants.SUPERADMIN_ACCOUNT_ID);
        volumeMetadata2.setPositionOfFirstSegmentInLogicVolume(rootVolumeSize + childVolumeSize1);

        volumeStore.saveVolume(rootVolumeMetadata);
        volumeStore.saveVolume(volumeMetadata1);
        volumeStore.saveVolume(volumeMetadata2);
        informationCenter.setVolumeStore(volumeStore);

        GetVolumeRequest request = new GetVolumeRequest(RequestIdBuilder.get(), rootVolumeId,
                Constants.SUPERADMIN_ACCOUNT_ID, null, false);
        request.setContainDeadVolume(false);

        List<SegId> gotSegIdList = new ArrayList<>();

        try {
            GetVolumeResponse response = informationCenter.getVolume(request);
            List<SegmentMetadata_Thrift> segmentMetadataThriftList = response.getVolumeMetadata().getSegmentsMetadata();
            for (SegmentMetadata_Thrift segmentMetadataThrift : segmentMetadataThriftList) {
                SegId segId = new SegId(segmentMetadataThrift.getVolumeId(), segmentMetadataThrift.getSegId());
                gotSegIdList.add(segId);
            }
        } catch (Exception e) {
            fail();
        }

        assertEquals(oriSegIdList.size(), gotSegIdList.size());

        int totalSegmentCount = rootVolumeSize + childVolumeSize1 + childVolumeSize2;

        for (int i = 0; i < totalSegmentCount; i++) {
            assertEquals(oriSegIdList.get(i), gotSegIdList.get(i));
        }

    }

    @Test
    public void testEnablePaginationGetVolume() {
        int segmentSize = 1;
        int rootVolumeSize = RandomUtils.nextInt(20) + 1;
        int childVolumeSize1 = RandomUtils.nextInt(30) + 1;
        int childVolumeSize2 = RandomUtils.nextInt(10) + 1;

        int paginationNumber = RandomUtils.nextInt(5) + 1;

        Long rootVolumeId = RequestIdBuilder.get();
        Long childVolumeId1 = RequestIdBuilder.get();
        Long childVolumeId2 = RequestIdBuilder.get();

        List<SegId> oriSegIdList = new ArrayList<>();

        VolumeMetadata rootVolumeMetadata = generateVolumeMetadata2(rootVolumeId, rootVolumeId, childVolumeId1,
                "rootVolume", rootVolumeSize, segmentSize, 1, oriSegIdList);
        rootVolumeMetadata.setAccountId(Constants.SUPERADMIN_ACCOUNT_ID);
        rootVolumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);

        VolumeMetadata volumeMetadata1 = generateVolumeMetadata2(rootVolumeId, childVolumeId1, childVolumeId2,
                "childVolumeId1", childVolumeSize1, segmentSize, 1, oriSegIdList);
        volumeMetadata1.setAccountId(Constants.SUPERADMIN_ACCOUNT_ID);
        volumeMetadata1.setPositionOfFirstSegmentInLogicVolume(rootVolumeSize);

        VolumeMetadata volumeMetadata2 = generateVolumeMetadata2(rootVolumeId, childVolumeId2, null, "childVolumeId2",
                childVolumeSize2, segmentSize, 1, oriSegIdList);
        volumeMetadata2.setAccountId(Constants.SUPERADMIN_ACCOUNT_ID);
        volumeMetadata2.setPositionOfFirstSegmentInLogicVolume(rootVolumeSize + childVolumeSize1);

        volumeStore.saveVolume(rootVolumeMetadata);
        volumeStore.saveVolume(volumeMetadata1);
        volumeStore.saveVolume(volumeMetadata2);
        informationCenter.setVolumeStore(volumeStore);

        GetVolumeRequest request = new GetVolumeRequest(RequestIdBuilder.get(), rootVolumeId,
                Constants.SUPERADMIN_ACCOUNT_ID, null, false);
        request.setContainDeadVolume(false);

        request.setEnablePagination(true);
        request.setStartSegmentIndex(0);
        request.setPaginationNumber(paginationNumber);

        List<SegId> paginationSegIdList = new ArrayList<>();

        while (true) {
            try {
                GetVolumeResponse response = informationCenter.getVolume(request);

                List<SegmentMetadata_Thrift> segmentMetadataThriftList = response.getVolumeMetadata()
                        .getSegmentsMetadata();
                for (SegmentMetadata_Thrift segmentMetadataThrift : segmentMetadataThriftList) {
                    SegId segId = new SegId(segmentMetadataThrift.getVolumeId(), segmentMetadataThrift.getSegId());
                    paginationSegIdList.add(segId);
                }
                if (!response.isLeftSegment()) {
                    break;
                } else {
                    assertTrue(response.isSetNextStartSegmentIndex());
                    request.setStartSegmentIndex(response.getNextStartSegmentIndex());
                    paginationNumber = RandomUtils.nextInt(5) + 1;
                    request.setPaginationNumber(paginationNumber);
                }
            } catch (Exception e) {
                logger.error("Cannot get volume!", e);
                fail();
            }
        }

        assertEquals(oriSegIdList.size(), paginationSegIdList.size());

        int totalSegmentCount = rootVolumeSize + childVolumeSize1 + childVolumeSize2;

        for (int i = 0; i < totalSegmentCount; i++) {
            assertEquals(oriSegIdList.get(i), paginationSegIdList.get(i));
        }

    }

    /**
     * Test list volumes with filter: if listVolumeRequest is set filter(volumesCanBeList),
     * then only the volumes in the list will be return to caller; if filter is not set,
     * then listVolumes will return all without filter.
     */
    @Test
    public void testListVolumesWithFilter() {
        VolumeMetadata volumeMetadata = generateVolumeMetadata(1, null, 6, 3, 1);
        volumeMetadata.setAccountId(123456);
        volumeMetadata.setInAction(NULL);
        VolumeMetadata volumeMetadata1 = generateVolumeMetadata(2, null, 6, 3, 1);
        volumeMetadata1.setAccountId(123456);
        volumeMetadata1.setInAction(NULL);
        volumeStore.saveVolume(volumeMetadata);
        volumeStore.saveVolume(volumeMetadata1);
        informationCenter.setVolumeStore(volumeStore);

        Set<Long> volumeCanBeList = new HashSet<>();

        ListVolumesRequest request = new ListVolumesRequest(RequestIdBuilder.get(), 123456);
        request.setVolumesCanBeList(volumeCanBeList);
        // filter list
        List<VolumeMetadata_Thrift> volumeMetadata_thrifts;

        // first with filter which has 0 volume can be list
        try {
            ListVolumesResponse response = informationCenter.listVolumes(request);
            volumeMetadata_thrifts = response.getVolumes();
            assertEquals(0, volumeMetadata_thrifts.size());
        } catch (Exception e) {
            logger.error("Cannot list volumes!", e);
            fail();
        }

        // then filter with 3 volumes which two exist and one doesn't
        volumeCanBeList.add(1L);
        volumeCanBeList.add(2L);
        volumeCanBeList.add(3L);
        request.setVolumesCanBeList(volumeCanBeList);
        try {
            ListVolumesResponse response = informationCenter.listVolumes(request);
            volumeMetadata_thrifts = response.getVolumes();
            assertEquals(2, volumeMetadata_thrifts.size());
        } catch (Exception e) {
            logger.error("Cannot list volumes!", e);
            fail();
        }

        // last close filter function
        request.setVolumesCanBeList(null);
        try {
            ListVolumesResponse response = informationCenter.listVolumes(request);
            volumeMetadata_thrifts = response.getVolumes();
            assertEquals(2, volumeMetadata_thrifts.size());
        } catch (Exception e) {
            logger.error("Cannot list volumes!", e);
            fail();
        }
    }

    @Test
    public void testListVolumesWithMigrationSpeed() throws Exception {
        VolumeMetadata volumeMetadata = generateVolumeMetadata(1, null, 6, 3, 1);
        volumeMetadata.setAccountId(123456);
        volumeMetadata.setInAction(NULL);
        SegmentUnitMetadata segUnit1 = new SegmentUnitMetadata(new SegId(1, 0), 0);
        SegmentUnitMetadata segUnit2 = new SegmentUnitMetadata(new SegId(1, 0), 1);
        SegmentUnitMetadata segUnit3 = new SegmentUnitMetadata(new SegId(1, 0), 2);
        SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);

        SegmentMembership highestMembershipInSegment = mock(SegmentMembership.class);
        volumeMetadata.addSegmentMetadata(segmentMetadata, highestMembershipInSegment);

        when(segmentMetadata.getIndex()).thenReturn(0);
        when(segmentMetadata.getSegmentUnitCount()).thenReturn(3);
        when(segmentMetadata.getSegId()).thenReturn(new SegId(1, 0));
        Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = new HashMap<>();
        segmentUnitMetadataMap.put(new InstanceId(1), segUnit1);
        segmentUnitMetadataMap.put(new InstanceId(2), segUnit2);
        segmentUnitMetadataMap.put(new InstanceId(3), segUnit3);
        List<SegmentUnitMetadata> segmentUnitMetadataArrayList = new ArrayList<>();
        segmentUnitMetadataArrayList.add(segUnit1);
        segmentUnitMetadataArrayList.add(segUnit2);
        segmentUnitMetadataArrayList.add(segUnit3);
        when(segmentMetadata.getSegmentUnitMetadataTable()).thenReturn(segmentUnitMetadataMap);
        when(segmentMetadata.getSegmentUnits()).thenReturn(segmentUnitMetadataArrayList);
        when(segmentMetadata.getLatestMembership()).thenReturn(highestMembershipInSegment);

        segUnit1.setMembership(highestMembershipInSegment);
        segUnit1.setInstanceId(new InstanceId(1));
        segUnit1.setCacheType(CacheType.NONE);
        segUnit1.setTotalPageToMigrate(100);
        segUnit1.setAlreadyMigratedPage(50);
        segUnit1.setMigrationSpeed(10);
        segUnit1.setStatus(SegmentUnitStatus.Primary);

        segUnit2.setMembership(highestMembershipInSegment);
        segUnit2.setInstanceId(new InstanceId(2));
        segUnit2.setCacheType(CacheType.NONE);
        segUnit2.setTotalPageToMigrate(100);
        segUnit2.setAlreadyMigratedPage(50);
        segUnit2.setMigrationSpeed(10);
        segUnit2.setStatus(SegmentUnitStatus.Secondary);

        segUnit3.setMembership(highestMembershipInSegment);
        segUnit3.setInstanceId(new InstanceId(3));
        segUnit3.setCacheType(CacheType.NONE);
        segUnit3.setTotalPageToMigrate(100);
        segUnit3.setAlreadyMigratedPage(50);
        segUnit3.setMigrationSpeed(10);
        segUnit3.setStatus(SegmentUnitStatus.Secondary);

        SegmentVersion segmentVersion = new SegmentVersion(0, 0);

        when(highestMembershipInSegment.getSegmentVersion()).thenReturn(segmentVersion);
        when(highestMembershipInSegment.getPrimary()).thenReturn(new InstanceId(1));
        Set<InstanceId> secondaries = new HashSet<>();
        secondaries.add(new InstanceId(2));
        secondaries.add(new InstanceId(3));
        when(highestMembershipInSegment.getSecondaries()).thenReturn(secondaries);
        when(highestMembershipInSegment.getArbiters()).thenReturn(new HashSet<>());
        when(highestMembershipInSegment.getInactiveSecondaries()).thenReturn(new HashSet<>());
        when(highestMembershipInSegment.getJoiningSecondaries()).thenReturn(new HashSet<>());
        when(highestMembershipInSegment.getMemberIOStatusMap()).thenReturn(new HashMap<>());
        volumeStore.saveVolume(volumeMetadata);
        informationCenter.setVolumeStore(volumeStore);

        ListVolumesRequest listVolumesRequest = new ListVolumesRequest(RequestIdBuilder.get(), 123456);
        ListVolumesResponse listVolumesResponse = informationCenter.listVolumes(listVolumesRequest);
        logger.warn("{}", listVolumesResponse);
        assertEquals(1, listVolumesResponse.getVolumesSize());
        VolumeMetadata_Thrift volumeGet = listVolumesResponse.getVolumes().get(0);
        assertEquals(15, volumeGet.getMigrationSpeed());
        assertTrue(50.0 == volumeGet.getMigrationRatio());
    }

    @Test
    public void testListVolumeAccessRulesByVolumeIds() {
        VolumeRuleRelationshipInformation volumeRuleRelationshipInformation = new VolumeRuleRelationshipInformation(1,
                1, 1);
        // relationship 1 volume 1 rule 1
        volumeRuleRelationshipStore.save(volumeRuleRelationshipInformation);
        volumeRuleRelationshipInformation.setRelationshipId(2);
        volumeRuleRelationshipInformation.setRuleId(2);
        // relationship 2 volume 1 rule 2
        volumeRuleRelationshipStore.save(volumeRuleRelationshipInformation);
        volumeRuleRelationshipInformation.setRelationshipId(3);
        volumeRuleRelationshipInformation.setRuleId(3);
        // relationship 3 volume 1 rule 3
        volumeRuleRelationshipStore.save(volumeRuleRelationshipInformation);
        volumeRuleRelationshipInformation.setVolumeId(2);
        volumeRuleRelationshipInformation.setRelationshipId(4);
        volumeRuleRelationshipInformation.setRuleId(1);
        // relationship 4 volume 2 rule 1
        volumeRuleRelationshipStore.save(volumeRuleRelationshipInformation);
        volumeRuleRelationshipInformation.setRelationshipId(5);
        volumeRuleRelationshipInformation.setRuleId(2);
        // relationship 5 volume 2 rule 2
        volumeRuleRelationshipStore.save(volumeRuleRelationshipInformation);
        volumeRuleRelationshipInformation.setRelationshipId(6);
        volumeRuleRelationshipInformation.setRuleId(3);
        // relationship 6 volume 2 rule 3
        volumeRuleRelationshipStore.save(volumeRuleRelationshipInformation);

        AccessRuleInformation accessRuleInformation = new AccessRuleInformation(1, "", 1);
        accessRuleInformation.setStatus(AccessRuleStatus.APPLIED.APPLIED.name());
        accessRuleStore.save(accessRuleInformation);
        accessRuleInformation.setRuleId(2);
        accessRuleInformation.setStatus(AccessRuleStatus.APPLIED.APPLIED.name());
        accessRuleStore.save(accessRuleInformation);
        accessRuleInformation.setRuleId(3);
        accessRuleInformation.setStatus(AccessRuleStatus.APPLIED.APPLIED.name());
        accessRuleStore.save(accessRuleInformation);

        Set<Long> volumeIds = new ArraySet<>(Long.class);

        try {
            volumeIds.add(1l);
            ListVolumeAccessRulesByVolumeIdsRequest request = new ListVolumeAccessRulesByVolumeIdsRequest(1, volumeIds);
            ListVolumeAccessRulesByVolumeIdsResponse response = informationCenter
                    .listVolumeAccessRulesByVolumeIds(request);
            assertEquals(1, response.getAccessRulesTableSize());
            List<VolumeAccessRule_Thrift> volumeAccessRule_thrifts = response.getAccessRulesTable().get(1l);
            assertEquals(3, volumeAccessRule_thrifts.size());

            volumeIds.add(2l);
            request.setVolumeIds(volumeIds);
            response = informationCenter.listVolumeAccessRulesByVolumeIds(request);
            assertEquals(2, response.getAccessRulesTableSize());
            volumeAccessRule_thrifts = response.getAccessRulesTable().get(1l);
            assertEquals(3, volumeAccessRule_thrifts.size());
            volumeAccessRule_thrifts = response.getAccessRulesTable().get(2l);
            assertEquals(3, volumeAccessRule_thrifts.size());

            volumeIds.clear();
            request.setVolumeIds(volumeIds);
            response = informationCenter.listVolumeAccessRulesByVolumeIds(request);
            assertEquals(0, response.getAccessRulesTableSize());
        } catch (Exception e) {
            logger.error("Cannot list volume access rule by volumeIds!", e);
            fail();
        }
    }

    /**
     * Test mark volume readOnly and then check volume is readOnly.
     * Volume is not connected by any client and is not applied any access rule.
     * Expected no IsNotReadOnlyException.
     */
    @Test
    public void markVolumeReadOnlyAndCheckVolumeIsReadOnlyTest() {
        MarkVolumeReadWriteRequest volumeReadOnlyRequest = new MarkVolumeReadWriteRequest();
        try {
            VolumeMetadata volume = generateVolumeMetadata(1, null, 3, 3, 1);
            volumeStore.saveVolume(volume);
            volumeReadOnlyRequest.setVolumeId(volume.getVolumeId());
            volumeReadOnlyRequest.setRequestId(RequestIdBuilder.get());
            volumeReadOnlyRequest.setReadWrite(ReadWriteType_Thrift.READ_ONLY);
            informationCenter.markVolumeReadWrite(volumeReadOnlyRequest);
            VolumeMetadata volumeAfterMarkAsReadOnly = volumeStore.getVolume(volume.getVolumeId());
            assertEquals(VolumeMetadata.ReadWriteType.READ_ONLY, volumeAfterMarkAsReadOnly.getReadWrite());
            IsVolumeReadOnlyRequest isVolumeReadOnlyRequest = new IsVolumeReadOnlyRequest();
            isVolumeReadOnlyRequest.setRequestId(RequestIdBuilder.get());
            isVolumeReadOnlyRequest.setVolumeId(volume.getVolumeId());
            informationCenter.isVolumeReadOnly(isVolumeReadOnlyRequest);
        } catch (Exception e) {
            logger.error("Caught an exception:", e);
            fail("Caught an exception.");
        }
    }

    /**
     * Test mark volume readOnly and then check volume is readOnly.
     * Volume is not connected by any client but is applied an access rule with ReadWrite permission.
     * Expected VolumeIsMarkReadOnlyException_Thrift.
     */
    @Test(expected = VolumeIsAppliedWriteAccessRuleException_Thrift.class)
    public void volumeIsReadOnlyWhenVolumeIsAppliedWriteAccessRuleTest() throws Exception {
        VolumeMetadata volume = generateVolumeMetadata(1, null, 3, 3, 1);
        volumeStore.saveVolume(volume);
        try {
            MarkVolumeReadWriteRequest volumeReadOnlyRequest = new MarkVolumeReadWriteRequest();
            volumeReadOnlyRequest.setVolumeId(volume.getVolumeId());
            volumeReadOnlyRequest.setRequestId(RequestIdBuilder.get());
            volumeReadOnlyRequest.setReadWrite(ReadWriteType_Thrift.READ_ONLY);
            informationCenter.markVolumeReadWrite(volumeReadOnlyRequest);
        } catch (TException fail) {
            fail();
        }

        try {
            VolumeRuleRelationshipInformation volumeRuleRelationshipInformation = new VolumeRuleRelationshipInformation(
                    1, 1, 1);
            volumeRuleRelationshipStore.save(volumeRuleRelationshipInformation);
            AccessRuleInformation accessRuleInformation = new AccessRuleInformation(1, "0.0.0.1", 3);
            accessRuleStore.save(accessRuleInformation);
            IsVolumeReadOnlyRequest isVolumeReadOnlyRequest = new IsVolumeReadOnlyRequest();
            isVolumeReadOnlyRequest.setRequestId(RequestIdBuilder.get());
            isVolumeReadOnlyRequest.setVolumeId(volume.getVolumeId());
            informationCenter.isVolumeReadOnly(isVolumeReadOnlyRequest);
        } catch (VolumeIsAppliedWriteAccessRuleException_Thrift e) {
            throw e;
        }
    }

    /**
     * Test mark volume readOnly and then mark back to readWrite.
     * Volume is not connected by any client and is not applied any access rule.
     * Expected VolumeIsMarkWriteException_Thrift.
     */
    @Test(expected = VolumeIsMarkWriteException_Thrift.class)
    public void markVolumeReadOnlyThenBackToReadWrite() throws Exception {
        try {
            VolumeMetadata volume = generateVolumeMetadata(1, null, 3, 3, 1);
            volume.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
            volumeStore.saveVolume(volume);

            MarkVolumeReadWriteRequest volumeReadOnlyRequest = new MarkVolumeReadWriteRequest();
            volumeReadOnlyRequest.setVolumeId(volume.getVolumeId());
            volumeReadOnlyRequest.setRequestId(RequestIdBuilder.get());
            volumeReadOnlyRequest.setReadWrite(ReadWriteType_Thrift.READ_ONLY);
            informationCenter.markVolumeReadWrite(volumeReadOnlyRequest);

            VolumeMetadata volumeAfterMarkAsReadOnly = volumeStore.getVolume(volume.getVolumeId());
            assertEquals(VolumeMetadata.ReadWriteType.READ_ONLY, volumeAfterMarkAsReadOnly.getReadWrite());

            MarkVolumeReadWriteRequest volumeReadWriteRequest = new MarkVolumeReadWriteRequest();
            volumeReadWriteRequest.setRequestId(RequestIdBuilder.get());
            volumeReadWriteRequest.setVolumeId(volume.getVolumeId());
            volumeReadWriteRequest.setReadWrite(ReadWriteType_Thrift.READ_WRITE);

            informationCenter.markVolumeReadWrite(volumeReadWriteRequest);

            IsVolumeReadOnlyRequest isVolumeReadOnlyRequest = new IsVolumeReadOnlyRequest();
            isVolumeReadOnlyRequest.setRequestId(RequestIdBuilder.get());
            isVolumeReadOnlyRequest.setVolumeId(volume.getVolumeId());
            informationCenter.isVolumeReadOnly(isVolumeReadOnlyRequest);
        } catch (VolumeIsMarkWriteException_Thrift e) {
            throw e;
        } catch (Exception e) {
            logger.error("Caught an exception:", e);
            fail("Caught an exception.");
        }

    }

    @Test
    public void testNewAThriftResponseObjectListIsNull() {
        GetVolumeResponse response = new GetVolumeResponse();
        assertNull(response.getVolumeMetadata());
        assertNull(response.getDriverMetadatas());
    }

    @Test
    public void testListArchives_SlotNo() {
        ListArchivesRequest_Thrift request_thrift = new ListArchivesRequest_Thrift();
        request_thrift.setRequestId(RequestIdBuilder.get());

        Map<Long, Integer> insId_insNoMap = new HashMap<>();
        List<InstanceMetadata> insMetadataList = new ArrayList<>();
        for (int i = 0; i < 3; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata ins = new InstanceMetadata(instanceId);

            insId_insNoMap.put(instanceId.getId(), i);
            ins.setEndpoint("10.0.2.79:2232");
            ins.setCapacity(15000 + i*2000);
            ins.setFreeSpace(10000 + i*2000);
            ins.setDatanodeStatus(InstanceMetadata.DatanodeStatus.OK);

            List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
            for (int j = 0; j < 4; j++){
                RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                rawArchiveMetadata.setArchiveId((long)j);
                char diskNo = (char)((long)'a'+j);
                rawArchiveMetadata.setSerialNumber("/dev/sd"+diskNo);
                rawArchiveMetadata.setSlotNo(String.valueOf(j));
                rawArchiveMetadata.setArchiveType(ArchiveType.RAW_DISK);
                rawArchiveMetadata.setStorageType(StorageType.SATA);

                rawArchiveMetadataList.add(rawArchiveMetadata);
            }
            ins.setArchives(rawArchiveMetadataList);
            ins.setDatanodeType(NORMAL);

            insMetadataList.add(ins);
        }

        when(storageStore.list()).thenReturn(insMetadataList);

        ListArchivesResponse_Thrift response_thrift = null;
        try {
            response_thrift = informationCenter.listArchives(request_thrift);
        } catch (TException e) {
            e.printStackTrace();
            assert(false);
        }

        List<InstanceMetadata_Thrift> instanceMetadata_thriftList = response_thrift.getInstanceMetadata();

        assertEquals(3, instanceMetadata_thriftList.size());

        for (InstanceMetadata_Thrift instanceMetadata_thrift : instanceMetadata_thriftList){
            Long insId = instanceMetadata_thrift.getInstanceId();
            assertTrue(insId_insNoMap.containsKey(insId));
            int i = insId_insNoMap.get(insId);

            assertEquals(15000 + i*2000, instanceMetadata_thrift.getCapacity());
            assertEquals(10000 + i*2000, instanceMetadata_thrift.getFreeSpace());

            List<ArchiveMetadata_Thrift> archiveMetadata_thriftList = instanceMetadata_thrift.getArchiveMetadata();
            assertEquals(4, archiveMetadata_thriftList.size());

            for (ArchiveMetadata_Thrift archiveMetadata_thrift : archiveMetadata_thriftList){
                int j = (int)archiveMetadata_thrift.getArchiveId();
                char diskNo = (char)((long)'a'+j);
                assertEquals("/dev/sd"+diskNo, archiveMetadata_thrift.getSerialNumber());
                assertEquals(String.valueOf(j), archiveMetadata_thrift.getSlotNo());
            }
        }
    }

    /**
     * pool level will changes to HIGH
     * 1 domain, 1 pool in domain1, 3 instance,
     * 1 simple datanode with 1 archive in domain1, 2 normal datanode with 1 archives in pool,
     * 1 PSA volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSA_IsGroupEnough_1Simple_2Normal() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 3;
        int simpleDatanodeCount = 1;
        int archiveCount = 1;
        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(domainId);

            if (i < simpleDatanodeCount){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);

                List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
                for (int j = 0; j < archiveCount; j++){
                    RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                    rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                    rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                    rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                    rawArchiveMetadata.setLogicalSpace(archiveSpace);
                    rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                    rawArchiveMetadataList.add(rawArchiveMetadata);
                    instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                }
                instance.setArchives(rawArchiveMetadataList);
            }

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
        storagePool1.setDomainId(domainId);

        //add volume
        for (int i = 0; i < 1; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.SMALL);
            volume1.setVolumeId(1);
            volume1.setRootVolumeId(1);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);

            volumeStore.saveVolume(volume1);

            storagePool1.addVolumeId(volume1.getVolumeId());
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);


        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool1.getStoragePoolLevel()));
    }
    /**
     * pool level will changes to LOW
     * 2 domain, 1 pool in domain1, 3 instance,
     * 1 simple datanode with no archive in domain2, 2 normal datanode with 1 archives in pool,
     * 1 PSA volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSA_IsGroupEnough_1SimpleInAnotherDomain_2Normal() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 3;
        int simpleDatanodeCount = 1;
        int archiveCount = 1;
        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);
            instance.setDomainId(domainId);

            if (i < simpleDatanodeCount){
                instance.setDatanodeType(SIMPLE);
                instance.setDomainId(2L);
            } else {
                instance.setDatanodeType(NORMAL);

                List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
                for (int j = 0; j < archiveCount; j++){
                    RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                    rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                    rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                    rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                    rawArchiveMetadata.setLogicalSpace(archiveSpace);
                    rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                    rawArchiveMetadataList.add(rawArchiveMetadata);
                    instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                }
                instance.setArchives(rawArchiveMetadataList);
            }


            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
        storagePool1.setDomainId(domainId);

        //add volume
        for (int i = 0; i < 1; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.SMALL);
            volume1.setVolumeId(1);
            volume1.setRootVolumeId(1);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);

            volumeStore.saveVolume(volume1);

            storagePool1.addVolumeId(volume1.getVolumeId());
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);


        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.LOW.name().equals(storagePool1.getStoragePoolLevel()));
    }
    /**
     * pool level will changes to HIGH
     * 1 domain, 1 pool in domain1, 3 instance,
     * 1 simple datanode with 1 archive in domain1, 2 normal datanode with 1 archives in pool,
     * 1 PSA volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSA_IsGroupEnough_1SimpleWithArchive_2Normal() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 3;
        int simpleDatanodeCount = 1;
        int archiveCount = 1;
        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(domainId);

            if (i < simpleDatanodeCount){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);
            }

            List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
            for (int j = 0; j < archiveCount; j++){
                RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                rawArchiveMetadata.setLogicalSpace(archiveSpace);
                rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                rawArchiveMetadataList.add(rawArchiveMetadata);
                instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
            }
            instance.setArchives(rawArchiveMetadataList);

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
        storagePool1.setDomainId(domainId);

        //add volume
        for (int i = 0; i < 1; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.SMALL);
            volume1.setVolumeId(1);
            volume1.setRootVolumeId(1);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);

            volumeStore.saveVolume(volume1);

            storagePool1.addVolumeId(volume1.getVolumeId());
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);


        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool1.getStoragePoolLevel()));
    }
    /**
     * pool level will changes to MIDDLE, when detach normalDatanode disk
     * 1 domain, 1 pool in domain1, 3 instance,
     * 1 simple datanode with no archive in domain1, 2 normal datanode with 1 archives in pool,
     * 1 PSA volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSA_DetachNormalDatanodeArchive_1SimpleWithArchive_2Normal() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 3;
        int simpleDatanodeCount = 1;
        int archiveCount = 1;
        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(domainId);

            if (i < simpleDatanodeCount){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);
            }

            List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
            for (int j = 0; j < archiveCount; j++){
                RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                rawArchiveMetadata.setLogicalSpace(archiveSpace);
                rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                if (i == simpleDatanodeCount && j == 0){
                    rawArchiveMetadata.setStatus(ArchiveStatus.OFFLINED);
                }
                rawArchiveMetadataList.add(rawArchiveMetadata);
                instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());

            }
            instance.setArchives(rawArchiveMetadataList);

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
        storagePool1.setDomainId(domainId);

        //add volume
        for (int i = 0; i < 1; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.SMALL);
            volume1.setVolumeId(1);
            volume1.setRootVolumeId(1);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);

            volumeStore.saveVolume(volume1);

            storagePool1.addVolumeId(volume1.getVolumeId());
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);


        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.LOW.name().equals(storagePool1.getStoragePoolLevel()));
    }
    /**
     * pool level will changes to LOW, when detach simpleDatanode disk
     * 1 domain, 1 pool in domain1, 3 instance,
     * 1 simple datanode with no archive in domain1, 2 normal datanode with 1 archives in pool,
     * 1 PSA volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSA_DetachSimpleDatanodeArchive_1SimpleWithArchive_2Normal() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 3;
        int simpleDatanodeCount = 1;
        int archiveCount = 1;
        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(domainId);

            if (i < simpleDatanodeCount){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);
            }

            List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
            for (int j = 0; j < archiveCount; j++){
                RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                rawArchiveMetadata.setLogicalSpace(archiveSpace);
                rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                if (i == 0 && j == 0){
                    rawArchiveMetadata.setStatus(ArchiveStatus.OFFLINED);
                }
                rawArchiveMetadataList.add(rawArchiveMetadata);
                instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());

            }
            instance.setArchives(rawArchiveMetadataList);

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
        storagePool1.setDomainId(domainId);

        //add volume
        for (int i = 0; i < 1; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.SMALL);
            volume1.setVolumeId(1);
            volume1.setRootVolumeId(1);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);

            volumeStore.saveVolume(volume1);

            storagePool1.addVolumeId(volume1.getVolumeId());
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);


        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.MIDDLE.name().equals(storagePool1.getStoragePoolLevel()));
    }

    /**
     * pool level will changes to HIGH
     * 1 domain, 1 pool in domain1, 3 instance,
     * 0 simple datanode with no archive in domain1, 3 normal datanode with 1 archives in pool,
     * 1 PSS volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSS_IsGroupEnough_0Simple_3Normal() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 3;
        int simpleDatanodeCount = 0;
        int archiveCount = 1;
        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(1L);

            if (i < simpleDatanodeCount){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);

                List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
                for (int j = 0; j < archiveCount; j++){
                    RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                    rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                    rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                    rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                    rawArchiveMetadata.setLogicalSpace(archiveSpace);
                    rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                    rawArchiveMetadataList.add(rawArchiveMetadata);
                    instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                }
                instance.setArchives(rawArchiveMetadataList);
            }

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
        storagePool1.setDomainId(domainId);

        //add volume
        for (int i = 0; i < 1; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.REGULAR);
            volume1.setVolumeId(1);
            volume1.setRootVolumeId(1);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);
            storagePool1.addVolumeId(volume1.getVolumeId());

            volumeStore.saveVolume(volume1);
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);


        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool1.getStoragePoolLevel()));
    }
    /**
     * pool level will changed to LOW;
     * 1 domain, 1 pool in domain1, 3 instance,
     * 1 simple datanode with no archive in domain1, 2 normal datanode with 1 archives in pool,
     * 1 PSS volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSS_IsGroupEnough_1Simple_2Normal() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 3;
        int simpleDatanodeCount = 1;
        int archiveCount = 1;
        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(domainId);

            if (i < simpleDatanodeCount){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);

                List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
                for (int j = 0; j < archiveCount; j++){
                    RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                    rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                    rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                    rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                    rawArchiveMetadata.setLogicalSpace(archiveSpace);
                    rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                    rawArchiveMetadataList.add(rawArchiveMetadata);
                    instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                }
                instance.setArchives(rawArchiveMetadataList);
            }

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
        storagePool1.setDomainId(domainId);

        //add volume
        for (int i = 0; i < 1; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.REGULAR);
            volume1.setVolumeId(1);
            volume1.setRootVolumeId(1);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);
            storagePool1.addVolumeId(volume1.getVolumeId());

            volumeStore.saveVolume(volume1);
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);


        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.LOW.name().equals(storagePool1.getStoragePoolLevel()));
    }
    /**
     * pool level will changes, when a pool disk detached;
     * 1 domain, 1 pool in domain1, 3 instance,
     * 0 simple datanode with no archive in domain1, 3 normal datanode with 1 archives in pool,
     * 1 PSS volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSS_DiskDetach_3Instance_3Archives() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 3;
        int simpleDatanodeCount = 0;
        int archiveCount = 1;
        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(domainId);

            if (i < simpleDatanodeCount){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);

                List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
                for (int j = 0; j < archiveCount; j++){
                    RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                    rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                    rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                    rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                    rawArchiveMetadata.setLogicalSpace(archiveSpace);
                    rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                    rawArchiveMetadataList.add(rawArchiveMetadata);
                    instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());

                    if (i == 0 && j == 0){
                        rawArchiveMetadata.setStatus(ArchiveStatus.OFFLINED);
                    }
                }
                instance.setArchives(rawArchiveMetadataList);
            }

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
        storagePool1.setDomainId(domainId);

        //add volume
        for (int i = 0; i < 1; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.REGULAR);
            volume1.setVolumeId(1);
            volume1.setRootVolumeId(1);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);
            storagePool1.addVolumeId(volume1.getVolumeId());

            volumeStore.saveVolume(volume1);
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);


        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.LOW.name().equals(storagePool1.getStoragePoolLevel()));
    }
    /**
     * pool level will changes, when a pool disk detached;
     * 2 domain, 1 pool in domain1, 4 instance,
     * 0 simple datanode with no archive in domain1, 4 normal datanode with 1 archives in pool,
     * 1 PSS volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSS_DiskDetach_4Instance_4Archives() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 4;
        int simpleDatanodeCount = 0;
        int archiveCount = 1;
        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(domainId);

            if (i < simpleDatanodeCount){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);

                List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
                for (int j = 0; j < archiveCount; j++){
                    RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                    rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                    rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                    rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                    rawArchiveMetadata.setLogicalSpace(archiveSpace);
                    rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                    rawArchiveMetadataList.add(rawArchiveMetadata);
                    instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());

                    if (i == 0 && j == 0){
                        rawArchiveMetadata.setStatus(ArchiveStatus.OFFLINED);
                    }
                }
                instance.setArchives(rawArchiveMetadataList);
            }

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
        storagePool1.setDomainId(domainId);

        //add volume
        for (int i = 0; i < 1; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.REGULAR);
            volume1.setVolumeId(1);
            volume1.setRootVolumeId(1);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);
            storagePool1.addVolumeId(volume1.getVolumeId());

            volumeStore.saveVolume(volume1);
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);


        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.MIDDLE.name().equals(storagePool1.getStoragePoolLevel()));
    }
    /**
     * pool level will be LOW;
     * 1 domain, 1 pool in domain1, 3 instance,
     * 1 simple datanode with no archive in domain1, 2 normal datanode with 1 archives in pool,
     * 1 PSA volume, 1 PSS volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSA_PSS_IsGroupEnough_1Simple_2Normal() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 3;
        int simpleDatanodeCount = 1;
        int archiveCount = 1;
        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(domainId);

            if (i < simpleDatanodeCount){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);

                List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
                for (int j = 0; j < archiveCount; j++){
                    RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                    rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                    rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                    rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                    rawArchiveMetadata.setLogicalSpace(archiveSpace);
                    rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                    rawArchiveMetadataList.add(rawArchiveMetadata);
                    instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                }
                instance.setArchives(rawArchiveMetadataList);
            }

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setDomainId(domainId);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);

        //add volume
        for (int i = 0; i < 2; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.SMALL);
            volume1.setVolumeId(i);
            volume1.setRootVolumeId(i);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);

            if (i == 0){
                volume1.setVolumeType(VolumeType.SMALL);
                storagePool1.addVolumeId(volume1.getVolumeId());
            } else if (i == 1){
                volume1.setVolumeType(VolumeType.REGULAR);
                storagePool1.addVolumeId(volume1.getVolumeId());
            }

            volumeStore.saveVolume(volume1);
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);

        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);
        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.LOW.name().equals(storagePool1.getStoragePoolLevel()));
    }
    /**
     * pool level will be HIGH;
     * 1 domain, 1 pool in domain1, 3 instance,
     * 0 simple datanode with no archive in domain1, 3 normal datanode with 1 archives in pool,
     * 1 PSA volume, 1 PSS volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSA_PSS_IsGroupEnough_0Simple_3Normal() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 3;
        int simpleDatanodeCount = 0;
        int archiveCount = 1;
        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(domainId);

            if (i < simpleDatanodeCount){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);

                List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
                for (int j = 0; j < archiveCount; j++){
                    RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                    rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                    rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                    rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                    rawArchiveMetadata.setLogicalSpace(archiveSpace);
                    rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                    rawArchiveMetadataList.add(rawArchiveMetadata);
                    instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                }
                instance.setArchives(rawArchiveMetadataList);
            }

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setDomainId(domainId);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);

        //add volume
        for (int i = 0; i < 2; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.SMALL);
            volume1.setVolumeId(i);
            volume1.setRootVolumeId(i);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);

            if (i == 0){
                volume1.setVolumeType(VolumeType.SMALL);
                storagePool1.addVolumeId(volume1.getVolumeId());
            } else if (i == 1){
                volume1.setVolumeType(VolumeType.REGULAR);
                storagePool1.addVolumeId(volume1.getVolumeId());
            }

            volumeStore.saveVolume(volume1);
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);

        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);
        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool1.getStoragePoolLevel()));
    }
    /**
     * pool level will be HIGH;
     * 1 domain, 1 pool in domain1, 5 instance,
     * 1 simple datanode with no archive in domain1, 4 normal datanode with 2 archives in pool,
     * 1 PSA volume, 1 PSSAA volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSA_PSSAA_IsGroupEnough_1Simple_4Normal() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 5;
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(domainId);

            if (i == 2){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);

                List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
                int archiveCount = 2;
                for (int j = 0; j < archiveCount; j++){
                    RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                    rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                    rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                    rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                    rawArchiveMetadata.setLogicalSpace(archiveSpace);
                    rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                    rawArchiveMetadataList.add(rawArchiveMetadata);
                }
                instance.setArchives(rawArchiveMetadataList);
            }

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        Multimap<Long, Long> instanceId2ArchiveIdInPool2 = HashMultimap.create();
        for (InstanceMetadata instance : instanceMetadataList){
            for (RawArchiveMetadata rawArchiveMetadata : instance.getArchives()){
                instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                if (instance.getInstanceId().getId() < 3){
                    instanceId2ArchiveIdInPool2.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                }
            }
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setDomainId(domainId);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);

        //add volume
        for (int i = 0; i < 2; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.SMALL);
            volume1.setVolumeId(i);
            volume1.setRootVolumeId(i);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);

            if (i == 1){
                volume1.setVolumeType(VolumeType.SMALL);
                storagePool1.addVolumeId(volume1.getVolumeId());
            } else if (i == 0){
                volume1.setVolumeType(VolumeType.LARGE);
                storagePool1.addVolumeId(volume1.getVolumeId());
            }

            volumeStore.saveVolume(volume1);
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);

        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);
        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool1.getStoragePoolLevel()));
//        assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool2.getStoragePoolLevel()));
    }
    /**
     * pool level will changes, when group not enough;
     * 1 domain, 1 pool in domain1, 4 instance,
     * 1 simple datanode with no archive in domain1, 3 normal datanode with 2 archives in pool,
     * 1 PSA volume, 1 PSSAA volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSA_PSSAA_IsGroupEnough_1Simple_3Normal() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 4;
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(domainId);

            if (i == 2){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);

                List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
                int archiveCount = 2;
                for (int j = 0; j < archiveCount; j++){
                    RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                    rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                    rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                    rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                    rawArchiveMetadata.setLogicalSpace(archiveSpace);
                    rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                    rawArchiveMetadataList.add(rawArchiveMetadata);
                }
                instance.setArchives(rawArchiveMetadataList);
            }

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        Multimap<Long, Long> instanceId2ArchiveIdInPool2 = HashMultimap.create();
        for (InstanceMetadata instance : instanceMetadataList){
            for (RawArchiveMetadata rawArchiveMetadata : instance.getArchives()){
                instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                if (instance.getInstanceId().getId() < 3){
                    instanceId2ArchiveIdInPool2.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                }
            }
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setDomainId(domainId);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);

        //add volume
        for (int i = 0; i < 2; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.SMALL);
            volume1.setVolumeId(i);
            volume1.setRootVolumeId(i);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);

            if (i == 0){
                volume1.setVolumeType(VolumeType.SMALL);
                storagePool1.addVolumeId(volume1.getVolumeId());
            } else if (i == 1){
                volume1.setVolumeType(VolumeType.LARGE);
                storagePool1.addVolumeId(volume1.getVolumeId());
            }

            volumeStore.saveVolume(volume1);
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);

        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);
        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.LOW.name().equals(storagePool1.getStoragePoolLevel()));
    }
    /**
     * pool level will changes, when group not enough;
     * 1 domain, 1 pool in domain1, 5 instance,
     * 2 simple datanode with no archive in domain1, 3 normal datanode with 2 archives in pool,
     * 1 PSA volume, 1 PSSAA volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSA_PSSAA_IsGroupEnough_2Simple_3Normal() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 5;
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(domainId);

            if (i == 2 || i == 3){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);

                List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
                int archiveCount = 2;
                for (int j = 0; j < archiveCount; j++){
                    RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                    rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                    rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                    rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                    rawArchiveMetadata.setLogicalSpace(archiveSpace);
                    rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                    rawArchiveMetadataList.add(rawArchiveMetadata);
                }
                instance.setArchives(rawArchiveMetadataList);
            }

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        Multimap<Long, Long> instanceId2ArchiveIdInPool2 = HashMultimap.create();
        for (InstanceMetadata instance : instanceMetadataList){
            for (RawArchiveMetadata rawArchiveMetadata : instance.getArchives()){
                instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                if (instance.getInstanceId().getId() < 3){
                    instanceId2ArchiveIdInPool2.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                }
            }
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setDomainId(domainId);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);

        //add volume
        for (int i = 0; i < 2; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.SMALL);
            volume1.setVolumeId(i);
            volume1.setRootVolumeId(i);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);

            if (i == 1){
                volume1.setVolumeType(VolumeType.SMALL);
                storagePool1.addVolumeId(volume1.getVolumeId());
            } else if (i == 0){
                volume1.setVolumeType(VolumeType.LARGE);
                storagePool1.addVolumeId(volume1.getVolumeId());
            }

            volumeStore.saveVolume(volume1);
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);

        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);
        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool1.getStoragePoolLevel()));
    }
    /**
     * pool level will changes, when group not enough;
     * 1 domain, 1 pool in domain1, 5 instance,
     * 3 simple datanode with no archive in domain1, 2 normal datanode with 2 archives in pool,
     * 1 PSA volume, 1 PSSAA volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSA_PSSAA_IsGroupEnough_3Simple_2Normal() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 5;
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(domainId);

            if (i < 3){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);

                List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
                int archiveCount = 2;
                for (int j = 0; j < archiveCount; j++){
                    RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                    rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                    rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                    rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                    rawArchiveMetadata.setLogicalSpace(archiveSpace);
                    rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                    rawArchiveMetadataList.add(rawArchiveMetadata);
                }
                instance.setArchives(rawArchiveMetadataList);
            }

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        Multimap<Long, Long> instanceId2ArchiveIdInPool2 = HashMultimap.create();
        for (InstanceMetadata instance : instanceMetadataList){
            for (RawArchiveMetadata rawArchiveMetadata : instance.getArchives()){
                instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                if (instance.getInstanceId().getId() < 3){
                    instanceId2ArchiveIdInPool2.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                }
            }
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setDomainId(domainId);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);

        //add volume
        for (int i = 0; i < 2; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.SMALL);
            volume1.setVolumeId(i);
            volume1.setRootVolumeId(i);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);

            if (i == 1){
                volume1.setVolumeType(VolumeType.SMALL);
                storagePool1.addVolumeId(volume1.getVolumeId());
            } else if (i == 0){
                volume1.setVolumeType(VolumeType.LARGE);
                storagePool1.addVolumeId(volume1.getVolumeId());
            }

            volumeStore.saveVolume(volume1);
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);

        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);
        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.LOW.name().equals(storagePool1.getStoragePoolLevel()));
    }
    /**
     * pool level will changes, when group not enough;
     * 1 domain, 1 pool in domain1, 5 instance,
     * 0 simple datanode with no archive in domain1, 5 normal datanode with 2 archives in pool,
     * 1 PSA volume, 1 PSSAA volume in pool;
     * @throws Exception
     */
    @Test
    public void testPoolLevel_PSA_PSSAA_IsGroupEnough_0Simple_5Normal() throws Exception {
        long childVolumeId = 1l;
        long volumeSize = 6l;
        long archiveSpace = volumeSize;
        long archiveFreeSpace = 4;
        long domainId = 1;

        appContext.setStatus(InstanceStatus.OK);

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList = new ArrayList<>();

        //add instance
        int instanceCount = 5;
        List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++){
            InstanceId instanceId = new InstanceId(i);
            InstanceMetadata instance = new InstanceMetadata(instanceId);
            instance.setGroup(new Group(i*10));
            instance.setLastUpdated(System.currentTimeMillis() + 80000);
            instance.setDatanodeStatus(OK);

            instance.setDomainId(domainId);

            if (i < 0){
                instance.setDatanodeType(SIMPLE);
            } else {
                instance.setDatanodeType(NORMAL);

                List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
                int archiveCount = 2;
                for (int j = 0; j < archiveCount; j++){
                    RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
                    rawArchiveMetadata.setArchiveId(Long.valueOf(i*10 + j));
                    rawArchiveMetadata.setInstanceId(instance.getInstanceId());
                    rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
                    rawArchiveMetadata.setLogicalSpace(archiveSpace);
                    rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
                    rawArchiveMetadataList.add(rawArchiveMetadata);
                }
                instance.setArchives(rawArchiveMetadataList);
            }

            instanceMetadataList.add(instance);

            when(instanceMaintenanceDBStore.getById(instanceId.getId())).thenReturn(new InstanceMaintenanceInformation());
        }

        Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
        Multimap<Long, Long> instanceId2ArchiveIdInPool2 = HashMultimap.create();
        for (InstanceMetadata instance : instanceMetadataList){
            for (RawArchiveMetadata rawArchiveMetadata : instance.getArchives()){
                instanceId2ArchiveIdInPool1.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                if (instance.getInstanceId().getId() < 3){
                    instanceId2ArchiveIdInPool2.put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
                }
            }
        }

        StoragePool storagePool1 = new StoragePool();
        storagePool1.setPoolId(1L);
        storagePool1.setDomainId(domainId);
        storagePool1.setName("pool1");
        storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);

        //add volume
        for (int i = 0; i < 2; i++){
            VolumeMetadata volume1 = new VolumeMetadata();
            volume1.setVolumeType(VolumeType.SMALL);
            volume1.setVolumeId(i);
            volume1.setRootVolumeId(i);
            volume1.setStoragePoolId(storagePool1.getPoolId());
            volume1.setVolumeSize(volumeSize);
            volume1.setVolumeStatus(VolumeStatus.Available);
            volume1.setSimpleConfiguration(true);
            volume1.setChildVolumeId(childVolumeId);
            volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
            volume1.setInAction(NULL);

            if (i == 1){
                volume1.setVolumeType(VolumeType.SMALL);
                storagePool1.addVolumeId(volume1.getVolumeId());
            } else if (i == 0){
                volume1.setVolumeType(VolumeType.LARGE);
                storagePool1.addVolumeId(volume1.getVolumeId());
            }

            volumeStore.saveVolume(volume1);
        }

        List<StoragePool> storagePoolList = new ArrayList<>();
        storagePoolList.add(storagePool1);

        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        when(instanceMaintenanceDBStore.listAll()).thenReturn(instanceMaintenanceDBStoreList);
        when(storageStore.list()).thenReturn(instanceMetadataList);

        Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
        EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
        poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);
        storageStoreSweeper.setStoragePoolId2EventDataWorker(poolId2EventDataWorker);
        storageStoreSweeper.setSegmentSize(1);

        storageStoreSweeper.doWork();

        assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool1.getStoragePoolLevel()));
    }

}