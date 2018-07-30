package py.infocenter.service;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import edu.emory.mathcs.backport.java.util.Collections;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.NetworkConfiguration;
import py.app.context.AppContext;
import py.app.thrift.ThriftProcessorFactory;
import py.archive.ArchiveMetadata;
import py.archive.ArchiveStatus;
import py.archive.RawArchiveMetadata;
import py.archive.segment.*;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.VolumeMetadataJSONParser;
import py.common.client.RequestResponseHelper;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.driver.*;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.exception.*;
import py.icshare.*;
import py.icshare.authorization.APIStore;
import py.icshare.authorization.AccountStore;
import py.icshare.authorization.ResourceStore;
import py.icshare.authorization.RoleStore;
import py.icshare.iscsiAccessRule.Iscsi2AccessRuleRelationship;
import py.icshare.iscsiAccessRule.IscsiAccessRule;
import py.icshare.iscsiAccessRule.IscsiAccessRuleInformation;
import py.icshare.iscsiAccessRule.IscsiRuleRelationshipInformation;
import py.icshare.qos.*;
import py.icshare.exception.SnapshotCountReachMaxException;
import py.icshare.exception.SnapshotNameExistException;
import py.icshare.exception.SnapshotNotFoundException;
import py.icshare.exception.VolumeNotFoundException;
import py.infocenter.DBManager.BackupDBManager;
import py.infocenter.DBManager.BackupDBManagerImpl;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.InformationCenterAppEngine;
import py.infocenter.common.InfoCenterConstants;
import py.infocenter.rebalance.SegmentUnitsDistributionManager;
import py.infocenter.rebalance.builder.SimulateInstanceBuilder;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.rebalance.struct.ReserveVolumeCombination;
import py.infocenter.service.selection.ComparisonSelectionStrategy;
import py.infocenter.service.selection.DriverContainerSelectionStrategy;
import py.infocenter.service.selection.RandomSelectionStrategy;
import py.infocenter.service.selection.SelectionStrategy;
import py.infocenter.store.*;
import py.infocenter.worker.StoragePoolSpaceCalculator;
import py.instance.*;
import py.license.LicenseStorage;
import py.membership.SegmentForm;
import py.membership.SegmentMembership;
import py.metrics.*;
import py.monitor.jmx.server.ResourceType;
import py.rebalance.RebalanceTask;
import py.service.debug.AbstractConfigurationServer;
import py.thrift.coordinator.service.AddOrModifyLimitationRequest;
import py.thrift.coordinator.service.Coordinator;
import py.thrift.coordinator.service.DeleteLimitationRequest;
import py.thrift.icshare.*;
import py.thrift.infocenter.service.*;
import py.thrift.share.*;
import py.utils.ValidateParam;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeMetadata.VolumeSourceType;
import py.volume.VolumeStatus;
import py.volume.VolumeType;
import py.volume.snapshot.SnapshotMetadata;
import py.volume.snapshot.VolumeInfoToSnapshotInfo;
import py.volume.snapshot.VolumeSnapshotManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;
import static py.common.client.RequestResponseHelper.*;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.icshare.InstanceMetadata.DatanodeStatus.UNKNOWN;
import static py.volume.VolumeMetadata.VolumeInAction.*;

@SuppressWarnings("ALL")
public class InformationCenterImpl extends AbstractConfigurationServer
        implements InformationCenter.Iface, ThriftProcessorFactory {
    private static final Logger logger = LoggerFactory.getLogger(InformationCenterImpl.class);
    /**
     * put 2 more candidates in tail of candidates list returned if amount of candidates is enough, this allows some
     * launching error occurred on heading candidates
     */
    private static final int AMOUNT_OF_REDUNDANT_DRIVER_CONTAINER_CANDIDATES = 2;
    final private InformationCenter.Processor<InformationCenter.Iface> processor;
    private InformationCenterAppEngine informationCenterAppEngine;
    private VolumeStore volumeStore; // store the all volume;
    private VolumeStatusTransitionStore volumeStatusStore; // store all volumes need to process their status;
    private SegmentUnitTimeoutStore segmentUnitTimeoutStore; // store all the segment unit to check if their report time is
    // timeout;
    private InfoCenterAppContext appContext;
    private InstanceStore instanceStore;
    private DriverStore driverStore;
    private StorageStore storageStore;
    private VolumeRuleRelationshipStore volumeRuleRelationshipStore;
    private AccessRuleStore accessRuleStore;
    private IscsiRuleRelationshipStore iscsiRuleRelationshipStore;
    private IscsiAccessRuleStore iscsiAccessRuleStore;
    private IOLimitationStore ioLimitationStore;
    private MigrationRuleStore migrationRuleStore;
    private ServerNodeStore serverNodeStore;
    private DiskInfoStore diskInfoStore;
    private DomainStore domainStore;
    private StoragePoolStore storagePoolStore;
    private AlarmStore alarmStore;
    private AccountStore accountStore;
    private APIStore apiStore;
    private RoleStore roleStore;
    private ResourceStore resourceStore;
    private DriverContainerSelectionStrategy driverContainerSelectionStrategy;
    private long segmentSize;
    private long deadVolumeToRemove;
    private int groupCount;
    private long nextActionTimeIntervalMs;
    private SelectionStrategy selectionStrategy;
    private NetworkConfiguration networkConfiguration;
    private CloneRelationshipsDBStore cloneRelationshipsStore;
    private LicenseStorage licenseStorage;
    private CoordinatorClientFactory coordinatorClientFactory;
    private OrphanVolumeStore orphanVolumes;
    private CapacityRecordStore capacityRecordStore;
    private Map<String, Long> serverNodeReportTimeMap;
    // if this is a limited Capacity, the total size of volumes created should
    // should not exceed the capacity, otherwise
    // throw no enough space exception, default size: 1T
    private long userMaxCapacityBytes = Long.MAX_VALUE;

    // shutdown flag
    private boolean shutDownFlag = false;

    private long lastRefreshTime;
    private long actualFreeSpace;

    private int pageWrappCount;
    private int segmentWrappCount;
    private InstanceMaintenanceDBStore instanceMaintenanceDBStore;

    // shutdown Thread mux
    private AtomicBoolean shutdownThreadRunningFlag = new AtomicBoolean(false);

    private BackupDBManager backupDBManager;
    private SegmentUnitsDistributionManager segmentUnitsDistributionManager;

    private PYMetric timerReportSegmentUnitMetadata;
//    private PYMetric timerReportSegmentUnitMetadata_IsOrphanVolume;
//    private PYMetric timerReportSegmentUnitMetadata_updateVolume;
    private PYMetric timerReserveSegmentUnit_CreateVolume;
    private PYMetric timerReserveSegmentUnit_ReserveVolume;

    // for unit test
    public void setBackupDBManager(BackupDBManager backupDBManager) {
        this.backupDBManager = backupDBManager;
    }

    public InformationCenterImpl() {
        processor = new InformationCenter.Processor<>(this);
        lastRefreshTime = 0;
        serverNodeReportTimeMap = new ConcurrentHashMap<>();

        initMetrics();
    }

    private void initMetrics(){
        PYMetricRegistry metricRegistry = PYMetricRegistry.getMetricRegistry();
        timerReportSegmentUnitMetadata = metricRegistry.register(
                MetricRegistry.name(InformationCenterImpl.class.getSimpleName(), "timer_report_segment_unit_metadata"),
                Timer.class);
//        timerReportSegmentUnitMetadata_IsOrphanVolume = metricRegistry.register(
//                MetricRegistry.name(InformationCenterImpl.class.getSimpleName(), "timer_report_segment_unit_metadata_is_orphan_volume"),
//                Timer.class);
//        timerReportSegmentUnitMetadata_updateVolume = metricRegistry.register(
//                MetricRegistry.name(InformationCenterImpl.class.getSimpleName(), "timer_report_segment_unit_metadata_update_volume"),
//                Timer.class);
        timerReserveSegmentUnit_CreateVolume = metricRegistry.register(MetricRegistry.name(InformationCenterImpl.class.getSimpleName(), "timer_reserve_segment_unit_create_volume"),
                Timer.class);
        timerReserveSegmentUnit_ReserveVolume = metricRegistry.register(MetricRegistry.name(InformationCenterImpl.class.getSimpleName(), "timer_reserve_segment_unit_reserve_volume"),
                Timer.class);
    }

    public void initBackupDBManager(long roundTimeInterval, int maxBackupCount) {
        this.backupDBManager = new BackupDBManagerImpl(roundTimeInterval, maxBackupCount, volumeRuleRelationshipStore,
                accessRuleStore, domainStore, storagePoolStore, capacityRecordStore, cloneRelationshipsStore,
                licenseStorage, accountStore, apiStore, roleStore, resourceStore, instanceStore,
                iscsiRuleRelationshipStore, iscsiAccessRuleStore, ioLimitationStore, migrationRuleStore);
    }

    public void setSegmentUnitsDistributionManager(SegmentUnitsDistributionManager segmentUnitsDistributionManager) {
        this.segmentUnitsDistributionManager = segmentUnitsDistributionManager;
    }

    @Override
    public TProcessor getProcessor() {
        return processor;
    }

    public long getLastRefreshTime() {
        return lastRefreshTime;
    }

    public void setLastRefreshTime(long lastRefreshTime) {
        this.lastRefreshTime = lastRefreshTime;
    }

    public long getActualFreeSpace() {
        return actualFreeSpace;
    }

    public void setActualFreeSpace(long actualFreeSpace) {
        this.actualFreeSpace = actualFreeSpace;
    }

    public void setSegmentSize(long segmentSize) {
        this.segmentSize = segmentSize;
    }

    public void setUserMaxCapacityByte(long userMaxCapacityBytes) {
        this.userMaxCapacityBytes = userMaxCapacityBytes;
    }

    public VolumeRuleRelationshipStore getVolumeRuleRelationshipStore() {
        return volumeRuleRelationshipStore;
    }

    public void setVolumeRuleRelationshipStore(VolumeRuleRelationshipStore volumeRuleRelationshipStore) {
        this.volumeRuleRelationshipStore = volumeRuleRelationshipStore;
    }

    public AccessRuleStore getAccessRuleStore() {
        return accessRuleStore;
    }

    public void setAccessRuleStore(AccessRuleStore accessRuleStore) {
        this.accessRuleStore = accessRuleStore;
    }

    public IscsiAccessRuleStore getIscsiAccessRuleStore() {
        return iscsiAccessRuleStore;
    }

    public void setIscsiAccessRuleStore(IscsiAccessRuleStore iscsiAccessRuleStore) {
        this.iscsiAccessRuleStore = iscsiAccessRuleStore;
    }

    public IscsiRuleRelationshipStore getIscsiRuleRelationshipStore() {
        return iscsiRuleRelationshipStore;
    }

    public void setIscsiRuleRelationshipStore(IscsiRuleRelationshipStore iscsiRuleRelationshipStore) {
        this.iscsiRuleRelationshipStore = iscsiRuleRelationshipStore;
    }

    public IOLimitationStore getIoLimitationStore() {
        return ioLimitationStore;
    }

    public void setIoLimitationStore(IOLimitationStore ioLimitationStore) {
        this.ioLimitationStore = ioLimitationStore;
    }

    public MigrationRuleStore getMigrationRuleStore() {
        return migrationRuleStore;
    }

    public void setMigrationRuleStore(MigrationRuleStore migrationRuleStore) {
        this.migrationRuleStore = migrationRuleStore;
    }

    public ServerNodeStore getServerNodeStore() {
        return serverNodeStore;
    }

    public void setServerNodeStore(ServerNodeStore serverNodeStore) {
        this.serverNodeStore = serverNodeStore;
    }

    public DiskInfoStore getDiskInfoStore() {
        return diskInfoStore;
    }

    public void setDiskInfoStore(DiskInfoStore diskInfoStore) {
        this.diskInfoStore = diskInfoStore;
    }

    public void setVolumeStatusStore(VolumeStatusTransitionStore volumeStatusStore) {
        this.volumeStatusStore = volumeStatusStore;
    }

    public void setSegmentUnitTimeoutStore(SegmentUnitTimeoutStore segmentUnitTimeoutStore) {
        this.segmentUnitTimeoutStore = segmentUnitTimeoutStore;
    }

    public SelectionStrategy getSelectionStrategy() {
        return selectionStrategy;
    }

    public void setSelectionStrategy(SelectionStrategy selectionStrategy) {
        this.selectionStrategy = selectionStrategy;
    }

    public AppContext getAppContext() {
        return appContext;
    }

    public void setAppContext(InfoCenterAppContext appContext) {
        this.appContext = appContext;
    }

    public OrphanVolumeStore getOrphanVolumes() {
        return orphanVolumes;
    }

    public CoordinatorClientFactory getCoordinatorClientFactory() {
        return coordinatorClientFactory;
    }

    public void setCoordinatorClientFactory(CoordinatorClientFactory coordinatorClientFactory) {
        this.coordinatorClientFactory = coordinatorClientFactory;
    }

    public void setOrphanVolumes(OrphanVolumeStore orphanVolumes) {
        this.orphanVolumes = orphanVolumes;
    }

    public AlarmStore getAlarmStore() {
        return alarmStore;
    }

    public void setAlarmStore(AlarmStore alarmStore) {
        this.alarmStore = alarmStore;
    }

    public NetworkConfiguration getNetworkConfiguration() {
        return networkConfiguration;
    }

    public void setNetworkConfiguration(NetworkConfiguration networkConfiguration) {
        this.networkConfiguration = networkConfiguration;
    }

    public LicenseStorage getLicenseStorage() {
        return licenseStorage;
    }

    public void setLicenseStorage(LicenseStorage licenseStorage) {
        this.licenseStorage = licenseStorage;
    }

    public Map<String, Long> getServerNodeReportTimeMap() {
        return serverNodeReportTimeMap;
    }

    public void setServerNodeReportTimeMap(Map<String, Long> serverNodeReportTimeMap) {
        this.serverNodeReportTimeMap = serverNodeReportTimeMap;
    }

    @Override
    public void ping() throws TException {
        // because the several infocenters exist in out system, but there is one
        // to supply service,
        // so when health checker gets status through ping(), the othes will
        // throw exception.
        // logger.info("pinged by a remote instance");
    }

    @Override
    public void shutdown() throws TException {
        this.shutDownFlag = true;

        if (!shutdownThreadRunningFlag.compareAndSet(false, true))
            return;

        Exception closeStackTrace = new Exception("Capture service shutdown signal");
        logger.warn("shutting down info center service. stack trace of this: ", closeStackTrace);

        Thread shutdownForcely = new Thread(new Runnable() {

            @Override
            public void run() {
                informationCenterAppEngine.stop();
            }
        });
        Thread shutdownThread = new Thread(new Runnable() {

            @Override
            public void run() {
                logger.info("InfoCenter is shutdown");
                try {
                    Thread.sleep(10000);
                    System.exit(0);
                } catch (Exception e) {
                    logger.error("caught an exception", e);
                }
            }
        }, "InfoCenterShutdownThread");
        shutdownThread.start();
        shutdownForcely.start();
    }

    @Override
    public RecoverDatabaseResponse recoverDatabase()
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend");
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("recoverDatabase request");

        boolean success = backupDBManager.recoverDatabase();
        RecoverDatabaseResponse response = new RecoverDatabaseResponse(success);
        logger.warn("recoverDatabase response: {}", response);
        return response;
    }

    /**
     * only for test
     */
    public void shutdownForTest() {
        this.shutDownFlag = true;
    }

    @Override
    public ReportArchivesResponse reportArchives(ReportArchivesRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, InvalidInputException_Thrift,
            InvalidGroupException_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        InstanceMetadata instance;
        NextActionInfo_Thrift datanodeNextAction = new NextActionInfo_Thrift();
        Map<Long, NextActionInfo_Thrift> archiveMapNextAction = new HashMap<>();
        ReportDBResponse_Thrift reportDBResponse = null;
        Map<Long, py.thrift.share.PageMigrationSpeedInfo_Thrift> archiveId2MigrationLimits = new HashMap<>();
        Map<Long, String> archiveId2MigrationStrategy = new HashMap<>();
        Map<Long, py.thrift.share.CheckSecondaryInactiveThreshold_Thrift> archiveIdMapCheckSecondaryInactiveThreshold = new HashMap<>();

        try {
            logger.warn("reportArchives request: {}", request);

            InstanceMetadata_Thrift instanceMetadata_Thrift = request.getInstance();
            if (instanceMetadata_Thrift == null) {
                logger.error("report archive is empty");
                throw new InvalidInputException_Thrift();
            }

            Validate.notNull(instanceMetadata_Thrift.getArchiveMetadata());

            // TODO: new instance or use the old instance from map
            instance = RequestResponseHelper.buildInstanceFrom(instanceMetadata_Thrift);
            instance.setLastUpdated(System.currentTimeMillis());

            InstanceId instanceId = instance.getInstanceId();

            // check datanode domain info
            boolean datanodeInDomainStore = false;
            boolean domainIdChanged = false;
            boolean domainTimePassedEnough = false;
            boolean domainIsDeleting = false;
            Long domainId = null;
            List<Domain> allDomains = domainStore.listAllDomains();
            // find the latest update domain which contains datanodeId
            Domain latestUpdateDomain = null;
            for (Domain domain : allDomains) {
                if (domain.getDataNodes().contains(instanceId.getId())) {
                    if (latestUpdateDomain == null) {
                        latestUpdateDomain = domain;
                        continue;
                    } else {
                        latestUpdateDomain = (
                                latestUpdateDomain.getLastUpdateTime().compareTo(domain.getLastUpdateTime()) >= 0) ?
                                latestUpdateDomain :
                                domain;
                    }
                }
            }

            if (latestUpdateDomain != null) {
                datanodeInDomainStore = true;
                domainTimePassedEnough = latestUpdateDomain.timePassedLongEnough(nextActionTimeIntervalMs);
                domainIsDeleting = latestUpdateDomain.isDeleting();
                if (!domainIsDeleting) {
                    domainId = latestUpdateDomain.getDomainId();
                }
                if (!latestUpdateDomain.getDomainId().equals(instance.getDomainId())) {
                    domainIdChanged = true;
                }
            }

            // report is not in any domain
            if (instance.isFree()) {
                // domain store show this instance is in one domain
                if (datanodeInDomainStore) {
                    instance.setDomainId(domainId);
                    // should wait enough time then new allocate domain id to datanode
                    if (!domainIsDeleting && domainTimePassedEnough) {
                        datanodeNextAction.setNextAction(NextAction_Thrift.NEWALLOC);
                        datanodeNextAction.setNewId(domainId);
                    } else {
                        datanodeNextAction.setNextAction(NextAction_Thrift.KEEP);
                    }
                } else {
                    datanodeNextAction.setNextAction(NextAction_Thrift.KEEP);
                }
                // report is in one domain
            } else {
                // domain store show this instance is still in one domain
                if (datanodeInDomainStore) {
                    instance.setDomainId(domainId);
                    if (domainIdChanged) {
                        // also wait enough time
                        if (!domainIsDeleting && domainTimePassedEnough) {
                            datanodeNextAction.setNextAction(NextAction_Thrift.CHANGE);
                            datanodeNextAction.setNewId(domainId);
                        } else {
                            datanodeNextAction.setNextAction(NextAction_Thrift.KEEP);
                        }
                    } else {
                        datanodeNextAction.setNextAction(NextAction_Thrift.KEEP);
                    }
                } else {
                    /*
                     * 1, datanode has domain id, but can't find this domain any more, means sweeper had deleted it (passed
                     * enough time) 2, also consider when infocenter starting with broken database, should wait database
                     * recovery from datanodes
                     */
                    if (backupDBManager.passedRecoveryTime()) {
                        datanodeNextAction.setNextAction(NextAction_Thrift.FREEMYSELF);
                        logger.warn("instance {} is no longer belong to any domain, set domainId to NULL");
                        instance.setFree();
                    }
                }
            }

            List<RawArchiveMetadata> archives = instance.getArchives();

            //save all diskInfo of current instance
            Map<String, DiskInfo> instanceDiskInfoMap = new HashMap<>();    //<diskSn, DiskInfo>
            if (serverNodeStore != null || archives.size() > 0){
                //get current instance ip
                String instanceIp = instance.getEndpoint();
                if (instanceIp != null && instanceIp.contains(":")){
                    instanceIp = instanceIp.substring(0, instanceIp.indexOf(":")).trim();
                }

                //get serverNode by ip
                ServerNode serverNode = serverNodeStore.getServerNodeByIp(instanceIp);
                if (serverNode != null){
                    //save all diskInfo of the serverNode
                    for (DiskInfo diskInfo : serverNode.getDiskInfoSet()){
                        instanceDiskInfoMap.put(diskInfo.getSn(), diskInfo);
                    }
                } else {
                    logger.warn("cannot find any serverNodes of the instance ip({}) ");
                }

                if (instanceDiskInfoMap.size() > 0){
                    logger.debug("find diskInfo of the serverNode:{}", instanceDiskInfoMap, serverNode);
                } else {
                    logger.warn("cannot find any diskInfo of the instance:{}", instanceDiskInfoMap, instance);
                }
            }

            // check archive info
            for (RawArchiveMetadata archive : archives) {
                Long archiveId = archive.getArchiveId();
                NextActionInfo_Thrift archiveNextAction = new NextActionInfo_Thrift();
                archiveMapNextAction.put(archiveId, archiveNextAction);
                PageMigrationSpeedInfo_Thrift pageMigrationSpeedInfoThrift = new PageMigrationSpeedInfo_Thrift();
                archiveId2MigrationLimits.put(archiveId, pageMigrationSpeedInfoThrift);

                //set archives slotNo
                if (instanceDiskInfoMap.containsKey(archive.getSerialNumber())){
                    String slotNo = instanceDiskInfoMap.get(archive.getSerialNumber()).getSlotNumber();
                    archive.setSlotNo(slotNo);
                    logger.debug("set archive:{} slotNo:{}", archive, slotNo);
                }

                boolean archiveInStoragePoolStore = false;
                boolean storagePoolIdChanged = false;
                boolean storagePoolTimePassedEnough = false;
                boolean storagePoolIsDeleting = false;
                Long storagePoolId = null;
                StoragePool latestStoragePool = null;
                List<StoragePool> allStoragePools = storagePoolStore.listAllStoragePools();
                for (StoragePool storagePool : allStoragePools) {
                    if (storagePool.getArchivesInDataNode().containsEntry(instanceId.getId(), archiveId)) {
                        if (latestStoragePool == null) {
                            latestStoragePool = storagePool;
                            continue;
                        } else {
                            latestStoragePool = (
                                    latestStoragePool.getLastUpdateTime().compareTo(storagePool.getLastUpdateTime())
                                            >= 0) ? latestStoragePool : storagePool;
                        }
                    }
                }
                if (latestStoragePool != null) {
                    archiveInStoragePoolStore = true;
                    storagePoolTimePassedEnough = latestStoragePool.timePassedLongEnough(nextActionTimeIntervalMs);
                    storagePoolId = latestStoragePool.getPoolId();

                    CheckSecondaryInactiveThreshold_Thrift checkSecondaryInactiveThresholdThrift = new CheckSecondaryInactiveThreshold_Thrift();
                    archiveIdMapCheckSecondaryInactiveThreshold.put(archiveId, checkSecondaryInactiveThresholdThrift);

                    MigrationRuleInformation migrationRuleInformation = migrationRuleStore
                            .get(latestStoragePool.getMigrationRuleId());

                    if (migrationRuleInformation != null) {
                        // belong to one storage pool and pool has migration rule
                        pageMigrationSpeedInfoThrift
                                .setMaxMigrationSpeed((migrationRuleInformation.getMaxMigrationSpeed()));
                        archiveId2MigrationStrategy.put(archiveId, migrationRuleInformation.getMigrationStrategy());

                        checkSecondaryInactiveThresholdThrift.setMode(CheckSecondaryInactiveThresholdMode_Thrift
                                .valueOf(migrationRuleInformation.getCheckSecondaryInactiveThresholdMode()));
                        checkSecondaryInactiveThresholdThrift.setStartTime(migrationRuleInformation.getStartTime());
                        checkSecondaryInactiveThresholdThrift.setEndTime(migrationRuleInformation.getEndTime());
                        checkSecondaryInactiveThresholdThrift.setWaitTime(migrationRuleInformation.getWaitTime());
                        checkSecondaryInactiveThresholdThrift
                                .setIgnoreMissPagesAndLogs(migrationRuleInformation.getIgnoreMissPagesAndLogs());

                    } else {
                        // belong to one storage pool but pool has not migration rule
                        pageMigrationSpeedInfoThrift
                                .setMaxMigrationSpeed(RawArchiveMetadata.DEFAULT_MAX_MIGRATION_SPEED);
                        archiveId2MigrationStrategy.put(archiveId, MigrationStrategy.Smart.name());

                        checkSecondaryInactiveThresholdThrift
                                .setMode(CheckSecondaryInactiveThresholdMode_Thrift.RelativeTime);
                        checkSecondaryInactiveThresholdThrift.setStartTime(-1);
                        checkSecondaryInactiveThresholdThrift.setEndTime(-1);
                        checkSecondaryInactiveThresholdThrift.setWaitTime(-1);
                        checkSecondaryInactiveThresholdThrift.setIgnoreMissPagesAndLogs(false);
                    }

                    if (!latestStoragePool.getPoolId().equals(archive.getStoragePoolId())) {
                        storagePoolIdChanged = true;
                    }
                } else {
                    // do not belong to any storage pool
                    pageMigrationSpeedInfoThrift.setMaxMigrationSpeed(RawArchiveMetadata.DEFAULT_MAX_MIGRATION_SPEED);
                    archiveId2MigrationStrategy.put(archiveId, MigrationStrategy.Smart.name());
                }
                if (archive.isFree()) {
                    if (archiveInStoragePoolStore) {
                        archive.setStoragePoolId(storagePoolId);
                        if (!storagePoolIsDeleting && storagePoolTimePassedEnough) {
                            archiveNextAction.setNextAction(NextAction_Thrift.NEWALLOC);
                            archiveNextAction.setNewId(storagePoolId);
                        } else {
                            archiveNextAction.setNextAction(NextAction_Thrift.KEEP);
                        }
                    } else {
                        archiveNextAction.setNextAction(NextAction_Thrift.KEEP);
                    }
                } else {
                    if (archiveInStoragePoolStore) {
                        archive.setStoragePoolId(storagePoolId);
                        if (storagePoolIdChanged) {
                            if (!storagePoolIsDeleting && storagePoolTimePassedEnough) {
                                archiveNextAction.setNextAction(NextAction_Thrift.CHANGE);
                                archiveNextAction.setNewId(storagePoolId);
                            } else {
                                archiveNextAction.setNextAction(NextAction_Thrift.KEEP);
                            }
                        } else {
                            archiveNextAction.setNextAction(NextAction_Thrift.KEEP);
                        }
                    } else {
                        /*
                         * also consider when infocenter starting with broken database, should wait database recovery from
                         * datanodes
                         */
                        if (backupDBManager.passedRecoveryTime()) {
                            archiveNextAction.setNextAction(NextAction_Thrift.FREEMYSELF);
                            archive.setStoragePoolId(null);
                        }
                    }
                }
            }

            /*
             * for monitor using
             */
            for (StoragePool storagePool : storagePoolStore.listAllStoragePools()) {
                Long totalFreeSpace = 0L;
                Long totalLogicSpace = 0L;
                logger.debug("get storage pool:{}", storagePool);
                if (storagePool.getArchivesInDataNode() == null || storagePool.getArchivesInDataNode().isEmpty()) {
                    continue;
                }
                for (Entry<Long, Long> entry : storagePool.getArchivesInDataNode().entries()) {
                    Long datanodeId = entry.getKey();
                    Long archiveId = entry.getValue();

                    InstanceMetadata datanode = storageStore.get(datanodeId);
                    if (datanode != null) {
                        RawArchiveMetadata archive = datanode.getArchiveById(archiveId);
                        if (archive != null) {
                            totalFreeSpace += archive.getLogicalFreeSpace();
                            totalLogicSpace += archive.getLogicalSpace();
                            logger.debug("get archive:{} at datanodeId:{}", archive, datanodeId);
                        } else {
                            logger.warn("can not find archive info by archive Id:{}, at datanode id:{}", archiveId,
                                    datanodeId);
                        }
                    } else {
                        logger.warn("can not find datanode info by datanode Id:{}", datanodeId);
                    }
                }
                logger.debug("get totalLogicSpace:{}, totalFreeSpace:{}, for storagePool:{}", totalLogicSpace,
                        totalFreeSpace, storagePool.getPoolId());
                if (totalLogicSpace != 0) {
                    Double usedRatio = (((double) totalLogicSpace - totalFreeSpace) / totalLogicSpace);
                    storagePool.setUsedRatio(usedRatio);
                    logger.debug("storage pool: {} usedRatio is {}", storagePool.getPoolId(),
                            storagePool.getUsedRatio());
                    try {
                        if (storagePool.getStoragePoolUsageGauge() == PYNullMetric.defaultNullMetric) {
                            PYMetricRegistry metricRegistry = PYMetricRegistry.getMetricRegistry();
                            Validate.isTrue(metricRegistry != null);

                            PYMetric storagePoolUsagMmetric = metricRegistry.registerInstance(
                                    PYMetricRegistry.nameEx("StoragePool", "storagepoolUsageGauge")
                                            .type(ResourceType.STORAGE_POOL).id(String.valueOf(storagePool.getPoolId()))
                                            .build(), new Gauge<Double>() {

                                        @Override
                                        public Double getValue() {
                                            return storagePool.getUsedRatio();
                                        }

                                    });
                            storagePool.setStoragePoolUsageGauge(storagePoolUsagMmetric);

                        }
                    } catch (Exception e) {
                        logger.warn("caught an exception", e);
                    }
                }
            }

            /*
             * when two datanodes report at the same time, should consider sync problem
             */
            synchronized (storageStore) {
                InstanceMetadata oldInstance = storageStore.get(instanceId.getId());

                /*
                 * Do next job such as group assignment, db storage e.g. base on the case if the new group reported and old
                 * group get from store is null.
                 */
                if (instance.getGroup() == null) {
                    // new group is null, old group is not null, give the old group
                    // to datanode
                    if (oldInstance != null) {
                        logger.warn("we set old group id {} to new instance", oldInstance.getGroup());
                        instance.setGroup(oldInstance.getGroup());
                    }

                    // new group is null, old group is null, assign a new group to
                    // datanode
                    if (oldInstance == null) {
                        // assign to a group
                        final Map<Group, Long> group2Capacity = new HashMap<>();
                        for (int i = 0; i < groupCount; i++) {
                            group2Capacity.put(new Group(i), 0L);
                        }

                        // count the capacity of each group
                        for (InstanceMetadata instanceFromStore : storageStore.list()) {
                            Group currentGroup = instanceFromStore.getGroup();
                            long currentCapacity = group2Capacity.get(currentGroup);
                            // The case capacity of instance got from store equals 0
                            // results in group total capacity being 0
                            // during the begin or initialization for datanodes
                            // reporting archives. And all datanodes will
                            // be assigned to one same group by capacity balance
                            // selection algorithm. As a result, user
                            // would fail to create volume with not enough space
                            // exception. To avoid this, give the zero
                            // capacity small size 1.
                            group2Capacity.put(currentGroup, currentCapacity + ((instanceFromStore.getCapacity() == 0) ?
                                    1L :
                                    instanceFromStore.getCapacity()));
                        }

                        SelectionStrategy frugalSelectionStrategy = new ComparisonSelectionStrategy<Group>(
                                new Comparator<Group>() {
                                    // Create a new comparator to compare current
                                    // capacity
                                    // of each group
                                    @Override
                                    public int compare(Group o1, Group o2) {
                                        if (group2Capacity.get(o1) > group2Capacity.get(o2)) {
                                            return 1;
                                        } else if (group2Capacity.get(o1) < group2Capacity.get(o2)) {
                                            return -1;
                                        } else if (o1.getGroupId() > o2.getGroupId()) {
                                            return 1;
                                        } else if (o1.getGroupId() < o2.getGroupId()) {
                                            return -1;
                                        }

                                        return 0;
                                    }
                                });

                        Group selectedGroup = frugalSelectionStrategy.select(group2Capacity.keySet(), 1).get(0);
                        logger.warn("we generate a group {} for new instance: {}", selectedGroup,
                                instance.getInstanceId().getId());
                        instance.setGroup(selectedGroup);
                    }
                } else {
                    // use the new group in reported instance
                }
                logger.debug("Save instance reported from datanode {} to storage store", instance);
                storageStore.save(instance);
            }

            /*
             * update simple datanode group id set, when any datanode's type is SIMPLE,
             * isArbiterGroupSet is set to true,
             * and put this datanode's group id into arbiterGroupIds;
             */
            segmentUnitsDistributionManager.updateSimpleDatanodeInfo(instance);

            /*
             * process report database request
             */
            try {
                reportDBResponse = backupDBManager.process(request.getReportDBRequest());
                logger.debug("get report DB response:{}", reportDBResponse);
            } catch (Exception e) {
                logger.error("failed to process report DB request:{}", request.getReportDBRequest(), e);
            }

            /*
             * get rebalance task of this datanode
             */
            try {
                RebalanceTask rebalanceTask = segmentUnitsDistributionManager.selectRebalanceTasks(0);
                RebalanceTask_Thrift rebalanceTask_Thrift = RequestResponseHelper.buildRebalanceTaskThrift(rebalanceTask);
                RetrieveARebalanceTaskResponse response = new RetrieveARebalanceTaskResponse();
                response.setRequestId(request.getRequestId());
                response.setRebalanceTask(rebalanceTask_Thrift);
                logger.warn("retrieveARebalanceTask response: {}", response);
                return response;
            } catch (NoNeedToRebalance e) {
                throw new NoNeedToRebalance_Thrift();
            }

        } catch (TException e) {
            logger.error("caught an exception with:{}", request.getRequestId(), e);
            throw e;
        } catch (Exception e) {
            logger.error("caught an exception with:{}", request.getRequestId(), e);
            throw new TException(e);
        }
        ReportArchivesResponse response;
        try {
            response = new ReportArchivesResponse();
            response.setRequestId(request.getRequestId());
            response.setGroup(new Group_Thrift(instance.getGroup().getGroupId()));
            response.setDatanodeNextAction(datanodeNextAction);
            response.setArchiveIdMapNextAction(archiveMapNextAction);
            response.setReportDBResponse(reportDBResponse);
            response.setArchiveIdMapMigrationSpeed(archiveId2MigrationLimits);
            response.setArchiveIdMapMigrationStrategy(archiveId2MigrationStrategy);
            response.setArchiveIdMapCheckSecondaryInactiveThreshold(archiveIdMapCheckSecondaryInactiveThreshold);

        } catch (Exception e) {
            logger.error("failed to build response", e);
            throw e;
        }
        logger.warn("reportArchives response: ");
        logger.debug("{}", response);
        return response;
    }

    @Override
    public ReportSegmentUnitsMetadataResponse reportSegmentUnitsMetadata(ReportSegmentUnitsMetadataRequest request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, TException {
        PYTimerContext processContext = PYNullTimerContext.defaultNullTimerContext;
        processContext = timerReportSegmentUnitMetadata.time();
        try{
            if (shutDownFlag) {
                throw new ServiceHavingBeenShutdown_Thrift();
            }
            if (InstanceStatus.SUSPEND == appContext.getStatus()) {
                logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
                throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
            }

            logger.info(request.toString());

            List<SegmentUnitMetadata_Thrift> returnedSegUnits = new ArrayList<>();
            InstanceId fromInstanceId = new InstanceId(request.getInstanceId());

            Set<Long> volumeIdSet = new HashSet<>();

            // this list is used to restore the conflict segment units
            List<SegUnitConflict_Thrift> conflictSegmentUnits = new ArrayList<>();
            for (SegmentUnitMetadata_Thrift receivedUnitMetadata_Thrift : request.getSegUnitsMetadata()) {
                long volumeId = receivedUnitMetadata_Thrift.getVolumeId();
                volumeIdSet.add(volumeId);
                SegmentUnitMetadata segUnit = RequestResponseHelper
                        .buildSegmentUnitMetadataFrom(receivedUnitMetadata_Thrift);
                segUnit.setInstanceId(fromInstanceId);

                logger.debug("received UnitMetadata is {}", segUnit);
                VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);
                if (volumeMetadata != null) {
                    volumeMetadata.setSegmentSize(segmentSize);
                }

                if (volumeMetadata != null) {
                    try {
                        volumeMetadata = processSegmentUnitWithVolumeExist(volumeMetadata, segUnit);
                    } catch (Exception e) {
                        logger.warn("catch an exception", e);
                        processExceptionWithSegmentUnitReport(conflictSegmentUnits, segUnit.getSegId(), e);
                    }
                } else {
                    volumeMetadata = processSegmentUnitWithVolumeNotExist(segUnit);
                }

                if (volumeMetadata == null) {
                    logger.debug("Still can not find volume after process the segment unit {}", segUnit);
                    returnedSegUnits.add(receivedUnitMetadata_Thrift);
                } else {
                    if (receivedUnitMetadata_Thrift.getStatus().equals(SegmentUnitStatus_Thrift.Primary)) {
                        updateSegmentsFreeRatio(receivedUnitMetadata_Thrift, volumeMetadata);
                    }
                    volumeStore.saveVolume(volumeMetadata);
                }
            }

            /*  get orphan volume will be cause long time, when large user calls this function
            *   (e.g. delete a large volume, when segment unit status changed from deleting to deleted, it will calls reportSegmentUnitMetadata)
            PYTimerContext isOrphanVolumeContext = PYNullTimerContext.defaultNullTimerContext;
            isOrphanVolumeContext = timerReportSegmentUnitMetadata_IsOrphanVolume.time();
            for (Long volumeId : volumeIdSet) {
                VolumeMetadata volumeMetadata;
                try {
                    volumeMetadata = getRootVolumeWithChildren(volumeId, true);
                } catch (VolumeNotFoundException e) {
                    // logger.error("caught an exception", e);
                    continue;
                }

                if (volumeMetadata.IsOrphanVolume()) {
                    orphanVolumes.addOrphanVolume(volumeId);
                } else {
                    orphanVolumes.removeOrphanVolume(volumeId);
                }
            }
            isOrphanVolumeContext.stop();
            */

            ReportSegmentUnitsMetadataResponse response = new ReportSegmentUnitsMetadataResponse(request.getRequestId(),
                    conflictSegmentUnits, returnedSegUnits);
            logger.info("reportSegmentUnitsMetadata response: {}", response);
            return response;
        } finally {
            processContext.stop();
        }
    }

    /**
     * add free space ratio to segment metadata & volume metadata.
     *
     * @param tSegUnitsMetadata
     * @param volumeMetadata
     * @author sxl
     */
    private void updateSegmentsFreeRatio(SegmentUnitMetadata_Thrift tSegUnitsMetadata, VolumeMetadata volumeMetadata) {
        logger.debug("Going to get free space ratio of volume: {}", volumeMetadata.getVolumeId());
        // modify the specified segments
        if (tSegUnitsMetadata.getVolumeId() == volumeMetadata.getVolumeId()) {
            SegmentMetadata segMetadata = volumeMetadata.getSegmentByIndex(tSegUnitsMetadata.getSegIndex());
            double segmentFreeSpaceRatio = tSegUnitsMetadata.getRatioFreePages();
            logger.debug("Going to set segment ratio to {},current segment is {}", segmentFreeSpaceRatio, segMetadata);
            segMetadata.setFreeRatio(segmentFreeSpaceRatio);
        }
    }

    private void processExceptionWithSegmentUnitReport(List<SegUnitConflict_Thrift> conflictSegmentUnits, SegId segId,
            Exception e) {
        if (e instanceof SegmentUnitStatusConflictExeption) {
            SegmentUnitStatusConflictExeption statusException = (SegmentUnitStatusConflictExeption) e;
            SegUnitConflict_Thrift conflict = new SegUnitConflict_Thrift(segId.getVolumeId().getId(), segId.getIndex(),
                    SegmentUnitStatusConflictCause_Thrift.valueOf(statusException.getConflictCause().toString()));
            conflictSegmentUnits.add(conflict);
        } else if (e instanceof SnapshotVersionMismatchException) {
            SnapshotVersionMismatchException versionException = (SnapshotVersionMismatchException) e;
            SegUnitConflict_Thrift conflict = new SegUnitConflict_Thrift(segId.getVolumeId().getId(), segId.getIndex(),
                    SegmentUnitStatusConflictCause_Thrift
                            .valueOf(SegmentUnitStatusConflictCause.StaleSnapshotVersion.toString()));
            conflict.setMySnapshotManagerInBinary(versionException.getMySnapshotManagerInBinary());
            conflictSegmentUnits.add(conflict);
        } else if (e instanceof SnapshotRollingBackException) {
            // this exception has been abandoned.
            //            SnapshotRollingBackException roolbackException = (SnapshotRollingBackException) e;
            //            SegUnitConflict_Thrift conflict = new SegUnitConflict_Thrift(segId.getVolumeId().getId(), segId.getIndex(),
            //                    SegmentUnitStatusConflictCause_Thrift
            //                            .valueOf(SegmentUnitStatusConflictCause.RollbackToSnapshot.toString()));
            //            conflict.setMySnpMgrJson(roolbackException.getMySnpMgrJson());
            //            conflictSegmentUnits.add(conflict);
        } else {
            logger.warn("an exception we can not handle {}", e.getClass().getSimpleName());
        }
    }

    /**
     * this function process segment unit reported from data node, but the volume in info center does not exist. this
     * can occur two scenarios: 1. info center reboot and no volume information, data node report volume information.
     * Info center reconstruct data 2. Volume has been dead for a long time. But data node still report segment unit,
     * maybe the data node is down when volume is deleting;
     */
    private VolumeMetadata processSegmentUnitWithVolumeNotExist(SegmentUnitMetadata segUnit) {
        VolumeMetadata volume;

        // volume not exist and segment unit is deleted and deleting, return
        // directly;
        if (segUnit.getStatus() == SegmentUnitStatus.Deleted || segUnit.getStatus() == SegmentUnitStatus.Deleting) {
            return null;
        }

        // from last report time, it has been more than 6 months. Create a fake
        // volume
        long currentTimeSecond = Math.round(System.currentTimeMillis() / 1000f);
        if ((currentTimeSecond - deadVolumeToRemove) > Math.round(segUnit.getLastUpdated() / 1000f)) {
            // if the segment is not the first segment, create a fake volume.
            // Set volume name is volume id + constant
            // When the first segment come from data node, it will replace the
            // volume name
            if (segUnit.getSegId().getIndex() != 0) {
                volume = createFakeVolume(segUnit);
                updateSegmentUnitToVolume(volume, segUnit);
                return volume;
            }
        }

        // First segment, create an volume. Info center rebuild the data of
        // volume
        if (segUnit.getSegId().getIndex() == 0) {
            volume = processFirstSegmentInVolume(null, segUnit);
            if (volume == null) {
                logger.warn("After process the first segment, the volume is still none. The segment unit is {}",
                        segUnit);
            } else {
                logger.warn("new volume reported {}", volume);
                updateSegmentUnitToVolume(volume, segUnit);
            }
            return volume;
        }

        return null;
    }

    private VolumeMetadata processSegmentUnitWithVolumeExist(VolumeMetadata volume, SegmentUnitMetadata segUnit)
            throws SegmentUnitStatusConflictExeption, SnapshotVersionMismatchException, SnapshotRollingBackException {
        // segment unit is the first segment, process first segment in volume
        if (segUnit.getSegId().getIndex() == 0) {
            volume = processFirstSegmentInVolume(volume, segUnit);
        }

        updateSegmentUnitToVolume(volume, segUnit);
        // volume is in deleting, deleted or dead status, but segment unit is still reported by data node.
        // If segment unit is not deleted and deleting, tell data node to release; if so, do nothing;
        // for primary, do not notify data node to delete it directly. Data node need delete secondary first, then
        // the
        // primary will change. And we can delete then

        if (volume.isDeletedByUser()) {
            if (segUnit.getStatus() != SegmentUnitStatus.Deleted && segUnit.getStatus() != SegmentUnitStatus.Deleting
                    && segUnit.getStatus() != SegmentUnitStatus.Primary) {
                throw new SegmentUnitStatusConflictExeption(SegmentUnitStatusConflictCause.VolumeDeleted);
            }
        }

        // check volume is not in deleting status, this scenario may happen when
        // user get the deleting volume back
        if (volume.isRecycling()) {
            if (segUnit.getStatus() == SegmentUnitStatus.Deleting) {
                throw new SegmentUnitStatusConflictExeption(SegmentUnitStatusConflictCause.VolumeRecycled);
            }
        }

        // check the snapshot meta data from segment unit
        if (segUnit.getSnapshotManager() != null) {
            processSnapshotReportedBySegmentUnit(volume, segUnit);
        }

        return volume;
    }

    private void processSnapshotReportedBySegmentUnit(VolumeMetadata volume, SegmentUnitMetadata segmentUnitMetadata)
            throws SnapshotVersionMismatchException {
        VolumeSnapshotManager localSnapshotManager = volume.getSnapshotManager();
        VolumeSnapshotManager reportedSnapshotManager = segmentUnitMetadata.getSnapshotManager();
        if (localSnapshotManager.getVersion() > reportedSnapshotManager.getVersion()) {
            throw new SnapshotVersionMismatchException(localSnapshotManager.toByteArray());
        }

        if (localSnapshotManager.isRollingBack()) {
            logger.debug("check if all segment units' rollback done");
            if (volume.isAllSegmentUnitRollbackDone()) {
                // only one thread will come into here at the same time
                SnapshotMetadata rollingBackSnapshot = localSnapshotManager.rollingBackSnapshot();
                logger.warn(" all segment units in {} have done rolling back to {}", volume.getVolumeId(),
                        rollingBackSnapshot);
                Validate.notNull(rollingBackSnapshot);
                try {
                    localSnapshotManager.doneRollingBack(rollingBackSnapshot.getSnapshotId());
                    updateSnapshotMetadataToCoordinator(volume);
                } catch (SnapshotNotFoundException | SnapshotNotRollingBackException e) {
                    logger.error("caught an impossible exception", e);
                    throw new RuntimeException(e);
                }
            }
        }

        if (localSnapshotManager.getVersion() < reportedSnapshotManager.getVersion()) {
            logger.warn(
                    "data node 's snapshot version is greater than mine, maybe some snapshot has become Unavailable "
                            + "or I am a new infocenter, going to update mine if not rolling back {} {}",
                    localSnapshotManager, reportedSnapshotManager);
            if (!localSnapshotManager.isRollingBack()) {
                logger.warn("updating snapshot manager");
                volume.setSnapshotManager(reportedSnapshotManager);
            }
        }
    }

    /**
     * update the segment unit reported from data node to volume. if segment unit meta already exists, just update the
     * content of the segment unit. if segment unit not exist, just put the segment unit in the volume.
     *
     * @param volume
     * @param newSegUnit
     */
    private void updateSegmentUnitToVolume(VolumeMetadata volume, SegmentUnitMetadata newSegUnit) {
        SegmentMetadata segmentInVolume = volume.getSegmentByIndex(newSegUnit.getSegId().getIndex());

        if (segmentInVolume == null) { // segment does not exist, create a new
            // one
            SegId segId = new SegId(newSegUnit.getSegId());
            SegmentMetadata newSegment = new SegmentMetadata(segId, newSegUnit.getSegId().getIndex());
            newSegment.putSegmentUnitMetadata(newSegUnit.getInstanceId(), newSegUnit);
            logger.info("Create a newSegment and add a new segment unit to volume: {}", newSegUnit);
            volume.addSegmentMetadata(newSegment, newSegUnit.getMembership());
            // new segment unit comes, put the volume to status transition
            // store.
            volumeStatusStore.addVolumeToStore(volume);
            // put it in the segment unit store to monitor if timeout
            segmentUnitTimeoutStore.addSegmentUnit(newSegUnit);
        } else {
            // compare current membership, if member ship reported from data
            // node is new, update membership
            SegmentMembership oldMembership = volume.getMembership(segmentInVolume.getIndex());
            SegmentMembership highestMembership = newSegUnit.getMembership();
            SegmentUnitMetadata oldSegUnit = segmentInVolume.getSegmentUnitMetadata(newSegUnit.getInstanceId());

            if (oldSegUnit == null) {
                // put the segment unit reported in store to monitor if timeout;
                segmentUnitTimeoutStore.addSegmentUnit(newSegUnit);
                logger.info("Add a new segment unit to volume: {}", newSegUnit);
                segmentInVolume.putSegmentUnitMetadata(newSegUnit.getInstanceId(), newSegUnit);
                if (segmentInVolume.isSegmentStatusChanged()) {
                    /*
                     * Segment status is changed, the volume status also may change. we put it in the status store.
                     * Because the volume status depends on segment status;
                     */
                    logger.debug(
                            "We put the volume in status store, for the segment status is changed. volume is {}, segment is {}",
                            volume, segmentInVolume);
                    volumeStatusStore.addVolumeToStore(volume);
                }
            } else {
                // update the content with report segment unit
                if (!oldSegUnit.equals(newSegUnit)) {
                    /*
                     * Be caution: we just update the object in the volume, not overwrite the object. Because the origin
                     * segment unit has been already put in other queue;
                     */
                    oldSegUnit.updateWithNewOne(newSegUnit);
                    logger.debug("After update a segment unit: {}", oldSegUnit);
                    /* update member ship */
                    if (oldMembership == null || oldMembership.compareTo(highestMembership) < 0) {
                        volume.addSegmentMetadata(segmentInVolume, highestMembership);
                    }

                    /*
                     * Segment status is changed, the volume status also may change. we put it in the status store.
                     * Because the volume status depends on segment status;
                     */
                    if (segmentInVolume.isSegmentStatusChanged()) {
                        logger.debug(
                                "put the volume in status store, for the segment status is changed. volume is {}, segment is {}",
                                volume, segmentInVolume);
                        volumeStatusStore.addVolumeToStore(volume);
                    }
                }

                oldSegUnit.setMigrationStatus(newSegUnit.getMigrationStatus());
                oldSegUnit.setRatioMigration(newSegUnit.getRatioMigration());
                oldSegUnit.setLastReported(newSegUnit.getLastReported());
            }
        }
    }

    /**
     * When roolback done in volume, notified the coordinator, some snapshots will be deleted
     */
    private void updateSnapshotMetadataToCoordinator(VolumeMetadata volume) {
        long volumeId = volume.getVolumeId();
        int timeout = 30000;
        List<DriverMetadata> volumeBindingDrivers = driverStore.get(volumeId);
        try {
            UpdateSnapshotRequest request = new UpdateSnapshotRequest(RequestIdBuilder.get(), volumeId,
                    ByteBuffer.wrap(volume.getSnapshotManager().toByteArray()));
            for (DriverMetadata driver : volumeBindingDrivers) {
                EndPoint coordinatorEndPoint = new EndPoint(driver.getHostName(), driver.getCoordinatorPort());

                Coordinator.Iface coordinator = coordinatorClientFactory.build(coordinatorEndPoint, timeout)
                        .getClient();
                coordinator.updateSnapshot(request);
            }
        } catch (Exception e) {
            logger.error("Caught an exception when update snapshot to coordinator {}", e);
        }
    }

    /**
     * Create a Fake volume, when a segment unit reported from datanode but volume does not exist. And segment unit's
     * last updated time is greater than a threshold.
     *
     * @return volumeMetadata
     */
    private VolumeMetadata createFakeVolume(SegmentUnitMetadata segUnit) {
        VolumeMetadata volumeMetadata = new VolumeMetadata();
        volumeMetadata.setAccountId(SUPERADMIN_ACCOUNT_ID);
        volumeMetadata.setVolumeSize(InfoCenterConstants.volumeSize);
        volumeMetadata.setSegmentSize(InfoCenterConstants.segmentSize);
        volumeMetadata.setRootVolumeId(segUnit.getSegId().getVolumeId().getId());
        // a trick solution, when an expired segment unit meta data come from
        // data node, set the name to the volume id +
        // a constant
        volumeMetadata.setName(segUnit.getSegId().getVolumeId() + InfoCenterConstants.name);
        volumeMetadata.setVolumeStatus(VolumeStatus.Unavailable);
        volumeMetadata.setVolumeId(segUnit.getSegId().getVolumeId().getId());
        volumeMetadata.setVolumeType(segUnit.getVolumeType());
        volumeMetadata.setCacheType(segUnit.getCacheType());
        volumeMetadata.setVolumeSource(VolumeSourceType.CREATE_VOLUME);
        volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
        return volumeMetadata;
    }

    /**
     * Compare the version of volume metadata reported from a datanode with that of an existing volume information at
     * the local database. If the reported one has a higher version, the local database will be updated.
     * <p>
     * Since volumeMetadataJSON was only stored in the segment metadata of the first unit at an volume, so name the
     * function processFirstSegmentInVolume
     *
     * @param volumeMetadata
     * @param segUnit
     */
    private VolumeMetadata processFirstSegmentInVolume(VolumeMetadata volumeMetadata, SegmentUnitMetadata segUnit) {
        //        if (segUnit.getAccountMetadataJson() != null) {
        //            AccountMetadataJSONParser accountParser = new AccountMetadataJSONParser(segUnit.getAccountMetadataJson());
        //            try {
        //                AccountMetadata accountFromDB = accountStore.getAccountById(accountParser.getAccountId());
        //                if (accountFromDB == null) {
        //                    ObjectMapper mapper = new ObjectMapper();
        //                    AccountMetadata accountFromDN = mapper
        //                            .readValue(accountParser.getAccountMetadataJSON(), AccountMetadata.class);
        //
        //                    logger.warn("update account : {}", accountFromDN);
        //                    accountStore.createAccount(accountFromDN.getAccountName(), defaulPassword,
        //                            accountFromDN.getAccountType(), accountFromDN.getAccountId());
        //                }
        //            } catch (Exception e) {
        //                logger.error("Failed to update account {}", segUnit.getAccountMetadataJson());
        //                // fall through
        //                return volumeMetadata;
        //            }
        //        }

        String volumeMetadataJSON = segUnit.getVolumeMetadataJson();
        if (volumeMetadataJSON == null) {
            return volumeMetadata;
        }

        VolumeMetadataJSONParser volumeParser = new VolumeMetadataJSONParser(volumeMetadataJSON);
        /*
         * if some data nodes had been dead for a long time, the volume they are belong to is dead already, and
         * volumeMetadata was deleted. when these data nodes come back again, they start to report themselves to info
         * center. In some cases, such as segmentIndex>0 report before segmentIndex=0. when segmentIndex>0 come first,
         * we set some special values to make sure volumeMetadata could display at console, but when segmentIndex=0 come
         * later, we also update volumeMetadata to display, so we can see if volume name contains volumeId, means we
         * should update volumeMetadata by VolumeMetadataJSON.
         */
        if ((volumeMetadata != null) && (volumeMetadata.getName() != null && volumeMetadata.getName()
                .contains(String.valueOf(volumeMetadata.getVolumeId())))) {
            VolumeMetadata volumeFromJson;
            try {
                ObjectMapper mapper = new ObjectMapper();
                volumeFromJson = mapper.readValue(volumeParser.getVolumeMetadataJSON(), VolumeMetadata.class);
                volumeMetadata.setVolumeSize(volumeFromJson.getVolumeSize());
                volumeMetadata.setName(volumeFromJson.getName());
                volumeMetadata.setVolumeType(volumeFromJson.getVolumeType());
                volumeMetadata.setVolumeLayout(volumeFromJson.getVolumeLayout());
                volumeMetadata.setSimpleConfiguration(volumeFromJson.isSimpleConfiguration());
                volumeMetadata.setSegmentNumToCreateEachTime(volumeFromJson.getSegmentNumToCreateEachTime());
                volumeMetadata.setDomainId(volumeFromJson.getDomainId());
                volumeMetadata.setStoragePoolId(volumeFromJson.getStoragePoolId());
                volumeMetadata.setSegmentSize(segmentSize);
                volumeMetadata.setVolumeCreatedTime(volumeFromJson.getVolumeCreatedTime());
                volumeMetadata.setLastExtendedTime(volumeFromJson.getLastExtendedTime());
                volumeMetadata.setVolumeSource(volumeFromJson.getVolumeSource());

                /**update the WrappCount and SegmentCount*/
                volumeMetadata.setReadWrite(volumeFromJson.getReadWrite());
                volumeMetadata.setPageWrappCount(volumeFromJson.getPageWrappCount());
                volumeMetadata.setSegmentWrappCount(volumeFromJson.getSegmentCount());
                // when memory updates, should update volume size, volume name,
                // volume type fields as well
                volumeStore.updatePersistedItems(volumeMetadata.getVolumeId(), volumeMetadata.getVolumeSize(),
                        volumeMetadata.getName(), volumeMetadata.getVolumeType().name(),
                        volumeMetadata.getVolumeLayout(), volumeMetadata.isSimpleConfiguration(),
                        volumeMetadata.getSegmentNumToCreateEachTime(), volumeMetadata.getDomainId(),
                        volumeMetadata.getStoragePoolId(), volumeMetadata.getVolumeCreatedTime(),
                        volumeMetadata.getLastExtendedTime(), volumeMetadata.getVolumeSource().name(),
                        volumeMetadata.getReadWrite().name());
                logger.info("update special volumeMetadata: {}", volumeMetadata);
            } catch (Exception e) {
                logger.error("failed to parse {}", volumeParser.getVolumeMetadataJSON(), e);
                return volumeMetadata;
            }
        }

        if (volumeMetadata != null && volumeParser.getVersion() <= volumeMetadata.getVersion()) {
            // there exists a volumeMetadata which has no lower version than
            // that reported. Do nothing
            return volumeMetadata;
        }

        logger.debug(
                "the version of the volume from the report request {} is larger than the current one. {}Updating it to the database",
                volumeMetadataJSON, volumeMetadata);
        VolumeMetadata volumeFromDN;
        try {
            ObjectMapper mapper = new ObjectMapper();
            volumeFromDN = mapper.readValue(volumeParser.getVolumeMetadataJSON(), VolumeMetadata.class);
            volumeFromDN.setSegmentSize(segmentSize);
        } catch (Exception e) {
            logger.error("failed to parse {}", volumeParser.getVolumeMetadataJSON(), e);
            return volumeMetadata;
        }

        logger.debug("volumeMetadata from datanode : {}", volumeFromDN);

        boolean volumeMetadataUpdateFlag = false;
        if (volumeMetadata == null) {
            logger.info("volumeMetadata is null, i will produce new volume metadata volume json: {}",
                    volumeParser.getVolumeMetadataJSON());
            volumeMetadata = volumeFromDN;
            volumeMetadata.setVolumeStatus(VolumeStatus.ToBeCreated);
            volumeMetadataUpdateFlag = true;
        }

        /*
         * An issue of doing the following is that it is not completed synchronized. In some scenarios where two data
         * nodes reports segment units at the same time, and there is no volume at the database, two different volume
         * metadatas are created and write to database, one will override the other.
         *
         * Such a situation is fine. The correct volume metadata will be stored eventually after the volume metadata is
         * saved to the database and the following synchronized block start to be effective
         */
        synchronized (volumeMetadata) {
            Long origChildId = volumeMetadata.getChildVolumeId();
            int origPos = volumeMetadata.getPositionOfFirstSegmentInLogicVolume();
            int origVersion = volumeMetadata.getVersion();
            if (volumeMetadata.compareTo(volumeFromDN) < 0) {
                Validate.isTrue((volumeMetadata.getChildVolumeId() == null) || volumeMetadata.getChildVolumeId()
                                .equals(volumeFromDN.getChildVolumeId()),
                        "the volume {} at database has non-zero child id while datanode reports non-zero child id {}",
                        volumeMetadata, volumeFromDN.getChildVolumeId());

                volumeMetadata.setChildVolumeId(volumeFromDN.getChildVolumeId());

                Validate.isTrue((volumeFromDN.getPositionOfFirstSegmentInLogicVolume() == volumeMetadata
                                .getPositionOfFirstSegmentInLogicVolume()) || (
                                volumeMetadata.getPositionOfFirstSegmentInLogicVolume() == 0),
                        "the volume {} at database has non-zero logic position while datanode reports non-zero logic position {}",
                        volumeMetadata, volumeFromDN.getPositionOfFirstSegmentInLogicVolume());

                volumeMetadata
                        .setPositionOfFirstSegmentInLogicVolume(volumeFromDN.getPositionOfFirstSegmentInLogicVolume());
                volumeMetadata.setVersion(volumeFromDN.getVersion());
                volumeMetadataUpdateFlag = true;

            }

            if (volumeMetadataUpdateFlag) {
                volumeMetadata.setUpdatedToDataNode(true);
                try {
                    volumeStore.saveVolume(volumeMetadata);
                    volumeMetadata.setPersistedToDatabase(true);
                    logger.debug("After updated, the volume metadata is {}", volumeMetadata);
                } catch (Exception e) {
                    volumeMetadata.setChildVolumeId(origChildId);
                    volumeMetadata.setPositionOfFirstSegmentInLogicVolume(origPos);
                    volumeMetadata.setVersion(origVersion);
                    return volumeMetadata;
                }
            }
        }
        return volumeMetadata;
    }

    /**
     * This interface just check the capacity of info center to create a volume. whether can create volume has been done
     * in createVolume.
     * <p>
     * And there will be a map of segment index to instance list in the response. For each segment, his segment units
     * will be created on the corresponding list of instance. The instance list will have at least the member count of
     * the to-be-created volume's type.
     * <p>
     * To keep the balance of the whole system, instance list is in fixed order, the first one of which is most likely
     * to be primary and the redundant ones will not be used most of times.
     *
     * @author tyr
     */
    @Override
    public synchronized ReserveVolumeResponse reserveVolume(ReserveVolumeRequest request)
            throws NotEnoughSpaceException_Thrift, InternalError_Thrift, InvalidInputException_Thrift,
            ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, ExceedUserMaxCapacityException_Thrift,
            VolumeNotFoundException_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("reserveVolume request: {}", request);

        long expectedSize = request.isNotCreateAllSegmentAtBegining() ?
                request.getLeastSegmentUnitCount() * segmentSize :
                request.getVolumeSize();
        if (expectedSize > request.getVolumeSize()) {
            expectedSize = request.getVolumeSize();
        }

        if (expectedSize < segmentSize) {
            logger.error("volume size : {} less than segment size : {}", expectedSize, segmentSize);
            throw new InvalidInputException_Thrift();
        }

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());

        // calculate the total capacity, should not exceed the threshold
        long freeSpace = 0L;
        for (InstanceMetadata entry : storageStore.list()) {
            if (entry.getDatanodeStatus().equals(OK)) {
                freeSpace += entry.getFreeSpace();
            }
        }

        if (this.getLastRefreshTime() == 0 || (System.currentTimeMillis() - this.getLastRefreshTime()
                >= InfoCenterConstants.getRefreshPeriodTime())) {
            this.setActualFreeSpace(freeSpace);
            this.setLastRefreshTime(System.currentTimeMillis());
        }

        VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());

        if (volumeMetadata == null) {
            throw new VolumeNotFoundException_Thrift();
        }

        logger.warn("Found volume: {}", volumeMetadata);

        PYTimerContext reserveSegmentUnitContext = PYNullTimerContext.defaultNullTimerContext;
        reserveSegmentUnitContext = timerReserveSegmentUnit_ReserveVolume.time();

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2InstancesToRemote = null;
        try{
            // reserve volume in existing instances
            segIndex2InstancesToRemote = segmentUnitsDistributionManager
                    .reserveVolume(expectedSize, volumeType, request.isSimpleConfiguration(), segmentWrappCount,
                            volumeMetadata.getStoragePoolId());
        } finally {
            reserveSegmentUnitContext.stop();
        }

        if (segIndex2InstancesToRemote == null) {
            logger.error("no space for create volume {}, volume size is {}, current free space is {}",
                    volumeMetadata.getName(), volumeMetadata.getVolumeSize(), freeSpace);
            throw new NotEnoughSpaceException_Thrift();
        }

        logger.info("Successfully reserved volume, going to save volume");
        volumeStore.saveVolume(volumeMetadata);
        logger.info("Successfully saved volume: {}", volumeMetadata);
        ReserveVolumeResponse response = new ReserveVolumeResponse(request.getRequestId(), segIndex2InstancesToRemote);
        logger.warn("reserveVolume response: {}", response);
        return response;
    }

    /**
     * @author david When a control center creating a volume, it will call CreateVolume, then call reserveVolume; this
     * interface check this volume can be created.
     */
    @Override
    public synchronized CreateVolumeResponse CreateVolume(CreateVolumeRequest request)
            throws NotEnoughSpaceException_Thrift, InternalError_Thrift, InvalidInputException_Thrift,
            ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, ExceedUserMaxCapacityException_Thrift,
            VolumeNameExistedException_Thrift, VolumeExistingException_Thrift, RootVolumeBeingDeletedException_Thrift,
            RootVolumeNotFoundException_Thrift, StoragePoolNotExistInDoaminException_Thrift,
            DomainNotExistedException_Thrift, StoragePoolNotExistedException_Thrift, DomainIsDeletingException_Thrift,
            StoragePoolIsDeletingException_Thrift, NotEnoughGroupException_Thrift, VolumeIsCloningException_Thrift,
            TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("CreateVolume request: {}", request);
        if (request.getName() == null || request.getName().length() > 100) {
            logger.error("volume name is null or too long");
            throw new InvalidInputException_Thrift();
        }
        if (!request.isSetDomainId() || !request.isSetStoragePoolId()) {
            logger.error("domainId or storage pool id is not set when create volume, request:{}", request);
            throw new InvalidInputException_Thrift();
        }
        Domain domain = null;
        try {
            domain = domainStore.getDomain(request.getDomainId());
        } catch (Exception e) {
            logger.warn("can not get domain", e);
        }
        if (domain == null) {
            logger.error("domain:{} is not exist", request.getDomainId());
            throw new DomainNotExistedException_Thrift();
        }
        if (domain.isDeleting()) {
            logger.error("domain:{} is deleting, can not create volume on it", domain);
            throw new DomainIsDeletingException_Thrift();
        }
        StoragePool storagePool = null;
        try {
            storagePool = storagePoolStore.getStoragePool(request.getStoragePoolId());
        } catch (Exception e) {
            logger.warn("can not get storage pool", e);
        }
        if (storagePool == null) {
            logger.error("storage pool:{} is not exist when create volume", request.getStoragePoolId());
            throw new StoragePoolNotExistedException_Thrift();
        }
        if (storagePool.isDeleting()) {
            logger.error("storagePool:{} is deleting, can not create volume on it", storagePool);
            throw new StoragePoolIsDeletingException_Thrift();
        }
        if (!domain.getStoragePools().contains(storagePool.getPoolId())) {
            logger.error("domain:{} does not contain storage pool:{}", domain, storagePool);
            throw new StoragePoolNotExistInDoaminException_Thrift();
        }

        long expectedSize = request.isNotCreateAllSegmentAtBegining() ?
                request.getLeastSegmentUnitCount() * segmentSize :
                request.getVolumeSize();
        if (expectedSize < segmentSize) {
            logger.error("volume size : {} less than segment size : {}", expectedSize, segmentSize);
            throw new InvalidInputException_Thrift();
        }

        if (expectedSize > request.getVolumeSize()) {
            expectedSize = request.getVolumeSize();
        }

        VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());
        CacheType cacheType = RequestResponseHelper.convertCacheType(request.getCacheType());
        int numberOfMembers = volumeType.getNumMembers() - volumeType.getNumArbiters();

        // calculate the total capacity, should not exceed the threshold
        long freeSpace = 0L;
        for (InstanceMetadata entry : storageStore.list()) {
            if (entry.getDatanodeStatus().equals(OK)) {
                freeSpace += entry.getFreeSpace();
            }
        }

        if (this.getLastRefreshTime() == 0 || (System.currentTimeMillis() - this.getLastRefreshTime()
                >= InfoCenterConstants.getRefreshPeriodTime())) {
            this.setActualFreeSpace(freeSpace);
            this.setLastRefreshTime(System.currentTimeMillis());
        }

        // TODO: Do we really need this filter to check that it has enough space
        // to create volume
        if (this.getActualFreeSpace() - numberOfMembers * expectedSize >= 0) {
            this.setActualFreeSpace(this.getActualFreeSpace() - numberOfMembers * expectedSize);
        } else if (!request.isSimpleConfiguration()) {
            logger.warn("Actual free space is not enough space:{} user apply space:{}", this.getActualFreeSpace(),
                    numberOfMembers * expectedSize);
            throw new NotEnoughSpaceException_Thrift();
        }

        VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());

        if (null != volumeMetadata) {
            logger.error("volume with same id {} has existed", request.getVolumeId());
            throw new VolumeExistingException_Thrift();
        }

        // throw exception, if extend a volume when it is cloning
        if (!cloneRelationshipsStore.listBySrcVolumeId(request.getRootVolumeId()).isEmpty() && request.getRequestType()
                .equals("EXTEND_VOLUME")) {
            logger.warn("root volume is cloning");
            throw new VolumeIsCloningException_Thrift();
        }

        volumeMetadata = new VolumeMetadata(request.getRootVolumeId(), request.getVolumeId(), request.getVolumeSize(),
                request.getSegmentSize(), volumeType, cacheType, request.getDomainId(), request.getStoragePoolId());

        volumeMetadata.setName(request.getName());
        volumeMetadata.setAccountId(request.getAccountId());
        volumeMetadata.setSegmentNumToCreateEachTime(request.getLeastSegmentUnitCount());
        volumeMetadata.initVolumeLayout();
        volumeMetadata.setVolumeCreatedTime(new Date());
        volumeMetadata.setVolumeSource(buildVolumeSourceTypeFrom(request.getRequestType()));
        volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
        volumeMetadata.setPageWrappCount(pageWrappCount);
        volumeMetadata.setSegmentWrappCount(segmentWrappCount);
        volumeMetadata.setSimpleConfiguration(request.isSimpleConfiguration());
        volumeMetadata.setEnableLaunchMultiDrivers(request.isEnableLaunchMultiDrivers());
        volumeMetadata.setCloningVolumeId(request.getCloningVolumeId());
        volumeMetadata.setCloningSnapshotId(request.getCloningSnapshotId());
        switch (volumeMetadata.getVolumeSource()) {
        case CREATE_VOLUME:
            volumeMetadata.setInAction(CREATING);
            break;
        case CLONE_VOLUME:
            volumeMetadata.setInAction(CLONING);
            break;
        case MOVE_VOLUME:
            volumeMetadata.setInAction(MOVING);
            break;
        case MOVE_ONLINE_VOLUME:
            volumeMetadata.setInAction(MOVE_ONLINE_MOVING);
            break;
        }

        if (request.getDomainId() == 0) {
            volumeMetadata.setDomainId(null);
        } else {
            volumeMetadata.setDomainId(request.getDomainId());
        }
        VolumeMetadata root = null;
        if (!volumeMetadata.isRoot()) {
            logger.debug("Extending volume {} the new volume that will be created is {} size is {}",
                    request.getRootVolumeId(), request.getVolumeId(), request.getVolumeSize());
            // Get the root volume
            try {
                root = volumeStore.getVolume(request.getRootVolumeId());
            } catch (Exception e) {
                logger.error("Can't get root volume {}", request.getRootVolumeId());
                throw new RootVolumeNotFoundException_Thrift();
            }

            if (root == null) {
                throw new RootVolumeNotFoundException_Thrift();
            }

            // if the root volume is deleting, throw
            // RootVolumeBeingDeletedException_Thrift
            if (root.isDeletedByUser()) {
                throw new RootVolumeBeingDeletedException_Thrift();
            }
        } else { // root volume need to check volume with same name exists
            if (null != volumeStore.getVolumeNotDeadByName(volumeMetadata.getName())) {
                logger.error("some volume has been exist, name is {}", volumeMetadata.getName());
                throw new VolumeNameExistedException_Thrift();
            }
        }

        PYTimerContext reserveSegmentUnitContext = PYNullTimerContext.defaultNullTimerContext;
        reserveSegmentUnitContext = timerReserveSegmentUnit_CreateVolume.time();

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2InstancesToRemote = null;
        try{
            segIndex2InstancesToRemote = segmentUnitsDistributionManager.reserveVolume(expectedSize, volumeMetadata.getVolumeType(), request.isSimpleConfiguration(),
                    segmentWrappCount, volumeMetadata.getStoragePoolId());
        } finally {
            reserveSegmentUnitContext.stop();
        }

        if (segIndex2InstancesToRemote != null) {
            logger.info("Successfully reserved volume, going to save volume");
            volumeStore.saveVolume(volumeMetadata);
            try {
                storagePoolStore.addVolumeId(volumeMetadata.getStoragePoolId(), volumeMetadata.getVolumeId());
            } catch (Exception e) {
                logger.error("can not add: {} to storage pool: {}", volumeMetadata.getVolumeId(),
                        volumeMetadata.getStoragePoolId(), e);
                throw new StoragePoolNotExistedException_Thrift();
            }
            logger.info("Successfully saved volume: {}", volumeMetadata);
            if (root != null) { // if extending the volume, extend the volume
                // size
                long extendingSize = root.getExtendingSize();
                root.setExtendingSize(extendingSize + request.getVolumeSize());
                // if create a new EXTENDED volume, update root volume last extended time
                switch (volumeMetadata.getVolumeSource()) {
                case CREATE_VOLUME:
                    root.setLastExtendedTime(volumeMetadata.getVolumeCreatedTime());
                    root.setInAction(EXTENDING);
                    //                    volumeStore.updateStatusAndVolumeInAction(volumeMetadata.getVolumeId(),
                    //                            volumeMetadata.getVolumeStatus().name(), EXTENDING.name());
                    break;
                // if clone or move a volume, clean the root last extended time
                case CLONE_VOLUME:
                    root.setLastExtendedTime(null);
                    root.setInAction(CLONING);
                    break;
                case MOVE_VOLUME:
                    root.setLastExtendedTime(null);
                    root.setInAction(MOVING);
                    break;
                case MOVE_ONLINE_VOLUME:
                    root.setLastExtendedTime(null);
                    root.setInAction(MOVE_ONLINE_MOVING);
                    break;
                }
                root.setNeedToPersistVolumeLayout(true);
                root.incVersion();
                root.setPageWrappCount(pageWrappCount);
                root.setSegmentWrappCount(segmentWrappCount);
                volumeStore.saveVolume(root); // update root volume to db
                volumeStatusStore.addVolumeToStore(root);
            }
            CreateVolumeResponse response = new CreateVolumeResponse(request.getRequestId(), request.getVolumeId());
            logger.warn("CreateVolume response: {}", response);
            return response;
        } else {
            logger.error("no space for create volume:{}, volume size:{}, current free space is {}",
                    volumeMetadata.getName(), volumeMetadata.getVolumeSize(), freeSpace);
            throw new NotEnoughSpaceException_Thrift();
        }
    }

    /**
     * If the volume that is being requested is the root, then it and its all children will be returned. Otherwise, only
     * itself is returned.
     *
     * @throws ServiceIsNotAvailable_Thrift
     */
    @Override
    public GetVolumeResponse getVolume(GetVolumeRequest request)
            throws VolumeNotFoundException_Thrift, InvalidInputException_Thrift, ServiceHavingBeenShutdown_Thrift,
            ServiceIsNotAvailable_Thrift {

        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getVolume request: {}", request);
        if (!request.isSetEnablePagination()) {
            request.setEnablePagination(false);
        }
        long volumeId = request.getVolumeId();

        VolumeMetadata rootVolumeWithChildren;
        try {
            rootVolumeWithChildren = getRootVolumeWithChildren(volumeId, true, request.isEnablePagination(),
                    request.getStartSegmentIndex(), request.getPaginationNumber());
            logger.debug("after merge the volume is {}", rootVolumeWithChildren);
        } catch (VolumeNotFoundException e) {
            throw new VolumeNotFoundException_Thrift();
        }
        GetVolumeResponse response = new GetVolumeResponse();
        List<DriverMetadata_Thrift> driverMetadata_thrifts = new ArrayList<>();
        response.setDriverMetadatas(driverMetadata_thrifts);

        response.setRequestId(request.getRequestId());

        boolean notContainDeadVolume = (request.isSetContainDeadVolume() && !request.isContainDeadVolume());

        if (notContainDeadVolume) {
            if (rootVolumeWithChildren.getVolumeStatus().equals(VolumeStatus.Dead)) {
                // do nothing just return
                logger.warn("do not need to return dead volume:{}, response:{}", rootVolumeWithChildren, response);
                return response;
            }
        }

        boolean withSegmentList = !(request.isWithOutSegmentList());
        VolumeMetadata_Thrift volumeMetadataThrift = RequestResponseHelper
                .buildThriftVolumeFrom(rootVolumeWithChildren, withSegmentList);
        response.setVolumeMetadata(volumeMetadataThrift);

        List<DriverMetadata> volumeBindingDrivers = driverStore.get(volumeId);
        if (volumeBindingDrivers != null && volumeBindingDrivers.size() > 0) {
            for (DriverMetadata driverMetadata : volumeBindingDrivers) {
                response.addToDriverMetadatas(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
            }
            logger.warn("got drivers:{} with:{}", volumeBindingDrivers, volumeId);
        }
        int segmentCount = withSegmentList ? volumeMetadataThrift.getSegmentsMetadataSize() : 0;
        logger.warn("getVolume response:{}, volumeId:{}, with:{} segment count:{}, volume status:{}",
                response.getRequestId(), volumeId, withSegmentList, segmentCount,
                volumeMetadataThrift.getVolumeStatus());

        if (request.isEnablePagination()) {
            response.setLeftSegment(rootVolumeWithChildren.isLeftSegment());
            response.setNextStartSegmentIndex(rootVolumeWithChildren.getNextStartSegmentIndex());
        }

        return response;
    }

    /**
     * If the volume that is being requested is the root, then it and its all children will be returned. Otherwise, only
     * itself is returned.
     *
     * @throws ServiceIsNotAvailable_Thrift
     */
    @Override
    public GetVolumeCloneStatusMoveOnlineRequestResponse getMoveOnlineCloneStatus(GetVolumeCloneStatusMoveOnlineRequest request)
            throws VolumeNotFoundException_Thrift, InvalidInputException_Thrift, ServiceHavingBeenShutdown_Thrift,
            ServiceIsNotAvailable_Thrift {

        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getMoveOnlineCloneStatus request: {}", request);

        long volumeId = request.getVolumeId();
        GetVolumeCloneStatusMoveOnlineRequestResponse response = new GetVolumeCloneStatusMoveOnlineRequestResponse();
        VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);
        if (volumeMetadata.isCloneStatusWhenMoveOnline()){
            response.setCloneStatus(true);
            logger.warn("VolumeStable moveOnline, the new volume:{} clone ok ", volumeId);
        }

        response.setRequestId(request.getRequestId());
        return response;
    }

    /**
     * If the volume that is being requested is the root, then it and its all children will be returned. Otherwise, only
     * itself is returned.
     *
     * @throws ServiceIsNotAvailable_Thrift
     */
    @Override
    public GetSegmentMetadataStatusResponse getVolumeSegmentMetadataStatus(GetSegmentMetadataStatusRequest request)
            throws VolumeNotFoundException_Thrift, InvalidInputException_Thrift, ServiceHavingBeenShutdown_Thrift,
            ServiceIsNotAvailable_Thrift {

        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getVolumeSegmentMetadataStatus request: {}", request);
        long volumeId = request.getVolumeId();

        VolumeMetadata rootVolumeWithChildren;
        try {
            rootVolumeWithChildren = getRootVolumeWithChildren(volumeId, true);
            logger.debug("after merge the volume is {}", rootVolumeWithChildren);
        } catch (VolumeNotFoundException e) {
            throw new VolumeNotFoundException_Thrift();
        }

        Map<Integer, SegmentStatus_Thrift> segmentMetadataStatus = new ConcurrentHashMap<>();
        Map<Integer, SegmentMetadata> segmentMetadataMap = rootVolumeWithChildren.getSegmentTable();
        for (Map.Entry<Integer, SegmentMetadata> entry : segmentMetadataMap.entrySet()) {
            int segIndex = entry.getKey();
            SegmentMetadata segmentMetadata = entry.getValue();

            if (segmentMetadata.getSegmentStatus().available()) {
                segmentMetadataStatus.put(segIndex, SegmentStatus_Thrift.Available);
            } else {
                segmentMetadataStatus.put(segIndex, SegmentStatus_Thrift.UnAvailable);
            }
        }

        GetSegmentMetadataStatusResponse response = new GetSegmentMetadataStatusResponse();
        response.setSegmentMetadataStatus(segmentMetadataStatus);
        response.setRequestId(request.getRequestId());

        logger.warn("getVolumeSegmentMetadataStatus response status :{}, volumeId:{}",
                response.getSegmentMetadataStatus(), volumeId);
        return response;
    }

    /**
     * Get volume regardless root volume or child volume. The interface getVolume just return the root volume. This
     * interface is used in clone
     *
     * @param request
     * @return
     * @throws InternalError_Thrift
     * @throws InvalidInputException_Thrift
     * @throws VolumeNotFoundException_Thrift
     * @throws ServiceHavingBeenShutdown_Thrift
     * @throws ServiceIsNotAvailable_Thrift
     * @throws TException
     */
    @Override
    public GetVolumeResponse getVolumeRegardlessRootOrChildren(GetVolumeRequest request)
            throws InternalError_Thrift, InvalidInputException_Thrift, VolumeNotFoundException_Thrift,
            ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getVolumeRegardlessRootOrChildren request: {}", request);

        VolumeMetadata volume = volumeStore.getVolume(request.getVolumeId());

        if (volume == null) {
            throw new VolumeNotFoundException_Thrift();
        }

        GetVolumeResponse response = new GetVolumeResponse(request.getRequestId(), null);
        response.setVolumeMetadata(RequestResponseHelper.buildThriftVolumeFrom(volume, true));
        logger.warn("getVolumeRegardlessRootOrChildren response: {}", response);
        return response;
    }

    @Override
    public GetRootVolumeWithChildrenResponse getRootVolumeWithChildren(GetRootVolumeWithChildrenRequest request)
            throws InternalError_Thrift, InvalidInputException_Thrift, VolumeNotFoundException_Thrift,
            ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getRootVolumeWithChildren request: {}", request);
        long volumeId = request.getVolumeId();

        List<VolumeMetadata> volumeMetadatas = volumeStore.listVolumesFromRoot(volumeId);
        if (volumeMetadatas == null || volumeMetadatas.size() == 0) {
            throw new VolumeNotFoundException_Thrift();
        }

        List<VolumeMetadata_Thrift> volumes = new ArrayList<>();
        for (VolumeMetadata volume : volumeMetadatas) {
            volumes.add(RequestResponseHelper.buildThriftVolumeFrom(volume, false));
        }
        GetRootVolumeWithChildrenResponse response = new GetRootVolumeWithChildrenResponse(request.getRequestId(),
                volumes);
        logger.warn("getRootVolumeWithChildren response: {}", response);
        return response;
    }

    @Override
    public GetSegmentResponse getSegment(GetSegmentRequest request)
            throws InternalError_Thrift, VolumeNotFoundException_Thrift, ServiceHavingBeenShutdown_Thrift,
            ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getSegment request: {}", request);
        long volumeId = request.getVolumeId();

        VolumeMetadata rootVolumeWithChildren;
        try {
            rootVolumeWithChildren = getRootVolumeWithChildren(volumeId, true);
        } catch (VolumeNotFoundException e) {
            throw new VolumeNotFoundException_Thrift();
        }

        GetSegmentResponse response = new GetSegmentResponse();
        response.setSegment(RequestResponseHelper
                .buildThriftSegmentMetadataFrom(rootVolumeWithChildren.getSegmentByIndex(request.getSegmentIndex()),
                        false));
        response.setRequestId(request.getRequestId());
        response.setStoragePoolId(rootVolumeWithChildren.getStoragePoolId());
        logger.warn("getSegment response: {}", response);
        return response;
    }

    @Override
    public GetSegmentListResponse getSegmentList(GetSegmentListRequest request)
            throws InternalError_Thrift, InvalidInputException_Thrift, NotEnoughSpaceException_Thrift,
            VolumeNotFoundException_Thrift, ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getSegmentList request: {}", request);
        long volumeId = request.getVolumeId();

        VolumeMetadata rootVolumeWithChildren;
        try {
            rootVolumeWithChildren = getRootVolumeWithChildren(volumeId, true);
        } catch (VolumeNotFoundException e) {
            throw new VolumeNotFoundException_Thrift();
        }

        GetSegmentListResponse response = new GetSegmentListResponse();
        response.setSegments(RequestResponseHelper
                .buildThriftSegmentListFrom(rootVolumeWithChildren, request.getStartSegmentIndex(),
                        request.getEndSegmentIndex()));

        response.setRequestId(request.getRequestId());
        logger.warn("getSegmentList response: {}", response);

        return response;
    }

    private VolumeMetadata getRootVolumeWithChildren(Long volumeId, boolean needAddSegments)
            throws VolumeNotFoundException {
        return getRootVolumeWithChildren(volumeId, needAddSegments, false, 0, 0);
    }

    /**
     * Get root volume include all children belong to itself
     *
     * @param volumeId
     * @param needAddSegments : does need add segment to the root volume
     * @return
     */
    private  VolumeMetadata getRootVolumeWithChildren(Long volumeId, boolean needAddSegments, boolean enablePagination,
            int startSegmentIndex, int paginationNumber) throws VolumeNotFoundException {
        VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);

        if (volumeMetadata == null) {
            logger.warn("Can't find volume metadata given its id {}", volumeId);
            throw new VolumeNotFoundException();
        }

        if (!volumeMetadata.isRoot()) {
            logger.warn("{} is not root volume. throw VolumeNotFoundException", volumeMetadata.getVolumeId());
            throw new VolumeNotFoundException();
        }

        // get the list of volume metadata first
        // make sure root volume, child volume, child volume in order
        List<VolumeMetadata> volumeMetadatas = volumeStore.listVolumesFromRoot(volumeId);
        if (volumeMetadatas.size() == 0) {
            logger.error("can not get volumeMetadata from volumeId: {}", volumeId);
            throw new VolumeNotFoundException();
        }

        if (volumeMetadatas.get(0).getVolumeId() != volumeId) {
            logger.error("The first volume {} has different volume id {}", volumeMetadatas.get(0), volumeId);
            throw new VolumeNotFoundException();
        }

        // iterate all segment unit's and change their statuses accordingly
        VolumeMetadata virtualVolumeMetadata = mergeVolumes(volumeMetadatas, needAddSegments, enablePagination,
                startSegmentIndex, paginationNumber);
        return virtualVolumeMetadata;
    }

    private synchronized VolumeMetadata mergeVolumes(List<VolumeMetadata> volumeMetadatas) {
        return mergeVolumes(volumeMetadatas, true);
    }

    private synchronized VolumeMetadata mergeVolumes(List<VolumeMetadata> volumeMetadatas, boolean addSegments) {
        return mergeVolumes(volumeMetadatas, addSegments, false, 0, 0);
    }

    public synchronized static VolumeMetadata mergeVolumes(List<VolumeMetadata> volumeMetadatas, boolean addSegments,
            boolean enablePagination, int startSegmentIndex, int paginationNumber) {
        // Set the basic information of volume meta data
        VolumeMetadata virtualVolume = new VolumeMetadata();
        VolumeMetadata root = volumeMetadatas.get(0);
        virtualVolume.setVolumeId(root.getVolumeId());
        virtualVolume.setRootVolumeId(root.getRootVolumeId());
        virtualVolume.setAccountId(root.getAccountId());
        virtualVolume.setChildVolumeId(null);
        virtualVolume.setName(root.getName());
        virtualVolume.setVolumeType(root.getVolumeType());
        virtualVolume.setCacheType(root.getCacheType());
        virtualVolume.setSegmentSize(root.getSegmentSize());
        virtualVolume.setDeadTime(root.getDeadTime());
        virtualVolume.setExtendingSize(root.getExtendingSize());
        virtualVolume.setDomainId(root.getDomainId());
        virtualVolume.setSimpleConfiguration(root.isSimpleConfiguration());
        virtualVolume.setSegmentNumToCreateEachTime(root.getSegmentNumToCreateEachTime());
        virtualVolume.setSnapshotManager(root.getSnapshotManager());
        virtualVolume.setStoragePoolId(root.getStoragePoolId());
        virtualVolume.setVolumeCreatedTime(root.getVolumeCreatedTime());
        virtualVolume.setLastExtendedTime(root.getLastExtendedTime());
        virtualVolume.setVolumeSource(root.getVolumeSource());
        virtualVolume.setReadWrite(root.getReadWrite());
        virtualVolume.setPageWrappCount(root.getPageWrappCount());
        virtualVolume.setSegmentWrappCount(root.getSegmentWrappCount());
        virtualVolume.setInAction(root.getInAction());
        virtualVolume.setEnableLaunchMultiDrivers(root.isEnableLaunchMultiDrivers());

        // merge all volume and calculate total free space ratio
        double totalFreeSpaceRatio = 0.0;
        double totalFreeSpace = 0.0;
        double totalSpace = 0.0;
        int logicSegIndex = 0;
        long volumeTotalPageToMigrate = 0;
        long volumeAlreadyMigratedPage = 0;
        long volumeMigrationSpeed = 0;

        // this is very important variable, please pay attention on currentSegmentIndex before try to modify pagination code
        AtomicInteger currentSegmentIndex = new AtomicInteger(0);

        for (VolumeMetadata volume : volumeMetadatas) {
            /*
             * if there is no segment unit in one segment, volume won't get real segId
             */
            int segmentCount = volume.getSegmentCount();

            /*
            Validate.isTrue(segmentCount < CARRY_MAX_SEGMENT_COUNT_PER_REQUEST,
                    "volume:" + volume.getVolumeId() + " can not have:" + segmentCount + "segments");
            if (segmentCount > CARRY_MAX_SEGMENT_COUNT_PER_REQUEST) {
                logger.error("we cut segment count to: {} here to avoid error occurs",
                        CARRY_MAX_SEGMENT_COUNT_PER_REQUEST);
                segmentCount = CARRY_MAX_SEGMENT_COUNT_PER_REQUEST;
            } */

            for (int i = 0; i < segmentCount; i++) {
                SegId segId = new SegId(volume.getVolumeId(), i);
                virtualVolume.recordLogicSegIndexAndSegId(logicSegIndex++, segId);
            }

            List<SegmentMetadata> segmentMetadatas = volume.getSegments();

            // calculate the total volume ratio
            double volumeFreeSpaceRatio;
            double volumeAllSegFreeSpaceRatio = 0.0;
            for (SegmentMetadata segmentMetadata : segmentMetadatas) {
                volumeAllSegFreeSpaceRatio += segmentMetadata.getFreeRatio();
                for (SegmentUnitMetadata segmentUnitMetadata : segmentMetadata.getSegmentUnits()) {
                    volumeTotalPageToMigrate += segmentUnitMetadata.getTotalPageToMigrate();
                    volumeAlreadyMigratedPage += segmentUnitMetadata.getAlreadyMigratedPage();
                    volumeMigrationSpeed += segmentUnitMetadata.getMigrationSpeed();
                }
            }
            volumeFreeSpaceRatio = volumeAllSegFreeSpaceRatio / segmentMetadatas.size();

            totalFreeSpace += volume.getSegments().size() * volumeFreeSpaceRatio;
            totalSpace += volume.getSegments().size();
        }
        if (totalSpace != 0) {
            totalFreeSpaceRatio = totalFreeSpace / totalSpace;
        }
        virtualVolume.setFreeSpaceRatio(totalFreeSpaceRatio);
        virtualVolume.setTotalPageToMigrate(volumeTotalPageToMigrate);
        virtualVolume.setAlreadyMigratedPage(volumeAlreadyMigratedPage);
        virtualVolume.setMigrationSpeed(volumeMigrationSpeed / 2);
        virtualVolume.setMigrationRatio(
                0 == volumeTotalPageToMigrate ? 100 : (volumeAlreadyMigratedPage * 100) / volumeTotalPageToMigrate);

        // Merge all segment and calculate total size
        long totalSize = 0;
        int currentSegmentCount = 0;
        for (VolumeMetadata volume : volumeMetadatas) {
            totalSize += volume.getVolumeSize();
            currentSegmentCount += volume.getSegmentCount();

            if (addSegments) {
                if (enablePagination) {
                    // if current volume can not reach start segment index, just skip it, and inc current segment index.
                    if (startSegmentIndex >= currentSegmentCount) {
                        currentSegmentIndex.addAndGet(volume.getSegmentCount());
                        continue;
                    }
                }
                addSegmentsToDestVolume(volume, virtualVolume, currentSegmentIndex, enablePagination, startSegmentIndex,
                        paginationNumber);
            }
        }

        long totalSegmentCount = totalSize / virtualVolume.getSegmentSize();

        if (totalSegmentCount > startSegmentIndex + paginationNumber) {
            virtualVolume.setLeftSegment(true);
            virtualVolume.setNextStartSegmentIndex(currentSegmentIndex.get());
        } else {
            virtualVolume.setLeftSegment(false);
        }

        virtualVolume.setVolumeSize(totalSize);

        // set the volume layout
        virtualVolume.initVolumeLayout();
        if (root.getSegmentSize() > 0 && root.getVolumeSize() > 0 && root.isSimpleConfiguration()) {
            logger.warn("now to update volume layout from {} to {}", 0, root.getSegmentCount());
            for (int index = 0; index < root.getSegmentCount(); index++) {
                virtualVolume.updateVolumeLayout(index, root.ifSegmentNeedsToBeHere(index));
            }
        }

        if (volumeMetadatas.size() == 1) { // if only one volume, set status
            // directly from first volume
            virtualVolume.setVolumeStatus(volumeMetadatas.get(0).getVolumeStatus());
        } else {
            // Set the status according to all volumes
            virtualVolume.setVolumeStatus(getVirtualVolumeStatus(volumeMetadatas));
        }

        logger.info("Current virtual volume: {}, current status: {}, segment count: {}", virtualVolume.getVolumeId(),
                virtualVolume.getVolumeStatus(), virtualVolume.getSegmentCount());
        return virtualVolume;
    }

    /**
     * @param volumeMetadatas Get the Virtual Volume status according to root volume and child volumes
     * @author david
     */
    private static VolumeStatus getVirtualVolumeStatus(List<VolumeMetadata> volumeMetadatas) {
        int numUnavailable = 0;
        int numDeleting = 0;
        int numDead = 0;
        int numDeleted = 0;

        for (VolumeMetadata volume : volumeMetadatas) {
            switch (volume.getVolumeStatus()) {
            case Dead:
                numDead++;
                break;
            case ToBeCreated:
            case Creating:
            case Unavailable:
                numUnavailable++;
                break;
            case Deleting:
                numDeleting++;
                break;
            case Deleted:
                numDeleted++;
                break;
            default:
                break;
            }
        }

        if (numDeleting > 0) {
            return VolumeStatus.Deleting;
        } else if (numDeleted > 0) {
            return VolumeStatus.Deleted;
        } else if (numUnavailable > 0) {
            return VolumeStatus.Unavailable;
        } else if (numDead > 0) {
            if (numDead == volumeMetadatas.size()) {
                return VolumeStatus.Dead;
            } else {
                return VolumeStatus.Deleted;
            }
        }
        return VolumeStatus.Available;
    }

    /**
     * Add segments of child and root volume to a virtual volume. the index of segments will be changed according to
     * their position of first segment in root volume
     *
     * @param srcVolume
     * @param dstVolume
     */
    private static void  addSegmentsToDestVolume(VolumeMetadata srcVolume, VolumeMetadata dstVolume,
            AtomicInteger currentSegmentIndex, boolean enablePagination, int startSegmentIndex, int paginationNumber) {
        // fill all segment metadata to the virtual volume
        for (SegmentMetadata segMetadata : srcVolume.getSegments()) {

            boolean addSegmentToDestVolume = false;

            if (!enablePagination) {
                addSegmentToDestVolume = true;
            } else {
                if (currentSegmentIndex.get() < startSegmentIndex) {
                    // still not reach segment start index, do nothing but inc current segment index
                } else if (currentSegmentIndex.get() >= startSegmentIndex && currentSegmentIndex.get() < (
                        startSegmentIndex + paginationNumber)) {
                    // current segment index in range[target startSegmentIndex, target start segment index + pagination number)
                    addSegmentToDestVolume = true;
                } else if (currentSegmentIndex.get() >= (startSegmentIndex + paginationNumber)) {
                    // if current segment index lager than startSegmentIndex + pagination number, means that all segments after won't be add to virtual volume
                    break;
                }

            }

            currentSegmentIndex.incrementAndGet();

            if (segMetadata.getSegmentUnitCount() != 0) { // for each segment

                if (addSegmentToDestVolume) {
                    int newSegmentId = segMetadata.getIndex() + srcVolume.getPositionOfFirstSegmentInLogicVolume();
                    SegmentMetadata segToBeAdded = new SegmentMetadata(new SegId(segMetadata.getSegId()), newSegmentId);
                    SegmentMembership latestMembershipInSegment = segMetadata.getLatestMembership();

                    // Add every segment unit in original segment to segToBeAdded
                    for (Map.Entry<InstanceId, SegmentUnitMetadata> entry : segMetadata.getSegmentUnitMetadataTable()
                            .entrySet()) {
                        SegmentUnitMetadata srcSegUnitMetadata = entry.getValue();
                        SegmentUnitMetadata segUnitMetadata = new SegmentUnitMetadata(srcSegUnitMetadata);
                        segUnitMetadata.setLastReported(System.currentTimeMillis());
                        segUnitMetadata.setSnapshotCapacityMap(srcSegUnitMetadata.getSnapshotCapacityMap());
                        segUnitMetadata.setSnapshotTotalCapacity(new AtomicLong(srcSegUnitMetadata.getSnapshotTotalCapacity()));
                        segToBeAdded.putSegmentUnitMetadata(segUnitMetadata.getInstanceId(), segUnitMetadata);
                    }

                    // before add to the dstVolume, exclude the segment units whose
                    // epoch and generation different the
                    // latest membership
                    excludeSegmentUnitVersionNotSameWithLastest(segToBeAdded);
                    // should enable add segment order
                    dstVolume.addSegmentMetadata(segToBeAdded, latestMembershipInSegment, true);
                }
            } else {
                logger.info("this segment:{} has no segment unit", segMetadata.getSegId());
            }

        }
    }

    /**
     * exclude the segment units whose epoch and generation are not same with lastest membership;
     *
     * @param segment
     */
    private static void excludeSegmentUnitVersionNotSameWithLastest(SegmentMetadata segment) {
        Collection<SegmentUnitMetadata> latestSegUnits = segment.chooseLatestSegUnits();
        Map<InstanceId, SegmentUnitMetadata> newSegmentUnitTable = new ConcurrentHashMap<InstanceId, SegmentUnitMetadata>();
        for (SegmentUnitMetadata segmentUnitMetadata : latestSegUnits) {
            newSegmentUnitTable.put(segmentUnitMetadata.getInstanceId(), segmentUnitMetadata);
        }
        segment.setSegmentUnitMetadataTable(newSegmentUnitTable);
    }

    public void setVolumeStore(VolumeStore volumeStore) {
        this.volumeStore = volumeStore;
    }

    public void setInstanceStore(InstanceStore instanceStore) {
        this.instanceStore = instanceStore;
    }

    public void setDriverContainerSelectionStrategy(DriverContainerSelectionStrategy driverContainerSelectionStrategy) {
        this.driverContainerSelectionStrategy = driverContainerSelectionStrategy;
    }

    public DriverStore getDriverStore() {
        return driverStore;
    }

    public void setDriverStore(DriverStore driverStore) {
        this.driverStore = driverStore;
    }

    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    @Override
    public ListVolumesResponse listVolumes(ListVolumesRequest request)
            throws InvalidInputException_Thrift, ServiceHavingBeenShutdown_Thrift, VolumeNotFoundException_Thrift,
            ServiceIsNotAvailable_Thrift {
        if (shutDownFlag) {
            logger.error("list volumes with request: {}, shutDownFlag: {}", request, shutDownFlag);
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("listVolumes request: {}", request);

        ListVolumesResponse response = new ListVolumesResponse();
        List<VolumeMetadata_Thrift> volumesThrift = new ArrayList<VolumeMetadata_Thrift>();

        Set<Long> volumesCanBeList = null;
        if (request.isSetVolumesCanBeList())
            volumesCanBeList = request.getVolumesCanBeList();

        List<VolumeMetadata> rootVolumes = volumeStore.listRootVolumes();
        logger.debug("All root volumes are {}", rootVolumes);
        boolean notContainDeadVolume = (request.isSetContainDeadVolume() && !request.isContainDeadVolume());
        // TODO: volume meta data now has only tag, so use query[0] to query
        // volumes
        for (VolumeMetadata volumeMetadata : rootVolumes) {
            if (volumesCanBeList != null && !volumesCanBeList.contains(volumeMetadata.getVolumeId())) {
                continue;
            }
            long volumeId = volumeMetadata.getVolumeId();
            // volume is a root and its tag key-value matches query's
            // check volume status and change it accordingly
            List<VolumeMetadata> volumeMetadataList = volumeStore.listVolumesFromRoot(volumeId);
            if (volumeMetadataList.size() == 0) {
                logger.warn("can not get volumeMetadata from volumeId:{}, just build this volume", volumeId);
                volumesThrift.add(RequestResponseHelper.buildThriftVolumeFrom(volumeMetadata, false));
            } else {
                if (volumeMetadataList.get(0).getVolumeId() != volumeId) {
                    logger.error("The first volume {} has different volume id {}", volumeMetadataList.get(0), volumeId);
                    continue;
                }

                List<VolumeMetadata> volumesWithSameRoot = volumeStore.listVolumesWithSameRoot(volumeId);
                boolean isChildNotFinishCreating = false;
                VolumeMetadata.VolumeInAction childInAction = volumeMetadata.getInAction();
                for (VolumeMetadata volume : volumesWithSameRoot) {
                    if (volume.getVolumeStatus().equals(VolumeStatus.ToBeCreated) || volume.getVolumeStatus()
                            .equals(VolumeStatus.Creating)) {
                        isChildNotFinishCreating = true;
                        childInAction = volume.getInAction();
                        break;
                    }
                }
                if (isChildNotFinishCreating) {
                    if (!volumeMetadata.getInAction().equals(EXTENDING)) {
                        volumeMetadata.setInAction(childInAction);
                    }
                } else {
                    if (CLONING.equals(volumeMetadata.getInAction()) || MOVING.equals(volumeMetadata.getInAction())) {
                        volumeMetadata.setInAction(NULL);
                    }
                }

                // merge all volumes to a virtual volume meta data without
                // adding segment units
                logger.debug("Current volume metadata is {}", volumeMetadata);
                VolumeMetadata virtualVolumeMetadata = mergeVolumes(volumeMetadataList, false);
                // if volume status is dead, no need to display at console
                if (notContainDeadVolume) {
                    if (virtualVolumeMetadata.getVolumeStatus().equals(VolumeStatus.Dead)) {
                        logger.debug("volume is in dead status, volume id is {}, name is {}",
                                virtualVolumeMetadata.getVolumeId(), virtualVolumeMetadata.getName());
                        continue;
                    }
                }

                try {
                    VolumeMetadata_Thrift volumeMetadata_thrift = RequestResponseHelper
                            .buildThriftVolumeFrom(virtualVolumeMetadata, false);
                    volumesThrift.add(volumeMetadata_thrift);
                } catch (Exception e) {
                    logger.error("failed to build volume metadata thrift from virtualVolumeMetadata: {}",
                            virtualVolumeMetadata, e);
                }
            }
        }

        response.setRequestId(request.getRequestId());
        response.setVolumes(volumesThrift);
        logger.warn("listVolumes response: {}", response);
        return response;
    }

    @Override
    public ListAllSnapshotsResponse listAllSnapshots(ListAllSnapshotsRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, ParametersIsErrorException_Thrift,
            TException {
        if (shutDownFlag) {
            logger.error("list snapshot information with request: {}, shutDownFlag: {}", request, shutDownFlag);
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("listAllSnapshots request: {}", request);
        ListAllSnapshotsResponse response = new ListAllSnapshotsResponse();
        response.setRequestId(request.getRequestId());
        Map<VolumeMetadata_Thrift, ByteBuffer> volumeId2SnapshotManagerBinary = new HashMap<>();
        response.setVolume2SnapshotManagerBinary(volumeId2SnapshotManagerBinary);
        Map<Long, Map<Integer, Long>> volumeId2SnapshotId2Capacity = new HashMap<>();
        response.setVolumeId2SnapshotId2Capacity(volumeId2SnapshotId2Capacity);
        Map<Long, Long> volumeId2TotalCapacity = new HashMap<>();
        response.setVolumeId2TotalCapacity(volumeId2TotalCapacity);

        for (Long volumeId : request.getVolumeIds()) {
            volumeId2TotalCapacity.put(volumeId, 0L);

            List<VolumeMetadata> volumeMetadataList = volumeStore.listVolumesFromRoot(volumeId);
            if (volumeMetadataList.size() == 0) {
                logger.warn("listAllSnapshots can not get volumeMetadata from volumeId:{}, just build this volume", volumeId);
                continue;
            } else {
                if (volumeMetadataList.get(0).getVolumeId() != volumeId) {
                    logger.error("listAllSnapshots The first volume {} has different volume id {}", volumeMetadataList.get(0), volumeId);
                    continue;
                }
            }
            VolumeMetadata volumeMetadata = mergeVolumes(volumeMetadataList, true);

//            VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);
            if (volumeMetadata == null) {
                logger.warn("can not find volume by id:{}", volumeId);
                continue;
            }

            volumeId2SnapshotManagerBinary.put(RequestResponseHelper.buildThriftVolumeFrom(volumeMetadata, false),
                    ByteBuffer.wrap(volumeMetadata.getSnapshotManager().toByteArray()));
            Map<Integer, Long> snapshotId2Capacity = volumeId2SnapshotId2Capacity.get(volumeId);
            if (snapshotId2Capacity == null) {
                snapshotId2Capacity = new HashMap<>();
                volumeId2SnapshotId2Capacity.put(volumeId, snapshotId2Capacity);
            }
            for (Integer snapshotId : volumeMetadata.getSnapshotManager().listSnapshotIds()) {
                snapshotId2Capacity.put(snapshotId, 0L);
            }
            for (SegmentMetadata segmentMetadata : volumeMetadata.getSegments()) {

                long segTotal = 0;
                long segUnitCountInOneSegment = 0;
                Map<Integer, Long> snapshotId2CapacityInOneSegment = new HashMap<>();
                for (SegmentUnitMetadata segmentUnitMetadata : segmentMetadata.getSegmentUnits()) {
                    for (Map.Entry<Integer, AtomicLong> snapshotId2CapacityEntry : segmentUnitMetadata
                            .getSnapshotCapacityMap().entrySet()) {

                        Integer snapshotId = snapshotId2CapacityEntry.getKey();
                        Long capacityOfOneSegmentUnitOfOneSnapshot = snapshotId2CapacityEntry.getValue().get();
                        Long capacityInOneSegmentOfOneSnapshot = snapshotId2CapacityInOneSegment.get(snapshotId);
                        if (capacityInOneSegmentOfOneSnapshot == null) {
                            capacityInOneSegmentOfOneSnapshot = 0L;
                        }
                        capacityInOneSegmentOfOneSnapshot += capacityOfOneSegmentUnitOfOneSnapshot;
                        snapshotId2CapacityInOneSegment.put(snapshotId, capacityInOneSegmentOfOneSnapshot);
                    } // for every snapshots in one segment unit

                    if (!segmentUnitMetadata.isArbiter()) {
                        segTotal += segmentUnitMetadata.getSnapshotTotalCapacity();
                        segUnitCountInOneSegment++;
                    }

                } // for every segment units in one segment

                if (segUnitCountInOneSegment != 0) {
                    for (Map.Entry<Integer, Long> entry : snapshotId2CapacityInOneSegment.entrySet()) {
                        Integer snapshotId = entry.getKey();
                        Long capacityInOneSegmentOfOneSnapshot = entry.getValue();

                        // cause volume has delete snapshot id, but segment unit didn't delete yet
                        if (!snapshotId2Capacity.containsKey(snapshotId)) {
                            snapshotId2Capacity.put(snapshotId, 0L);
                        }

                        Long capacityTotalOfOneSnapshot =
                                snapshotId2Capacity.get(snapshotId) + (capacityInOneSegmentOfOneSnapshot
                                        / segUnitCountInOneSegment);
                        logger.debug("snapshotId: {}, snapshotCapacityInOneSegment: {}, segUnitCountInOneSegment: {}",
                                snapshotId, capacityInOneSegmentOfOneSnapshot, segUnitCountInOneSegment);

                        snapshotId2Capacity.put(snapshotId, capacityTotalOfOneSnapshot);

                    }

                    logger.debug("segId: {}, segTotal: {}, segUnitCountInOneSegment: {}", segmentMetadata.getSegId(),
                            segTotal, segUnitCountInOneSegment);
                    Long segTotalInOneVolume =
                            volumeId2TotalCapacity.get(volumeId) + (segTotal / segUnitCountInOneSegment);
                    volumeId2TotalCapacity.put(volumeId, segTotalInOneVolume);
                }

            } // for every segments in one volume

        } // for each volume
        logger.warn("listAllSnapshots response: {}", response);
        return response;
    }

    /**
     * The method use to get different drivers by different request from driverStore;
     *
     * @param request
     * @return
     * @throws ServiceIsNotAvailable_Thrift
     * @throws ParametersIsErrorException_Thrift
     * @throws TException
     */
    @Override
    public ListAllDriversResponse listAllDrivers(ListAllDriversRequest request)
            throws ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            logger.error("list all drivers  with request: {}, shutDownFlag: {}", request, shutDownFlag);
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("listAllDrivers request: {}", request);
        ListAllDriversResponse response = new ListAllDriversResponse();
        response.setRequestId(request.getRequestId());

        List<DriverMetadata> driverMetadatas = driverStore.list();

        if (request.isSetVolumeId()) {
            driverMetadatas = getDriversByVolumeId(driverMetadatas, request.getVolumeId());
        }
        if (request.isSetSnapshotId()) {
            driverMetadatas = getDriversBySnapshotId(driverMetadatas, request.getSnapshotId());
        }

        if (request.isSetDrivercontainerHost()) {
            driverMetadatas = getDriversByDcHost(driverMetadatas, request.getDrivercontainerHost());
        }

        if (request.isSetDriverHost()) {
            driverMetadatas = getDriversByDriverHost(driverMetadatas, request.getDriverHost());
        }
        if (request.isSetDriverType()) {
            driverMetadatas = getDriversByDriverType(driverMetadatas,
                    DriverType.valueOf(request.getDriverType().name()));
        }

        List<DriverMetadata_Thrift> driverMetadata_thrifts = new ArrayList<>();
        for (DriverMetadata driverMetadata : driverMetadatas) {
            driverMetadata_thrifts.add(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
        }
        response.setDriverMetadatas_thrift(driverMetadata_thrifts);
        return response;
    }

    public List<DriverMetadata> getDriversByVolumeId(List<DriverMetadata> drivers, long volumeId) {
        List<DriverMetadata> driverMetadatas = new ArrayList<>();

        for (DriverMetadata driverMetadata : drivers) {
            if (driverMetadata.getVolumeId() == volumeId) {
                driverMetadatas.add(driverMetadata);
            }
        }
        return driverMetadatas;
    }

    public List<DriverMetadata> getDriversBySnapshotId(List<DriverMetadata> drivers, int snapshotId) {
        List<DriverMetadata> driverMetadatas = new ArrayList<>();

        for (DriverMetadata driverMetadata : drivers) {
            if (driverMetadata.getSnapshotId() == snapshotId) {
                driverMetadatas.add(driverMetadata);
            }
        }
        return driverMetadatas;
    }

    public List<DriverMetadata> getDriversByDcHost(List<DriverMetadata> drivers, String drivercontainerHost) {
        List<DriverMetadata> driverMetadatas = new ArrayList<>();

        for (DriverMetadata driverMetadata : drivers) {
            if (driverMetadata.getQueryServerIp().equals(drivercontainerHost)) {
                driverMetadatas.add(driverMetadata);
            }
        }
        return driverMetadatas;
    }

    public List<DriverMetadata> getDriversByDriverHost(List<DriverMetadata> drivers, String driverHost) {
        List<DriverMetadata> driverMetadatas = new ArrayList<>();

        for (DriverMetadata driverMetadata : drivers) {
            if (driverMetadata.getHostName().equals(driverHost)) {
                driverMetadatas.add(driverMetadata);
            }
        }
        return driverMetadatas;
    }

    public List<DriverMetadata> getDriversByDriverType(List<DriverMetadata> drivers, DriverType driverType) {
        List<DriverMetadata> driverMetadatas = new ArrayList<>();

        for (DriverMetadata driverMetadata : drivers) {
            if (driverMetadata.getDriverType().equals(driverType)) {
                driverMetadatas.add(driverMetadata);
            }
        }

        return driverMetadatas;
    }

    public VolumeInfoToSnapshotInfo buildVolumeInfoToSnapshotInfo(VolumeMetadata volumeMetadata,
            List<SnapshotMetadata> snapshotMetadataList) {
        VolumeInfoToSnapshotInfo volumeInfoToSnapshotInfo = new VolumeInfoToSnapshotInfo();
        volumeInfoToSnapshotInfo.setRootVolumeId(volumeMetadata.getRootVolumeId());
        volumeInfoToSnapshotInfo.setRootVolumeName(volumeMetadata.getName());
        volumeInfoToSnapshotInfo.setDomainId(volumeMetadata.getDomainId());
        volumeInfoToSnapshotInfo.setStoragePoolId(volumeMetadata.getStoragePoolId());
        volumeInfoToSnapshotInfo.setSnapshotMetadataList(snapshotMetadataList);
        return volumeInfoToSnapshotInfo;
    }

    //when user login loadVolume first
    @Override
    public LoadVolumeResponse loadVolume(LoadVolumeRequest request) throws LoadVolumeException_Thrift {
        logger.debug("loadVolume request {}", request);
        try {
            volumeStore.loadVolumeInDb();
            return new LoadVolumeResponse();
        } catch (Exception e) {
            throw new LoadVolumeException_Thrift();
        }
    }

    /**
     * update driver information
     * <p>
     * (non-Javadoc)
     *
     * @see py.thrift.infocenter.service.InformationCenter.Iface#reportDriverMetadata
     * (py.thrift.infocenter.service.ReportDriverMetadataRequest)
     */
    @Override
    public ReportDriverMetadataResponse reportDriverMetadata(ReportDriverMetadataRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("reportDriverMetadata request: {}", request);
        try {
            synchronized (driverStore) {
                final long currentTime = System.currentTimeMillis();
                List<DriverMetadata> oldDriversInOneContainerNeedDelete = driverStore
                        .getByDriverContainerId(request.getDrivercontainerId());

                for (DriverMetadata_Thrift driverMetadataThrift : request.getDrivers()) {
                    DriverMetadata reportingDriverMetadata = RequestResponseHelper
                            .buildDriverMetadataFrom(driverMetadataThrift);
                    DriverMetadata driverInStore = driverStore
                            .get(reportingDriverMetadata.getDriverContainerId(), reportingDriverMetadata.getVolumeId(),
                                    reportingDriverMetadata.getDriverType(), reportingDriverMetadata.getSnapshotId());
                    if (driverInStore == null && reportingDriverMetadata.getDriverStatus()
                            .equals(DriverStatus.REMOVING)) {
                        logger.warn(
                                "reporting driver is removing and has been already released, no need to store it back.");
                        continue;
                    }
                    // a driver report from driver container, save it to store
                    reportingDriverMetadata.setDriverStatus(reportingDriverMetadata.getDriverStatus()
                            .turnToNextStatusOnEvent(DriverStateEvent.NEWREPORT));
                    reportingDriverMetadata.setLastReportTime(currentTime);
                    if (driverInStore != null) {
                        reportingDriverMetadata.setDynamicIOLimitationId(driverInStore.getDynamicIOLimitationId());
                        reportingDriverMetadata.setStaticIOLimitationId(driverInStore.getStaticIOLimitationId());
                        oldDriversInOneContainerNeedDelete.remove(driverInStore);
                    }
                    String volumeName = volumeStore.getVolume(reportingDriverMetadata.getVolumeId()).getName();
                    reportingDriverMetadata.setVolumeName(volumeName);

                    driverStore.save(reportingDriverMetadata);
                }
                for (DriverMetadata driverToDelete : oldDriversInOneContainerNeedDelete) {
                    driverStore.delete(driverToDelete.getDriverContainerId(), driverToDelete.getVolumeId(),
                            driverToDelete.getDriverType(), driverToDelete.getSnapshotId());
                }
            }
        } catch (Exception e) {
            logger.error("caught an exception", e);
            throw e;
        }
        // a table of volume id to driver, which is used to help determine role of FS server and assistant

        ReportDriverMetadataResponse response = new ReportDriverMetadataResponse();
        response.setRequestId(request.getRequestId());
        logger.warn("reportDriverMetadata response: {}", response);
        return response;
    }

    /*
     * get driver container if request volume is not in state launching
     * 
     * (non-Javadoc)
     * 
     * @see py.thrift.infocenter.service.InformationCenter.Iface#getDriverContainer
     * (py.thrift.infocenter.service.GetDriverContainerRequest)
     */
    @Override
    public AllocDriverContainerResponse allocDriverContainer(AllocDriverContainerRequest request)
            throws TooManyDriversException_Thrift, VolumeNotFoundException_Thrift, VolumeBeingDeletedException_Thrift,
            ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("allocDriverContainer request: {}", request);

        // check volume does exists or in deleting, deleted or dead status
        VolumeMetadata volume = volumeStore.getVolume(request.getVolumeId());

        if (volume == null) {
            throw new VolumeNotFoundException_Thrift();
        }

        if (volume.isDeletedByUser()) {
            throw new VolumeBeingDeletedException_Thrift();
        }

        // prepare response for request getting driver container
        AllocDriverContainerResponse response = new AllocDriverContainerResponse();
        response.setRequestId(request.getRequestId());

        synchronized (driverStore) {
            // filter all driver container which is able to mount driver on
            // request volume
            Set<Instance> allDriverContainers = instanceStore
                    .getAll(PyService.DRIVERCONTAINER.getServiceName(), InstanceStatus.OK);
            List<DriverMetadata> volumeBindingDriverList = driverStore.get(request.getVolumeId());
            logger.warn("get all driver containers:{}, volumeBindingDriverList:{}", allDriverContainers,
                    volumeBindingDriverList);
            Set<Instance> candidateDriverContainers = new HashSet<>();
            for (Instance instance : allDriverContainers) {
                candidateDriverContainers.add(instance);
            }

            // available driver container is less than expected driver amount;
            if (candidateDriverContainers.size() < request.getDriverAmount()) {
                logger.error(
                        "Only {} driver containers is able to launch driver for volume {}, less than expected {} driver containers",
                        candidateDriverContainers.size(), request.getVolumeId(), request.getDriverAmount());
                throw new TooManyDriversException_Thrift();
            }

            // balance sort all candidates according to amount of launched
            // drivers
            List<DriverMetadata> allDriverMetadatas = driverStore.list();
            List<DriverContainerCandidate> allCandidatesSortedInBalance = driverContainerSelectionStrategy
                    .select(candidateDriverContainers, allDriverMetadatas);

            logger.debug("return candidates from balance sorted all driver container instances {},size:{}",
                    allCandidatesSortedInBalance, allCandidatesSortedInBalance.size());

            // we should return all candidate drivercontainer service ,because one or more system which drivercontainer
            // service
            // belong to is out of memory,then control center should loop all candidate drivercontainer service.
            for (DriverContainerCandidate oneCandidateOfAll : allCandidatesSortedInBalance) {
                response.addToCandidates(RequestResponseHelper.buildThriftDriverContainerCandidata(oneCandidateOfAll));
            }
        }
        logger.warn("response driver containers {}, size: {}", response.getCandidates(), response.getCandidatesSize());
        logger.warn("allocDriverContainer response: {}", response);
        return response;
    }

    @Override
    public ReserveSegUnitsResponse reserveSegUnits(ReserveSegUnitsRequest request)
            throws NotEnoughSpaceException_Thrift, InternalError_Thrift, InvalidInputException_Thrift,
            ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("reserveSegUnits request: {}", request);

        //if arbiter group not enough, arbiter can create at normal datanode
//        if (isArbiterGroupSet && SegmentUnitType_Thrift.Arbiter == request.getSegmentUnitType()) {
//            String errorMsg = "Need specified group to create arbiter segment unit.";
//            ArbiterGroupNotFoundException_Thrift arbiterGroupNotFoundExceptionThrift = new ArbiterGroupNotFoundException_Thrift();
//            arbiterGroupNotFoundExceptionThrift.setDetail(errorMsg);
//            logger.error(errorMsg, arbiterGroupNotFoundExceptionThrift);
//            throw arbiterGroupNotFoundExceptionThrift;
//        }

        Set<Integer> excludedGroups = new HashSet<>();
        for (long instanceId : request.getExcludedInstanceIds()) {
            InstanceMetadata instance = storageStore.get(instanceId);
            Validate.notNull(instance, "Datanode with id " + instanceId + " still in membership, but cannot find it");
            Validate.isTrue(instance.getDatanodeStatus().equals(OK));
            Group excludedGroup = instance.getGroup();
            excludedGroups.add(excludedGroup.getGroupId());
        }

        /*
         * try to get volume information
         */
        VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());
        if (volumeMetadata == null) {
            logger.error("can't get volume: {} information", request.getVolumeId());
            throw new InternalError_Thrift();
        }

        Long domainId = volumeMetadata.getDomainId();
        Long storagePoolId = volumeMetadata.getStoragePoolId();

        Domain domain = null;
        try {
            domain = domainStore.getDomain(domainId);
        } catch (Exception e) {
            logger.warn("can not get domain", e);
        }
        if (domain == null) {
            logger.error("domain: {} is not exist", domainId);
            throw new InternalError_Thrift();
        }
        StoragePool storagePool = null;
        try {
            storagePool = storagePoolStore.getStoragePool(storagePoolId);
        } catch (Exception e) {
            logger.warn("can not get storage pool", e);
        }
        if (storagePool == null) {
            logger.error("storage pool: {} is not exist", storagePoolId);
            throw new InternalError_Thrift();
        }
        if (!domain.getStoragePools().contains(storagePool.getPoolId())) {
            logger.error("domain: {} does not contain storage pool:{}", domain, storagePool);
            throw new InternalError_Thrift();
        }

        List<InstanceMetadata> storageList = new ArrayList<>();
        // build datanode instance with storage pool id
        logger.warn("storage pool: {}", storagePool);
        for (Entry<Long, Collection<Long>> entry : storagePool.getArchivesInDataNode().asMap().entrySet()) {
            Long datanodeId = entry.getKey();
            Collection<Long> archiveIds = entry.getValue();
            InstanceMetadata datanode = storageStore.get(datanodeId);
            if (datanode == null) {
                logger.warn("can not get datanode by ID:{}", datanodeId);
                continue;
            }
            if (!datanode.getDatanodeStatus().equals(OK)) {
                logger.warn("datanode:{} is not OK status.", datanode);
                continue;
            }

            InstanceMetadata buildDatanode = RequestResponseHelper
                    .buildInstanceWithPartOfArchives(datanode, archiveIds);
            storageList.add(buildDatanode);
        }

        logger.warn("after select from storage pool, get datanodes:{}", storageList);

        /**
         * get data node information in pool
         */
        Map<Long, InstanceMetadata> instanceId2InstanceMap = new HashMap<>();    //all instance information in  pool
        Set<Long> normalIdSet = new HashSet<>();    //all normal data node in pool
        Set<Long> simpleIdSet = new HashSet<>();    //all simple data node in pool
        Set<Integer> simpleGroupIdSet = new HashSet<>();    //all simple group in pool
        Set<Integer> allGroupSet = new HashSet<>(); //all data node group

        //get instance Info in pool
        for (InstanceMetadata instance : storageList){
            if (instance.getDatanodeType() == InstanceMetadata.DatanodeType.SIMPLE){
                simpleIdSet.add(instance.getInstanceId().getId());
                simpleGroupIdSet.add(instance.getGroup().getGroupId());
            } else if (instance.getDatanodeType() == InstanceMetadata.DatanodeType.NORMAL){
                normalIdSet.add(instance.getInstanceId().getId());
            } else {
                logger.error("datanode type not set! instance:{}", instance.getInstanceId().getId());
                new DatanodeTypeNotSetException_Thrift();
            }
            instanceId2InstanceMap.put(instance.getInstanceId().getId(), instance);
            allGroupSet.add(instance.getGroup().getGroupId());
        }

        // get all simple data node
        Set<Long> allSimpleDatanodeIdSet = segmentUnitsDistributionManager.getSimpleDatanodeInstanceIdSet();

        //simpleDatanode may be have no archives, so it cannot add to pool
        //we will create arbiter at all simpleDatanode which be owned domain
        for (long simpleDatanodeInstanceId : allSimpleDatanodeIdSet) {
            InstanceMetadata instance = storageStore.get(simpleDatanodeInstanceId);

            if (instance.getDomainId() == domainId) {
                simpleIdSet.add(simpleDatanodeInstanceId);
                instanceId2InstanceMap.put(simpleDatanodeInstanceId, instance);
                simpleGroupIdSet.add(instance.getGroup().getGroupId());
                allGroupSet.add(instance.getGroup().getGroupId());
            }
        }

        /**
         * get the use information of segment unit
         */
        //normal segment unit counter of data node when wrapper index is same as current segment(used for overload)
        ObjectCounter<Long> normalOfInstanceInWrapperCounter = new TreeSetObjectCounter<>();
        //secondary combinations when primary is same as current segment primary
        ObjectCounter<Long> secondaryOfPrimaryCounter = new TreeSetObjectCounter<>();
        ObjectCounter<Long> primaryCounter = new TreeSetObjectCounter<>();      //primary combination
        ObjectCounter<Long> secondaryCounter = new TreeSetObjectCounter<>();    //secondary combination
        ObjectCounter<Long> arbiterCounter = new TreeSetObjectCounter<>();    //arbiter combination

        //volume segment traversal
        boolean isInRequestSegWrapper = false;
        Map<Integer, SegmentMetadata> segmentTable = volumeMetadata.getSegmentTable();
        //when infocenter just start, volumeMetadata is not null because it load from DB, but segmentTable may be null because it load after datanode report
        if (segmentTable == null){
            logger.error("Has no segment in volume:{}.",
                    volumeMetadata.getVolumeId());
            throw new SegmentNotFoundException_Thrift();
        } else if (segmentTable.size() != volumeMetadata.getSegmentCount()){
            logger.error("segmentTable size:{} not equal with volume segment count:{}",
                    segmentTable.size(), volumeMetadata.getSegmentCount());
            throw new SegmentNotFoundException_Thrift();
        }

        SegmentMetadata requestSegment = segmentTable.get((int)request.getSegIndex());
        if (requestSegment == null){
            logger.error("Segment:{} not found in volume:{}.",request.getSegIndex(), volumeMetadata.getVolumeId());
            throw new SegmentNotFoundException_Thrift();
        }

        // Get the primary id that the current reservation segment unit belongs to
        // then record the secondary combinations
        long requestPrimaryId = 0;
        for (Map.Entry<InstanceId, SegmentUnitMetadata> reqSegmentUnitEntry : requestSegment.getSegmentUnitMetadataTable().entrySet()){
            InstanceId reqInsId = reqSegmentUnitEntry.getKey();
            if (reqSegmentUnitEntry.getValue().getMembership().isPrimary(reqInsId)){
                requestPrimaryId = reqInsId.getId();
                break;
            }
        }

        for (Map.Entry<Integer, SegmentMetadata> segmentEntry : segmentTable.entrySet()){
            int segmentIndex = segmentEntry.getKey();
            SegmentMetadata segmentMetadata = segmentEntry.getValue();

            // calculate wrapper index
            // when segment's warpper index is equals with reservation segment unit warpper index，
            // we will record normal segment unit counter of data node to used for overload
            if (segmentIndex/segmentWrappCount == (int)request.segIndex/segmentWrappCount){
                isInRequestSegWrapper = true;
            }

            //segment unit traversal
            boolean isRequestPrimary = false;
            Set<Long> secondarySet = new HashSet<>();
            Map<InstanceId, SegmentUnitMetadata> instanceId2SegmentUnitMap = segmentMetadata.getSegmentUnitMetadataTable();
            for (Map.Entry<InstanceId, SegmentUnitMetadata> segmentUnitEntry : instanceId2SegmentUnitMap.entrySet()){
                InstanceId instanceId = segmentUnitEntry.getKey();
                SegmentUnitMetadata segmentUnitMetadata = segmentUnitEntry.getValue();

                if (segmentUnitMetadata.getMembership().isPrimary(instanceId)){
                    //primary
                    primaryCounter.increment(instanceId.getId());

                    //used for overload
                    if (isInRequestSegWrapper){
                        normalOfInstanceInWrapperCounter.increment(instanceId.getId());
                    }

                    if (instanceId.getId() == requestPrimaryId){
                        isRequestPrimary = true;
                    }
                } else if (segmentUnitMetadata.getMembership().isSecondary(instanceId)){
                    //secondary
                    secondaryCounter.increment(instanceId.getId());

                    secondarySet.add(instanceId.getId());

                    //used for overload
                    if (isInRequestSegWrapper){
                        normalOfInstanceInWrapperCounter.increment(instanceId.getId());
                    }
                } else if (segmentUnitMetadata.getMembership().isArbiter(instanceId)){
                    //arbiter
                    arbiterCounter.increment(instanceId.getId());
                }
            }   //for (Map.Entry<InstanceId, SegmentUnitMetadata> segmentUnitEntry : instanceId2SegmentUnitMap.entrySet()){

            //save secondary combinations of request P
            if (isRequestPrimary){
                for (long secondaryIdOfReqPrimary : secondarySet){
                    secondaryOfPrimaryCounter.increment(secondaryIdOfReqPrimary);
                }
            }
        }

        logger.warn("normalOfInstanceInWrapperCounter:{}; secondaryOfPrimaryCounter:{}", normalOfInstanceInWrapperCounter, secondaryOfPrimaryCounter);

        /**
         * select destination instance
         */
        List<Long> selInstancesIdList;
        //if (instance.getFreeSpace() >= expectedSize && !excludedGroups.contains(instance.getGroup()))
        if (request.getSegmentUnitType() == SegmentUnitType_Thrift.Arbiter){
            //get can be used group set
            Set<Integer> usedGroup = new HashSet<>(excludedGroups);

            // the number 1 is the size of segment unit to reserve, and number 1 is
            // redundant number for fail-tolerant
            int expectedMembers = Math.min(request.getNumberOfSegUnits()+1, allGroupSet.size()-usedGroup.size());

            //reserve arbiter segment unit
            selInstancesIdList = reserveArbiterSegmentUnit(usedGroup, request.getNumberOfSegUnits(), expectedMembers, simpleIdSet,
                    normalIdSet, instanceId2InstanceMap, arbiterCounter);
        } else {
            //get can be used group set
            Set<Integer> usedGroup = new HashSet<>(excludedGroups);
            usedGroup.addAll(simpleGroupIdSet);

            // the number 1 is the size of segment unit to reserve, and number 2 is redundant number for fail-tolerant
            int expectedMembers = Math.min(request.getNumberOfSegUnits()+2, allGroupSet.size()-usedGroup.size());
            long expectedSize = request.getSegmentSize();

            SimulateInstanceBuilder simulateInstanceBuilder = new SimulateInstanceBuilder(storagePool, storageStore, segmentSize).collectionInstance();

            //reserve arbiter segment unit
            selInstancesIdList = reserveSecondarySegmentUnit(simulateInstanceBuilder, volumeMetadata, requestPrimaryId, usedGroup, request.getNumberOfSegUnits(), expectedMembers, expectedSize,
                    normalIdSet, instanceId2InstanceMap, secondaryOfPrimaryCounter, secondaryCounter, normalOfInstanceInWrapperCounter);
        }

        List<InstanceMetadata_Thrift> instances = new ArrayList<>();
        for (long instanceId : selInstancesIdList){
            instances.add(RequestResponseHelper.buildThriftInstanceFrom(instanceId2InstanceMap.get(instanceId)));
        }

        logger.warn("at last get instances:{}", instances);
        if (instances.size() >= request.getNumberOfSegUnits()) {
            Set<Integer> groupSet = new HashSet<>();
            for (InstanceMetadata_Thrift instanceMetadata_thrift : instances) {
                groupSet.add(instanceMetadata_thrift.getGroup().getGroupId());
            }
            Validate.isTrue(groupSet.size() >= request.getNumberOfSegUnits(),
                    "group set:" + groupSet + "instances:" + instances);

            ReserveSegUnitsResponse response = new ReserveSegUnitsResponse(request.getRequestId(), instances,
                    storagePoolId);
            logger.warn("reserveSegUnits response: {}", response);
            return response;
        } else {
            // no instance reserved
            logger.error("can not reserve segment unit for request:{}", request);
            throw new NotEnoughSpaceException_Thrift();
        }
    }

    /**
     * reserve arbiter segment unit
     * will be the datanode with the least arbiter as the reserve object，
     * select from simple datanode first, when simple datanode not enough ,it will select from normal datanode
     * @param usedGroup group which had been used
     * @param necessaryCount necessary secondary segment count
     * @param expectedMembers expect arbiter segment count
     * @param simpleIdSet all simple datanode
     * @param normalIdSet all normal datanode
     * @param instanceId2InstanceMap all instance
     * @param arbiterCounter    datanode used to be arbiter count
     * @return  select arbiter segment unit
     * @throws NotEnoughGroupException_Thrift
     */
    private List<Long> reserveArbiterSegmentUnit(Set<Integer> usedGroup, int necessaryCount, int expectedMembers, Set<Long> simpleIdSet, Set<Long> normalIdSet,
                                                Map<Long, InstanceMetadata> instanceId2InstanceMap, ObjectCounter<Long> arbiterCounter) throws NotEnoughGroupException_Thrift {
        logger.warn("usedGroup:{}, simpleIdSet:{}, normalIdSet:{}, necessaryCount:{}, expectedMembers:{}",
                usedGroup, simpleIdSet, normalIdSet, necessaryCount, expectedMembers);
        List<Long> selInstancesIdList = new ArrayList<>();

        ObjectCounter<Long> bestSimpleList = new TreeSetObjectCounter<>();
        for (long instanceId : simpleIdSet){
            InstanceMetadata instanceTemp = instanceId2InstanceMap.get(instanceId);
            //has already used in this group
            if (usedGroup.contains(instanceTemp.getGroup().getGroupId())) {
                continue;
            }

            if (arbiterCounter.get(instanceId) == 0){
                bestSimpleList.set(instanceId, 0);
            } else {
                bestSimpleList.set(instanceId, arbiterCounter.get(instanceId));
            }
        }
        ObjectCounter<Long> bestNormalList = new TreeSetObjectCounter<>();
        for (long instanceId : normalIdSet){
            InstanceMetadata instanceTemp = instanceId2InstanceMap.get(instanceId);
            if (usedGroup.contains(instanceTemp.getGroup().getGroupId())) {
                continue;
            }
            if (arbiterCounter.get(instanceId) == 0){
                bestNormalList.set(instanceId, 0);
            } else {
                bestNormalList.set(instanceId, arbiterCounter.get(instanceId));
            }
        }

        logger.warn("get arbiter best simple instance list:{}", bestSimpleList);
        logger.warn("get arbiter best normal instance list:{}", bestNormalList);

        Iterator<Long> simpleListItor = bestSimpleList.iterator();
        Iterator<Long> normalListItor = bestNormalList.iterator();
        while (selInstancesIdList.size() < expectedMembers){
            if (simpleListItor.hasNext()) {
                Long instanceId = simpleListItor.next();
                InstanceMetadata instanceTemp = instanceId2InstanceMap.get(instanceId);
                if (usedGroup.contains(instanceTemp.getGroup().getGroupId())){
                    continue;
                }
                //arbiter priority selection simple datanode to be created
                selInstancesIdList.add(instanceId);
                usedGroup.add(instanceTemp.getGroup().getGroupId());
            } else if (normalListItor.hasNext()) {
                Long instanceId = normalListItor.next();
                InstanceMetadata instanceTemp = instanceId2InstanceMap.get(instanceId);
                if (usedGroup.contains(instanceTemp.getGroup().getGroupId())){
                    continue;
                }
                //if simple datanode is not enough , arbiter can create at normal datanode
                selInstancesIdList.add(instanceId);
                usedGroup.add(instanceTemp.getGroup().getGroupId());
            } else {
                //if necessary secondary count is already selected, exception can not be cause
                if (selInstancesIdList.size() >=  necessaryCount){
                    logger.warn("redundancy arbiter not created!");
                    break;
                }

                logger.error("Groups not enough to reserve segment unit! expected arbiter count:{}", expectedMembers);
                throw new NotEnoughGroupException_Thrift().setMinGroupsNumber(expectedMembers);
            }
        }   //while (selInstancesIdList.size() < expectedMembers)

        return selInstancesIdList;
    }

    /**
     * reserve secondary segment unit
     *  we will select the least secondary combination of primary datanode to be reserved segment unit, and will consider overload in a wrapper count
     * @param simulateInstanceBuilder simulate instance object
     * @param usedGroup group which had been used
     * @param necessaryCount necessary secondary segment count
     * @param expectedMembers expect secondary segment count
     * @param expectedSize  expect size of segment
     * @param normalIdSet   all normal datanode
     * @param instanceId2InstanceMap all instance
     * @param secondaryOfPrimaryCounter secondary combinations when primary is same as current segment primary
     * @param secondaryCounter The distribution of secondary on all nodes
     * @param normalOfInstanceInWrapperCounter  normal segment unit counter of data node when wrapper index is same as current segment(used for overload)
     * @return  select secondary segment unit
     * @throws NotEnoughSpaceException_Thrift
     */
    private List<Long> reserveSecondarySegmentUnit(SimulateInstanceBuilder simulateInstanceBuilder, VolumeMetadata volumeMetadata, long primaryId, Set<Integer> usedGroup, int necessaryCount,
                                                   int expectedMembers, long expectedSize, Set<Long> normalIdSet,
                                                 Map<Long, InstanceMetadata> instanceId2InstanceMap, ObjectCounter<Long> secondaryOfPrimaryCounter,
                                                   ObjectCounter<Long> secondaryDistributeCounter, ObjectCounter<Long> normalOfInstanceInWrapperCounter)
            throws NotEnoughSpaceException_Thrift {
        logger.warn("usedGroup:{}, normalIdSet:{}, necessaryCount:{}, expectedMembers:{}, expectedSize:{}", usedGroup, normalIdSet, necessaryCount, expectedMembers, expectedSize);
        List<Long> selInstancesIdList = new ArrayList<>();

        //get secondary datanode priority list
        LinkedList<Long> secondaryListOfPrimary = simulateInstanceBuilder.getSecondaryPriorityList(new LinkedList<>(normalIdSet), primaryId,
                secondaryOfPrimaryCounter, secondaryDistributeCounter, volumeMetadata.getSegmentCount(), volumeMetadata.getVolumeType().getNumSecondaries());
//        LinkedList<Long> secondaryListOfPrimary = ReserveVolumeCombination.getSecondaryPriorityList(bestNormalList, secondaryDistributeCounter);
        secondaryListOfPrimary.removeIf(value -> usedGroup.contains(instanceId2InstanceMap.get(value).getGroup().getGroupId()));

        ObjectCounter<Long> secondaryOfPrimaryCounterBac = new TreeSetObjectCounter<>();
        ObjectCounter<Long> secondaryDistributeCounterBac = new TreeSetObjectCounter<>();
        for (long instanceId : normalIdSet){
            secondaryOfPrimaryCounterBac.set(instanceId, secondaryOfPrimaryCounter.get(instanceId));
            secondaryDistributeCounterBac.set(instanceId, secondaryDistributeCounter.get(instanceId));
        }

        logger.warn("get secondary best normal instance list:{}", secondaryListOfPrimary);

        Set<Long> overloadSet = new HashSet<>();
        Iterator<Long> normalListItor = secondaryListOfPrimary.iterator();
        Iterator<Long> overloadItor = null;
        while (selInstancesIdList.size() < expectedMembers){
            if (normalListItor.hasNext()) {
                InstanceMetadata instanceTemp = instanceId2InstanceMap.get(normalListItor.next());
                long instanceId = instanceTemp.getInstanceId().getId();

                //group had already used
                if (usedGroup.contains(instanceTemp.getGroup().getGroupId())){
                    continue;
                }

                //has no enough space to reserve segment unit
                if (instanceTemp.getFreeSpace() < expectedSize){
                    logger.warn("instance:{} has not enough space:{}", instanceId, expectedSize);
                    continue;
                }

                //overload
                long normalCountOnInstance = normalOfInstanceInWrapperCounter.get(instanceId);
                if (normalCountOnInstance >= instanceTemp.getArchives().size()){
                    overloadSet.add(instanceId);
                    continue;
                }

                secondaryOfPrimaryCounterBac.increment(instanceId);
                secondaryDistributeCounterBac.increment(instanceId);
                selInstancesIdList.add(instanceId);
                usedGroup.add(instanceTemp.getGroup().getGroupId());
            } else if (overloadItor == null) {
                overloadItor = secondaryListOfPrimary.iterator();
            } else if (overloadItor != null && overloadItor.hasNext()){
                long overloadInsId = overloadItor.next();
                if (!overloadSet.contains(overloadInsId)){
                    continue;
                }

                InstanceMetadata instanceTemp = instanceId2InstanceMap.get(overloadInsId);
                if (instanceTemp == null){
                    logger.error("Groups not enough space to reserve segment unit! expected arbiter count:{}; expectedSize:{}", expectedMembers, expectedSize);
                    throw new NotEnoughSpaceException_Thrift();
                }

                long instanceId = instanceTemp.getInstanceId().getId();

                //group had already used
                if (usedGroup.contains(instanceTemp.getGroup().getGroupId())){
                    continue;
                }

                //has no enough space to reserve segment unit
                if (instanceTemp.getFreeSpace() < expectedSize){
                    logger.warn("instance:{} has not enough space:{}", instanceId, expectedSize);
                    continue;
                }

                selInstancesIdList.add(instanceId);
                usedGroup.add(instanceTemp.getGroup().getGroupId());
            } else {
                //if necessary secondary count is already selected, exception can not be cause
                if (selInstancesIdList.size() >=  necessaryCount){
                    logger.warn("redundancy secondary not created!");
                    break;
                }

                logger.error("Groups not enough space to reserve segment unit! expected arbiter count:{}; expectedSize:{}", expectedMembers, expectedSize);
                throw new NotEnoughSpaceException_Thrift();
            }
        }   //while (selInstancesIdList.size() < expectedMembers){
        return selInstancesIdList;
    }

    @Override
    public GetCapacityResponse getCapacity(GetCapacityRequest request)
            throws StorageEmptyException_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getCapacity request: {}", request);
        if (storageStore.size() == 0) {
            throw new StorageEmptyException_Thrift();
        }

        long physicalSpaces = 0L;
        long freeSpace = 0L;
        long logicalSpaces = 0L;
        for (InstanceMetadata entry : storageStore.list()) {
            if (entry.getDatanodeStatus().equals(OK)) {
                physicalSpaces += entry.getCapacity();
                freeSpace += entry.getFreeSpace();
                logicalSpaces += entry.getLogicalCapacity();
            }
        }

        GetCapacityResponse response = new GetCapacityResponse();
        response.setRequestId(request.getRequestId());
        response.setLogicalCapacity(logicalSpaces);
        response.setFreeSpace(freeSpace);
        response.setCapacity(physicalSpaces);
        logger.warn("getCapacity response: {}", response);
        return response;
    }

    public ListStoragePoolCapacityResponse_Thrift listStoragePoolCapacity(ListStoragePoolCapacityRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, InvalidInputException_Thrift, ServiceIsNotAvailable_Thrift,
            TException {
        if (shutDownFlag) {
            logger.error("InfoCenter:{} had been shutdown", appContext.getMainEndPoint());
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("listStoragePoolCapacity request: {}", request);
        try {
            ValidateParam.validateListStoragePoolCapacityRequest(request);
        } catch (InvalidInputException_Thrift e) {
            logger.error("invalid input");
            throw e;
        }

        // check if list some or all storage pool
        logger.debug("check if list some or all storage pool");
        List<StoragePool> storagePoolList = new ArrayList<>();
        if (request.isSetStoragePoolIdList()) {
            logger.debug("list some storage pool");
            try {
                storagePoolList.addAll(storagePoolStore.listStoragePools(request.getStoragePoolIdList()));
            } catch (Exception e) {
                logger.warn("can not list storage pool", e);
            }
        } else {
            logger.debug("list all storage pool");
            try {
                storagePoolList.addAll(storagePoolStore.listStoragePools(request.getDomainId()));
            } catch (Exception e) {
                logger.warn("can not list storage pool", e);
            }
        }
        if (storagePoolList == null || storagePoolList.isEmpty()) {
            logger.error("storage pool is empty");
            throw new StorageEmptyException_Thrift();
        }

        logger.debug("storage pool list size is {}", storagePoolList.size());
        logger.debug("storage pool list is {}", storagePoolList);
        ListStoragePoolCapacityResponse_Thrift response = new ListStoragePoolCapacityResponse_Thrift();
        List<StoragePoolCapacity_Thrift> storagePoolCapacityThriftList = new ArrayList<StoragePoolCapacity_Thrift>();
        for (StoragePool storagePool : storagePoolList) {
            long volumeFreeSpace = 0L;//the space allocate for volume but not use
            long volumeSpace = 0L;////the space allocate for volume
            for (Long volumeID : storagePool.getVolumeIds()) {
                VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeID);
                if (volumeMetadata == null) {
                    logger.warn("can not found volume {}", volumeID);
                    continue;
                }
                int writeCount = volumeMetadata.getVolumeType().getNumSecondaries() + 1;//secondary + primary
                for (SegmentMetadata segmentMetadata : volumeMetadata.getSegments()) {
                    volumeFreeSpace += segmentSize * writeCount * segmentMetadata.getFreeRatio();
                    volumeSpace += segmentSize * writeCount;
                }
            }
            logger.debug("the allocate space is {} ,not used space is {},", volumeSpace, volumeFreeSpace);
            StoragePoolCapacity_Thrift storagePoolCapacityThrift = new StoragePoolCapacity_Thrift();
            storagePoolCapacityThrift.setDomainId(storagePool.getDomainId());
            storagePoolCapacityThrift.setStoragePoolId(storagePool.getPoolId());
            storagePoolCapacityThrift.setStoragePoolName(storagePool.getName());
            storagePoolCapacityThrift.setUsedSpace(volumeSpace - volumeFreeSpace);
            long freeSpace = 0L;
            long totalSpace = 0L;
            for (Entry<Long, Long> entry : storagePool.getArchivesInDataNode().entries()) {
                Long datanodeId = entry.getKey();
                logger.debug("datanode id is {}", datanodeId);
                Long archiveId = entry.getValue();
                logger.debug("archive id is {}", archiveId);
                InstanceMetadata datanode = storageStore.get(datanodeId);
                if (datanode != null && datanode.getDatanodeStatus().equals(OK)) {
                    RawArchiveMetadata archive = datanode.getArchiveById(archiveId);
                    if (archive != null) {
                        freeSpace += archive.getLogicalFreeSpace();
                        totalSpace += archive.getLogicalSpace();
                        logger.debug("free space is {}, total space is {}", freeSpace, totalSpace);
                    }
                }
            }
            storagePoolCapacityThrift.setFreeSpace(freeSpace);
            storagePoolCapacityThrift.setTotalSpace(totalSpace);
            storagePoolCapacityThriftList.add(storagePoolCapacityThrift);
            logger.debug("storage pool capacity thrift is {}", storagePoolCapacityThrift);
        }
        logger.debug("storage pool capacity thrift list size is {}", storagePoolCapacityThriftList.size());
        response.setStoragePoolCapacityList(storagePoolCapacityThriftList);
        response.setRequestId(request.getRequestId());
        logger.warn("listStoragePoolCapacity response: {}", response);
        return response;
    }

    public InformationCenterAppEngine getInformationCenterAppEngine() {
        return informationCenterAppEngine;
    }

    public void setInformationCenterAppEngine(InformationCenterAppEngine informationCenterAppEngine) {
        this.informationCenterAppEngine = informationCenterAppEngine;
    }

    /**
     * list archives of all datanode instances.
     */
    @Override
    public ListArchivesResponse_Thrift listArchives(ListArchivesRequest_Thrift request)
            throws InternalError_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("listArchives request: {}", request);
        ListArchivesResponse_Thrift response = new ListArchivesResponse_Thrift();
        List<InstanceMetadata_Thrift> archivesThrift = new ArrayList<>();
        for (InstanceMetadata instance : storageStore.list()) {
            if (instance.getDatanodeStatus().equals(OK)) {
                archivesThrift.add(RequestResponseHelper.buildThriftInstanceFrom(instance));
            }
        }

        response.setRequestId(request.getRequestId());
        response.setInstanceMetadata(archivesThrift);

        logger.warn("listArchives response: {}", response);
        return response;
    }

    @Override
    public GetArchiveResponse_Thrift getArchive(GetArchiveRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {

        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getArchive request: {}", request);

        GetArchiveResponse_Thrift response = new GetArchiveResponse_Thrift();
        List<Long> archiveIds = request.getArchiveIds();
        List<InstanceMetadata_Thrift> archivesThrift = new ArrayList<>();

        for (long archiveId : archiveIds) {
            OUT:
            for (InstanceMetadata instance : storageStore.list()) {
                if (instance.getDatanodeStatus().equals(OK)) {
                    for (RawArchiveMetadata rawArchiveMetadata : instance.getArchives()) {
                        if (rawArchiveMetadata != null && rawArchiveMetadata.getArchiveId().equals(archiveId)) {
                            archivesThrift.add(RequestResponseHelper.buildThriftInstanceFrom(instance));
                            break OUT;
                        }
                    }

                    for (ArchiveMetadata archiveMetadata : instance.getArchiveMetadatas()) {
                        if (archiveMetadata != null && archiveMetadata.getArchiveId().equals(archiveId)) {
                            archivesThrift.add(RequestResponseHelper.buildThriftInstanceFrom(instance));
                            break OUT;
                        }
                    }

                }

            }
        }

        response.setInstanceMetadata(archivesThrift);
        response.setRequestId(request.getRequestId());
        logger.warn("getArchive response: {}", response);
        return response;
    }

    /**
     * list archive of the specified datanode instance
     */
    @Override
    public GetArchivesResponse_Thrift getArchives(GetArchivesRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getArchives request: {}", request);

        GetArchivesResponse_Thrift response = new GetArchivesResponse_Thrift();
        InstanceMetadata instance = storageStore.get(request.getInstanceId());
        if (null != instance) {
            response.setInstanceMetadata(RequestResponseHelper.buildThriftInstanceFrom(instance));
        }

        response.setRequestId(request.getRequestId());
        logger.warn("getArchives response: {}", response);
        return response;
    }

    // ****** Access Rules for Volume ***********/

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#createVolumeAccessRules(CreateVolumeAccessRulesRequest)} which create an specified
     * access rule in request to database.
     * <p>
     * Status of a new access rule is always "available".
     *
     * @param {@link CreateVolumeAccessRulesRequest} request
     * @return {@link CreateVolumeAccessRulesResponse}
     * @author zjm
     */
    @Override
    public synchronized CreateVolumeAccessRulesResponse createVolumeAccessRules(CreateVolumeAccessRulesRequest request)
            throws VolumeAccessRuleDuplicate_Thrift, ServiceIsNotAvailable_Thrift, InvalidInputException_Thrift,
            TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("createVolumeAccessRules request: {}", request);

        CreateVolumeAccessRulesResponse response = new CreateVolumeAccessRulesResponse(request.getRequestId());

        if (request.getAccessRules() == null || request.getAccessRulesSize() == 0) {
            logger.debug("No access rule to create in request, nothing to do");
            return response;
        }

        // checkout each of the rule that were stored in the request to find out
        // whether there are some rules duplicated
        for (VolumeAccessRule_Thrift volumeAccessRule_Thrift1 : request.getAccessRules()) {
            int counter = 0;
            for (VolumeAccessRule_Thrift volumeAccessRule_Thrift2 : request.getAccessRules()) {
                if (volumeAccessRule_Thrift1.equals(volumeAccessRule_Thrift2)) {
                    counter++;
                }
            }

            if (counter > 1) {
                throw new InvalidInputException_Thrift();
            }
        }

        // get access rule list from accessRuleStore
        List<AccessRuleInformation> rulesFromDB = accessRuleStore.list();

        // checkout each of the rules that were created by the user to find out
        // which rule is already existed in the
        // system.
        for (VolumeAccessRule_Thrift volumeAccessRule_Thrift : request.getAccessRules()) {
            boolean accessRuleExisted = false;
            for (AccessRuleInformation rule : rulesFromDB) {
                if (rule.getIpAddress().equals(volumeAccessRule_Thrift.getIncomingHostName())
                        && rule.getPermission() == volumeAccessRule_Thrift.getPermission().getValue()) {
                    accessRuleExisted = true;
                    break;
                } else {
                    // do nothing
                }
            }

            if (!accessRuleExisted) {
                logger.debug("information center going to save the access rules");
                VolumeAccessRule volumeAccessRule = RequestResponseHelper
                        .buildVolumeAccessRuleFrom(volumeAccessRule_Thrift);
                // status "available" for a new access rule
                volumeAccessRule.setStatus(AccessRuleStatus.AVAILABLE);
                accessRuleStore.save(volumeAccessRule.toAccessRuleInformation());
            } else {
                // throw an exception out
                logger.debug("information center throws an exception VolumeAccessRuleDuplicate_Thrift");
                throw new VolumeAccessRuleDuplicate_Thrift();
            }
        }

        logger.warn("createVolumeAccessRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#deleteVolumeAccessRules(DeleteVolumeAccessRulesRequest)} which delete specified
     * rules in request.
     * <p>
     * It is allowed to delete a access rule only if the access rule is not intermediate state such as "applying" or
     * "canceling".
     * <p>
     * If the access rule is applied to any volume, a confirm is required to delete the access rule.
     *
     * @param {@link DeleteVolumeAccessRulesRequest} request
     * @return {@link DeleteVolumeAccessRulesResponse}
     * @author zjm
     */
    @Override
    public DeleteVolumeAccessRulesResponse deleteVolumeAccessRules(DeleteVolumeAccessRulesRequest request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("deleteVolumeAccessRules request: {}", request);

        // prepare a response for deleting volume access rule request
        DeleteVolumeAccessRulesResponse response = new DeleteVolumeAccessRulesResponse(request.getRequestId());

        if (request.getRuleIds() == null || request.getRuleIds().size() == 0) {
            logger.debug("No access rules existing in request to delete");
            return response;
        }

        for (long ruleId : request.getRuleIds()) {
            AccessRuleInformation accessRuleInformation = accessRuleStore.get(ruleId);
            if (accessRuleInformation == null) {
                logger.debug("No access rule with id {}", ruleId);
                continue;
            }
            VolumeAccessRule volumeAccessRule = new VolumeAccessRule(accessRuleInformation);

            List<Volume2AccessRuleRelationship> relationshipList = new ArrayList<>();
            List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
                    .getByRuleId(ruleId);
            if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
                for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
                    relationshipList.add(new Volume2AccessRuleRelationship(relationshipInfo));
                }
            } else {
                logger.debug("Rule {} is not applied before", volumeAccessRule);
            }

            // a flag used to check if the access rule is able to be deleted
            boolean existAirAccessRule = false;
            for (Volume2AccessRuleRelationship relationship : relationshipList) {

                /*
                 * Get some conclusion of action deleting. Those canceling or applying rules are not allowed to delete.
                 */
                switch (relationship.getStatus()) {
                case FREE:
                    // do nothing
                    break;
                case APPLIED:
                    // do nothing
                    break;
                case APPLING:
                case CANCELING:
                    existAirAccessRule = true;
                    break;
                default:
                    logger.warn("Unknown status {} of relationship", relationship.getStatus());
                    break;
                }

                if (existAirAccessRule) {
                    // unable to delete the access rule, so just break
                    break;
                }
            }

            if (existAirAccessRule) {
                logger.debug("Access rule {} already has an operation on it before deleting", volumeAccessRule);
                response.addToAirAccessRuleList(
                        RequestResponseHelper.buildVolumeAccessRuleThriftFrom(volumeAccessRule));
                continue;
            }

            if (!request.isCommit()) {
                if (volumeAccessRule.getStatus() != AccessRuleStatus.DELETING) {
                    volumeAccessRule.setStatus(AccessRuleStatus.DELETING);
                    accessRuleStore.save(volumeAccessRule.toAccessRuleInformation());
                } else {
                    // do nothing
                }

                continue;
            }

            // delete the volume access rule and relationship with volume from
            // database if exists
            accessRuleStore.delete(ruleId);
            volumeRuleRelationshipStore.deleteByRuleId(ruleId);
        }

        logger.warn("deleteVolumeAccessRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface {@link InformationCenter.Iface#getVolumeAccessRules(GetVolumeAccessRulesRequest)}
     * which get the specified access rules applied to the volume in request.
     *
     * @param {@link GetVolumeAccessRulesRequest} request
     * @return {@link GetVolumeAccessRulesResponse}
     * @author zjm
     */
    @Override
    public GetVolumeAccessRulesResponse getVolumeAccessRules(GetVolumeAccessRulesRequest request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("getVolumeAccessRules request: {}", request);

        // prepare a response to getting access rules request
        GetVolumeAccessRulesResponse response = new GetVolumeAccessRulesResponse();
        response.setRequestId(request.getRequestId());
        response.setAccessRules(new ArrayList<>());

        List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
                .getByVolumeId(request.getVolumeId());
        if (relationshipInfoList == null || relationshipInfoList.size() == 0) {
            logger.debug("The volume {} hasn't be applied any access rules", request.getVolumeId());
            return response;
        }

        List<Volume2AccessRuleRelationship> relationshipList = new ArrayList<Volume2AccessRuleRelationship>();
        for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
            Volume2AccessRuleRelationship relationship = new Volume2AccessRuleRelationship(relationshipInfo);
            relationshipList.add(relationship);
        }

        for (Volume2AccessRuleRelationship relationship : relationshipList) {
            VolumeAccessRule accessRule = new VolumeAccessRule(accessRuleStore.get(relationship.getRuleId()));
            // build access rule to remote
            VolumeAccessRule_Thrift accessRuleToRemote = RequestResponseHelper
                    .buildVolumeAccessRuleThriftFrom(accessRule);
            accessRuleToRemote.setStatus(AccessRuleStatus_Thrift.valueOf(relationship.getStatus().name()));
            response.addToAccessRules(accessRuleToRemote);
        }

        logger.warn("getVolumeAccessRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#applyVolumeAccessRules(ApplyVolumeAccessRulesRequest)} which apply specified
     * access rule to the volume in request.
     *
     * @param {@link ApplyVolumeAccessRulesRequest} request
     * @return {@link ApplyVolumeAccessRulesResponse}
     * <p>
     * If exist some air rules which means unable to apply those rules to specified volume in request, add those
     * rules in air list of response.
     * @author zjm
     */
    @Override
    public ApplyVolumeAccessRulesResponse applyVolumeAccessRules(ApplyVolumeAccessRulesRequest request)
            throws VolumeNotFoundException_Thrift, VolumeBeingDeletedException_Thrift, ServiceIsNotAvailable_Thrift,
            ApplyFailedDueToVolumeIsReadOnlyException_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("applyVolumeAccessRules request: {}", request);

        // check volume does exists or in deleting, deleted or dead status
        VolumeMetadata volume = volumeStore.getVolume(request.getVolumeId());

        if (volume == null) {
            throw new VolumeNotFoundException_Thrift();
        }

        if (volume.isDeletedByUser()) {
            throw new VolumeBeingDeletedException_Thrift();
        }

        ApplyVolumeAccessRulesResponse response = new ApplyVolumeAccessRulesResponse(request.getRequestId());

        List<Long> toApplyRuleIdList = request.getRuleIds();
        if (toApplyRuleIdList == null || toApplyRuleIdList.isEmpty()) {
            logger.debug("No rule exists in applying request, do nothing");
            return response;
        }

        boolean needThrowException = false;
        String exceptionDetail = new String();
        for (long toApplyRuleId : toApplyRuleIdList) {
            VolumeAccessRule accessRule = new VolumeAccessRule(accessRuleStore.get(toApplyRuleId));

            // unable to apply a deleting access rule to volume
            if (accessRule.getStatus() == AccessRuleStatus.DELETING) {
                logger.debug("The access rule {} is deleting now, unable to apply this rule to volume {}", accessRule,
                        request.getVolumeId());
                response.addToAirAccessRuleList(RequestResponseHelper.buildVolumeAccessRuleThriftFrom(accessRule));
                continue;
            }

            if (volume.getReadWrite().equals(VolumeMetadata.ReadWriteType.READ_ONLY) && !accessRule.getPermission()
                    .equals(AccessPermissionType.READ)) {
                needThrowException = true;
                exceptionDetail += "Failed: " + accessRule + "\n";
                continue;
            }

            // turn relationship info get from db to relationship structure
            List<Volume2AccessRuleRelationship> relationshipList = new ArrayList<>();
            List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
                    .getByRuleId(toApplyRuleId);
            if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
                for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
                    Volume2AccessRuleRelationship relationship = new Volume2AccessRuleRelationship(relationshipInfo);
                    relationshipList.add(relationship);
                }
            }

            /*
             * Check if the relationship exists in db. If it doesn't, in the case, the relationship is a fresh one, we
             * should set its status to "free"; Otherwise, keep the status of relationship get from db.
             */
            boolean existInRelationshipStore = false;
            Volume2AccessRuleRelationship relationship2Apply = new Volume2AccessRuleRelationship();
            for (Volume2AccessRuleRelationship relationship : relationshipList) {
                if (relationship.getVolumeId() == request.getVolumeId()) {
                    existInRelationshipStore = true;
                    relationship2Apply = relationship;
                }
            }
            if (!existInRelationshipStore) {
                logger.debug("The rule {} is not being applied to the volume {} before", accessRule,
                        request.getVolumeId());
                relationship2Apply = new Volume2AccessRuleRelationship();
                relationship2Apply.setRelationshipId(RequestIdBuilder.get());
                relationship2Apply.setRuleId(toApplyRuleId);
                relationship2Apply.setVolumeId(request.getVolumeId());
                relationship2Apply.setStatus(AccessRuleStatusBindingVolume.FREE);
                relationshipList.add(relationship2Apply);
            }

            /*
             * Turn all relationship to a proper status base on state machine below.
             */
            // A state machine to transfer status of relationship to proper
            // status.
            switch (relationship2Apply.getStatus()) {
            case FREE:
                if (request.isCommit()) {
                    relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
                } else {
                    relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLING);
                }
                // status of relationship has changed, save it to db
                logger.debug("FREE, save {}", relationship2Apply.getStatus());
                volumeRuleRelationshipStore.save(relationship2Apply.toVolumeRuleRelationshipInformation());
                break;
            case APPLING:
                if (request.isCommit()) {
                    relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
                    logger.debug("APPLYING , save {}", relationship2Apply.getStatus());
                    volumeRuleRelationshipStore.save(relationship2Apply.toVolumeRuleRelationshipInformation());
                }
                break;
            case APPLIED:
                logger.debug("APPLIED, DO NOTHING");
                // do nothing
                break;
            case CANCELING:
                logger.debug("CANCELING, ERROR");
                VolumeAccessRule_Thrift accessRuleToRemote = RequestResponseHelper
                        .buildVolumeAccessRuleThriftFrom(accessRule);
                accessRuleToRemote.setStatus(AccessRuleStatus_Thrift.valueOf(relationship2Apply.getStatus().name()));
                response.addToAirAccessRuleList(accessRuleToRemote);
                break;
            default:
                logger.warn("Unknown status {}", relationship2Apply.getStatus());
                break;
            }
        }

        if (needThrowException) {
            ApplyFailedDueToVolumeIsReadOnlyException_Thrift exception = new ApplyFailedDueToVolumeIsReadOnlyException_Thrift();
            exception.setDetail(exceptionDetail);
            logger.warn("{}", exception);
            throw exception;
        }

        logger.warn("applyVolumeAccessRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#cancelVolumeAccessRules(CancelVolumeAccessRulesRequest)} which to cancel access
     * rule from specified volume in request.
     *
     * @param {@link CancelVolumeAccessRulesRequest} request
     * @return {@link CancelVolumeAccessRulesResponse}
     * @author zjm
     */
    @Override
    public CancelVolumeAccessRulesResponse cancelVolumeAccessRules(CancelVolumeAccessRulesRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, AccessRuleNotApplied_Thrift,
            TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("cancelVolumeAccessRules request: {}", request);

        CancelVolumeAccessRulesResponse response = new CancelVolumeAccessRulesResponse(request.getRequestId());

        if (request.getRuleIds() == null || request.getRuleIdsSize() == 0) {
            return response;
        }

        for (long toCancelRuleId : request.getRuleIds()) {
            AccessRuleInformation accessRuleInformation = accessRuleStore.get(toCancelRuleId);
            if (accessRuleInformation == null) {
                continue;
            }

            VolumeAccessRule volumeAccessRule = new VolumeAccessRule(accessRuleInformation);
            // unable cancel deleting access rule from volume
            if (volumeAccessRule.getStatus() == AccessRuleStatus.DELETING) {
                logger.debug("The access rule {} is deleting now, unable to cancel this rule from volume {}",
                        volumeAccessRule, request.getVolumeId());
                response.addToAirAccessRuleList(
                        RequestResponseHelper.buildVolumeAccessRuleThriftFrom(volumeAccessRule));
                continue;
            }

            // turn relationship info got from db to relationship structure
            List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
                    .getByRuleId(toCancelRuleId);
            Volume2AccessRuleRelationship relationship = null;
            if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
                for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
                    if (relationshipInfo.getVolumeId() == request.getVolumeId()) {
                        relationship = new Volume2AccessRuleRelationship(relationshipInfo);
                        break;
                    }
                }
            }
            if (relationship == null) {
                logger.error("The access rule {} is deleting now, unable to cancel", volumeAccessRule);
                throw new AccessRuleNotApplied_Thrift();
            }

            // turn status of relationship to proper status after action "cancel"
            switch (relationship.getStatus()) {
            case FREE:
                // do nothing
                break;
            case APPLING:
                VolumeAccessRule_Thrift accessRuleToRemote = RequestResponseHelper
                        .buildVolumeAccessRuleThriftFrom(volumeAccessRule);
                accessRuleToRemote.setStatus(AccessRuleStatus_Thrift.valueOf(relationship.getStatus().name()));
                response.addToAirAccessRuleList(accessRuleToRemote);
                break;
            case APPLIED:
                if (request.isCommit()) {
                    volumeRuleRelationshipStore.deleteByRuleIdandVolumeID(request.getVolumeId(), toCancelRuleId);
                } else {
                    relationship.setStatus(AccessRuleStatusBindingVolume.CANCELING);
                    volumeRuleRelationshipStore.save(relationship.toVolumeRuleRelationshipInformation());
                }
                break;
            case CANCELING:
                if (request.isCommit()) {
                    volumeRuleRelationshipStore.deleteByRuleIdandVolumeID(request.getVolumeId(), toCancelRuleId);
                }
                break;
            default:
                logger.warn("unknown status {} of relationship", relationship.getStatus());
                break;
            }
        }

        logger.warn("cancelVolumeAccessRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * which apply specified access rule to list of volumes in request.
     */
    @Override
    public ApplyVolumeAccessRuleOnVolumesResponse applyVolumeAccessRuleOnVolumes(
            ApplyVolumeAccessRuleOnVolumesRequest request)
            throws VolumeNotFoundException_Thrift, VolumeBeingDeletedException_Thrift, ServiceIsNotAvailable_Thrift,
            ApplyFailedDueToVolumeIsReadOnlyException_Thrift, AccessRuleUnderOperation_Thrift,
            AccessRuleNotFound_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("applyVolumeAccessRules request: {}", request);

        ApplyVolumeAccessRuleOnVolumesResponse response = new ApplyVolumeAccessRuleOnVolumesResponse(
                request.getRequestId());
        response.setAirVolumeList(new ArrayList<>());
        if (request.getVolumeIds() == null || request.getVolumeIdsSize() == 0) {
            logger.warn("given volume is null");
            return response;
        }
        long toApplyRuleId = request.getRuleId();
        AccessRuleInformation accessRuleInformation = accessRuleStore.get(toApplyRuleId);
        if (accessRuleInformation == null) {
            logger.error("get access rule failed");
            throw new AccessRuleNotFound_Thrift();
        }
        VolumeAccessRule volumeAccessRule = new VolumeAccessRule(accessRuleInformation);
        // unable cancel deleting access rule from volume
        if (volumeAccessRule.getStatus() == AccessRuleStatus.DELETING) {
            logger.error("The access rule {} is deleting now, unable to cancel", volumeAccessRule);
            throw new AccessRuleUnderOperation_Thrift();
        }

        boolean needThrowException = false;
        String exceptionDetail = new String();
        // turn relationship info get from db to relationship structure
        List<Volume2AccessRuleRelationship> relationshipList = new ArrayList<>();
        List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
                .getByRuleId(toApplyRuleId);
        if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
            for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
                Volume2AccessRuleRelationship relationship = new Volume2AccessRuleRelationship(relationshipInfo);
                relationshipList.add(relationship);
            }
        }

        logger.debug("relationshipList {} ", relationshipList);
        /*
         * Check if the relationship exists in db. If it doesn't, in the case, the relationship is a fresh one, we
         * should set its status to "free"; Otherwise, keep the status of relationship get from db.
        */
        for (long toAppliedVolumeId : request.getVolumeIds()) {
            // check volume does exists or in deleting, deleted or dead status
            VolumeMetadata volume = volumeStore.getVolume(toAppliedVolumeId);
            if (volume == null) {
                logger.error("the volume {} get from volumeStore is null", toAppliedVolumeId);
                volume.setVolumeId(toAppliedVolumeId);
                response.addToAirVolumeList(RequestResponseHelper.buildThriftVolumeFrom(volume, false));
                continue;
            }
            if (volume.isDeletedByUser()) {
                logger.error("the volume {} is being deleted", toAppliedVolumeId);
                response.addToAirVolumeList(RequestResponseHelper.buildThriftVolumeFrom(volume, false));
                continue;
            }
            if (volume.getReadWrite().equals(VolumeMetadata.ReadWriteType.READ_ONLY) && !volumeAccessRule
                    .getPermission().equals(AccessPermissionType.READ)) {
                needThrowException = true;
                exceptionDetail += "Failed: " + volumeAccessRule + "\n";
                response.addToAirVolumeList(RequestResponseHelper.buildThriftVolumeFrom(volume, false));
                continue;
            }

            //diffrent access rules that witch has same ip, cannot apply to volume
            List<VolumeRuleRelationshipInformation> volumeRelationshipInfoList = volumeRuleRelationshipStore.getByVolumeId(toAppliedVolumeId);
            if (volumeRelationshipInfoList != null) {
                boolean hasSameIpRule = false;
                for (VolumeRuleRelationshipInformation relationshipInfo : volumeRelationshipInfoList) {
                    AccessRuleInformation volumeAccessRuleInfo = accessRuleStore.get(relationshipInfo.getRuleId());

                    //same ip rule already be applied with this volume
                    if (accessRuleInformation.getIpAddress().equals(volumeAccessRuleInfo.getIpAddress())) {
                        logger.warn("apply rule:{} failed. same ip:{} rules already be applied with this volume:{}",
                                toApplyRuleId, volumeAccessRuleInfo.getIpAddress(), toAppliedVolumeId);
                        response.addToAirVolumeList(RequestResponseHelper.buildThriftVolumeFrom(volume, false));
                        hasSameIpRule = true;
                        break;
                    }
                }
                if (hasSameIpRule){
                    continue;
                }
            }

            boolean existInRelationshipStore = false;
            Volume2AccessRuleRelationship relationship2Apply = new Volume2AccessRuleRelationship();
            for (Volume2AccessRuleRelationship relationship : relationshipList) {
                if (relationship.getVolumeId() == toAppliedVolumeId) {
                    existInRelationshipStore = true;
                    relationship2Apply = relationship;
                }
            }
            if (!existInRelationshipStore) {
                logger.warn("The rule {} is not being applied to the volume {} before", volumeAccessRule,
                        toAppliedVolumeId);
                relationship2Apply = new Volume2AccessRuleRelationship();
                relationship2Apply.setRelationshipId(RequestIdBuilder.get());
                relationship2Apply.setRuleId(toApplyRuleId);
                relationship2Apply.setVolumeId(toAppliedVolumeId);
                relationship2Apply.setStatus(AccessRuleStatusBindingVolume.FREE);
                relationshipList.add(relationship2Apply);
            }

            //Turn all relationship to a proper status base on state machine below.
            switch (relationship2Apply.getStatus()) {
            case FREE:
                if (request.isCommit()) {
                    relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
                } else {
                    relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLING);
                }
                // status of relationship has changed, save it to db
                logger.debug("FREE, save {}", relationship2Apply.getStatus());
                volumeRuleRelationshipStore.save(relationship2Apply.toVolumeRuleRelationshipInformation());
                break;
            case APPLING:
                if (request.isCommit()) {
                    relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
                    logger.debug("APPLYING , save {}", relationship2Apply.getStatus());
                    volumeRuleRelationshipStore.save(relationship2Apply.toVolumeRuleRelationshipInformation());
                }
                break;
            case APPLIED:
                logger.debug("APPLIED, DO NOTHING");
                // do nothing
                break;
            case CANCELING:
                logger.debug("CANCELING, ERROR");
                VolumeAccessRule_Thrift accessRuleToRemote = RequestResponseHelper
                        .buildVolumeAccessRuleThriftFrom(volumeAccessRule);
                accessRuleToRemote.setStatus(AccessRuleStatus_Thrift.valueOf(relationship2Apply.getStatus().name()));

                response.addToAirVolumeList(RequestResponseHelper.buildThriftVolumeFrom(volume, false));
                break;
            default:
                logger.warn("Unknown status {}", relationship2Apply.getStatus());
                break;
            }
        }

        if (needThrowException) {
            ApplyFailedDueToVolumeIsReadOnlyException_Thrift exception = new ApplyFailedDueToVolumeIsReadOnlyException_Thrift();
            exception.setDetail(exceptionDetail);
            logger.warn("{}", exception);
            throw exception;
        }

        logger.warn("applyVolumeAccessRuleOnVolumes response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * which to cancel access rule from specified list of volumes in request.
     */
    @Override
    public CancelVolAccessRuleAllAppliedResponse cancelVolAccessRuleAllApplied(
            CancelVolAccessRuleAllAppliedRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, AccessRuleNotApplied_Thrift,
            AccessRuleUnderOperation_Thrift, AccessRuleNotFound_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("cancelVolumeAccessRules request: {}", request);

        CancelVolAccessRuleAllAppliedResponse response = new CancelVolAccessRuleAllAppliedResponse(
                request.getRequestId());
        if (request.getVolumeIds() == null || request.getVolumeIdsSize() == 0) {
            logger.warn("given volume is null");
            return response;
        }

        long toCancelRuleId = request.getRuleId();
        AccessRuleInformation accessRuleInformation = accessRuleStore.get(toCancelRuleId);
        if (accessRuleInformation == null) {
            logger.error("get access rule failed");
            throw new AccessRuleNotFound_Thrift();
        }
        VolumeAccessRule volumeAccessRule = new VolumeAccessRule(accessRuleInformation);
        // unable cancel deleting access rule from volume
        if (volumeAccessRule.getStatus() == AccessRuleStatus.DELETING) {
            logger.error("The access rule {} is deleting now, unable to cancel", volumeAccessRule);
            throw new AccessRuleUnderOperation_Thrift();
        }

        // turn relationship info got from db to relationship structure
        List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
                .getByRuleId(toCancelRuleId);

        for (long toAppliedVolumeId : request.getVolumeIds()) {
            Volume2AccessRuleRelationship relationship = null;
            if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
                for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
                    if (relationshipInfo.getVolumeId() == toAppliedVolumeId) {
                        relationship = new Volume2AccessRuleRelationship(relationshipInfo);
                        break;
                    }
                }
            }
            if (relationship == null) {
                logger.error("relationship is null, {} not applied", volumeAccessRule);
                throw new AccessRuleNotApplied_Thrift();
            }

            // turn status of relationship to proper status after action "cancel"
            switch (relationship.getStatus()) {
            case FREE:
                // do nothing
                break;
            case APPLING:
                VolumeAccessRule_Thrift accessRuleToRemote = RequestResponseHelper
                        .buildVolumeAccessRuleThriftFrom(volumeAccessRule);
                accessRuleToRemote.setStatus(AccessRuleStatus_Thrift.valueOf(relationship.getStatus().name()));
                response.addToAirVolumeIds(toAppliedVolumeId);
                break;
            case APPLIED:
                if (request.isCommit()) {
                    volumeRuleRelationshipStore.deleteByRuleIdandVolumeID(toAppliedVolumeId, toCancelRuleId);
                } else {
                    relationship.setStatus(AccessRuleStatusBindingVolume.CANCELING);
                    volumeRuleRelationshipStore.save(relationship.toVolumeRuleRelationshipInformation());
                }
                break;
            case CANCELING:
                if (request.isCommit()) {
                    volumeRuleRelationshipStore.deleteByRuleIdandVolumeID(toAppliedVolumeId, toCancelRuleId);
                }
                break;
            default:
                logger.warn("unknown status {} of relationship", relationship.getStatus());
                break;
            }
        }
        logger.warn("cancelVolAccessRuleAllApplied response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#listVolumeAccessRules(ListVolumeAccessRulesRequest)} which list all created access
     * rules.
     *
     * @param {@link ListVolumeAccessRulesRequest} request
     * @return {@link ListVolumeAccessRulesResponse}
     */
    @Override
    public ListVolumeAccessRulesResponse listVolumeAccessRules(ListVolumeAccessRulesRequest request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("listVolumeAccessRules request: {}", request);

        ListVolumeAccessRulesResponse listVolumeAccessRulesResponse = new ListVolumeAccessRulesResponse();
        listVolumeAccessRulesResponse.setRequestId(request.getRequestId());

        List<AccessRuleInformation> accessRuleInformations = accessRuleStore.list();
        if (accessRuleInformations != null && accessRuleInformations.size() > 0) {
            for (AccessRuleInformation accessRuleInformation : accessRuleInformations) {
                listVolumeAccessRulesResponse.addToAccessRules(RequestResponseHelper
                        .buildVolumeAccessRuleThriftFrom(new VolumeAccessRule(accessRuleInformation)));
            }
        }

        logger.warn("listVolumeAccessRules response: {}", listVolumeAccessRulesResponse);
        return listVolumeAccessRulesResponse;
    }

    @Override
    public ListVolumeAccessRulesByVolumeIdsResponse listVolumeAccessRulesByVolumeIds(
            ListVolumeAccessRulesByVolumeIdsRequest request) throws ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("ListVolumeAccessRulesByVolumeIdsResponse request: {}", request);

        ListVolumeAccessRulesByVolumeIdsResponse listVolumeAccessRulesByVolumeIdsResponse = new ListVolumeAccessRulesByVolumeIdsResponse();
        listVolumeAccessRulesByVolumeIdsResponse.setRequestId(request.getRequestId());
        Set<Long> volumeIds = request.getVolumeIds();

        List<VolumeRuleRelationshipInformation> volumeRuleRelationshipInformationList = volumeRuleRelationshipStore
                .list();
        List<AccessRuleInformation> accessRuleInformations = accessRuleStore.list();

        for (Long volumeId : volumeIds) {
            List<VolumeAccessRule_Thrift> accessRule_thriftList = new ArrayList<>();
            for (VolumeRuleRelationshipInformation volumeRule : volumeRuleRelationshipInformationList) {
                if (volumeId == volumeRule.getVolumeId()) {
                    Long ruleId = volumeRule.getRuleId();
                    for (AccessRuleInformation rule : accessRuleInformations) {
                        if (ruleId == rule.getRuleId()) {
                            accessRule_thriftList.add(RequestResponseHelper
                                    .buildVolumeAccessRuleThriftFrom(new VolumeAccessRule(rule)));
                        }
                    }
                }
            }
            listVolumeAccessRulesByVolumeIdsResponse.putToAccessRulesTable(volumeId, accessRule_thriftList);
        }

        logger.warn("ListVolumeAccessRulesByVolumeIdsResponse response: {}", listVolumeAccessRulesByVolumeIdsResponse);
        return listVolumeAccessRulesByVolumeIdsResponse;
    }
    // ****** Access Rules for Volume End ***********/

    // ****** Access Rules for Iscsi begin***********/

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#createIscsiAccessRules(CreateIscsiAccessRulesRequest)} which create an specified
     * access rule in request to database.
     * <p>
     * Status of a new access rule is always "available".
     *
     * @param {@link CreateIscsiAccessRulesRequest} request
     * @return {@link CreateIscsiAccessRulesResponse}
     * @author zjm
     */
    @Override
    public synchronized CreateIscsiAccessRulesResponse createIscsiAccessRules(CreateIscsiAccessRulesRequest request)
            throws IscsiAccessRuleDuplicate_Thrift, IscsiAccessRuleFormatError_Thrift, ServiceIsNotAvailable_Thrift,
            InvalidInputException_Thrift, ChapSameUserPasswdError_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("creatIscsiAccessRules request: {}", request);

        CreateIscsiAccessRulesResponse response = new CreateIscsiAccessRulesResponse(request.getRequestId());
        if (request.getAccessRules() == null || request.getAccessRulesSize() == 0) {
            logger.debug("No access rule to create in request, nothing to do");
            return response;
        }
        //check initiatorname format
        for (IscsiAccessRule_Thrift iscsiAccessRule_Thrift : request.getAccessRules()) {
            boolean b = iscsiAccessRule_Thrift.getInitiatorName().matches("iqn.*");
            if (!b) {
                logger.error("Initiator Name format fail");
                throw new IscsiAccessRuleFormatError_Thrift();
            }
        }
        // check user of incoming and outgoing are not the same
        // when outgoing set incoming should set first
        for (IscsiAccessRule_Thrift iscsiAccessRule_Thrift : request.getAccessRules()) {
            String user = iscsiAccessRule_Thrift.getUser();
            String outUser = iscsiAccessRule_Thrift.getOutUser();
            if ((user != null && user != "") && (outUser != null && outUser != "")) {
                if (user.equals(outUser)) {
                    logger.error("incoming and outgoing user should not the same");
                    throw new ChapSameUserPasswdError_Thrift();
                }
            }
            if ((user == null || user == "") && (outUser != null && outUser != "")) {
                logger.error("incoming user should set first");
                throw new InvalidInputException_Thrift();
            }
        }
        // checkout each of the rule that were stored in the request to find out
        // whether there are some rules duplicated
        for (IscsiAccessRule_Thrift iscsiAccessRule_Thrift1 : request.getAccessRules()) {
            int counter = 0;
            for (IscsiAccessRule_Thrift iscsiAccessRule_Thrift2 : request.getAccessRules()) {
                if (iscsiAccessRule_Thrift1.equals(iscsiAccessRule_Thrift2)) {
                    counter++;
                }
            }
            if (counter > 1) {
                throw new InvalidInputException_Thrift();
            }
        }
        // get iscsi access rule list from iscsiAccessRuleStore
        List<IscsiAccessRuleInformation> iscsiRulesFromDB = iscsiAccessRuleStore.list();
        // checkout each of the rules that were created by the user to find out
        // which rule is already existed in the
        // system.
        for (IscsiAccessRule_Thrift iscsiAccessRule_Thrift : request.getAccessRules()) {
            boolean accessRuleExisted = false;
            for (IscsiAccessRuleInformation rule : iscsiRulesFromDB) {
                if (rule.getInitiatorName().equals(iscsiAccessRule_Thrift.getInitiatorName()) && rule.getUser()
                        .equals(iscsiAccessRule_Thrift.getUser()) && rule.getPassed()
                        .equals(iscsiAccessRule_Thrift.getPassed()) && rule.getPermission() == iscsiAccessRule_Thrift
                        .getPermission().getValue()) {
                    accessRuleExisted = true;
                    break;
                } else {
                    // do nothing
                }
            }
            if (!accessRuleExisted) {
                logger.debug("information center going to save the access rules");
                IscsiAccessRule iscsiAccessRule = RequestResponseHelper
                        .buildIscsiAccessRuleFrom(iscsiAccessRule_Thrift);
                // status "available" for a new access rule
                iscsiAccessRule.setStatus(AccessRuleStatus.AVAILABLE);
                iscsiAccessRuleStore.save(iscsiAccessRule.toIscsiAccessRuleInformation());
            } else {
                // throw an exception out
                logger.debug("information center throws an exception IscsiAccessRuleDuplicate_Thrift");
                throw new IscsiAccessRuleDuplicate_Thrift();
            }
        }
        logger.warn("createIscsiAccessRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#deleteIscsiAccessRules(DeleteIscsiAccessRulesRequest)} which delete specified
     * rules in request.
     * <p>
     * It is allowed to delete a iscsi access rule only if the access rule is not intermediate state such as "applying" or
     * "canceling".
     * <p>
     * If the iscsi access rule is applied to any driver, a confirm is required to delete the iscsi access rule.
     *
     * @param {@link DeleteIscsiAccessRulesRequest} request
     * @return {@link DeleteIscsiAccessRulesResponse}
     * @author zjm
     */
    @Override
    public DeleteIscsiAccessRulesResponse deleteIscsiAccessRules(DeleteIscsiAccessRulesRequest request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("deleteIscsiAccessRules request: {}", request);
        // prepare a response for deleting iscsi access rule request
        DeleteIscsiAccessRulesResponse response = new DeleteIscsiAccessRulesResponse(request.getRequestId());
        if (request.getRuleIds() == null || request.getRuleIds().size() == 0) {
            logger.debug("No iscsi access rules existing in request to delete");
            return response;
        }
        for (long ruleId : request.getRuleIds()) {
            IscsiAccessRuleInformation iscsiAccessRuleInformation = iscsiAccessRuleStore.get(ruleId);
            if (iscsiAccessRuleInformation == null) {
                logger.debug("No iscsi access rule with id {}", ruleId);
                continue;
            }
            IscsiAccessRule iscsiAccessRule = new IscsiAccessRule(iscsiAccessRuleInformation);

            List<Iscsi2AccessRuleRelationship> iscsiRelationshipList = new ArrayList<Iscsi2AccessRuleRelationship>();
            List<IscsiRuleRelationshipInformation> iscsiRelationshipInfoList = iscsiRuleRelationshipStore
                    .getByRuleId(ruleId);
            if (iscsiRelationshipInfoList != null && iscsiRelationshipInfoList.size() > 0) {
                for (IscsiRuleRelationshipInformation relationshipInfo : iscsiRelationshipInfoList) {
                    iscsiRelationshipList.add(new Iscsi2AccessRuleRelationship(relationshipInfo));
                }
            } else {
                logger.debug("Rule {} is not applied before", iscsiAccessRule);
            }
            // a flag used to check if the access rule is able to be deleted
            boolean existAirAccessRule = false;
            for (Iscsi2AccessRuleRelationship relationship : iscsiRelationshipList) {
                /*
                 * Get some conclusion of action deleting. Those canceling or applying rules are not allowed to delete.
                 */
                switch (relationship.getStatus()) {
                case FREE:
                    // do nothing
                    break;
                case APPLIED:
                    // do nothing
                    break;
                case APPLING:
                case CANCELING:
                    existAirAccessRule = true;
                    break;
                default:
                    logger.warn("Unknown status {} of relationship", relationship.getStatus());
                    break;
                }

                if (existAirAccessRule) {
                    // unable to delete the access rule, so just break
                    break;
                }
            }
            if (existAirAccessRule) {
                logger.debug("Iscsi Access rule {} already has an operation on it before deleting", iscsiAccessRule);
                response.addToAirAccessRuleList(RequestResponseHelper.buildIscsiAccessRuleThriftFrom(iscsiAccessRule));
                continue;
            }
            if (!request.isCommit()) {
                if (iscsiAccessRule.getStatus() != AccessRuleStatus.DELETING) {
                    iscsiAccessRule.setStatus(AccessRuleStatus.DELETING);
                    iscsiAccessRuleStore.save(iscsiAccessRule.toIscsiAccessRuleInformation());
                } else {
                    // do nothing
                }
                continue;
            }

            // delete the iscsi access rule and relationship with driverKey from
            // database if exists
            iscsiAccessRuleStore.delete(ruleId);
            iscsiRuleRelationshipStore.deleteByRuleId(ruleId);
        }
        logger.warn("deleteIscsiAccessRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface {@link InformationCenter.Iface#getIscsiAccessRules(GetIscsiAccessRulesRequest)}
     * which get the specified access rules applied to the iscsi in request.
     *
     * @param {@link GetIscsiAccessRulesRequest} request
     * @return {@link GetIscsiAccessRulesResponse}
     * @author zjm
     */
    @Override
    public GetIscsiAccessRulesResponse getIscsiAccessRules(GetIscsiAccessRulesRequest request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("getIscsiAccessRules request: {}", request);

        // prepare a response to getting iscsi access rules request
        GetIscsiAccessRulesResponse response = new GetIscsiAccessRulesResponse();
        response.setRequestId(request.getRequestId());

        List<IscsiRuleRelationshipInformation> relationshipInfoList = iscsiRuleRelationshipStore
                .getByDriverKey(request.getDriverKey());
        if (relationshipInfoList == null || relationshipInfoList.size() == 0) {
            logger.debug("The driver {} hasn't be applied any access rules", request.getDriverKey());
            return response;
        }

        List<Iscsi2AccessRuleRelationship> relationshipList = new ArrayList<Iscsi2AccessRuleRelationship>();
        for (IscsiRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
            Iscsi2AccessRuleRelationship relationship = new Iscsi2AccessRuleRelationship(relationshipInfo);
            relationshipList.add(relationship);
        }

        for (Iscsi2AccessRuleRelationship relationship : relationshipList) {
            IscsiAccessRule iscsiAccessRule = new IscsiAccessRule(iscsiAccessRuleStore.get(relationship.getRuleId()));
            // build access rule to remote
            IscsiAccessRule_Thrift iscsiAccessRuleToRemote = RequestResponseHelper
                    .buildIscsiAccessRuleThriftFrom(iscsiAccessRule);
            iscsiAccessRuleToRemote.setStatus(AccessRuleStatus_Thrift.valueOf(relationship.getStatus().name()));
            response.addToAccessRules(iscsiAccessRuleToRemote);
        }

        logger.warn("getIscsiAccessRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#applyIscsiAccessRules(ApplyIscsiAccessRulesRequest)} which apply specified
     * access rule to the drivers in request.
     *
     * @param {@link ApplyIscsiAccessRulesRequest} request
     * @return {@link ApplyIscsiAccessRulesResponse}
     * <p>
     * If exist some air rules which means unable to apply those rules to specified driver in request, add those
     * rules in air list of response.
     * @author zjm
     */
    @Override
    public ApplyIscsiAccessRulesResponse applyIscsiAccessRules(ApplyIscsiAccessRulesRequest request)
            throws IscsiNotFoundException_Thrift, IscsiBeingDeletedException_Thrift, ServiceIsNotAvailable_Thrift,
            ApplyFailedDueToConflictException_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("applyIscsiAccessRules request: {}", request);

        // check driver does exists or in deleting, deleted or dead status
        DriverMetadata driver;
        try {
            DriverKey_Thrift driverKey = request.getDriverKey();
            driver = driverStore.get(driverKey.getDriverContainerId(), driverKey.getVolumeId(),
                    DriverType.valueOf(driverKey.getDriverType().name()), driverKey.getSnapshotId());
        } catch (Exception e) {
            throw new DriverNotFoundException_Thrift();
        }

        if (driver == null) {
            throw new DriverNotFoundException_Thrift();
        }

        if (driver.getDriverStatus() == DriverStatus.REMOVING) {
            throw new IscsiBeingDeletedException_Thrift();
        }

        ApplyIscsiAccessRulesResponse response = new ApplyIscsiAccessRulesResponse(request.getRequestId());

        List<Long> toApplyRuleIdList = request.getRuleIds();
        if (toApplyRuleIdList == null || toApplyRuleIdList.isEmpty()) {
            logger.debug("No rule exists in applying request, do nothing");
            return response;
        }

        String exceptionDetail = new String();
        for (long toApplyRuleId : toApplyRuleIdList) {
            IscsiAccessRule iscsiAccessRule = new IscsiAccessRule(iscsiAccessRuleStore.get(toApplyRuleId));

            // unable to apply a deleting access rule to iscsi
            if (iscsiAccessRule.getStatus() == AccessRuleStatus.DELETING) {
                logger.debug("The iscsi access rule {} is deleting now, unable to apply this rule to driver {}",
                        iscsiAccessRule, request.getDriverKey());
                response.addToAirAccessRuleList(RequestResponseHelper.buildIscsiAccessRuleThriftFrom(iscsiAccessRule));
                continue;
            }

            // turn relationship info get from db to relationship structure
            List<Iscsi2AccessRuleRelationship> relationshipList = new ArrayList<>();
            List<IscsiRuleRelationshipInformation> relationshipInfoList = iscsiRuleRelationshipStore
                    .getByRuleId(toApplyRuleId);
            if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
                for (IscsiRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
                    Iscsi2AccessRuleRelationship relationship = new Iscsi2AccessRuleRelationship(relationshipInfo);
                    relationshipList.add(relationship);
                }
            }
            /*
             * Check if the relationship exists in db. If it doesn't, in the case, the relationship is a fresh one, we
             * should set its status to "free"; Otherwise, keep the status of relationship get from db.
             */
            DriverKey_Thrift driverKey = request.getDriverKey();
            boolean existInRelationshipStore = false;
            Iscsi2AccessRuleRelationship relationship2Apply = new Iscsi2AccessRuleRelationship();
            for (Iscsi2AccessRuleRelationship relationship : relationshipList) {
                if (relationship.getDriverContainerId() == driverKey.getDriverContainerId()
                        && relationship.getVolumeId() == driverKey.getVolumeId()
                        && relationship.getSnapshotId() == driverKey.getSnapshotId() && relationship.getDriverType()
                        .equals(driverKey.getDriverType().name())) {
                    existInRelationshipStore = true;
                    relationship2Apply = relationship;
                }
            }
            if (!existInRelationshipStore) {
                logger.debug("The rule {} is not being applied to the driver {} before", iscsiAccessRule,
                        request.getDriverKey());
                relationship2Apply = new Iscsi2AccessRuleRelationship();
                relationship2Apply.setRelationshipId(RequestIdBuilder.get());
                relationship2Apply.setRuleId(toApplyRuleId);
                relationship2Apply.setDriverContainerId(driverKey.getDriverContainerId());
                relationship2Apply.setDriverType(driverKey.getDriverType().name());
                relationship2Apply.setSnapshotId(driverKey.getSnapshotId());
                relationship2Apply.setVolumeId(driverKey.getVolumeId());
                relationship2Apply.setStatus(AccessRuleStatusBindingVolume.FREE);
                relationshipList.add(relationship2Apply);
            }
            /*
             * Turn all relationship to a proper status base on state machine below.
             * A state machine to transfer status of relationship to proper status.
            */
            switch (relationship2Apply.getStatus()) {
            case FREE:
                if (request.isCommit()) {
                    relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
                } else {
                    relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLING);
                }
                // status of relationship has changed, save it to db
                logger.debug("FREE, save {}", relationship2Apply.getStatus());
                iscsiRuleRelationshipStore.save(relationship2Apply.toIscsiRuleRelationshipInformation());
                break;
            case APPLING:
                if (request.isCommit()) {
                    relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
                    logger.debug("APPLYING , save {}", relationship2Apply.getStatus());
                    iscsiRuleRelationshipStore.save(relationship2Apply.toIscsiRuleRelationshipInformation());
                }
                break;
            case APPLIED:
                logger.debug("APPLIED, DO NOTHING");
                // do nothing
                break;
            case CANCELING:
                logger.debug("CANCELING, ERROR");
                IscsiAccessRule_Thrift iscsiAccessRuleToRemote = RequestResponseHelper
                        .buildIscsiAccessRuleThriftFrom(iscsiAccessRule);
                iscsiAccessRuleToRemote
                        .setStatus(AccessRuleStatus_Thrift.valueOf(relationship2Apply.getStatus().name()));
                response.addToAirAccessRuleList(iscsiAccessRuleToRemote);
                break;
            default:
                logger.warn("Unknown status {}", relationship2Apply.getStatus());
                break;
            }
        }

        logger.warn("applyIscsiAccessRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#cancelIscsiAccessRules(CancelIscsiAccessRulesRequest)} which to cancel access
     * rule from specified iscsi in request.
     *
     * @param {@link CancelIscsiAccessRulesRequest} request
     * @return {@link CancelIscsiAccessRulesResponse}
     * @author zjm
     */
    @Override
    public CancelIscsiAccessRulesResponse cancelIscsiAccessRules(CancelIscsiAccessRulesRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, AccessRuleNotApplied_Thrift,
            TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("cancelIscsiAccessRules request: {}", request);

        CancelIscsiAccessRulesResponse response = new CancelIscsiAccessRulesResponse(request.getRequestId());

        if (request.getRuleIds() == null || request.getRuleIdsSize() == 0) {
            return response;
        }

        for (long toCancelRuleId : request.getRuleIds()) {

            IscsiAccessRuleInformation iscsiAccessRuleInformation = iscsiAccessRuleStore.get(toCancelRuleId);
            if (iscsiAccessRuleInformation == null) {
                continue;
            }
            IscsiAccessRule iscsiAccessRule = new IscsiAccessRule(iscsiAccessRuleInformation);
            // unable cancel deleting access rule from volume
            if (iscsiAccessRule.getStatus() == AccessRuleStatus.DELETING) {
                logger.debug("The iscsi access rule {} is deleting now, unable to cancel this rule from drivers {}",
                        iscsiAccessRule, request.getDriverKey());
                response.addToAirAccessRuleList(RequestResponseHelper.buildIscsiAccessRuleThriftFrom(iscsiAccessRule));
                continue;
            }
            // turn relationship info got from db to relationship structure
            List<IscsiRuleRelationshipInformation> relationshipInfoList = iscsiRuleRelationshipStore
                    .getByRuleId(toCancelRuleId);
            Iscsi2AccessRuleRelationship relationship = null;
            if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
                for (IscsiRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
                    DriverKey_Thrift driverKey = request.getDriverKey();
                    if (relationshipInfo.getDriverContainerId() == driverKey.getDriverContainerId()
                            && relationshipInfo.getVolumeId() == driverKey.getVolumeId()
                            && relationshipInfo.getSnapshotId() == driverKey.getSnapshotId() && relationshipInfo
                            .getDriverType().equals(driverKey.getDriverType().name())) {
                        relationship = new Iscsi2AccessRuleRelationship(relationshipInfo);
                        break;
                    }
                }
            }
            if (relationship == null) {
                throw new AccessRuleNotApplied_Thrift();
            }
            /*
             * turn status of relationship to proper status after action "cancel"
             */
            switch (relationship.getStatus()) {
            case FREE:
                // do nothingin
                break;
            case APPLING:
                IscsiAccessRule_Thrift iscsiAccessRuleToRemote = RequestResponseHelper
                        .buildIscsiAccessRuleThriftFrom(iscsiAccessRule);
                iscsiAccessRuleToRemote.setStatus(AccessRuleStatus_Thrift.valueOf(relationship.getStatus().name()));
                response.addToAirAccessRuleList(iscsiAccessRuleToRemote);
                break;
            case APPLIED:
                if (request.isCommit()) {
                    iscsiRuleRelationshipStore.deleteByRuleIdandDriverKey(request.getDriverKey(), toCancelRuleId);
                } else {
                    relationship.setStatus(AccessRuleStatusBindingVolume.CANCELING);
                    iscsiRuleRelationshipStore.save(relationship.toIscsiRuleRelationshipInformation());
                }
                break;
            case CANCELING:
                if (request.isCommit()) {
                    iscsiRuleRelationshipStore.deleteByRuleIdandDriverKey(request.getDriverKey(), toCancelRuleId);
                }
                break;
            default:
                logger.warn("unknown status {} of relationship", relationship.getStatus());
                break;
            }
        }
        logger.debug("cancelIscsiAccessRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * which apply specified access rule to specified drivers in request.
     */
    @Override
    public ApplyIscsiAccessRuleOnIscsisResponse applyIscsiAccessRuleOnIscsis(
            ApplyIscsiAccessRuleOnIscsisRequest request)
            throws IscsiNotFoundException_Thrift, IscsiBeingDeletedException_Thrift, ServiceIsNotAvailable_Thrift,
            ApplyFailedDueToConflictException_Thrift, IscsiAccessRuleUnderOperation_Thrift,
            IscsiAccessRuleNotFound_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("applyIscsiAccessRuleOnIscsis request: {}", request);

        ApplyIscsiAccessRuleOnIscsisResponse response = new ApplyIscsiAccessRuleOnIscsisResponse(
                request.getRequestId());
        if (request.getDriverKeys() == null || request.getDriverKeysSize() == 0) {
            logger.error("given drive is null");
            return response;
        }

        long toApplyRuleId = request.getRuleId();
        IscsiAccessRuleInformation iscsiAccessRuleInformation = iscsiAccessRuleStore.get(toApplyRuleId);
        if (iscsiAccessRuleInformation == null) {
            logger.error("The iscsi access rule not found error");
            throw new IscsiAccessRuleNotFound_Thrift();
        }
        IscsiAccessRule iscsiAccessRule = new IscsiAccessRule(iscsiAccessRuleInformation);
        // unable cancel deleting access rule from volume
        if (iscsiAccessRule.getStatus() == AccessRuleStatus.DELETING) {
            logger.error("The iscsi access rule {} is deleting now, unable to cancel", iscsiAccessRule);
            throw new IscsiAccessRuleUnderOperation_Thrift();
        }

        // turn relationship info get from db to relationship structure
        List<Iscsi2AccessRuleRelationship> relationshipList = new ArrayList<>();
        List<IscsiRuleRelationshipInformation> relationshipInfoList = iscsiRuleRelationshipStore
                .getByRuleId(toApplyRuleId);
        if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
            for (IscsiRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
                Iscsi2AccessRuleRelationship relationship = new Iscsi2AccessRuleRelationship(relationshipInfo);
                relationshipList.add(relationship);
            }
        }

        /*
        * Check if the relationship exists in db. If it doesn't, in the case, the relationship is a fresh one, we
        * should set its status to "free"; Otherwise, keep the status of relationship get from db.
        */
        for (DriverKey_Thrift driverKey : request.getDriverKeys()) {
            // check driver does exists or in deleting, deleted or dead status
            DriverMetadata driver = null;
            try {
                driver = driverStore.get(driverKey.getDriverContainerId(), driverKey.getVolumeId(),
                        DriverType.valueOf(driverKey.getDriverType().name()), driverKey.getSnapshotId());
            } catch (Exception e) {
                logger.error("get driverKey exception", e);
            }
            if (driver == null) {
                //throw new DriverNotFoundException_Thrift();
                logger.error("driverKey {} not in driverStore", driverKey);
                response.addToAirDriverKeyList(driverKey);
                continue;
            }
            if (driver.getDriverStatus() == DriverStatus.REMOVING) {
                //throw new IscsiBeingDeletedException_Thrift();
                logger.error("driverKey {} is removing", driverKey);
                response.addToAirDriverKeyList(driverKey);
                continue;
            }

            boolean existInRelationshipStore = false;
            Iscsi2AccessRuleRelationship relationship2Apply = new Iscsi2AccessRuleRelationship();
            for (Iscsi2AccessRuleRelationship relationship : relationshipList) {
                if (relationship.getDriverContainerId() == driverKey.getDriverContainerId()
                        && relationship.getVolumeId() == driverKey.getVolumeId()
                        && relationship.getSnapshotId() == driverKey.getSnapshotId() && relationship.getDriverType()
                        .equals(driverKey.getDriverType().name())) {
                    existInRelationshipStore = true;
                    relationship2Apply = relationship;
                }
            }
            if (!existInRelationshipStore) {
                logger.debug("The rule {} is not being applied to driver {} before", iscsiAccessRule, driverKey);
                relationship2Apply = new Iscsi2AccessRuleRelationship();
                relationship2Apply.setRelationshipId(RequestIdBuilder.get());
                relationship2Apply.setRuleId(toApplyRuleId);
                relationship2Apply.setDriverContainerId(driverKey.getDriverContainerId());
                relationship2Apply.setDriverType(driverKey.getDriverType().name());
                relationship2Apply.setSnapshotId(driverKey.getSnapshotId());
                relationship2Apply.setVolumeId(driverKey.getVolumeId());
                relationship2Apply.setStatus(AccessRuleStatusBindingVolume.FREE);
                relationshipList.add(relationship2Apply);
            }
            /*
             * Turn all relationship to a proper status base on state machine below.
             * A state machine to transfer status of relationship to proper status.
            */
            switch (relationship2Apply.getStatus()) {
            case FREE:
                if (request.isCommit()) {
                    relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
                } else {
                    relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLING);
                }
                // status of relationship has changed, save it to db
                logger.debug("FREE, save {}", relationship2Apply.getStatus());
                iscsiRuleRelationshipStore.save(relationship2Apply.toIscsiRuleRelationshipInformation());
                break;
            case APPLING:
                if (request.isCommit()) {
                    relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
                    logger.debug("APPLYING , save {}", relationship2Apply.getStatus());
                    iscsiRuleRelationshipStore.save(relationship2Apply.toIscsiRuleRelationshipInformation());
                }
                break;
            case APPLIED:
                logger.debug("APPLIED, DO NOTHING");
                // do nothing
                break;
            case CANCELING:
                logger.debug("CANCELING, ERROR");
                IscsiAccessRule_Thrift iscsiAccessRuleToRemote = RequestResponseHelper
                        .buildIscsiAccessRuleThriftFrom(iscsiAccessRule);
                iscsiAccessRuleToRemote
                        .setStatus(AccessRuleStatus_Thrift.valueOf(relationship2Apply.getStatus().name()));
                response.addToAirDriverKeyList(driverKey);
                break;
            default:
                logger.warn("Unknown status {}", relationship2Apply.getStatus());
                break;
            }
        }
        logger.warn("applyIscsiAccessRuleOnIscsis response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * which to cancel access
     * rule from specified iscsi in request.
     */
    @Override
    public CancelIscsiAccessRuleAllAppliedResponse cancelIscsiAccessRuleAllApplied(
            CancelIscsiAccessRuleAllAppliedRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, AccessRuleNotApplied_Thrift,
            IscsiAccessRuleUnderOperation_Thrift, IscsiAccessRuleNotFound_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("cancelIscsiAccessRuleAllApplied request: {}", request);
        CancelIscsiAccessRuleAllAppliedResponse response = new CancelIscsiAccessRuleAllAppliedResponse(
                request.getRequestId());

        if (request.getDriverKeys() == null || request.getDriverKeysSize() == 0) {
            logger.error("given drive is null");
            return response;
        }

        long toCancelRuleId = request.getRuleId();
        IscsiAccessRuleInformation iscsiAccessRuleInformation = iscsiAccessRuleStore.get(toCancelRuleId);
        if (iscsiAccessRuleInformation == null) {
            logger.error("The iscsi access rule not found error");
            throw new IscsiAccessRuleNotFound_Thrift();
        }
        IscsiAccessRule iscsiAccessRule = new IscsiAccessRule(iscsiAccessRuleInformation);
        // unable cancel deleting access rule from volume
        if (iscsiAccessRule.getStatus() == AccessRuleStatus.DELETING) {
            logger.error("The iscsi access rule {} is deleting now, unable to cancel", iscsiAccessRule);
            throw new IscsiAccessRuleUnderOperation_Thrift();
        }
        // turn relationship info got from db to relationship structure
        List<IscsiRuleRelationshipInformation> relationshipInfoList = iscsiRuleRelationshipStore
                .getByRuleId(toCancelRuleId);

        for (DriverKey_Thrift driverKey : request.getDriverKeys()) {
            Iscsi2AccessRuleRelationship relationship = null;
            if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
                for (IscsiRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
                    if (relationshipInfo.getDriverContainerId() == driverKey.getDriverContainerId()
                            && relationshipInfo.getVolumeId() == driverKey.getVolumeId()
                            && relationshipInfo.getSnapshotId() == driverKey.getSnapshotId() && relationshipInfo
                            .getDriverType().equals(driverKey.getDriverType().name())) {
                        relationship = new Iscsi2AccessRuleRelationship(relationshipInfo);
                        break;
                    }
                }
            }
            if (relationship == null) {
                logger.error("access rule not applied");
                throw new AccessRuleNotApplied_Thrift();
            }

            // turn status of relationship to proper status after action "cancel"
            switch (relationship.getStatus()) {
            case FREE:
                // do nothingin
                break;
            case APPLING:
                IscsiAccessRule_Thrift iscsiAccessRuleToRemote = RequestResponseHelper
                        .buildIscsiAccessRuleThriftFrom(iscsiAccessRule);
                iscsiAccessRuleToRemote.setStatus(AccessRuleStatus_Thrift.valueOf(relationship.getStatus().name()));
                response.addToAirDriverKeyList(driverKey);
                break;
            case APPLIED:
                if (request.isCommit()) {
                    iscsiRuleRelationshipStore.deleteByRuleIdandDriverKey(driverKey, toCancelRuleId);
                } else {
                    relationship.setStatus(AccessRuleStatusBindingVolume.CANCELING);
                    iscsiRuleRelationshipStore.save(relationship.toIscsiRuleRelationshipInformation());
                }
                break;
            case CANCELING:
                if (request.isCommit()) {
                    iscsiRuleRelationshipStore.deleteByRuleIdandDriverKey(driverKey, toCancelRuleId);
                }
                break;
            default:
                logger.warn("unknown status {} of relationship", relationship.getStatus());
                break;
            }
        }

        logger.warn("cancelIscsiAccessRuleAllApplied response: {}", response);
        return response;
    }

    /**
     * when umount driver should delete the relationship with iscsiAccessRules
     */
    @Override
    public CancelDriversRulesResponse cancelDriversRules(CancelDriversRulesRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("CancelDriversRules request: {}", request);

        CancelDriversRulesResponse response = new CancelDriversRulesResponse(request.getRequestId());

        if (request.getDriverKey() == null) {
            return response;
        }

        try {
            //TODO should not use thrift internal
            iscsiRuleRelationshipStore.deleteByDriverKey(request.getDriverKey());
        } catch (Exception e) {
            logger.error("iscsiRuleRelationship deleteByDriverKey: {} failed", request);
            throw e;
        }

        logger.debug("CancelDriversRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#listVolumeAccessRules(ListVolumeAccessRulesRequest)} which list all created access
     * rules.
     *
     * @param {@link ListVolumeAccessRulesRequest} request
     * @return {@link ListVolumeAccessRulesResponse}
     */
    @Override
    public ListIscsiAccessRulesResponse listIscsiAccessRules(ListIscsiAccessRulesRequest request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("listIscsiAccessRules request: {}", request);

        ListIscsiAccessRulesResponse listIscsiAccessRulesResponse = new ListIscsiAccessRulesResponse();
        listIscsiAccessRulesResponse.setRequestId(request.getRequestId());

        List<IscsiAccessRuleInformation> iscsiAccessRuleInformations = iscsiAccessRuleStore.list();
        if (iscsiAccessRuleInformations != null && iscsiAccessRuleInformations.size() > 0) {
            for (IscsiAccessRuleInformation iscsiAccessRuleInformation : iscsiAccessRuleInformations) {
                listIscsiAccessRulesResponse.addToAccessRules(RequestResponseHelper
                        .buildIscsiAccessRuleThriftFrom(new IscsiAccessRule(iscsiAccessRuleInformation)));
            }
        }
        logger.warn("listVolumeAccessRules response: {}", listIscsiAccessRulesResponse);
        return listIscsiAccessRulesResponse;
    }

    @Override
    public ListIscsiAccessRulesByDriverKeysResponse listIscsiAccessRulesByDriverKeys(
            ListIscsiAccessRulesByDriverKeysRequest request) throws ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("ListIscsiAccessRulesByDriverKeysRequest request: {}", request);

        ListIscsiAccessRulesByDriverKeysResponse listIscsiAccessRulesByDriverKeysResponse = new ListIscsiAccessRulesByDriverKeysResponse();
        listIscsiAccessRulesByDriverKeysResponse.setRequestId(request.getRequestId());
        Set<DriverKey_Thrift> driverKeys = request.getDriverKeys();

        List<IscsiRuleRelationshipInformation> iscsiRuleRelationshipInformationList = iscsiRuleRelationshipStore.list();
        List<IscsiAccessRuleInformation> iscsiAccessRuleInformations = iscsiAccessRuleStore.list();

        for (DriverKey_Thrift driverKey : driverKeys) {
            List<IscsiAccessRule_Thrift> iscsiAccessRule_thriftList = new ArrayList<>();
            for (IscsiRuleRelationshipInformation iscsiRule : iscsiRuleRelationshipInformationList) {
                if (driverKey.getDriverContainerId() == iscsiRule.getDriverContainerId()
                        && driverKey.getVolumeId() == iscsiRule.getVolumeId() && driverKey.getSnapshotId() == iscsiRule
                        .getSnapshotId() && driverKey.getDriverType().name().equals(iscsiRule.getDriverType())) {
                    Long ruleId = iscsiRule.getRuleId();
                    for (IscsiAccessRuleInformation rule : iscsiAccessRuleInformations) {
                        if (ruleId == rule.getRuleId()) {
                            iscsiAccessRule_thriftList.add(RequestResponseHelper
                                    .buildIscsiAccessRuleThriftFrom(new IscsiAccessRule(rule)));
                        }
                    }
                }
            }
            listIscsiAccessRulesByDriverKeysResponse.putToAccessRulesTable(driverKey, iscsiAccessRule_thriftList);
        }

        logger.warn("ListIscsiAccessRulesByDriverKeysResponse response: {}", listIscsiAccessRulesByDriverKeysResponse);
        return listIscsiAccessRulesByDriverKeysResponse;
    }

    /**
     * when move
     * volume online
     * first create
     * access rule for new volume
     */

    @Override
    public CreateAccessRuleOnNewVolumeResponse createAccessRuleOnNewVolume(CreateAccessRuleOnNewVolumeRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, InvalidInputException_Thrift,
            VolumeNotFoundException_Thrift, VolumeNameExistedException_Thrift, TException {
        logger.warn("UpdateAccessRuleWhenMoveVolume {}", request);
        if (shutDownFlag) {
            logger.warn("Cannot deal with any request due to service driver container is being shutdown ...");
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        CreateAccessRuleOnNewVolumeResponse response = new CreateAccessRuleOnNewVolumeResponse();
        response.setRequestId(request.getRequestId());
        long oldVolId = request.getDriver().getVolumeId();
        long newVolId = request.getNewVolumeId();
        // iscsi access rule
        List<IscsiRuleRelationshipInformation> relationshipInfoList = iscsiRuleRelationshipStore
                .getByDriverKey(request.getDriver());
        if (relationshipInfoList == null || relationshipInfoList.size() == 0) {
            logger.debug("The driver {} hasn't be applied any access rules", request.getDriver());
        }
        for (IscsiRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
            relationshipInfo.setRelationshipId(RequestIdBuilder.get());
            relationshipInfo.setVolumeId(newVolId);
            iscsiRuleRelationshipStore.save(relationshipInfo);
        }
        // volume access rule
        List<VolumeRuleRelationshipInformation> volRelationshipInfoList = volumeRuleRelationshipStore
                .getByVolumeId(oldVolId);
        if (relationshipInfoList == null || relationshipInfoList.size() == 0) {
            logger.debug("The volume {} hasn't be applied any access rules");
        }
        for (VolumeRuleRelationshipInformation relationshipInfo : volRelationshipInfoList) {
            relationshipInfo.setRelationshipId(RequestIdBuilder.get());
            relationshipInfo.setVolumeId(newVolId);
            volumeRuleRelationshipStore.save(relationshipInfo);
        }
        return response;
    }

    /* when move volume online delete access rule for old volume when delete old volume*/
    @Override
    public DeleteAccessRuleOnOldVolumeResponse deleteAccessRuleOnOldVolume(DeleteAccessRuleOnOldVolumeRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, InvalidInputException_Thrift,
            VolumeNotFoundException_Thrift, VolumeNameExistedException_Thrift, TException {
        logger.warn("UpdateAccessRuleWhenMoveVolume {}", request);
        if (shutDownFlag) {
            logger.warn("Cannot deal with any request due to service driver container is being shutdown ...");
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        DeleteAccessRuleOnOldVolumeResponse response = new DeleteAccessRuleOnOldVolumeResponse();
        response.setRequestId(request.getRequestId());
        DriverKey driverKey = RequestResponseHelper.buildDriverKeyFrom(request.getDriver());
        long oldVolId = request.getOldVolumeId();
        try {
            // iscsi access rule
            iscsiRuleRelationshipStore.deleteByDriverKey(request.getDriver());
        } catch (Exception e) {
            logger.error("deleteAccessRuleOnOldVolume deleteByDriverKey: {} failed", request);
            throw e;
        }
        try {
            // volume access rule
            volumeRuleRelationshipStore.deleteByVolumeId(oldVolId);
        } catch (Exception e) {
            logger.error("deleteAccessRuleOnOldVolume deleteByVolumeId: {} failed", request);
            throw e;
        }
        return response;
    }

    @Override
    public GetAppliedIscsisResponse getAppliedIscsis(GetAppliedIscsisRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getAppliedIscsis request: {}", request);
        GetAppliedIscsisResponse response = new GetAppliedIscsisResponse();
        response.setRequestId(request.getRequestId());
        response.setDriverList(new ArrayList<>());

        List<IscsiRuleRelationshipInformation> relationshipInfoList = iscsiRuleRelationshipStore
                .getByRuleId(request.getRuleId());
        if (relationshipInfoList == null || relationshipInfoList.isEmpty()) {
            return response;
        }

        DriverMetadata driverMetadata = null;
        for (IscsiRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
            DriverKey_Thrift driverKey = new DriverKey_Thrift(relationshipInfo.getDriverContainerId(),
                    relationshipInfo.getVolumeId(), relationshipInfo.getSnapshotId(),
                    DriverType_Thrift.valueOf(relationshipInfo.getDriverType()));
            try {
                driverMetadata = driverStore
                        .get(relationshipInfo.getDriverContainerId(), relationshipInfo.getVolumeId(),
                                DriverType.findByName(relationshipInfo.getDriverType()),
                                relationshipInfo.getSnapshotId());
            } catch (Exception e) {
                logger.error("Caught an exception", e);
            }

            if (driverMetadata == null) {
                logger.warn("volume is not exist,delete it from rule relation ship table,{}",
                        relationshipInfo.getVolumeId());
                //                iscsiRuleRelationshipStore.deleteByDriverKey(driverKey);
                continue;
            }
            response.addToDriverList(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
            //response.addToDriverKeyList(new DriverKey_Thrift(driverKey));
        }

        logger.warn("getAppliedIscsis response: {}", response);
        return response;
    }
    // ****** Access Rules for Iscsi End***********/

    //********** Driver Qos begin*****************/

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#createIOLimitations(CreateIOLimitationsRequest)} which create IOLimitations.
     *
     * @param {@link CreateIOLimitationsRequest} request
     * @return {@link CreateIOLimitationsResponse}
     */
    @Override
    public synchronized CreateIOLimitationsResponse createIOLimitations(CreateIOLimitationsRequest request)
            throws ServiceIsNotAvailable_Thrift, InvalidInputException_Thrift, ServiceHavingBeenShutdown_Thrift,
            IOLimitationTimeInterLeaving_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("createIOLimitations request: {}", request);

        CreateIOLimitationsResponse response = new CreateIOLimitationsResponse(request.getRequestId());
        if (request.getIoLimitation() == null) {
            logger.error("IoLimitation null error");
            throw new InvalidInputException_Thrift();
        }

        if (request.getIoLimitation().getEntries() == null) {
            logger.error("IoLimitationEntry null error");
            throw new InvalidInputException_Thrift();
        }

        if (request.getIoLimitation().getLimitType() == LimitType_Thrift.Static
                && request.getIoLimitation().getEntries().size() != 1) {
            logger.error("IoLimitationEntry too many when static error");
            throw new InvalidInputException_Thrift();
        }

        IOLimitation_Thrift ioLimitation_Thrift = request.getIoLimitation();
        IOLimitation toIOLimitation = RequestResponseHelper.buildIOLimitationFrom(ioLimitation_Thrift);

        //check internal timezone
        boolean conflict = RequestResponseHelper.judgeDynamicIOLimitationTimeInterleaving(toIOLimitation);
        if (conflict) {
            logger.error("createIOLimitations timeInterLeaving fail!");
            throw new IOLimitationTimeInterLeaving_Thrift();
        }

        List<IOLimitationInformation> ioLimitationFromDB = ioLimitationStore.list();
        // checkout each of the rules that were created by the user to find out
        // which rule is already existed in the system
        boolean ioLimitationExisted = false;
        for (IOLimitationInformation rule : ioLimitationFromDB) {
            IOLimitation limitation = new IOLimitation(rule);
            if (limitation.getLimitType() == toIOLimitation.getLimitType() && limitation.getId() == toIOLimitation
                    .getId() && limitation.getName().equals(toIOLimitation.getName()) && listContentEquals(
                    limitation.getEntries(), toIOLimitation.getEntries())) {
                ioLimitationExisted = true;
                break;
            } else {
                // do nothing
            }
        }
        if (!ioLimitationExisted) {
            IOLimitation ioLimitation = RequestResponseHelper.buildIOLimitationFrom(ioLimitation_Thrift);
            ioLimitation.setStatus(IOLimitationStatus.AVAILABLE);
            logger.debug("ioLimitation {}-{}", ioLimitation, ioLimitation.toIOLimitationInformation(ioLimitationStore));
            ioLimitationStore.save(ioLimitation.toIOLimitationInformation(ioLimitationStore));
        } else {
            // throw an exception out
            logger.error("information center throws an exception IOLimitationsDuplicate_Thrift");
            throw new IOLimitationsDuplicate_Thrift();
        }

        logger.warn("createIOLimitations response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#updateIOLimitations(UpdateIOLimitationRulesRequest)} which create IOLimitations.
     *
     * @param {@link UpdateIOLimitationRulesRequest} request
     * @return {@link UpdateIOLimitationsResponse}
     */
    @Override
    public synchronized UpdateIOLimitationsResponse updateIOLimitations(UpdateIOLimitationRulesRequest request)
            throws ServiceIsNotAvailable_Thrift, InvalidInputException_Thrift, IOLimitationsNotExists,
            IOLimitationTimeInterLeaving_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("updateIOLimitations request: {}", request);

        UpdateIOLimitationsResponse response = new UpdateIOLimitationsResponse(request.getRequestId());
        if (request.getIoLimitation() == null) {
            logger.debug("No access rule to create in request, nothing to do");
            return response;
        }

        IOLimitation_Thrift ioLimitation_Thrift = request.getIoLimitation();
        IOLimitation toIOLimitaion = RequestResponseHelper.buildIOLimitationFrom(request.getIoLimitation());

        // old from db
        IOLimitationInformation ioLimitationInformation = ioLimitationStore.get(ioLimitation_Thrift.getLimitationId());
        if (ioLimitationInformation == null) {
            logger.error("No IOLimitations with id {}", ioLimitation_Thrift.getLimitationId());
            throw new IOLimitationsNotExists();
        }

        //check internal timezone
        boolean conflict = RequestResponseHelper.judgeDynamicIOLimitationTimeInterleaving(toIOLimitaion);
        if (conflict) {
            logger.error("updateIOLimitations timeInterLeaving fail!");
            throw new IOLimitationTimeInterLeaving_Thrift();
        }

        List<IOLimitationInformation> ioLimitationFromDB = ioLimitationStore.list();
        // checkout each of the rules that were created by the user to find out
        // which rule is already existed in the system
        boolean ioLimitationExisted = false;
        for (IOLimitationInformation rule : ioLimitationFromDB) {
            IOLimitation limitation = new IOLimitation(rule);
            if (limitation.getLimitType() == toIOLimitaion.getLimitType() && limitation.getId() == toIOLimitaion.getId()
                    && limitation.getName() == toIOLimitaion.getName() && listContentEquals(limitation.getEntries(),
                    toIOLimitaion.getEntries())) {
                ioLimitationExisted = true;
                break;
            } else {
                // do nothing
            }
        }
        if (!ioLimitationExisted) {
            logger.debug("updateIOLimitations {} to {}", ioLimitationInformation, toIOLimitaion);
            toIOLimitaion.setStatus(IOLimitationStatus.AVAILABLE);
            ioLimitationStore.update(toIOLimitaion.toIOLimitationInformation(ioLimitationStore));
        } else {
            // throw an exception out
            logger.error("information center throws an exception IOLimitationsDuplicate_Thrift");
            throw new IOLimitationsDuplicate_Thrift();
        }

        logger.warn("UpdateIOLimitationsResponse response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#deleteIOLimitations(DeleteIOLimitationsRequest)} which list all IOLimitations.
     *
     * @param {@link DeleteIOLimitationsRequest} request
     * @return {@link DeleteIOLimitationsResponse}
     */
    @Override
    public DeleteIOLimitationsResponse deleteIOLimitations(DeleteIOLimitationsRequest request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("deleteIOLimitations request: {}", request);
        DeleteIOLimitationsResponse response = new DeleteIOLimitationsResponse();
        response.setRequestId(request.getRequestId());
        response.setDeletedIOLimitationNames(new ArrayList<>());
        if (request.getRuleIds() == null || request.getRuleIds().size() == 0) {
            logger.debug("No IOLimitations existing in request to delete");
            return response;
        }

        // get all drivers
        List<DriverMetadata> driverMetadataList = driverStore.list();

        for (long ruleId : request.getRuleIds()) {
            IOLimitationInformation ioLimitationInformation = ioLimitationStore.get(ruleId);
            if (ioLimitationInformation == null) {
                logger.debug("No IOLimitations with id {}", ruleId);
                continue;
            }
            IOLimitation ioLimitation = new IOLimitation(ioLimitationInformation);

            // unable to apply a deleting migrationSpeed
            if (ioLimitation.getStatus() == IOLimitationStatus.DELETING) {
                logger.warn("The ioLimitation {} is deleting now, unable to apply this rule to driver", ioLimitation);
                response.addToAirIOLimitationList(RequestResponseHelper.buildThriftIOLimitationFrom(ioLimitation));
                continue;
            }

            if (!request.isCommit()) {
                if (ioLimitation.getStatus() != IOLimitationStatus.DELETING) {
                    ioLimitation.setStatus(IOLimitationStatus.DELETING);
                    ioLimitationStore.save(ioLimitation.toIOLimitationInformation(ioLimitationStore));
                }
                continue;
            }
            // update driverStore
            for (DriverMetadata driverMetadataToBeUpdate : driverMetadataList) {
                if (ioLimitation.getLimitType() == IOLimitation.LimitType.Dynamic) {
                    if (driverMetadataToBeUpdate.getDynamicIOLimitationId() == ruleId) {
                        logger.warn("deleteIOLimitations Dynamic {} on {}", ioLimitation, driverMetadataToBeUpdate);
                        driverMetadataToBeUpdate.setDynamicIOLimitationId(0);
                        driverStore.save(driverMetadataToBeUpdate);
                    }
                } else {
                    if (driverMetadataToBeUpdate.getStaticIOLimitationId() == ruleId) {
                        logger.warn("deleteIOLimitations static {} on {}", ioLimitation, driverMetadataToBeUpdate);
                        driverMetadataToBeUpdate.setStaticIOLimitationId(0);
                        driverStore.save(driverMetadataToBeUpdate);
                    }
                }
            }
            // delete ioLimitation
            ioLimitationStore.delete(ruleId);
            response.addToDeletedIOLimitationNames(ioLimitation.getName());
        }
        logger.warn("deleteIOLimitations response: {}", response);
        return response;
    }

    /*
     * An implementation of interface
     * {@link InformationCenter.Iface#applyIOLimitations(ApplyIOLimitationsRequest)} which list all IOLimitations.
     * @param {@link ApplyIOLimitationsRequest} request
     * @return {@link ApplyIOLimitationsResponse}
     */
    @Override
    public ApplyIOLimitationsResponse applyIOLimitations(ApplyIOLimitationsRequest request)
            throws ServiceIsNotAvailable_Thrift, TException {

        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("applyIOLimitations request: {}", request);

        ApplyIOLimitationsResponse response = new ApplyIOLimitationsResponse();
        response.setRequestId(request.getRequestId());
        long toApplyRuleId = request.getRuleId();
        if (toApplyRuleId == 0) {
            logger.debug("default rule, do nothing");
        }

        logger.warn("toApplyRuleId {} {}", toApplyRuleId, ioLimitationStore.get(toApplyRuleId));
        IOLimitation ioLimitation = new IOLimitation(ioLimitationStore.get(toApplyRuleId));
        if (ioLimitation.getStatus() == IOLimitationStatus.DELETING) {
            logger.debug("The IoLimitation {} is deleting now, unable to apply this rule to drivers {}", ioLimitation,
                    request.getDriverKeys());
            throw new IOLimitationIsDeleting();
        }
        response.setIoLimitationName(ioLimitation.getName());
        response.setAppliedDriverNames(new ArrayList<>());

        // check driver does exists or in deleting, deleted or dead status
        List<DriverKey_Thrift> driverKeys = request.getDriverKeys();
        for (DriverKey_Thrift driverKey : driverKeys) {
            DriverMetadata driverMetadata = driverStore.get(driverKey.getDriverContainerId(), driverKey.getVolumeId(),
                    DriverType.valueOf(driverKey.getDriverType().name()), driverKey.getSnapshotId());
            if (driverMetadata == null) {
                throw new DriverNotFoundException_Thrift();
            }
            if (driverMetadata.getDriverStatus() == DriverStatus.REMOVING) {
                throw new DriverIsLaunchingException_Thrift();
            }

            if (ioLimitation.getLimitType() == IOLimitation.LimitType.Dynamic) {
                if (driverMetadata.getDynamicIOLimitationId() == 0) {//default 0 no applied
                    logger.debug("applyIOLimitations {} on {}", ioLimitation, driverMetadata);
                    driverMetadata.setDynamicIOLimitationId(ioLimitation.getId());
                    driverStore.save(driverMetadata);
                    response.addToAppliedDriverNames(driverMetadata.getDriverName());
                } else {
                    logger.error("applyIOLimitations already applied a dynamic IOLimitation");
                    response.addToAirDriverKeyList(buildThriftDriverMetadataFrom(driverMetadata));
                }
            } else {
                if (driverMetadata.getStaticIOLimitationId() == 0) {//default 0 no applied
                    logger.debug("applyIOLimitations {} on {}", ioLimitation, driverMetadata);
                    driverMetadata.setStaticIOLimitationId(ioLimitation.getId());
                    driverStore.save(driverMetadata);
                    response.addToAppliedDriverNames(driverMetadata.getDriverName());
                } else {
                    logger.error("applyIOLimitations already applied a static IOLimitation");
                    response.addToAirDriverKeyList(buildThriftDriverMetadataFrom(driverMetadata));
                }
            }

        }
        logger.warn("applyIOLimitations response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#cancelIOLimitations(CancelIOLimitationsRequest)} which list all IOLimitations.
     *
     * @param {@link CancelIOLimitationsRequest} request
     * @return {@link CancelIOLimitationsResponse}
     */
    @Override
    public CancelIOLimitationsResponse cancelIOLimitations(CancelIOLimitationsRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, AccessRuleNotApplied_Thrift,
            TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("cancelIOLimitations request: {}", request);

        CancelIOLimitationsResponse response = new CancelIOLimitationsResponse();
        response.setRequestId(request.getRequestId());
        long toCancelRuleId = request.getRuleId();
        if (toCancelRuleId == 0) {
            logger.debug("default rule, do nothing");
            return response;
        }

        IOLimitationInformation ioLimitationInformation = ioLimitationStore.get(toCancelRuleId);
        IOLimitation ioLimitation = new IOLimitation(ioLimitationInformation);
        if (ioLimitation.getStatus() == IOLimitationStatus.DELETING) {
            logger.debug("The IOLimitation {} is deleting now, unable to cancel this rule from drivers {}",
                    ioLimitation, request.getDriverKeys());
            throw new IOLimitationIsDeleting();
        }

        response.setIoLimitationName(ioLimitation.getName());
        response.setCanceledDriverNames(new ArrayList<>());

        List<DriverKey_Thrift> driverKeys = request.getDriverKeys();
        for (DriverKey_Thrift driverKey : driverKeys) {

            DriverMetadata driverMetadata = driverStore.get(driverKey.getDriverContainerId(), driverKey.getVolumeId(),
                    DriverType.valueOf(driverKey.getDriverType().name()), driverKey.getSnapshotId());
            if (driverMetadata == null) {
                throw new DriverNotFoundException_Thrift();
            }
            if (ioLimitation.getLimitType() == IOLimitation.LimitType.Dynamic) {
                if (driverMetadata.getDynamicIOLimitationId() == toCancelRuleId) {
                    logger.debug("cancelIOLimitations {} on {}", ioLimitation, driverMetadata);
                    driverMetadata.setDynamicIOLimitationId(0);
                    driverStore.save(driverMetadata);
                    response.addToCanceledDriverNames(driverMetadata.getDriverName());
                } else {
                    logger.error("cancelIOLimitations fail, {} not applied on {}", ioLimitation, driverMetadata);
                    response.addToAirDriverKeyList(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
                }
            } else {
                if (driverMetadata.getStaticIOLimitationId() == toCancelRuleId) {
                    logger.debug("cancelIOLimitations {} on {}", ioLimitation, driverMetadata);
                    driverMetadata.setStaticIOLimitationId(0);
                    driverStore.save(driverMetadata);
                    response.addToCanceledDriverNames(driverMetadata.getDriverName());
                } else {
                    logger.error("cancelIOLimitations fail, {} not applied on {}", ioLimitation, driverMetadata);
                    response.addToAirDriverKeyList(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
                }
            }
        }
        logger.debug("cancelIOLimitations response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#listIOLimitations(ListIOLimitationsRequest)} which list all IOLimitations.
     *
     * @param {@link ListIOLimitationsRequest} request
     * @return {@link ListIOLimitationsResponse}
     */
    @Override
    public ListIOLimitationsResponse listIOLimitations(ListIOLimitationsRequest request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, TException {
        if (shutDownFlag) {
            logger.error("Refuse request from remote due to shutdown, request content: {}", request);
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("listIOLimitations request: {}", request);

        ListIOLimitationsResponse listIOLimitationsResponse = new ListIOLimitationsResponse();
        listIOLimitationsResponse.setRequestId(request.getRequestId());

        List<IOLimitationInformation> ioLimitationInformations = ioLimitationStore.list();
        logger.warn("listIOLimitations ioLimitationInformations: {}", ioLimitationInformations);

        if (ioLimitationInformations != null && ioLimitationInformations.size() > 0) {
            for (IOLimitationInformation limitationInfo : ioLimitationInformations) {
                logger.debug("listIOLimitations limitationInfo {}-{} ", limitationInfo,
                        limitationInfo.getIoLimitationEntriesJson());
                listIOLimitationsResponse.addToIoLimitations(
                        RequestResponseHelper.buildThriftIOLimitationFrom(new IOLimitation(limitationInfo)));
            }
        } else {
            logger.warn("listIOLimitations no rules ");
            listIOLimitationsResponse.setIoLimitations(new ArrayList<>());
        }
        logger.warn("listIOLimitations response: {}", listIOLimitationsResponse);
        return listIOLimitationsResponse;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#listIOLimitationsByDriverKeys(ListIOLimitationsByDriverKeysRequest)} which list all IOLimitations.
     *
     * @param {@link ListIOLimitationsByDriverKeysRequest} request
     * @return {@link ListIOLimitationsByDriverKeysResponse}
     */
    @Override
    public ListIOLimitationsByDriverKeysResponse listIOLimitationsByDriverKeys(
            ListIOLimitationsByDriverKeysRequest request) throws ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("listIOLimitationsByDriverKeys request: {}", request);

        ListIOLimitationsByDriverKeysResponse listIOLimitationsByDriverKeysResponse = new ListIOLimitationsByDriverKeysResponse();
        listIOLimitationsByDriverKeysResponse.setRequestId(request.getRequestId());

        Set<DriverKey_Thrift> driverKeys = request.getDriverKeys();
        for (DriverKey_Thrift driverKey : driverKeys) {
            DriverMetadata driverMetadata = driverStore.get(driverKey.getDriverContainerId(), driverKey.getVolumeId(),
                    DriverType.valueOf(driverKey.getDriverType().name()), driverKey.getSnapshotId());
            if (driverMetadata == null) {
                logger.error("driverMetadata is null: {}", driverKey);
                throw new DriverNotFoundException_Thrift();
            }
            // get dynamic IOLimitation
            List<IOLimitation_Thrift> ioLimitation_thriftList = new ArrayList<>();
            if (driverMetadata.getDynamicIOLimitationId() != 0) {
                IOLimitationInformation limitationInformation = ioLimitationStore
                        .get(driverMetadata.getDynamicIOLimitationId());
                ioLimitation_thriftList.add(RequestResponseHelper
                        .buildThriftIOLimitationFrom(new IOLimitation(limitationInformation)));
            }
            // get static IOLimitation
            if (driverMetadata.getStaticIOLimitationId() != 0) {
                IOLimitationInformation limitationInformation = ioLimitationStore
                        .get(driverMetadata.getStaticIOLimitationId());
                ioLimitation_thriftList.add(RequestResponseHelper
                        .buildThriftIOLimitationFrom(new IOLimitation(limitationInformation)));
            }

            listIOLimitationsByDriverKeysResponse.putToMapDriver2ItsIOLimitations(driverKey, ioLimitation_thriftList);
        }
        logger.warn("listIOLimitationsByDriverKeys response: {}", listIOLimitationsByDriverKeysResponse);
        return listIOLimitationsByDriverKeysResponse;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#getIOLimitations(GetIOLimitationsRequest)} which get IOLimitations.
     *
     * @param {@link GetIOLimitationsRequest} request
     * @return {@link GetIOLimitationsResponse}
     */
    @Override
    public GetIOLimitationsResponse getIOLimitations(GetIOLimitationsRequest request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("getIOLimitations request: {}", request);

        // prepare a response to getting IOLimitations request
        GetIOLimitationsResponse response = new GetIOLimitationsResponse();
        response.setRequestId(request.getRequestId());

        DriverKey_Thrift driverKey = request.getDriverKey();
        DriverMetadata driverMetadata = driverStore.get(driverKey.getDriverContainerId(), driverKey.getVolumeId(),
                DriverType.valueOf(driverKey.getDriverType().name()), driverKey.getSnapshotId());
        if (driverMetadata == null) {
            logger.error("get driver failed");
            throw new DriverNotFoundException_Thrift();
        }

        if (driverMetadata.getDynamicIOLimitationId() != 0) {
            IOLimitationInformation limitationInformation = ioLimitationStore
                    .get(driverMetadata.getDynamicIOLimitationId());
            response.addToIoLimitations(
                    RequestResponseHelper.buildThriftIOLimitationFrom(new IOLimitation(limitationInformation)));
        }
        // get static IOLimitation
        if (driverMetadata.getStaticIOLimitationId() != 0) {
            IOLimitationInformation limitationInformation = ioLimitationStore
                    .get(driverMetadata.getStaticIOLimitationId());
            response.addToIoLimitations(
                    RequestResponseHelper.buildThriftIOLimitationFrom(new IOLimitation(limitationInformation)));
        }
        logger.warn("getIOLimitations response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#getIOLimitationAppliedDrivers(GetIOLimitationAppliedDriversRequest)} which get applied drivers.
     *
     * @param {@link GetIOLimitationAppliedDriversRequest} request
     * @return {@link GetIOLimitationAppliedDriversResponse}
     */
    @Override
    public GetIOLimitationAppliedDriversResponse getIOLimitationAppliedDrivers(
            GetIOLimitationAppliedDriversRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, IOLimitationsNotExists, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getIOLimitationAppliedDrivers request: {}", request);
        GetIOLimitationAppliedDriversResponse response = new GetIOLimitationAppliedDriversResponse();
        response.setRequestId(request.getRequestId());
        response.setDriverList(new ArrayList<>());

        IOLimitationInformation ioLimitationInformation = ioLimitationStore.get(request.getRuleId());
        if (ioLimitationInformation == null) {
            logger.warn("No IOLimitations with id {}", request.getRuleId());
            throw new IOLimitationsNotExists();
        }

        // get all drivers
        List<DriverMetadata> driverMetadatalist = driverStore.list();
        if (driverMetadatalist == null || driverMetadatalist.isEmpty()) {
            logger.warn("driverStore is null");
            return response;
        }

        IOLimitation ioLimitation = new IOLimitation(ioLimitationInformation);
        for (DriverMetadata driverMetadata : driverMetadatalist) {
            // check dynamic IOLimation
            if (driverMetadata.getDynamicIOLimitationId() == request.getRuleId()) {
                logger.debug("getIOLimitationAppliedDrivers {} on {}", driverMetadata, ioLimitation);
                response.addToDriverList(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
            }
            // check static IOLimation
            if (driverMetadata.getStaticIOLimitationId() == request.getRuleId()) {
                logger.debug("getIOLimitationAppliedDrivers {} on {}", driverMetadata, ioLimitation);
                response.addToDriverList(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
            }
        }
        logger.warn("getIOLimitationAppliedDrivers response: {}", response);
        return response;
    }
    //********** Driver Qos end*******************/

    //********** StoragePool Qos begin************/

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#createMigrationRules(CreateMigrationRulesRequest)} which create MigrationSpeedRules.
     *
     * @param {@link CreateMigrationRulesRequest} request
     * @return {@link CreateMigrationRulesResponse}
     */
    @Override
    public CreateMigrationRulesResponse createMigrationRules(CreateMigrationRulesRequest request)
            throws MigrationRuleDuplicate_Thrift, InvalidInputException_Thrift, ServiceHavingBeenShutdown_Thrift,
            PermissionNotGrantException_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("createMigrationSpeedRules request: {}", request);

        CreateMigrationRulesResponse response = new CreateMigrationRulesResponse(request.getRequestId());

        // MigrationRule rule list from migrationRuleStore
        List<MigrationRuleInformation> rulesFromDB = migrationRuleStore.list();

        // checkout each of the rules that were created by the user to find out
        // which rule is already existed in the
        // system.
        MigrationRule_Thrift migrationRuleThrift = request.getMigrationRule();
        boolean migrationRuleExisted = false;
        for (MigrationRuleInformation rule : rulesFromDB) {
            if (rule.getRuleId() == migrationRuleThrift.getRuleId()) {
                migrationRuleExisted = true;
                break;
            } else {
                // do nothing
            }
        }

        if (!migrationRuleExisted) {
            logger.debug("information center going to save the migrationSpeed");
            MigrationRule migrationSpeed = RequestResponseHelper.buildMigrationRuleFrom(migrationRuleThrift);
            migrationSpeed.setStatus(MigrationRuleStatus.AVAILABLE);
            migrationRuleStore.save(migrationSpeed.toMigrationRuleInformation());
        } else {
            // throw an exception out
            logger.error("MigrationRule:{} is already exists in DB! throws an exception MigrationRuleDuplicate_Thrift! ",
                    migrationRuleThrift);
            throw new MigrationRuleDuplicate_Thrift();
        }

        logger.warn("createMigrationSpeedRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#updateMigrationRules(UpdateMigrationRulesRequest)} which create MigrationSpeedRules.
     *
     * @param {@link UpdateMigrationRulesRequest} request
     * @return {@link UpdateMigrationRulesResponse}
     */
    @Override
    public UpdateMigrationRulesResponse updateMigrationRules(UpdateMigrationRulesRequest request)
            throws MigrationRuleDuplicate_Thrift, InvalidInputException_Thrift, ServiceHavingBeenShutdown_Thrift,
            PermissionNotGrantException_Thrift, MigrationRuleNotExists, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("updateMigrationSpeedRules request: {}", request);

        UpdateMigrationRulesResponse response = new UpdateMigrationRulesResponse(request.getRequestId());

        // get all storage pools
        List<StoragePool> storagePools;
        try {
            storagePools = storagePoolStore.listAllStoragePools();
        } catch (Exception e) {
            logger.error("can not get storage pool", e);
            throw new TException();
        }

        // checkout each of the rules that were update by the user to update storage pool
        // which applied old rules.
        MigrationRule_Thrift migrationSpeedRule_Thrift = request.getMigrationRule();

        Long ruleId = migrationSpeedRule_Thrift.getRuleId();
        MigrationRule migrationSpeed = new MigrationRule(migrationRuleStore.get(ruleId));
        // unable to apply a deleting migrationSpeed
        if (migrationSpeed.getStatus() == MigrationRuleStatus.DELETING) {
            logger.debug("The migrationSpeed {} is deleting now, unable to apply this rule to pools", migrationSpeed);
            response.addToAirMigrationRuleList(RequestResponseHelper.buildMigrationRuleThriftFrom(migrationSpeed));
        } else {

            // save migrationSpeed
            MigrationRule migrationSpeedUpdate = RequestResponseHelper
                    .buildMigrationRuleFrom(migrationSpeedRule_Thrift);
            logger.debug("updateMigrationSpeedRules {} to {}", migrationSpeed, migrationSpeedUpdate);
            migrationSpeedUpdate.setStatus(MigrationRuleStatus.AVAILABLE);
            migrationRuleStore.update(migrationSpeedUpdate.toMigrationRuleInformation());
        }
        logger.warn("updateMigrationSpeedRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#applyMigrationRules(ApplyMigrationRulesRequest)} apply MigrationSpeedRules.
     *
     * @param {@link ApplyMigrationRulesRequest} request
     * @return {@link ApplyMigrationRulesResponse}
     */
    @Override
    public ApplyMigrationRulesResponse applyMigrationRules(ApplyMigrationRulesRequest request)
            throws VolumeNotFoundException_Thrift, MigrationRuleIsDeleting,
            ApplyFailedDueToVolumeIsReadOnlyException_Thrift, PermissionNotGrantException_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("applyMigrationSpeedRules request: {}", request);
        // check pool does exists or in deleting, deleted or dead status
        ApplyMigrationRulesResponse response = new ApplyMigrationRulesResponse();
        response.setRequestId(request.getRequestId());

        Long ruleId = request.getRuleId();
        MigrationRule migrationRule = new MigrationRule(migrationRuleStore.get(ruleId));
        // unable to apply a deleting migrationSpeed
        if (migrationRule.getStatus() == MigrationRuleStatus.DELETING) {
            logger.error("The migrationSpeed {} is deleting now, unable to apply this rule to pools", migrationRule);
            throw new MigrationRuleIsDeleting();
        }
        logger.info("get migrationRule success! ruleId:{}, migrationRule:{}", ruleId, migrationRule);

        response.setRuleName(migrationRule.getMigrationRuleName());
        response.setAppliedStoragePoolNames(new ArrayList<>());

        List<Long> toApplyPoolIdList = request.getStoragePoolIds();
        if (toApplyPoolIdList == null || toApplyPoolIdList.isEmpty()) {
            logger.warn("No pools exists to applying request, do nothing");
            return response;
        }
        // apply to pools
        for (long toApplyPoolId : toApplyPoolIdList) {

            StoragePool storagePoolToBeUpdate;
            try {
                storagePoolToBeUpdate = storagePoolStore.getStoragePool(toApplyPoolId);
                logger.warn("storagePoolStore {} {} ", toApplyPoolId, storagePoolToBeUpdate);
            } catch (Exception e) {
                logger.error("can not get storage pool, pollId:{}", toApplyPoolId, e);
                throw new TException(e);
            }

            if (storagePoolToBeUpdate == null) {
                logger.error("can not get storage pool, pollId:{}", toApplyPoolId);
                throw new StoragePoolNotExistedException_Thrift();
            }

            if (storagePoolToBeUpdate.isDeleting()) {
                logger.error("can not get storage pool, pollId:{}", toApplyPoolId);
                throw new StoragePoolIsDeletingException_Thrift();
            }

            if (storagePoolToBeUpdate.getMigrationRuleId() != 0) {
                logger.warn("storage pool already applied! change rule from {} to {}. storage pool:{}",
                        storagePoolToBeUpdate.getMigrationRuleId(), ruleId, storagePoolToBeUpdate);
                response.addToAirStoragePoolList(
                        RequestResponseHelper.buildThriftStoragePoolFrom(storagePoolToBeUpdate, migrationRule));
            }

            // update storagePool
            logger.warn("applyMigrationSpeedRules {} on {}", migrationRule, storagePoolToBeUpdate.getPoolId());
            storagePoolToBeUpdate.setMigrationRuleId(migrationRule.getRuleId());
            storagePoolStore.saveStoragePool(storagePoolToBeUpdate);
            response.addToAppliedStoragePoolNames(storagePoolToBeUpdate.getName());
        }

        logger.warn("applyMigrationSpeedRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#cancelMigrationRules(CancelMigrationRulesRequest)} which cancel MigrationSpeedRules.
     *
     * @param {@link CancelMigrationRulesRequest} request
     * @return {@link CancelMigrationRulesResponse}
     */
    @Override
    public CancelMigrationRulesResponse cancelMigrationRules(CancelMigrationRulesRequest request)
            throws AccessRuleNotApplied_Thrift, PermissionNotGrantException_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("cancelMigrationSpeedRules request: {}", request);
        CancelMigrationRulesResponse response = new CancelMigrationRulesResponse();
        response.setRequestId(request.getRequestId());

        Long ruleId = request.getRuleId();
        if (ruleId == 0) {
            throw new AccessRuleNotApplied_Thrift();
        }

        MigrationRule migrationSpeed = new MigrationRule(migrationRuleStore.get(ruleId));
        // unable to apply a deleting migrationSpeed
        if (migrationSpeed.getStatus() == MigrationRuleStatus.DELETING) {
            logger.debug("The migrationSpeed {} is deleting now, unable to apply this rule to pools", migrationSpeed);
            throw new MigrationRuleIsDeleting();
        }
        response.setRuleName(migrationSpeed.getMigrationRuleName());
        response.setCanceledStoragePoolNames(new ArrayList<>());

        // apply to pools
        List<Long> toApplyPoolIdList = request.getStoragePoolIds();
        if (toApplyPoolIdList == null || toApplyPoolIdList.isEmpty()) {
            logger.debug("No pools exists to applying request, do nothing");
            return response;
        }
        for (long toApplyPoolId : toApplyPoolIdList) {

            StoragePool storagePoolToBeUpdate;
            try {
                storagePoolToBeUpdate = storagePoolStore.getStoragePool(toApplyPoolId);
                logger.warn("storagePoolToBeUpdate {} ", storagePoolToBeUpdate);
            } catch (Exception e) {
                logger.warn("can not get storage pool", e);
                throw new NotEnoughGroupException_Thrift();
            }
            if (storagePoolToBeUpdate == null) {
                throw new StoragePoolNotExistedException_Thrift();
            }
            if (storagePoolToBeUpdate.isDeleting()) {
                throw new StoragePoolIsDeletingException_Thrift();
            }

            // check apply or not
            if (storagePoolToBeUpdate.getMigrationRuleId() != ruleId) {
                throw new AccessRuleNotApplied_Thrift();
            }

            // update storagePool to default
            logger.debug("CancelMigrationSpeedRulesResponse {} on {}", migrationSpeed, storagePoolToBeUpdate);
            storagePoolToBeUpdate.setMigrationRuleId(StoragePool.DEFAULT_MIGRATION_RULE_ID);
            storagePoolStore.saveStoragePool(storagePoolToBeUpdate);
            response.addToCanceledStoragePoolNames(storagePoolToBeUpdate.getName());
        }

        logger.warn("cancelMigrationSpeedRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#deleteMigrationRules(DeleteMigrationRulesRequest)} which delete MigrationSpeedRules.
     *
     * @param {@link DeleteMigrationRulesRequest} request
     * @return {@link DeleteMigrationRulesResponse}
     */
    @Override
    public DeleteMigrationRulesResponse deleteMigrationRules(DeleteMigrationRulesRequest request)
            throws ServiceHavingBeenShutdown_Thrift, PermissionNotGrantException_Thrift, TException {

        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("deleteMigrationSpeedRules request: {}", request);

        DeleteMigrationRulesResponse response = new DeleteMigrationRulesResponse();
        response.setRequestId(request.getRequestId());
        response.setDeletedRuleNames(new ArrayList<>());
        if (request.getRuleIds() == null || request.getRuleIds().size() == 0) {
            logger.debug("No rules existing in request to delete");
            return response;
        }

        // get all storage pools
        List<StoragePool> storagePools = null;
        try {
            storagePools = storagePoolStore.listAllStoragePools();
        } catch (Exception e) {
            logger.error("can not get storage pool", e);
        }

        if (storagePools == null) {
            logger.error("failed to get storage pool");
            throw new ServiceIsNotAvailable_Thrift();
        }

        logger.warn("storagePoolStore : {}", storagePools);

        for (long ruleId : request.getRuleIds()) {
            MigrationRuleInformation migrationSpeedInformation = migrationRuleStore.get(ruleId);
            if (migrationSpeedInformation == null) {
                logger.debug("No migrationSpeed rule with id {}", ruleId);
                continue;
            }
            MigrationRule migrationSpeed = new MigrationRule(migrationSpeedInformation);

            // unable to apply a deleting migrationSpeed
            if (migrationSpeed == null || migrationSpeed.getStatus() == MigrationRuleStatus.DELETING) {
                logger.debug("The migrationSpeed {} is deleting now, unable to apply this rule to pools",
                        migrationSpeed);
                response.addToAirMigrationRuleList(RequestResponseHelper.buildMigrationRuleThriftFrom(migrationSpeed));
                continue;
            }

            if (!request.isCommit()) {
                if (migrationSpeed.getStatus() != MigrationRuleStatus.DELETING) {
                    migrationSpeed.setStatus(MigrationRuleStatus.DELETING);
                    migrationRuleStore.save(migrationSpeed.toMigrationRuleInformation());
                } else {
                    // do nothing
                }
                continue;
            }

            for (StoragePool storagePoolToBeUpdate : storagePools) {
                logger.warn("storagePoolToBeUpdate : {}", storagePoolToBeUpdate);

                if (storagePoolToBeUpdate.getMigrationRuleId() == ruleId) {
                    // update storagePool to default
                    logger.debug("CancelMigrationSpeedRulesResponse {} on {}", migrationSpeed, storagePoolToBeUpdate);
                    storagePoolToBeUpdate.setMigrationRuleId(StoragePool.DEFAULT_MIGRATION_RULE_ID);
                    storagePoolStore.saveStoragePool(storagePoolToBeUpdate);
                }
            }

            // delete the migration rule and relationship with storagePool from
            // database if exists
            migrationRuleStore.delete(ruleId);
            response.addToDeletedRuleNames(migrationSpeed.getMigrationRuleName());
        }

        logger.warn("deleteMigrationSpeedRules response: {}", response);
        return response;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#listMigrationRules(ListMigrationRulesRequest)} which list all MigrationSpeedRules.
     *
     * @param {@link ListMigrationRulesRequest} request
     * @return {@link ListMigrationRulesResponse}
     */
    @Override
    public ListMigrationRulesResponse listMigrationRules(ListMigrationRulesRequest request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("listMigrationSpeedRules request: {}", request);

        ListMigrationRulesResponse listMigrationSpeedRulesResponse = new ListMigrationRulesResponse();
        listMigrationSpeedRulesResponse.setRequestId(request.getRequestId());

        List<MigrationRuleInformation> migrationSpeedInfoList = migrationRuleStore.list();
        if (migrationSpeedInfoList != null && migrationSpeedInfoList.size() > 0) {
            for (MigrationRuleInformation migrationSpeedInfo : migrationSpeedInfoList) {
                listMigrationSpeedRulesResponse.addToMigrationRules(
                        RequestResponseHelper.buildMigrationRuleThriftFrom(new MigrationRule(migrationSpeedInfo)));
            }
        } else {
            // set null list
            listMigrationSpeedRulesResponse.setMigrationRules(new ArrayList<>());
        }
        logger.warn("listMigrationSpeedRules response: {}", listMigrationSpeedRulesResponse);
        return listMigrationSpeedRulesResponse;
    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#getMigrationRules(GetMigrationRulesRequest)} which list
     * MigrationSpeedRules of specified storagePools.
     *
     * @param {@link ListMigrationRulesRequest} request
     * @return {@link GetMigrationRulesResponse}
     */

    @Override
    public GetMigrationRulesResponse getMigrationRules(GetMigrationRulesRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("getMigrationSpeedRules request: {}", request);
        GetMigrationRulesResponse response = new GetMigrationRulesResponse();
        response.setRequestId(request.getRequestId());
        response.setAccessRules(new ArrayList<>());

        // get all storage pools
        StoragePool storagePool;
        try {
            storagePool = storagePoolStore.getStoragePool(request.getStoragePoolId());
        } catch (Exception e) {
            logger.error("can not get storage pool", e);
            throw new TException();
        }

        if (storagePool == null) {
            logger.error("failed to get storage pool");
            return response;
        }

        MigrationRule migrationSpeed = new MigrationRule(migrationRuleStore.get(storagePool.getMigrationRuleId()));
        MigrationRule_Thrift migrationSpeedToRemote = RequestResponseHelper
                .buildMigrationRuleThriftFrom(migrationSpeed);
        response.addToAccessRules(migrationSpeedToRemote);
        logger.warn("getMigrationSpeedRules response: {}", response);
        return response;

    }

    /**
     * An implementation of interface
     * {@link InformationCenter.Iface#getAppliedStoragePools(GetAppliedStoragePoolsRequest)} list
     * pools on which this migrateSpeed applied.
     *
     * @param {@link GetAppliedStoragePoolsRequest} request
     * @return {@link GetAppliedStoragePoolsResponse}
     */

    @Override
    public GetAppliedStoragePoolsResponse getAppliedStoragePools(GetAppliedStoragePoolsRequest request)
            throws ServiceHavingBeenShutdown_Thrift, PermissionNotGrantException_Thrift, MigrationRuleNotExists,
            TException {
        if (shutDownFlag) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("getAppliedStoragePools request: {}", request);

        MigrationRuleInformation migrationRuleInformation = migrationRuleStore.get(request.getRuleId());
        if (migrationRuleInformation == null) {
            logger.error("can not found migration rule by:{}", request.getRuleId());
            throw new MigrationRuleNotExists();
        }

        MigrationRule migrationRule = migrationRuleInformation.toMigrationRule();
        GetAppliedStoragePoolsResponse response = new GetAppliedStoragePoolsResponse();
        response.setRequestId(request.getRequestId());
        response.setStoragePoolList(new ArrayList<>());

        // get all storage pools
        List<StoragePool> storagePools = null;
        try {
            storagePools = storagePoolStore.listAllStoragePools();
        } catch (Exception e) {
            logger.error("can not get storage pool", e);
        }

        if (storagePools == null) {
            logger.error("failed to get storage pool");
            return response;
        }

        for (StoragePool storagePoolToBeUpdate : storagePools) {
            logger.debug("getAppliedStoragePools MigrationSpeedId {} ruleId {}",
                    storagePoolToBeUpdate.getMigrationRuleId(), request.getRuleId());

            if (storagePoolToBeUpdate.getMigrationRuleId() != StoragePool.DEFAULT_MIGRATION_RULE_ID.longValue()
                    && storagePoolToBeUpdate.getMigrationRuleId() == request.getRuleId()) {
                // update storagePool to default
                logger.warn("getAppliedStoragePools on {}", request.getRuleId());
                response.addToStoragePoolList(
                        RequestResponseHelper.buildThriftStoragePoolFrom(storagePoolToBeUpdate, migrationRule));
            }
        }

        logger.warn("getAppliedStoragePools response: {}", response);
        return response;
    }
    //********** StoragePool Qos end*************/

    @Override
    public SetIscsiChapControlResponse_Thrift setIscsiChapControl(SetIscsiChapControlRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {

        if (this.shutDownFlag) {
            logger.error("control center is going to shutdown, do not accept request:{} for now", request);
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("{}", request);
        SetIscsiChapControlResponse_Thrift response = new SetIscsiChapControlResponse_Thrift();
        response.setRequestId(request.getRequestId());

        DriverMetadata driverMetadata = driverStore.get(request.getDriverContainerId(), request.getVolumeId(),
                DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId());

        if (driverMetadata != null) {
            driverMetadata.setChapControl(request.getChapControl());
        } else {
            logger.error("setIscsiChapControl can not find any driver by request:{}", request);
            throw new DriverNotFoundException_Thrift();
        }

        driverStore.save(driverMetadata);

        logger.warn("setIscsiChapControl response: {} driverMetadata {}", response, driverMetadata);
        return response;
    }

    @Override
    public GetDriversResponse_Thrift getDrivers(GetDriversRequest_Thrift request)
            throws VolumeNotFoundException_Thrift, ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift,
            TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getDrivers request: {}", request);

        GetDriversResponse_Thrift returnResponse = new GetDriversResponse_Thrift();
        returnResponse.setRequestId(request.getRequestId());

        VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());

        if (volumeMetadata == null) {
            logger.error("No volume found with id {}", request.getVolumeId());
            throw new VolumeNotFoundException_Thrift();
        }

        // get all driver bound to the volume
        List<DriverMetadata> driversList = driverStore.get(request.getVolumeId());
        if ((driversList == null) || (driversList.isEmpty())) {
            logger.debug("No drivers launched");
            return returnResponse;
        }

        List<DriverMetadata_Thrift> drivers_ThriftList = new ArrayList<>();

        for (DriverMetadata driverMetadata : driversList) {
            drivers_ThriftList.add(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
        }

        returnResponse.setDrivers(drivers_ThriftList);

        logger.warn("getDrivers response: {}", returnResponse);
        return returnResponse;
    }

    public long getDeadVolumeToRemove() {
        return deadVolumeToRemove;
    }

    public void setDeadVolumeToRemove(long deadVolumeToRemove) {
        this.deadVolumeToRemove = deadVolumeToRemove;
    }

    @Override
    public GetVolumeResponse getVolumeNotDeadByName(GetVolumeRequest request)
            throws InternalError_Thrift, InvalidInputException_Thrift, NotEnoughSpaceException_Thrift,
            VolumeNotFoundException_Thrift, ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("getVolumeNotDeadByName request: {}", request);

        VolumeMetadata volumeMetadata = volumeStore.getVolumeNotDeadByName(request.getName());

        if (volumeMetadata == null) {
            logger.warn("Can't find volume metadata given its name {} ", request.getName());
            throw new VolumeNotFoundException_Thrift();
        }

        if (!volumeMetadata.isRoot()) {
            logger.warn("{} is not root volume. throw VolumeNotFoundException", volumeMetadata);
            throw new VolumeNotFoundException_Thrift();
        }

        // get the list of volume metadata first
        long volumeId = volumeMetadata.getVolumeId();
        List<VolumeMetadata> volumeMetadatas = volumeStore.listVolumesFromRoot(volumeId);
        if (volumeMetadatas.size() == 0) {
            logger.error("can not get volumeMetadata from volumeId: {}", volumeId);
            throw new VolumeNotFoundException_Thrift();
        }

        // iterate all segment unit's and change their statuses accordingly
        VolumeMetadata virtualVolumeMetadata = mergeVolumes(volumeMetadatas);

        logger.debug("After merging, the virtual volume metadata is {} ", virtualVolumeMetadata);
        GetVolumeResponse response = new GetVolumeResponse();
        response.setVolumeMetadata(RequestResponseHelper.buildThriftVolumeFrom(virtualVolumeMetadata, true));
        long rootVolumeId = volumeMetadata.getRootVolumeId();

        List<DriverMetadata> volumeBindingDrivers = driverStore.get(rootVolumeId);
        if (volumeBindingDrivers != null && volumeBindingDrivers.size() > 0) {
            for (DriverMetadata driverMetadata : volumeBindingDrivers) {
                if (driverMetadata.getDriverStatus() == DriverStatus.LAUNCHED) {
                    response.addToDriverMetadatas(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
                }
            }
        } else {
            logger.debug("Can't get driver metadata from volume {} ", volumeId);
        }

        response.setRequestId(request.getRequestId());
        logger.warn("getVolumeNotDeadByName response: {}", response);
        return response;
    }

    @Override
    public DeleteVolumeResponse deleteVolume(DeleteVolumeRequest request)
            throws InternalError_Thrift, VolumeNotFoundException_Thrift, VolumeBeingDeletedException_Thrift,
            ServiceHavingBeenShutdown_Thrift, VolumeInExtendingException_Thrift,
            LaunchedVolumeCannotBeDeletedException_Thrift, ServiceIsNotAvailable_Thrift,
            VolumeIsCloningException_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("deleteVolume request: {}", request);

        VolumeMetadata rootVolume = volumeStore.getVolume(request.getVolumeId());
        // volume not exist
        if (rootVolume == null || !rootVolume.isRoot()) {
            logger.debug("Can't find volume metadata given its id {}", request.getVolumeId());
            throw new VolumeNotFoundException_Thrift();
        }

        // volume not in deleting or deleted status
        if (rootVolume.isDeletedByUser()) {
            logger.debug("volume is being deleting or deleted {} ", rootVolume);
            throw new VolumeBeingDeletedException_Thrift();
        }

        // volume status is extending, deleting operation is not allowed
        if (rootVolume.getExtendingSize() != 0) {
            logger.debug("volume is in extending status, do not allow to delete it");
            throw new VolumeInExtendingException_Thrift();
        }

        //throw exception ,if delete a volume when it is cloning
        if (!cloneRelationshipsStore.listBySrcVolumeId(request.getVolumeId()).isEmpty()) {
            logger.warn("this volume is cloning");
            throw new VolumeIsCloningException_Thrift();
        }

        // check if the volume has been launched
        List<DriverMetadata> volumeBindingDrivers = driverStore.get(rootVolume.getVolumeId());
        if (volumeBindingDrivers.size() > 0) {
            LaunchedVolumeCannotBeDeletedException_Thrift launchedVolumeCannotBeDeletedException_Thrift = new LaunchedVolumeCannotBeDeletedException_Thrift();
            launchedVolumeCannotBeDeletedException_Thrift.setIsDriverUnknown(false);
            for (DriverMetadata driverMetadata : volumeBindingDrivers) {
                if (driverMetadata.getDriverStatus() == DriverStatus.UNKNOWN) {
                    launchedVolumeCannotBeDeletedException_Thrift.setIsDriverUnknown(true);
                }
            }
            logger.debug("volume has been launched, deleting operation not allowed");
            throw launchedVolumeCannotBeDeletedException_Thrift;
        }

        // get the list of volume metadata first, and set all the volume is
        // deleting status
        List<VolumeMetadata> volumeMetadatas = volumeStore.listVolumesFromRoot(rootVolume.getVolumeId());

        // set the volume status to deleting;
        for (VolumeMetadata volumeMetadata : volumeMetadatas) {
            volumeMetadata.setVolumeStatus(VolumeStatus.Deleting);
            volumeStatusStore.addVolumeToStore(volumeMetadata);

            long destId = volumeMetadata.getVolumeId();
            CloneRelationshipInformation cr = cloneRelationshipsStore.getFromDB(destId);
            if (cr != null) {

                long srcId = cr.getScrVolumeId();
                cloneRelationshipsStore.deleteFromDB(volumeMetadata.getVolumeId());
                VolumeMetadata srcVolumeMetadata = volumeStore.getVolume(srcId);
                VolumeMetadata.VolumeInAction oldInAction = srcVolumeMetadata.getInAction();

                if (cloneRelationshipsStore.listBySrcVolumeId(srcId).isEmpty()) {
                    if (oldInAction.equals(VolumeMetadata.VolumeInAction.BEING_CLONED) || oldInAction
                            .equals(VolumeMetadata.VolumeInAction.BEING_MOVED)) {

                        logger.warn(
                                "there is one volume is done with clone or move, root volume {} action reset to null.",
                                srcId);

                        volumeStore.updateStatusAndVolumeInAction(srcId, srcVolumeMetadata.getVolumeStatus().name(),
                                NULL.name());
                    }
                }
            }

            volumeStore.updateStatusAndVolumeInAction(volumeMetadata.getVolumeId(), VolumeStatus.Deleting.toString(),
                    DELETING.name());
            logger.debug("user delete volume {}, volume status change to {}", volumeMetadata, VolumeStatus.Deleting);
        }

        // delete volume access rule related to the volume
        volumeRuleRelationshipStore.deleteByVolumeId(rootVolume.getVolumeId());

        DeleteVolumeResponse response = new DeleteVolumeResponse(request.getRequestId());
        logger.warn("deleteVolume response: {}", response);
        return response;
    }

    @Override
    public RecycleVolumeResponse recycleVolume(RecycleVolumeRequest request)
            throws InternalError_Thrift, VolumeNotFoundException_Thrift, VolumeCannotBeRecycledException_Thrift,
            ServiceHavingBeenShutdown_Thrift, VolumeInExtendingException_Thrift, ExistsDriverException_Thrift,
            ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("recycleVolume request: {}", request);
        VolumeMetadata rootVolume = volumeStore.getVolume(request.getVolumeId());

        // volume not exist
        if (rootVolume == null || !rootVolume.isRoot()) {
            logger.debug("Can't find volume metadata given its id {}", request.getVolumeId());
            throw new VolumeNotFoundException_Thrift();
        }

        // volume not in deleting or deleted status
        if (!rootVolume.canBeRecycled()) {
            logger.debug("volume cannot be recycled {} ", rootVolume);
            throw new VolumeCannotBeRecycledException_Thrift();
        }

        // volume status is extending, deleting operation is not allowed
        if (rootVolume.getExtendingSize() != 0) {
            logger.debug("volume is in extending status, do not allow to delete it");
            throw new VolumeInExtendingException_Thrift();
        }

        // get the list of volume metadata first, and then set all the volume
        // status to deleting
        List<VolumeMetadata> volumeMetadatas = volumeStore.listVolumesFromRoot(rootVolume.getVolumeId());

        /** when recycleVolume ,check the volume status*/
        for (VolumeMetadata volume : volumeMetadatas){
            VolumeStatus volumeStatus = volume.getVolumeStatus();
            if (volumeStatus == VolumeStatus.Dead){
                logger.warn("when recycleVolume :{}, the volume is Dead", request.getVolumeId());
                throw new VolumeCannotBeRecycledException_Thrift();
            }
        }

        // set the volume status to deleting;
        for (VolumeMetadata volumeMetadata : volumeMetadatas) {
            volumeMetadata.setVolumeStatus(VolumeStatus.Recycling);
            volumeStatusStore.addVolumeToStore(volumeMetadata);
            volumeStore.updateStatusAndVolumeInAction(volumeMetadata.getVolumeId(), VolumeStatus.Recycling.toString(),
                    RECYCLING.name());
            logger.debug("user delete volume {}, volume status change to {}", volumeMetadata, VolumeStatus.Recycling);
        }
        RecycleVolumeResponse response = new RecycleVolumeResponse(request.getRequestId());
        logger.warn("recycleVolume response: {}", response);
        return response;
    }

    @Override
    public GetAppliedVolumesResponse getAppliedVolumes(GetAppliedVolumesRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getAppliedVolumes request: {}", request);
        GetAppliedVolumesResponse response = new GetAppliedVolumesResponse();
        response.setRequestId(request.getRequestId());
        response.setVolumeIdList(new ArrayList<>());

        List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
                .getByRuleId(request.getRuleId());
        if (relationshipInfoList == null || relationshipInfoList.isEmpty()) {
            return response;
        }

        VolumeMetadata volumeMetadata;
        for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
            volumeMetadata = volumeStore.getVolume(relationshipInfo.getVolumeId());

            if (volumeMetadata == null) {
                logger.warn("volume is not exist,delete it from rule relation ship table,{}",
                        relationshipInfo.getVolumeId());
                volumeRuleRelationshipStore.deleteByVolumeId(relationshipInfo.getVolumeId());
                continue;
            }
            response.addToVolumeIdList(relationshipInfo.getVolumeId());
        }

        logger.warn("getAppliedVolumes response: {}", response);
        return response;
    }

    public int getGroupCount() {
        return groupCount;
    }

    public void setGroupCount(int groupCount) {
        this.groupCount = groupCount;
    }

    /**
     * After successfully umount driver from driver container, this interface will be invoked to remove driver info from
     * driver store.
     */
    @Override
    public ReleaseDriverContainerResponse releaseDriverContainer(ReleaseDriverContainerRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("releaseDriverContainer request: {}", request);
        if (request.getDriverIpTargetList() == null || request.getDriverIpTargetList().isEmpty()) {
            logger.warn("No driver to release");
            return new ReleaseDriverContainerResponse(request.getRequestId());
        }

        synchronized (driverStore) {
            for (DriverIpTarget_Thrift driverIpTarget_Thrift : request.getDriverIpTargetList()) {
                DriverType driverType = DriverType.valueOf(driverIpTarget_Thrift.getDriverType().name());
                driverStore.delete(driverIpTarget_Thrift.getDriverContainerId(), request.getVolumeId(), driverType,
                        driverIpTarget_Thrift.getSnapshotId());
            }
        }

        ReleaseDriverContainerResponse response = new ReleaseDriverContainerResponse(request.getRequestId());
        logger.warn("releaseDriverContainer response: {}", response);
        return response;
    }

    @Override
    public ChangeDriverBoundVolumeResponse changeDriverBoundVolume(ChangeDriverBoundVolumeRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("changeDriverBoundVolume request: {}", request);
        DriverKey_Thrift driverKeyThrift = request.getDriver();

        synchronized (driverStore) {
            DriverType driverType = DriverType.valueOf(driverKeyThrift.getDriverType().name());
            DriverMetadata oldDriver = driverStore
                    .get(driverKeyThrift.getDriverContainerId(), driverKeyThrift.getVolumeId(), driverType,
                            driverKeyThrift.getSnapshotId());
            if (oldDriver != null) {
                oldDriver.setVolumeId(request.getNewVolumeId());
                driverStore.save(oldDriver);
            }
        }

        ChangeDriverBoundVolumeResponse response = new ChangeDriverBoundVolumeResponse();
        response.setRequestId(request.getRequestId());
        logger.warn("changeDriverBoundVolume response: {}", response);
        return response;
    }

    public DomainStore getDomainStore() {
        return domainStore;
    }

    public void setDomainStore(DomainStore domainStore) {
        this.domainStore = domainStore;
    }

    @Override
    public CreateDomainResponse createDomain(CreateDomainRequest request)
            throws ServiceHavingBeenShutdown_Thrift, InvalidInputException_Thrift, ServiceIsNotAvailable_Thrift,
            DomainExistedException_Thrift, DomainNameExistedException_Thrift, DatanodeNotFreeToUseException_Thrift,
            DatanodeNotFoundException_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("createDomain request: {}", request);
        CreateDomainResponse createDomainResponse = new CreateDomainResponse();
        createDomainResponse.setRequestId(request.getRequestId());

        Domain domain = RequestResponseHelper.buildDomainFrom(request.getDomain());
        if (domain == null) {
            logger.warn("Invalid request, should check it");
            throw new InvalidInputException_Thrift();
        }
        // check domain name
        if (domain.getDomainName() == null || domain.getDomainName().length() > 100) {
            logger.warn("Invalid domain name:{}", domain.getDomainName());
            throw new InvalidInputException_Thrift();
        }
        // check storage pool ids, when create one domain, it can not carry with storage pool for now
        if (!domain.getStoragePools().isEmpty()) {
            logger.error("Invalid storage pool info, can not be here, request:{}", request);
            throw new InvalidInputException_Thrift();
        }
        // should check duplicate domainName and duplicate domainId
        List<Domain> allDomains;
        try {
            allDomains = domainStore.listAllDomains();
        } catch (SQLException | IOException e) {
            logger.error("caught an exception", e);
            throw new TException();
        }
        for (Domain domainFromMem : allDomains) {
            if (domainFromMem.getDomainName().contentEquals(domain.getDomainName())) {
                logger.warn("domain name: {} has already exists", domain.getDomainName());
                throw new DomainNameExistedException_Thrift();
            } else if (domainFromMem.getDomainId().equals(domain.getDomainId())) {
                logger.warn("domain Id: {} has already exists", domain.getDomainId());
                throw new DomainExistedException_Thrift();
            }
        }
        List<Long> addedDatanode = new ArrayList<>();
        if (request.getDomain().isSetDatanodes()) {
            // check new add datanode is free or not
            for (Long datanodeId : domain.getDataNodes()) {
                InstanceMetadata datanode = storageStore.get(datanodeId);
                if (datanode == null) {
                    logger.error("can not find datanode by id:{}", datanodeId);
                    throw new DatanodeNotFoundException_Thrift();
                }
                if (!datanode.isFree()) {
                    logger.error("datanode:{} not free now", datanode);
                    throw new DatanodeNotFreeToUseException_Thrift();
                }
                addedDatanode.add(datanodeId);
                // mark datanode is in use
                datanode.setDomainId(domain.getDomainId());
                storageStore.save(datanode);
            }
        }
        domain.setLastUpdateTime(System.currentTimeMillis());
        domainStore.saveDomain(domain);
        createDomainResponse.setAddedDatanodes(addedDatanode);
        createDomainResponse.setDomainThrift(RequestResponseHelper.buildDomainThriftFrom(domain));
        logger.warn("createDomain response: {}", createDomainResponse);
        return createDomainResponse;
    }

    @Override
    public UpdateDomainResponse updateDomain(UpdateDomainRequest request)
            throws ServiceHavingBeenShutdown_Thrift, InvalidInputException_Thrift, ServiceIsNotAvailable_Thrift,
            DatanodeNotFreeToUseException_Thrift, DatanodeNotFoundException_Thrift, DomainNotExistedException_Thrift,
            DomainIsDeletingException_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("updateDomain request: {}", request);
        UpdateDomainResponse updateDomainResponse = new UpdateDomainResponse();
        updateDomainResponse.setRequestId(request.getRequestId());

        Domain domainToBeUpdate = RequestResponseHelper.buildDomainFrom(request.getDomain());
        if (domainToBeUpdate == null) {
            logger.warn("Invalid request, should check it");
            throw new InvalidInputException_Thrift();
        }
        Domain domainExist = null;
        try {
            domainExist = domainStore.getDomain(domainToBeUpdate.getDomainId());
        } catch (Exception e) {
            logger.warn("can not get domain", e);
        }
        if (domainExist == null) {
            logger.error("original domain not exist, domain Id:{}", domainToBeUpdate.getDomainId());
            throw new DomainNotExistedException_Thrift();
        }

        if (domainExist.getStatus() == Status.Deleting) {
            logger.error("domain:{} is deleting", domainExist);
            throw new DomainIsDeletingException_Thrift();
        }

        String newDomainName = domainToBeUpdate.getDomainName();
        if (!domainExist.getDomainName().equals(newDomainName)) {
            // if has new domain name is changed, should check name is unique
            List<Domain> domains;
            try {
                domains = domainStore.listAllDomains();
            } catch (Exception e) {
                logger.warn("can not list domain", e);
                throw new InvalidInputException_Thrift();
            }
            for (Domain domain : domains) {
                if (!domain.getDomainId().equals(domainToBeUpdate.getDomainId()) && domain.getDomainName()
                        .equals(newDomainName)) {
                    logger.error("update domain name is exist, new domain name:{}, exist domain:{}", newDomainName,
                            domain);
                    throw new InvalidInputException_Thrift();
                }
            }
            //If domain name has been changed ,we should change storagePools domain name which belong new domain
            for (long storagePoolId : domainToBeUpdate.getStoragePools()) {
                StoragePool storagePool;
                try {
                    storagePool = storagePoolStore.getStoragePool(storagePoolId);
                    if (storagePool != null) {
                        storagePool.setDomainName(newDomainName);
                        storagePoolStore.saveStoragePool(storagePool);
                    }
                } catch (Exception e) {
                    logger.warn("can not get storage pool", e);
                }
            }
        }
        List<Long> addedDatanode = new ArrayList<>();
        long logicalSpace = 0;
        long freeSpace = 0;
        // check new add datanode is free or not
        for (Long datanodeId : domainToBeUpdate.getDataNodes()) {
            if (!domainExist.getDataNodes().contains(datanodeId)) {
                InstanceMetadata datanode = storageStore.get(datanodeId);
                if (datanode == null) {
                    logger.error("can not find datanode by id:{}", datanodeId);
                    throw new DatanodeNotFoundException_Thrift();
                }
                if (!datanode.isFree()) {
                    logger.error("datanode:{} not free now", datanode);
                    throw new DatanodeNotFreeToUseException_Thrift();
                }
                addedDatanode.add(datanodeId);
                logicalSpace += datanode.getLogicalCapacity();
                freeSpace += datanode.getFreeSpace();
                // mark datanode is in use
                datanode.setDomainId(domainToBeUpdate.getDomainId());
                storageStore.save(datanode);
                domainToBeUpdate.setLastUpdateTime(System.currentTimeMillis());
            }
        }

        domainToBeUpdate.setLogicalSpace(domainExist.getLogicalSpace() + logicalSpace);
        domainToBeUpdate.setFreeSpace(domainExist.getFreeSpace() + freeSpace);
        for (Long datanodeId : domainExist.getDataNodes()) {
            domainToBeUpdate.addDatanode(datanodeId);
        }
        for (Long storagePoolId : domainExist.getStoragePools()) {
            domainToBeUpdate.addStoragePool(storagePoolId);
        }
        domainStore.saveDomain(domainToBeUpdate);
        updateDomainResponse.setAddedDatanodes(addedDatanode);
        logger.warn("updateDomain response: {}", updateDomainResponse);
        return updateDomainResponse;
    }

    @Override
    public DeleteDomainResponse deleteDomain(DeleteDomainRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, InvalidInputException_Thrift,
            DomainNotExistedException_Thrift, StillHaveStoragePoolException_Thrift, DomainIsDeletingException_Thrift,
            TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("deleteDomain request: {}", request);
        DeleteDomainResponse deleteDomainResponse = new DeleteDomainResponse();
        deleteDomainResponse.setRequestId(request.getRequestId());
        Domain domain = null;
        try {
            domain = domainStore.getDomain(request.getDomainId());
        } catch (Exception e) {
            logger.warn("can not get domain", e);
        }
        if (domain == null) {
            logger.warn("don't have domain: {} " + request.getDomainId());
            throw new DomainNotExistedException_Thrift();
        }
        if (domain.getStatus() == Status.Deleting) {
            logger.error("domain:{} is deleting", domain);
            throw new DomainIsDeletingException_Thrift();
        }
        if (domain.hasStoragePool()) {
            logger.error("still has storage pool:{} in domain:{}", domain.getStoragePools(), domain.getDomainId());
            throw new StillHaveStoragePoolException_Thrift();
        }
        List<Long> removedDatanode = new ArrayList<>();
        // mark all datanodes in domain free
        for (Long datanodeId : domain.getDataNodes()) {
            InstanceMetadata datanode = storageStore.get(datanodeId);
            if (datanode == null) {
                /*
                 * if datanode is not available, we can remove this domain any way
                 */
                continue;
            }
            removedDatanode.add(datanodeId);
            datanode.setFree();
            storageStore.save(datanode);
        }
        domain.setLastUpdateTime(System.currentTimeMillis());
        domain.setStatus(Status.Deleting);
        domainStore.saveDomain(domain);
        deleteDomainResponse.setRemovedDatanode(removedDatanode);
        deleteDomainResponse.setDomainName(domain.getDomainName());
        logger.warn("deleteDomain response: {}", deleteDomainResponse);
        return deleteDomainResponse;
    }

    @Override
    public ListDomainResponse listDomains(ListDomainRequest request)
            throws ServiceHavingBeenShutdown_Thrift, InvalidInputException_Thrift, ServiceIsNotAvailable_Thrift,
            TException {
        logger.info(request.toString());
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("listDomains request: {}", request);
        ListDomainResponse listDomainResponse = new ListDomainResponse();

        listDomainResponse.setRequestId(request.getRequestId());
        List<Domain> domains = null;
        List<OneDomainDisplay_Thrift> domainThriftList = new ArrayList<>();
        if (request.getDomainIds() == null) {
            try {
                domains = domainStore.listAllDomains();
            } catch (Exception e) {
                logger.error("can not get domains", e);
            }
        } else {
            try {
                domains = domainStore.listDomains(request.getDomainIds());
            } catch (Exception e) {
                logger.error("can not get domains", e);
            }
        }
        if (domains != null && !domains.isEmpty()) {
            for (Domain domain : domains) {
                List<InstanceMetadata_Thrift> datanodes = new ArrayList<>();
                for (Long datanodeInstanceId : domain.getDataNodes()) {
                    InstanceMetadata datanode = storageStore.get(datanodeInstanceId);
                    if (datanode != null && datanode.getDatanodeStatus().equals(OK)) {
                        datanodes.add(RequestResponseHelper.buildInstanceMetadataThriftFrom(datanode));
                    }
                }
                OneDomainDisplay_Thrift domainDisplay = new OneDomainDisplay_Thrift();
                domainDisplay.setDomainThrift(RequestResponseHelper.buildDomainThriftFrom(domain));
                domainDisplay.setDatanodes(datanodes);
                domainThriftList.add(domainDisplay);
            }
        }
        listDomainResponse.setDomainDisplays(domainThriftList);
        logger.warn("listDomains response: {}", listDomainResponse);
        return listDomainResponse;
    }

    @Override
    public GetLimitsResponse getLimits(GetLimitsRequest request)
            throws DriverNotFoundException_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("getLimits request: {}", request);
        GetLimitsResponse response = new GetLimitsResponse();
        // TODO
        //        DriverMetadata driverMetadata = driverStore.get(request.getDriverContainerId(), request.getVolumeId(),
        //                DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId());
        //        if (driverMetadata == null) {
        //            throw new DriverNotFoundException_Thrift();
        //        }
        //        List<IOLimitation_Thrift> ioLimitation_ThriftList = new ArrayList<>();
        //        boolean staticLimit = false;
        //        for (IOLimitation limit : driverMetadata.getIoLimitations()) {
        //            if (limit.getLimitType() == IOLimitation.LimitType.Static) {
        //                staticLimit = true;
        //            }
        //            ioLimitation_ThriftList.add(RequestResponseHelper.buildThriftIOLimitationFrom(limit));
        //        }
        //        response.setIoLimitations(ioLimitation_ThriftList);
        //        response.setVolumeId(request.getVolumeId());
        //        response.setRequestId(request.getRequestId());
        //        response.setStaticLimit(staticLimit);
        //        logger.warn("getLimits response: {}", response);
        return response;
    }

    @Override
    public AddOrModifyIOLimitResponse addOrModifyIOLimit(AddOrModifyIOLimitRequest request)
            throws DriverNotFoundException_Thrift, InvalidInputException_Thrift, ServiceIsNotAvailable_Thrift,
            AlreadyExistStaticLimitationException_Thrift, DynamicIOLimitationTimeInterleavingException_Thrift,
            TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("addOrModifyIOLimit request: {}", request);
        IOLimitation updateIOLimitation = RequestResponseHelper.buildIOLimitationFrom(request.getIoLimitation());
        AddOrModifyIOLimitResponse response = new AddOrModifyIOLimitResponse();
        if (!updateIOLimitation.validate()) {
            logger.error("invalid ioLimitation value modifying : {}", updateIOLimitation);
            throw new InvalidInputException_Thrift();
        }

        String volumeName = "";
        String driverType;
        VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());
        if (volumeMetadata != null) {
            volumeName = volumeMetadata.getName();
        }
        DriverMetadata driver = driverStore.get(request.getDriverContainerId(), request.getVolumeId(),
                DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId());
        EndPoint endPoint = null;
        if (driver != null) {
            try {
                driverStore.updateIOLimit(request.getDriverContainerId(), request.getVolumeId(),
                        DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId(),
                        updateIOLimitation);
            } catch (Exception e) {
                logger.error("caught an exception", e);
                throw e;
            }
            try {
                String hostName = driver.getHostName();
                int port = driver.getCoordinatorPort();
                endPoint = EndPointParser.parseLocalEndPoint(port, hostName);
                Coordinator.Iface coordinatorClient = coordinatorClientFactory.build(endPoint).getClient();
                AddOrModifyLimitationRequest modifyRequest = new AddOrModifyLimitationRequest();
                modifyRequest.setIoLimitation(request.getIoLimitation());
                modifyRequest.setRequestId(RequestIdBuilder.get());
                modifyRequest.setVolumeId(request.getVolumeId());
                coordinatorClient.addOrModifyLimitation(modifyRequest);
            } catch (Exception e) {
                logger.warn("cannot update io limit:{} to the driver {}", updateIOLimitation, driver);
            }
        } else {
            logger.error("addOrModifyIOLimit can not find any driver by request:{}", request);
            throw new DriverNotFoundException_Thrift();
        }

        response.setRequestId(request.getRequestId());
        response.setVolumeName(volumeName);
        if (driver.getDriverType().equals(DriverType.NBD)) {
            driverType = "PYD";
        } else {
            driverType = driver.getDriverType().name();
        }
        response.setDriverTypeAndEndPoint(driverType + " " + endPoint.toString());
        logger.warn("addOrModifyIOLimit response: {}", response);
        return response;
    }

    @Override
    public DeleteIOLimitResponse deleteIOLimit(DeleteIOLimitRequest request)
            throws InvalidInputException_Thrift, ServiceIsNotAvailable_Thrift, TException,
            DriverNotFoundException_Thrift {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("deleteIOLimit request: {}", request);
        String volumeName = "";
        String driverType;
        VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());
        if (volumeMetadata != null) {
            volumeName = volumeMetadata.getName();
        }
        DriverMetadata driver = driverStore.get(request.getDriverContainerId(), request.getVolumeId(),
                DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId());
        EndPoint endPoint = null;
        if (driver != null) {
            try {
                endPoint = EndPointParser.parseLocalEndPoint(driver.getCoordinatorPort(), driver.getHostName());
                Coordinator.Iface coordinatorClient = coordinatorClientFactory.build(endPoint).getClient();

                DeleteLimitationRequest deleteRequest = new DeleteLimitationRequest(RequestIdBuilder.get(),
                        request.getVolumeId(), request.getLimitId());
                coordinatorClient.deleteLimitation(deleteRequest);
            } catch (Exception e) {
                logger.error("cannot delete io limit for driver {}", driver);
            }
            if (-1 == driverStore.deleteIOLimit(request.getDriverContainerId(), request.getVolumeId(),
                    DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId(),
                    request.getLimitId())) {
                throw new DriverNotFoundException_Thrift();
            }
        } else {
            logger.error("deleteIOLimit can not find any driver by request:{}", request);
            throw new DriverNotFoundException_Thrift();
        }
        if (driver.getDriverType().equals(DriverType.NBD)) {
            driverType = "PYD";
        } else {
            driverType = driver.getDriverType().name();
        }
        DeleteIOLimitResponse response = new DeleteIOLimitResponse(request.getRequestId(), volumeName,
                driverType + " " + endPoint.toString());
        logger.warn("deleteIOLimit response: {}", response);
        return response;
    }

    @Override
    public ChangeLimitTypeResponse changeLimitType(ChangeLimitTypeRequest request)
            throws VolumeNotFoundException_Thrift, InvalidInputException_Thrift, ServiceIsNotAvailable_Thrift,
            DriverNotFoundException_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("changeLimitType request: {}", request);
        DriverMetadata driver = driverStore.get(request.getDriverContainerId(), request.getVolumeId(),
                DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId());
        if (driver != null) {
            driverStore.changeLimitType(request.getDriverContainerId(), request.getVolumeId(),
                    DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId(), request.getLimitId(),
                    request.isStaticLimit());
        } else {
            logger.error("changeLimitType can not find any driver by request:{}", request);
            throw new DriverNotFoundException_Thrift();
        }
        ChangeLimitTypeResponse response = new ChangeLimitTypeResponse(request.getRequestId());
        logger.warn("changeLimitType response: {}", response);
        return response;
    }

    /**
     * This interface is called by data node. When segment unit recover failed
     */
    @Override
    public ReportSegmentUnitRecycleFailResponse reportSegmentUnitRecycleFail(
            ReportSegmentUnitRecycleFailRequest request)
            throws InternalError_Thrift, ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("reportSegmentUnitRecycleFail request: {}", request);
        // store the root volume which need to turn back again
        Set<Long> volumeNeedToDeletingAgain = new HashSet<>();

        for (SegmentUnitMetadata_Thrift segmentUnit : request.getSegUnitsMetadata()) {
            logger.warn("segment unit recover faild: segment unit {}", segmentUnit);
            // check if it is a root volume
            VolumeMetadata volume = null;
            try {
                volume = volumeStore.getVolume(segmentUnit.getVolumeId());
            } catch (Exception e) {
                logger.warn("Can not get volume ", e);
            }

            if (volume != null) {
                long rootVolumeId = volume.isRoot() ? volume.getVolumeId() : volume.getRootVolumeId();
                volumeNeedToDeletingAgain.add(rootVolumeId);
            }
        }

        // set the volume deleting again
        for (Long volumeId : volumeNeedToDeletingAgain) {
            VolumeMetadata volume = null;
            try {
                volume = volumeStore.getVolume(volumeId);
            } catch (Exception e) {
                logger.warn("Can not get volume ", e);
            }

            // deleting all volume include children volume
            if (volume != null) {
                logger.warn("turn the volume to deleting again {}", volume);
                List<VolumeMetadata> volumeMetadatas = volumeStore.listVolumesFromRoot(volume.getVolumeId());
                // set the volume status is deleting;
                for (VolumeMetadata volumeMetadata : volumeMetadatas) {
                    volumeMetadata.setVolumeStatus(VolumeStatus.Deleting);
                    volumeStatusStore.addVolumeToStore(volumeMetadata);
                    volumeStore.updateStatusAndVolumeInAction(volumeMetadata.getVolumeId(),
                            VolumeStatus.Deleting.toString(), DELETING.name());
                }

            }
        }
        ReportSegmentUnitRecycleFailResponse response = new ReportSegmentUnitRecycleFailResponse(
                request.getRequestId());
        logger.warn("reportSegmentUnitRecycleFail response: {}", response);
        return response;
    }

    @Override
    public OrphanVolumeResponse listOrphanVolume(OrphanVolumeRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("listOrphanVolume request: {}", request);
        List<Long> orphanVolumeIds = orphanVolumes.getOrphanVolume();

        OrphanVolumeResponse response = new OrphanVolumeResponse();
        List<VolumeMetadata_Thrift> orphanVolumeThriftList = new ArrayList<VolumeMetadata_Thrift>();

        for (Long orphanVolumeId : orphanVolumeIds) {
            VolumeMetadata orphanRootVolume;
            try {
                orphanRootVolume = getRootVolumeWithChildren(orphanVolumeId, true);
            } catch (VolumeNotFoundException e) {
                logger.error("Catch an exception when get root volume with children", e);
                continue;
            }
            // if volume status is dead, no need to display at console
            if (orphanRootVolume.getVolumeStatus().equals(VolumeStatus.Dead)) {
                logger.debug("volume is in dead status, volume id is {}, name is {}", orphanRootVolume.getVolumeId(),
                        orphanRootVolume.getName());
                continue;
            }
            orphanVolumeThriftList.add(RequestResponseHelper.buildThriftVolumeFrom(orphanRootVolume, false));
        }

        response.setRequestId(request.getRequestId());
        response.setOrphanVolumes(orphanVolumeThriftList);
        logger.warn("listOrphanVolume response: {}", response);
        return response;
    }

    public MarkDriverStatusResponse_Thrift markDriverStatus(MarkDriverStatusRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, InvalidInputException_Thrift, ServiceIsNotAvailable_Thrift,
            TException {
        MarkDriverStatusResponse_Thrift response = new MarkDriverStatusResponse_Thrift();
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        logger.warn("markDriverStatus request: {}", request);

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());

        if (volumeMetadata == null) {
            logger.error("No volume found with id {}", request.getVolumeId());
            throw new VolumeNotFoundException_Thrift();
        }

        List<DriverIpTarget_Thrift> driverTargets = request.getDriverIpTargetList();
        if (driverTargets == null || driverTargets.isEmpty()) {
            return response;
        }

        long volumeId = request.getVolumeId();
        for (DriverIpTarget_Thrift driverTarget : driverTargets) {
            DriverType driverType = DriverType.valueOf(driverTarget.getDriverType().name());
            DriverMetadata driver = driverStore
                    .get(driverTarget.getDriverContainerId(), volumeId, driverType, driverTarget.getSnapshotId());
            if (driver == null) {
                logger.warn("can not get the driver: volumeId {}, hostname {}, snapshot ID {}", volumeId,
                        driverTarget.getDriverIp(), driverTarget.getSnapshotId());
                continue;
            }

            driver.setDriverStatus(DriverStatus.REMOVING);
            driverStore.save(driver);
        }

        logger.warn("markDriverStatus response: {}", response);
        return response;
    }

    @Override
    public TGetAlarmResponse getAlarms(TGetAlarmRequest request)
            throws ServiceHavingBeenShutdown_Thrift, InvalidInputException_Thrift, ServiceIsNotAvailable_Thrift,
            TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        logger.warn("getAlarms request: {}", request);
        TGetAlarmResponse response = new TGetAlarmResponse();
        response.setResponseId(request.getRequestId());
        List<TAlarmInformation> tAlarmInformations = new ArrayList<>();

        // disk
        for (InstanceMetadata instance : storageStore.list()) {
            List<RawArchiveMetadata> archives = instance.getArchives();
            for (RawArchiveMetadata archive : archives) {
                if (archive.getStatus().equals(ArchiveStatus.BROKEN) || archive.getStatus()
                        .equals(ArchiveStatus.DEGRADED) || archive.getStatus().equals(ArchiveStatus.EJECTED)) {
                    TAlarmInformation alarmInformation = new TAlarmInformation();
                    alarmInformation.setAlarmLevel("0");
                    alarmInformation.setAlarmName("disk-alarm");
                    alarmInformation.setAlarmObject("DISK");
                    alarmInformation.setTimeStamp(Long.toString(System.currentTimeMillis()));
                    String description = "Disk is not work well. current status is " + archive.getStatus().name();
                    alarmInformation.setDescription(description);

                    tAlarmInformations.add(alarmInformation);
                }
            }
        }

        // volume
        List<VolumeMetadata> volumes = volumeStore.listVolumes();
        for (VolumeMetadata volume : volumes) {
            if (volume.getVolumeStatus().equals(VolumeStatus.Unavailable)) {
                TAlarmInformation alarmInformation = new TAlarmInformation();
                alarmInformation.setAlarmLevel("0");
                alarmInformation.setAlarmName("volume-alarm");
                alarmInformation.setAlarmObject("VOLUME");
                alarmInformation.setTimeStamp(Long.toString(System.currentTimeMillis()));
                alarmInformation.setDescription(String.format("Volume %s is not available", volume.getName()));

                tAlarmInformations.add(alarmInformation);
            }
        }

        // driver
        List<DriverMetadata> drivers = driverStore.list();
        for (DriverMetadata driver : drivers) {
            if (driver.getDriverStatus().equals(DriverStatus.UNKNOWN)) {
                TAlarmInformation alarmInformation = new TAlarmInformation();
                alarmInformation.setAlarmLevel("0");
                alarmInformation.setAlarmName("driver-alarm");
                alarmInformation.setAlarmObject("DRIVER");
                alarmInformation.setTimeStamp(Long.toString(System.currentTimeMillis()));
                alarmInformation.setDescription(
                        String.format("Driver on volume %s:%d is in bad status", driver.getHostName(),
                                driver.getPort()));

                tAlarmInformations.add(alarmInformation);
            }
        }

        // instance
        Set<Instance> instances = instanceStore.getAll();
        for (Instance instance : instances) {
            if (instance.getStatus().equals(InstanceStatus.INC) || instance.getStatus().equals(InstanceStatus.FAILED)) {
                TAlarmInformation alarmInformation = new TAlarmInformation();
                alarmInformation.setAlarmLevel("0");
                alarmInformation.setAlarmName("node-alarm");
                alarmInformation.setAlarmObject("NODE");
                alarmInformation.setTimeStamp(Long.toString(System.currentTimeMillis()));
                alarmInformation.setDescription(String.format("Node %s is in bad status", instance.getName()));

                tAlarmInformations.add(alarmInformation);
            }
        }
        response.setAlarms(tAlarmInformations);
        logger.warn("getAlarms response: {}", response);
        return response;
    }

    @Override
    public TGetAlarmResponse getAlarmsFromSyslog(TGetAlarmRequest request)
            throws ServiceHavingBeenShutdown_Thrift, InvalidInputException_Thrift, ServiceIsNotAvailable_Thrift,
            TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("getAlarmsFromSyslog request: {}", request);
        TGetAlarmResponse response = new TGetAlarmResponse();
        response.setResponseId(request.getRequestId());
        List<TAlarmInformation> tAlarmInformations = new ArrayList<>();

        // network & others
        List<AlarmInfo> activeAlarms = alarmStore.listActiveAlarm();
        logger.debug("Active alarms are : {}", activeAlarms);
        for (AlarmInfo alarm : activeAlarms) {
            TAlarmInformation alarmInformation = new TAlarmInformation();
            alarmInformation.setAlarmLevel("Emergency");
            alarmInformation.setAlarmName(alarm.getEndpoint().getHostName());
            alarmInformation.setAlarmObject(String.format("END-POINT: [ %s:(%s) ]", alarm.getEndpoint().getHostName(),
                    alarm.getSourceObject()));
            alarmInformation.setTimeStamp(Long.toString(System.currentTimeMillis()));
            alarmInformation.setDescription(alarm.getDescription());

            tAlarmInformations.add(alarmInformation);
        }

        // disk
        for (InstanceMetadata instance : storageStore.list()) {
            List<RawArchiveMetadata> archives = instance.getArchives();
            for (RawArchiveMetadata archive : archives) {
                if (!archive.getStatus().equals(ArchiveStatus.GOOD)) {
                    TAlarmInformation alarmInformation = new TAlarmInformation();
                    alarmInformation.setAlarmLevel("Critical");
                    alarmInformation.setAlarmName("disk-alarm");
                    alarmInformation.setAlarmObject(
                            String.format("DISK: [ %s:(%s) ]]", new EndPoint(instance.getEndpoint()).getHostName(),
                                    archive.getDeviceName()));
                    alarmInformation.setTimeStamp(Long.toString(System.currentTimeMillis()));
                    String description = String
                            .format("Disk %s is not work well. current status is %s", archive.getDeviceName(),
                                    archive.getStatus().name());
                    alarmInformation.setDescription(description);

                    tAlarmInformations.add(alarmInformation);
                }
            }
        }

        // volume
        List<VolumeMetadata> volumes = volumeStore.listVolumes();
        for (VolumeMetadata volume : volumes) {
            if (volume.getVolumeStatus().equals(VolumeStatus.Unavailable)) {
                TAlarmInformation alarmInformation = new TAlarmInformation();
                alarmInformation.setAlarmLevel("");
                alarmInformation.setAlarmName("volume-alarm");
                alarmInformation.setAlarmObject(String.format("VOLUME: [%s]", volume.getName()));
                alarmInformation.setTimeStamp(Long.toString(System.currentTimeMillis()));
                alarmInformation.setDescription(String.format("Volume %s is not available", volume.getName()));

                tAlarmInformations.add(alarmInformation);
            }
        }

        // driver
        // List<DriverMetadata> drivers = driverStore.list();
        // for (DriverMetadata driver : drivers) {
        // if (driver.getDriverStatus().equals(DriverStatus.UNKNOWN)) {
        // TAlarmInformation alarmInformation = new TAlarmInformation();
        // alarmInformation.setAlarmLevel("Error");
        // alarmInformation.setAlarmName("driver-alarm");
        // alarmInformation.setAlarmObject(String.format("DRIVER: [%s:%s]", driver.getHostName(), driver.getPort()));
        // alarmInformation.setTimeStamp(Long.toString(System.currentTimeMillis()));
        // alarmInformation.setDescription(String.format("Driver on volume %s:%d is in bad status",
        // driver.getHostName(), driver.getPort()));
        //
        // tAlarmInformations.add(alarmInformation);
        // }
        // }

        // instance/machine
        Set<Instance> instances = instanceStore.getAll(PyService.DIH.getServiceName());
        for (Instance instance : instances) {
            if (instance.getStatus().equals(InstanceStatus.INC) || instance.getStatus().equals(InstanceStatus.FAILED)) {
                TAlarmInformation alarmInformation = new TAlarmInformation();
                alarmInformation.setAlarmLevel("Critical");
                alarmInformation.setAlarmName(instance.getEndPoint().getHostName());
                alarmInformation.setAlarmObject(String.format("SERVER: [%s]", instance.getEndPoint().getHostName()));
                alarmInformation.setTimeStamp(Long.toString(System.currentTimeMillis()));
                alarmInformation.setDescription(
                        String.format("Node %s is closed or the link is down", instance.getEndPoint().getHostName()));

                tAlarmInformations.add(alarmInformation);
            }
        }

        response.setAlarms(tAlarmInformations);
        logger.warn("getAlarmsFromSyslog response: {}", response);
        return response;
    }

    @Override
    public CreateSnapshotResponse createSnapshot(CreateSnapshotRequest request)
            throws SnapshotNameExistException_Thrift, SnapshotExistException_Thrift,
            SnapshotCountReachMaxException_Thrift, ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift,
            VolumeNotFoundException_Thrift, SnapshotDescriptionTooLongException_Thrift,
            SnapshotNameTooLongException_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("createSnapshot request: {}", request);

        VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());

        // volume not exist
        if (volumeMetadata == null || !volumeMetadata.isRoot()) {
            logger.debug("Can't find volume metadata given its id {}", request.getVolumeId());
            throw new VolumeNotFoundException_Thrift();
        }

        VolumeMetadata rootVolumeWithChildren;
        try {
            rootVolumeWithChildren = getRootVolumeWithChildren(volumeMetadata.getVolumeId(), false);
        } catch (VolumeNotFoundException e) {
            throw new VolumeNotFoundException_Thrift();
        }

        SnapshotMetadata snp;
        int baseVersion;
        long volumeSize;
        try {
            VolumeSnapshotManager snapshotManager = volumeMetadata.getSnapshotManager();
            baseVersion = snapshotManager.getVersion();
            volumeSize = rootVolumeWithChildren.getVolumeSize();
            snp = snapshotManager
                    .createSnapshot(request.getSnapshotName(), request.getDescription(), request.getCreatedTime(),
                            baseVersion, volumeSize);
        } catch (SnapshotCountReachMaxException e) {
            logger.error("caught an exception ", e);
            throw new SnapshotCountReachMaxException_Thrift();
        } catch (SnapshotNameExistException e) {
            logger.error("caught an exception ", e);
            throw new SnapshotNameExistException_Thrift();
        } catch (SnapshotRollingBackException e) {
            logger.error("caught an exception ", e);
            throw new SnapshotRollingBackException_Thrift();
        } catch (SnapshotVersionMismatchException e) {
            logger.error("caught an impossible exception ", e);
            throw new InternalError_Thrift();
        } catch (SnapshotDescriptionTooLongException e) {
            logger.error("caught an exception ", e);
            throw new SnapshotDescriptionTooLongException_Thrift();
        } catch (SnapshotNameTooLongException e) {
            logger.error("caught an exception ", e);
            throw new SnapshotNameTooLongException_Thrift();
        }

        CreateSnapshotResponse response = new CreateSnapshotResponse();
        response.setRequestId(request.getRequestId());
        response.setSnapshotId(snp.getSnapshotId());
        response.setVersion(baseVersion);

        logger.warn("createSnapshot response: {}", response);
        return response;
    }

    @Override
    public DeleteSnapshotResponse deleteSnapshot(DeleteSnapshotRequest request)
            throws ServiceHavingBeenShutdown_Thrift, SnapshotNotFoundException_Thrift, ServiceIsNotAvailable_Thrift,
            VolumeNotFoundException_Thrift, SnapshotRollingBackException_Thrift,
            SnapshotVersionMismatchException_Thrift, SnapshotIsInCloningException_Thrift {

        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("deleteSnapshot request: {}", request);
        VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());

        // volume not exist
        if (volumeMetadata == null || !volumeMetadata.isRoot()) {
            logger.debug("Can't find volume metadata given its id {}", request.getVolumeId());
            throw new VolumeNotFoundException_Thrift();
        }

        List<VolumeMetadata> volumeMetadatas = volumeStore.listRootVolumes();
        for (VolumeMetadata tmp : volumeMetadatas) {
            if (tmp.getCloningVolumeId() == request.getVolumeId() && tmp.getCloningSnapshotId() == request.getSnapshotId()) {
                logger.warn("snapshot is in cloning by volume, snapshotId={}, volumeId={}.", request.getSnapshotId(),
                        tmp.getVolumeId());
                throw new SnapshotIsInCloningException_Thrift();
            }
        }

        DeleteSnapshotResponse response = new DeleteSnapshotResponse();
        try {
            VolumeSnapshotManager snapshotManager = volumeMetadata.getSnapshotManager();
            int baseVersion = snapshotManager.getVersion();
            snapshotManager.deleteSnapshot(request.getSnapshotId(), baseVersion);
            response.setVersion(baseVersion);
        } catch (SnapshotNotFoundException e) {
            logger.error("caught an exception", e);
            throw new SnapshotNotFoundException_Thrift();
        } catch (SnapshotRollingBackException e) {
            throw new SnapshotRollingBackException_Thrift();
        } catch (SnapshotVersionMismatchException e) {
            throw new SnapshotVersionMismatchException_Thrift();
        }

        response.setRequestId(request.getRequestId());

        logger.warn("deleteSnapshot response: {}", response);
        return response;
    }

    @Override
    public RollbackFromSnapshotResponse rollbackFromSnapshot(RollbackFromSnapshotRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, VolumeNotFoundException_Thrift,
            SnapshotNotFoundException_Thrift, LaunchedVolumeCannotRollbackException_Thrift,
            SnapshotRollingBackException_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("rollbackFromSnapshot request: {}", request);

        VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());

        // volume not exist
        if (volumeMetadata == null || !volumeMetadata.isRoot()) {
            logger.debug("Can't find volume metadata given its id {}", request.getVolumeId());
            throw new VolumeNotFoundException_Thrift();
        }

        // check if the volume has been launched
        List<DriverMetadata> volumeBindingDrivers = driverStore.get(request.getVolumeId());
        if (volumeBindingDrivers.size() > 0) {
            throw new LaunchedVolumeCannotRollbackException_Thrift();
        }

        // TODO consider save the roll back status to DB in case the info center of OK become SUSPEND or INC
        VolumeSnapshotManager snapshotManager = volumeMetadata.getSnapshotManager();
        int baseVersion = snapshotManager.getVersion();
        try {
            snapshotManager.startRollingBack(request.getSnapshotId(), baseVersion);
        } catch (SnapshotNotFoundException e) {
            logger.error("caught an exception", e);
            throw new SnapshotNotFoundException_Thrift();
        } catch (SnapshotRollingBackException e) {
            logger.error("caught an exception", e);
            throw new SnapshotRollingBackException_Thrift();
        } catch (SnapshotVersionMismatchException e) {
            logger.error("caught an impossible exception", e);
            throw new InternalError_Thrift();
        }

        RollbackFromSnapshotResponse response = new RollbackFromSnapshotResponse(request.getRequestId(), baseVersion);
        logger.warn("rollbackFromSnapshot response: {}", response);
        return response;
    }

    @Override
    public ReportSegmentUnitCloneFailResponse reportCloneFailed(ReportSegmentUnitCloneFailRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, VolumeNotFoundException_Thrift,
            TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("reportCloneFailed request: {}", request);
        VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());

        if (volumeMetadata == null) {
            throw new VolumeNotFoundException_Thrift();
        }

        if (!volumeMetadata.isDeletedByUser()) {
            volumeMetadata.setVolumeStatus(VolumeStatus.Deleting);
            volumeStatusStore.addVolumeToStore(volumeMetadata);
            volumeStore.updateStatusAndVolumeInAction(volumeMetadata.getVolumeId(), VolumeStatus.Deleting.toString(),
                    DELETING.name());
            logger.warn("user delete volume {}, volume status change to {}", volumeMetadata, VolumeStatus.Deleting);
        }

        /*
         * for clone volume which has been extended before, the child volume may also clone failed, in that case, we
         * should also delete the root volume If the root volume clone failed, we also should need delete the children
         * volume.
         *
         */
        VolumeMetadata rootVolume = null;
        if (volumeMetadata.isRoot()) {
            rootVolume = volumeMetadata;
        } else {
            rootVolume = volumeStore.getVolume(volumeMetadata.getRootVolumeId());
        }

        if (rootVolume != null) {
            List<VolumeMetadata> volumeMetadatas = volumeStore.listVolumesFromRoot(volumeMetadata.getVolumeId());
            for (VolumeMetadata volume : volumeMetadatas) {
                if (!volume.isDeletedByUser()) {
                    volume.setVolumeStatus(VolumeStatus.Deleting);
                    volumeStatusStore.addVolumeToStore(volume);
                    volumeStore.updateStatusAndVolumeInAction(volume.getVolumeId(), VolumeStatus.Deleting.toString(),
                            DELETING.name());
                    logger.warn("user delete volume {}, volume status change to {}", volume, VolumeStatus.Deleting);
                }
            }
        }

        ReportSegmentUnitCloneFailResponse response = new ReportSegmentUnitCloneFailResponse(request.getRequestId());
        logger.warn("reportCloneFailed response: {}", response);
        return response;
    }

    @Override
    public CreateStoragePoolResponse_Thrift createStoragePool(CreateStoragePoolRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, InvalidInputException_Thrift, StoragePoolExistedException_Thrift,
            StoragePoolNameExistedException_Thrift, ServiceIsNotAvailable_Thrift, ArchiveNotFoundException_Thrift,
            ArchiveNotFreeToUseException_Thrift, DomainNotExistedException_Thrift, DomainIsDeletingException_Thrift,
            TException {
        if (shutDownFlag) {
            logger.error("InfoCenter:{} had been shutdown", appContext.getMainEndPoint());
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("createStoragePool request: {}", request);
        try {
            ValidateParam.validateCreateStoragePoolRequest(request);
        } catch (InvalidInputException_Thrift e) {
            throw e;
        }
        CreateStoragePoolResponse_Thrift response = new CreateStoragePoolResponse_Thrift();

        StoragePool storagePoolToBeCreate = RequestResponseHelper.buildStoragePoolFromThrift(request.getStoragePool());

        // check domain id exist
        Domain domain = null;
        try {
            domain = domainStore.getDomain(storagePoolToBeCreate.getDomainId());
        } catch (Exception e) {
            logger.warn("can not get domain", e);
        }
        if (domain == null) {
            logger.warn("can't find any domain by id:{}", storagePoolToBeCreate.getDomainId());
            throw new DomainNotExistedException_Thrift();
        }

        if (domain.getStatus() == Status.Deleting) {
            logger.error("domain:{} is deleting", domain);
            throw new DomainIsDeletingException_Thrift();
        }
        // check pool id not exist
        StoragePool storagePoolExist = null;
        try {
            storagePoolExist = storagePoolStore.getStoragePool(storagePoolToBeCreate.getPoolId());
        } catch (Exception e) {
            logger.warn("can not get storage pool", e);
        }
        if (storagePoolExist != null) {
            throw new StoragePoolExistedException_Thrift();
        }
        // check name not exist
        List<StoragePool> allStoragePool = null;
        try {
            allStoragePool = storagePoolStore.listAllStoragePools();
        } catch (Exception e) {
            logger.error("can not get storage pools", e);
        }
        if (allStoragePool == null) {
            logger.error("failed to get storage pool");
            throw new ServiceIsNotAvailable_Thrift();
        }
        if (!allStoragePool.isEmpty()) {
            for (StoragePool storagePool : allStoragePool) {
                if (storagePool.getDomainId().equals(storagePoolToBeCreate.getDomainId()) && storagePool.getName()
                        .equals(storagePoolToBeCreate.getName())) {
                    throw new StoragePoolNameExistedException_Thrift();
                }
            }
        }

        // can't contain volume ids
        Validate.isTrue(storagePoolToBeCreate.getVolumeIds().isEmpty(),
                "it is impossiable contain volume ids:" + storagePoolToBeCreate.getVolumeIds());

        Map<Long, List<Long>> datanodeMapArchives = new HashMap<>();

        // check if has any archive
        if (!storagePoolToBeCreate.getArchivesInDataNode().isEmpty()) {
            // find all refer datanodes first
            for (Entry<Long, Long> entry : storagePoolToBeCreate.getArchivesInDataNode().entries()) {
                Long datanodeId = entry.getKey();
                Long archiveId = entry.getValue();
                if (!datanodeMapArchives.containsKey(datanodeId)) {
                    List<Long> addedArchiveIds = new ArrayList<>();
                    datanodeMapArchives.put(datanodeId, addedArchiveIds);
                }

                InstanceMetadata datanode = storageStore.get(datanodeId);
                if (datanode == null) {
                    logger.error("can't find datanode by given id:{}, storage:{}", datanodeId, storageStore.list());
                    throw new ArchiveNotFoundException_Thrift();
                }
                // check if datanode in this domain
                if (datanode.getDomainId() != null && !domain.getDomainId().equals(datanode.getDomainId())) {
                    logger.error(
                            "can not add archive:{} to storage pool:{}, because this datanode:{} is not in domain:{}",
                            archiveId, storagePoolToBeCreate.getPoolId(), datanodeId, domain.getDomainId());
                    throw new ArchiveNotFoundException_Thrift();
                }

                List<Long> archiveIds = datanodeMapArchives.get(datanodeId);
                RawArchiveMetadata archive = datanode.getArchiveById(archiveId);
                if (archive == null) {
                    logger.error("can't find archive:{} at datanode:{}, request:{}", archiveId, datanode, request);
                    throw new ArchiveNotFoundException_Thrift();
                } else {
                    if (!archive.isFree()) {
                        throw new ArchiveNotFreeToUseException_Thrift();
                    } else {
                        archiveIds.add(archiveId);
                        // mark archive is in use
                        archive.setStoragePoolId(storagePoolToBeCreate.getPoolId());
                    }
                }
                storageStore.save(datanode);
            }
        }
        // now we can save to storage pool store
        storagePoolToBeCreate.setLastUpdateTime(System.currentTimeMillis());
        storagePoolToBeCreate.setDomainName(domain.getDomainName());
        storagePoolToBeCreate.setStoragePoolLevel(StoragePoolLevel.HIGH.name());
        storagePoolStore.saveStoragePool(storagePoolToBeCreate);
        // also remember to update domain store
        domain = null;
        try {
            domain = domainStore.getDomain(storagePoolToBeCreate.getDomainId());
        } catch (Exception e) {
            logger.warn("can not get domain", e);
        }
        if (domain == null) {
            logger.error("can not add storage pool:{} to domain", storagePoolToBeCreate.getPoolId());
            throw new DomainNotExistedException_Thrift();
        }

        domain.addStoragePool(storagePoolToBeCreate.getPoolId());
        domainStore.saveDomain(domain);

        MigrationRule migrationRule = null;
        if (storagePoolToBeCreate.getMigrationRuleId() != StoragePool.DEFAULT_MIGRATION_RULE_ID.longValue()) {
            MigrationRuleInformation migrationRuleInformation = migrationRuleStore.get(storagePoolToBeCreate.getMigrationRuleId());
            if (migrationRuleInformation != null) {
                migrationRule = migrationRuleInformation.toMigrationRule();
            }
        }

        response.setStoragePoolThrift(RequestResponseHelper.buildThriftStoragePoolFrom(storagePoolToBeCreate, migrationRule));
        response.setRequestId(request.getRequestId());
        response.setDatanodeMapAddedArchives(datanodeMapArchives);
        logger.warn("createStoragePool response: {}", response);
        return response;
    }

    @Override
    public UpdateStoragePoolResponse_Thrift updateStoragePool(UpdateStoragePoolRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, InvalidInputException_Thrift, ServiceIsNotAvailable_Thrift,
            ArchiveNotFoundException_Thrift, StoragePoolNotExistedException_Thrift, DomainNotExistedException_Thrift,
            StoragePoolIsDeletingException_Thrift, TException {
        if (shutDownFlag) {
            logger.error("InfoCenter:{} had been shutdown", appContext.getMainEndPoint());
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("updateStoragePool request: {}", request);

        try {
            ValidateParam.validateUpdateStoragePoolRequest(request);
        } catch (InvalidInputException_Thrift e) {
            throw e;
        }
        UpdateStoragePoolResponse_Thrift response = new UpdateStoragePoolResponse_Thrift();
        StoragePool storagePoolToBeUpdate = RequestResponseHelper.buildStoragePoolFromThrift(request.getStoragePool());

        // check domain id exist
        Domain domain = null;
        try {
            domain = domainStore.getDomain(storagePoolToBeUpdate.getDomainId());
        } catch (Exception e) {
            logger.warn("can not get domain", e);
        }
        if (domain == null) {
            logger.warn("can't find any domain by id:{}", storagePoolToBeUpdate.getDomainId());
            throw new DomainNotExistedException_Thrift();
        }
        // check pool id exist
        StoragePool storagePoolExist = null;
        try {
            storagePoolExist = storagePoolStore.getStoragePool(storagePoolToBeUpdate.getPoolId());
        } catch (Exception e) {
            logger.warn("can not get storage pool", e);
        }
        if (storagePoolExist == null) {
            throw new StoragePoolNotExistedException_Thrift();
        }

        if (storagePoolExist.getStatus() == Status.Deleting) {
            logger.error("storage pool is deleting, storage pool:{}", storagePoolExist);
            throw new StoragePoolIsDeletingException_Thrift();
        }

        if (!storagePoolExist.getName().equals(storagePoolToBeUpdate.getName())) {
            // check update name not exist, except original storage pool name
            List<StoragePool> allStoragePool;
            try {
                allStoragePool = storagePoolStore.listAllStoragePools();
            } catch (Exception e) {
                logger.error("can not list storage pools", e);
                throw new ServiceIsNotAvailable_Thrift();
            }
            Validate.notNull(allStoragePool);
            for (StoragePool storagePool : allStoragePool) {
                if (storagePool.getDomainId().equals(storagePoolToBeUpdate.getDomainId()) && storagePool.getName()
                        .equals(storagePoolToBeUpdate.getName())) {
                    logger.error("update name is exist, exist storage pool:{}, update storage pool:{}", storagePool,
                            storagePoolToBeUpdate);
                    throw new StoragePoolNameExistedException_Thrift();
                }
            }
        }
        Map<Long, List<Long>> datanodeMapArchives = new HashMap<>();
        // check if removing any archive(for now don't care about remove), and which archive to be add
        // find which archive to be add
        for (Entry<Long, Long> entry : storagePoolToBeUpdate.getArchivesInDataNode().entries()) {
            Long datanodeId = entry.getKey();
            Long archiveId = entry.getValue();
            if (!datanodeMapArchives.containsKey(datanodeId)) {
                List<Long> addedArchiveIds = new ArrayList<>();
                datanodeMapArchives.put(datanodeId, addedArchiveIds);
            }
            if (!storagePoolExist.getArchivesInDataNode().containsEntry(datanodeId, archiveId)) {
                InstanceMetadata datanode = storageStore.get(datanodeId);
                if (datanode == null) {
                    logger.error("can't find datanode by given id:{}, storage:{}", datanodeId, storageStore.list());
                    throw new ArchiveNotFoundException_Thrift();
                }

                // check if datanode in this domain
                if (!domain.getDomainId().equals(datanode.getDomainId())) {
                    logger.error(
                            "can not add archive:{} to storage pool:{}, because this datanode:{} is not in domain:{}",
                            archiveId, storagePoolToBeUpdate.getPoolId(), datanodeId, domain.getDomainId());
                    throw new ArchiveNotFoundException_Thrift();
                }
                List<Long> archiveIds = datanodeMapArchives.get(datanodeId);
                RawArchiveMetadata archive = datanode.getArchiveById(archiveId);
                if (archive == null) {
                    logger.error("can't find archive by given id:{}, datanode:{}", archiveId, datanode);
                    throw new ArchiveNotFoundException_Thrift();
                } else {
                    if (!archive.isFree()) {
                        throw new ArchiveNotFreeToUseException_Thrift();
                    } else {
                        archiveIds.add(archiveId);
                        // mark archive is in use
                        archive.setStoragePoolId(storagePoolToBeUpdate.getPoolId());
                    }
                }
                storageStore.save(datanode);
                storagePoolToBeUpdate.setLastUpdateTime(System.currentTimeMillis());
            }
        }

        for (Entry<Long, Collection<Long>> entry : storagePoolExist.getArchivesInDataNode().asMap().entrySet()) {
            storagePoolToBeUpdate.getArchivesInDataNode().putAll(entry.getKey(), entry.getValue());
        }

        Map<Long, InstanceMetadata> instanceId2InstanceMetadata = new HashMap<>();
        Map<Long, RawArchiveMetadata> archiveId2Archive = new HashMap<>();
        for (InstanceMetadata instance : storageStore.list()) {
            if (instance.getDatanodeStatus().equals(OK)) {
                instanceId2InstanceMetadata.put(instance.getInstanceId().getId(), instance);
                for (RawArchiveMetadata archiveMetadata : instance.getArchives()) {
                    archiveId2Archive.put(archiveMetadata.getArchiveId(), archiveMetadata);
                }
            }
        }
        long logicalSpace = 0;
        long logicalFreeSpace = 0;
        for (Long archiveId : storagePoolExist.getArchivesInDataNode().values()) {
            RawArchiveMetadata archiveMetadata = archiveId2Archive.get(archiveId);
            if (archiveMetadata != null) {
                logicalSpace += archiveMetadata.getLogicalSpace();
                logicalFreeSpace += archiveMetadata.getLogicalFreeSpace();
            }
        }

        storagePoolExist.setTotalSpace(logicalSpace);
        storagePoolExist.setFreeSpace(logicalFreeSpace);
        storagePoolToBeUpdate.setLogicalPSSFreeSpace(StoragePoolSpaceCalculator
                .calculateFreeSpace(storagePoolToBeUpdate, instanceId2InstanceMetadata, archiveId2Archive, 3,
                        segmentSize));
        storagePoolToBeUpdate.setLogicalPSAFreeSpace(StoragePoolSpaceCalculator
                .calculateFreeSpace(storagePoolToBeUpdate, instanceId2InstanceMetadata, archiveId2Archive, 2,
                        segmentSize));

        // now we can save to storage pool store
        storagePoolToBeUpdate.setVolumeIds(storagePoolExist.getVolumeIds());
        storagePoolToBeUpdate.setStoragePoolLevel(storagePoolExist.getStoragePoolLevel());
        storagePoolToBeUpdate.setMigrationRuleId(storagePoolExist.getMigrationRuleId());
        storagePoolToBeUpdate.setDomainName(domain.getDomainName());
        storagePoolStore.saveStoragePool(storagePoolToBeUpdate);
        backupDBManager.backupDatabase();
        response.setRequestId(request.getRequestId());
        response.setDatanodeMapAddedArchives(datanodeMapArchives);
        logger.warn("updateStoragePool response: {}", response);
        return response;
    }

    @Override
    public DeleteStoragePoolResponse_Thrift deleteStoragePool(DeleteStoragePoolRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, InvalidInputException_Thrift,
            StoragePoolNotExistedException_Thrift, ServiceIsNotAvailable_Thrift, StillHaveVolumeException_Thrift,
            DomainNotExistedException_Thrift, StoragePoolIsDeletingException_Thrift, TException {
        if (shutDownFlag) {
            logger.error("InfoCenter:{} had been shutdown", appContext.getMainEndPoint());
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("deleteStoragePool request: {}", request);
        try {
            ValidateParam.validateDeleteStoragePoolRequest(request);
        } catch (InvalidInputException_Thrift e) {
            throw new InvalidInputException_Thrift();
        }
        DeleteStoragePoolResponse_Thrift response = new DeleteStoragePoolResponse_Thrift();
        // check domain id exist
        Domain domain = null;
        try {
            domain = domainStore.getDomain(request.getDomainId());
        } catch (Exception e) {
            logger.warn("can not get domain", e);
        }
        if (domain == null) {
            logger.warn("can't find any domain by id:{}", request.getDomainId());
            throw new DomainNotExistedException_Thrift();
        }
        // check pool id exist
        StoragePool storagePoolExist = null;
        try {
            storagePoolExist = storagePoolStore.getStoragePool(request.getStoragePoolId());
        } catch (Exception e) {
            logger.warn("can not get storage pool", e);
        }
        if (storagePoolExist == null) {
            logger.error("storage pool is not exist any more");
            throw new StoragePoolNotExistedException_Thrift();
        }

        if (storagePoolExist.getStatus() == Status.Deleting) {
            logger.error("storage pool is deleting, storage pool:{}", storagePoolExist);
            throw new StoragePoolIsDeletingException_Thrift();
        }
        for (Long volumeId : storagePoolExist.getVolumeIds()) {
            VolumeMetadata volume = volumeStore.getVolume(volumeId);
            if (volume != null && volume.getVolumeStatus() != VolumeStatus.Dead) {
                logger.error("still has volume:{} not dead in storage pool:{}, domain:{}", volumeId,
                        request.getStoragePoolId(), request.getDomainId());
                throw new StillHaveVolumeException_Thrift();
            }
        }
        // free all archives in storage pool
        for (Entry<Long, Long> entry : storagePoolExist.getArchivesInDataNode().entries()) {
            Long datanodeId = entry.getKey();
            Long archiveId = entry.getValue();
            InstanceMetadata datanode = storageStore.get(datanodeId);
            if (datanode == null) {
                // for now, datanode is not here, should wait it report and process it
                logger.warn("can't find datanode by given id:{}, storage:{}", datanodeId, storageStore.list());
                continue;
            }
            RawArchiveMetadata archive = datanode.getArchiveById(archiveId);
            if (archive != null) {
                archive.setFree();
            }
            storageStore.save(datanode);
        }
        // now can delete storage pool
        storagePoolExist.setLastUpdateTime(System.currentTimeMillis());
        storagePoolExist.setStatus(Status.Deleting);
        storagePoolStore.saveStoragePool(storagePoolExist);
        // also need to return to control center to free all archives in storage pool
        Map<Long, List<Long>> mapDatanodeToArchiveIds = new HashMap<>();
        for (Entry<Long, Collection<Long>> entry : storagePoolExist.getArchivesInDataNode().asMap().entrySet()) {
            List<Long> datanodeIds = new ArrayList<>();
            datanodeIds.addAll(entry.getValue());
            mapDatanodeToArchiveIds.put(entry.getKey(), datanodeIds);
        }
        // response to control center to free all archives in datanode
        response.setDatanodeMapRemovedArchiveIds((mapDatanodeToArchiveIds));
        // now can delete storage pool in volume
        domain = null;
        try {
            domain = domainStore.getDomain(request.getDomainId());
        } catch (Exception e) {
            logger.error("get domain failed", e);
            throw new DomainNotExistedException_Thrift();
        }
        domain.deleteStoragePool(request.getStoragePoolId());
        domainStore.saveDomain(domain);
        response.setRequestId(request.getRequestId());
        response.setStoragePoolName(storagePoolExist.getName());
        logger.warn("deleteStoragePool response: {}", response);
        return response;
    }

    @Override
    public ListStoragePoolResponse_Thrift listStoragePools(ListStoragePoolRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, InvalidInputException_Thrift, ServiceIsNotAvailable_Thrift,
            TException {
        if (shutDownFlag) {
            logger.error("InfoCenter:{} had been shutdown", appContext.getMainEndPoint());
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("listStoragePools request: {}", request);

        /*try {
            ValidateParam.validateListStoragePoolRequest(request);
        } catch (InvalidInputException_Thrift e) {
            throw e;
        }*/
        ListStoragePoolResponse_Thrift response = new ListStoragePoolResponse_Thrift();
        // check if list some or all storage pool
        List<StoragePool> storagePoolList = new ArrayList<>();
        try {
            storagePoolList.addAll(storagePoolStore.listStoragePools(request.getStoragePoolIds()));
        } catch (Exception e) {
            logger.warn("list storage pools caught an exception", e);
        }

        List<OneStoragePoolDisplay_Thrift> storagePoolDisplayList = new ArrayList<>();
        for (StoragePool storagePool : storagePoolList) {
            long storagePoolMigrationSpeed = 0;
            long storagePoolTotalPageToMigrate = 0;
            long storagePoolAlreadyMigratedPage = 0;
            List<ArchiveMetadata_Thrift> archiveList = new ArrayList<>();
            for (Entry<Long, Long> entry : storagePool.getArchivesInDataNode().entries()) {
                Long datanodeId = entry.getKey();
                Long archiveId = entry.getValue();
                InstanceMetadata datanode = storageStore.get(datanodeId);
                if (datanode != null) {
                    RawArchiveMetadata archive = datanode.getArchiveById(archiveId);
                    if (archive != null) {
                        if (OK == datanode.getDatanodeStatus()) {
                            storagePoolMigrationSpeed += archive.getMigrationSpeed();
                            storagePoolTotalPageToMigrate += archive.getTotalPageToMigrate();
                            storagePoolAlreadyMigratedPage += archive.getAlreadyMigratedPage();
                        }
                        archiveList.add(RequestResponseHelper.buildThriftArchiveMetadataFrom(archive));
                    } else {
                        logger.info("can not find archive info by archive Id:{}, at datanode id:{}", archiveId,
                                datanodeId);
                    }
                } else {
                    logger.info("can not find datanode info by datanode Id:{}", datanodeId);
                }
            }
            storagePool.setMigrationSpeed(storagePoolMigrationSpeed / 2);
            double storagePoolMigrationRatio = (0 == storagePoolTotalPageToMigrate) ?
                    100 :
                    (storagePoolAlreadyMigratedPage * 100) / storagePoolTotalPageToMigrate;
            storagePool.setMigrationRatio(storagePoolMigrationRatio);
            OneStoragePoolDisplay_Thrift storagePoolThrift = new OneStoragePoolDisplay_Thrift();
            MigrationRule migrationRule = null;
            if (storagePool.getMigrationRuleId() != StoragePool.DEFAULT_MIGRATION_RULE_ID.longValue()) {
                MigrationRuleInformation migrationRuleInformation = migrationRuleStore
                        .get(storagePool.getMigrationRuleId());
                if (migrationRuleInformation != null) {
                    migrationRule = migrationRuleInformation.toMigrationRule();
                }
            }
            storagePoolThrift
                    .setStoragePoolThrift(RequestResponseHelper.buildThriftStoragePoolFrom(storagePool, migrationRule));
            storagePoolThrift.setArchiveThrifts(archiveList);
            storagePoolDisplayList.add(storagePoolThrift);
        }

        response.setStoragePoolDisplays(storagePoolDisplayList);
        response.setRequestId(request.getRequestId());
        logger.warn("listStoragePools response: {}", response);
        return response;
    }

    @Override
    public RemoveDatanodeFromDomainResponse removeDatanodeFromDomain(RemoveDatanodeFromDomainRequest request)
            throws ServiceHavingBeenShutdown_Thrift, InvalidInputException_Thrift, ServiceIsNotAvailable_Thrift,
            DatanodeNotFoundException_Thrift, DomainNotExistedException_Thrift, DomainIsDeletingException_Thrift,
            TException {
        if (shutDownFlag) {
            logger.error("InfoCenter:{} had been shutdown", appContext.getMainEndPoint());
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("removeDatanodeFromDomain request: {}", request);
        try {
            ValidateParam.validateRemoveDatanodeFromDomainRequest(request);
        } catch (InvalidInputException_Thrift e) {
            throw e;
        }
        RemoveDatanodeFromDomainResponse response = new RemoveDatanodeFromDomainResponse();
        // check domain id exist
        Domain domain = null;
        try {
            domain = domainStore.getDomain(request.getDomainId());
        } catch (Exception e) {
            logger.warn("can not get domain", e);
        }
        if (domain == null) {
            logger.warn("can't find any domain by id:{}", request.getDomainId());
            throw new DomainNotExistedException_Thrift();
        }
        if (domain.getStatus() == Status.Deleting) {
            logger.error("domain:{} is deleting", domain);
            throw new DomainIsDeletingException_Thrift();
        }
        // should remove all archives from storage pools before delete datanode
        Long datanodeId = request.getDatanodeInstanceId();
        InstanceMetadata datanode = storageStore.get(datanodeId);
        if (datanode == null) {
            logger.error("can not find datanode by Id:{}", datanodeId);
            throw new DatanodeNotFoundException_Thrift();
        }
        List<StoragePool> storagePoolList;
        try {
            storagePoolList = storagePoolStore.listAllStoragePools();
        } catch (Exception e) {
            logger.error("can not get any storage pools", e);
            throw new TException();
        }
        Validate.notNull(storagePoolList);
        for (RawArchiveMetadata archiveMetadata : datanode.getArchives()) {
            for (StoragePool storagePool : storagePoolList) {
                Long archiveId = archiveMetadata.getArchiveId();
                if (storagePool.getArchivesInDataNode().containsEntry(datanodeId, archiveId)) {
                    // remove archive from storage pool in silence
                    // mark archive is free
                    archiveMetadata.setFree();
                    storagePool.removeArchiveFromDatanode(datanodeId, archiveId);
                    storagePool.setLastUpdateTime(System.currentTimeMillis());
                    storagePoolStore.saveStoragePool(storagePool);
                    break;
                }
            }
        }
        // mark datanode free
        datanode.setFree();
        storageStore.save(datanode);
        domain.setLogicalSpace(domain.getLogicalSpace() - datanode.getLogicalCapacity());
        domain.setFreeSpace(domain.getFreeSpace() - datanode.getFreeSpace());

        domain.setLastUpdateTime(System.currentTimeMillis());
        domain.deleteDatanode(datanodeId);
        domainStore.saveDomain(domain);
        response.setRequestId(request.getRequestId());
        response.setDomainName(domain.getDomainName());
        logger.warn("removeDatanodeFromDomain response: {}", response);
        return response;
    }

    @Override
    public RemoveArchiveFromStoragePoolResponse_Thrift removeArchiveFromStoragePool(
            RemoveArchiveFromStoragePoolRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, InvalidInputException_Thrift, ServiceIsNotAvailable_Thrift,
            FailToRemoveArchiveFromStoragePoolException_Thrift, ArchiveNotFoundException_Thrift,
            StoragePoolNotExistedException_Thrift, DomainNotExistedException_Thrift,
            StoragePoolIsDeletingException_Thrift, TException {
        if (shutDownFlag) {
            logger.error("InfoCenter:{} had been shutdown", appContext.getMainEndPoint());
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("removeArchiveFromStoragePool request: {}", request);
        try {
            ValidateParam.validateRemoveArchiveFromStoragePoolRequest(request);
        } catch (InvalidInputException_Thrift e) {
            throw e;
        }
        RemoveArchiveFromStoragePoolResponse_Thrift response = new RemoveArchiveFromStoragePoolResponse_Thrift();
        // check domain id exist
        Domain domain = null;
        try {
            domain = domainStore.getDomain(request.getDomainId());
        } catch (Exception e) {
            logger.warn("can not get domain", e);
        }
        if (domain == null) {
            logger.warn("can't find any domain by id:{}", request.getDomainId());
            throw new DomainNotExistedException_Thrift();
        }
        // check pool id exist
        StoragePool storagePoolExist = null;
        try {
            storagePoolExist = storagePoolStore.getStoragePool(request.getStoragePoolId());
        } catch (Exception e) {
            logger.warn("can not get storage pool", e);
        }
        if (storagePoolExist == null) {
            throw new StoragePoolNotExistedException_Thrift();
        }

        if (storagePoolExist.getStatus() == Status.Deleting) {
            logger.error("storage pool is deleting, storage pool:{}", storagePoolExist);
            throw new StoragePoolIsDeletingException_Thrift();
        }

        InstanceMetadata datanode = storageStore.get(request.getDatanodeInstanceId());
        if (datanode == null) {
            logger.error("can not find datanode by id:{}", request.getDatanodeInstanceId());
            throw new ArchiveNotFoundException_Thrift();
        }
        RawArchiveMetadata archive = datanode.getArchiveById(request.getArchiveId());
        if (archive == null) {
            logger.error("can not find archive by id:{}", request.getArchiveId());
            throw new ArchiveNotFoundException_Thrift();
        }
        // mark archive is free
        archive.setFree();
        storageStore.save(datanode);
        // now remove archive id from storage pool
        storagePoolExist.removeArchiveFromDatanode(request.getDatanodeInstanceId(), archive.getArchiveId());
        storagePoolExist.setLastUpdateTime(System.currentTimeMillis());

        Map<Long, InstanceMetadata> instanceId2InstanceMetadata = new HashMap<>();
        Map<Long, RawArchiveMetadata> archiveId2Archive = new HashMap<>();
        for (InstanceMetadata instance : storageStore.list()) {
            if (instance.getDatanodeStatus().equals(OK)) {
                instanceId2InstanceMetadata.put(instance.getInstanceId().getId(), instance);
                for (RawArchiveMetadata archiveMetadata : instance.getArchives()) {
                    archiveId2Archive.put(archiveMetadata.getArchiveId(), archiveMetadata);
                }
            }
        }

        long logicalSpace = 0;
        long logicalFreeSpace = 0;
        for (Long archiveId : storagePoolExist.getArchivesInDataNode().values()) {
            RawArchiveMetadata archiveMetadata = archiveId2Archive.get(archiveId);
            if (archiveMetadata != null) {
                logicalSpace += archiveMetadata.getLogicalSpace();
                logicalFreeSpace += archiveMetadata.getLogicalFreeSpace();
            }
        }

        storagePoolExist.setTotalSpace(logicalSpace);
        storagePoolExist.setFreeSpace(logicalFreeSpace);
        storagePoolExist.setLogicalPSSFreeSpace(StoragePoolSpaceCalculator
                .calculateFreeSpace(storagePoolExist, instanceId2InstanceMetadata, archiveId2Archive, 3, segmentSize));
        storagePoolExist.setLogicalPSAFreeSpace(StoragePoolSpaceCalculator
                .calculateFreeSpace(storagePoolExist, instanceId2InstanceMetadata, archiveId2Archive, 2, segmentSize));

        storagePoolStore.saveStoragePool(storagePoolExist);
        backupDBManager.backupDatabase();
        response.setRequestId(request.getRequestId());
        response.setStoragePoolName(storagePoolExist.getName());
        logger.warn("removeArchiveFromStoragePool response: {}", response);
        return response;
    }

    public StoragePoolStore getStoragePoolStore() {
        return storagePoolStore;
    }

    public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
        this.storagePoolStore = storagePoolStore;
    }

    public long getNextActionTimeIntervalMs() {
        return nextActionTimeIntervalMs;
    }

    public void setNextActionTimeIntervalMs(long nextActionTimeIntervalMs) {
        this.nextActionTimeIntervalMs = nextActionTimeIntervalMs;
    }

    /**
     * Just update volume layout
     */
    @Override
    public CreateSegmentsResponse CreateSegments(CreateSegmentsRequest request)
            throws NotEnoughSpaceException_Thrift, InternalError_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("CreateSegments request: {}", request);
        try {
            volumeStore.updateVolumeLayout(request.getVolumeId(), request.getSegmentIndex(), request.getSegmentNum(),
                    null);
            VolumeMetadata volume = volumeStore.getVolume(request.getVolumeId());
            logger.warn("try to persist modified volume layout to data node");
            volume.setNeedToPersistVolumeLayout(true);
            volume.incVersion();
            volumeStatusStore.addVolumeToStore(volume);
        } catch (Exception e) {
            logger.error("cannot update volume layout", e);
            throw new InternalError_Thrift();
        }

        CreateSegmentsResponse response = new CreateSegmentsResponse();
        logger.warn("CreateSegments response: {}", response);
        return response;
    }

    public CapacityRecordStore getCapacityRecordStore() {
        return capacityRecordStore;
    }

    public void setCapacityRecordStore(CapacityRecordStore capacityRecordStore) {
        this.capacityRecordStore = capacityRecordStore;
    }

    @Override
    public GetCapacityRecordResponse_Thrift getCapacityRecord(GetCapacityRecordRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("getCapacityRecord request: {}", request);
        GetCapacityRecordResponse_Thrift response = new GetCapacityRecordResponse_Thrift();
        response.setRequestId(request.getRequestId());
        CapacityRecord capacityRecord;
        try {
            capacityRecord = capacityRecordStore.getCapacityRecord();
        } catch (Exception e) {
            logger.error("caught an exception");
            throw new TException();
        }
        response.setCapacityRecord(RequestResponseHelper.buildThriftCapacityRecordFrom(capacityRecord));
        logger.warn("getCapacityRecord response: {}", response);
        return response;
    }

    @Deprecated
    @Override
    public RetrieveARebalanceTaskResponse retrieveARebalanceTask(RetrieveARebalanceTaskRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, NoNeedToRebalance_Thrift,
            TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }

        logger.warn("retrieveARebalanceTask request: {}", request);

        try {
            RebalanceTask rebalanceTask = segmentUnitsDistributionManager.selectRebalanceTask(request.isRecord());
            RebalanceTask_Thrift rebalanceTask_Thrift = RequestResponseHelper.buildRebalanceTaskThrift(rebalanceTask);
            RetrieveARebalanceTaskResponse response = new RetrieveARebalanceTaskResponse();
            response.setRequestId(request.getRequestId());
            response.setRebalanceTask(rebalanceTask_Thrift);
            logger.warn("retrieveARebalanceTask response: {}", response);
            return response;
        } catch (NoNeedToRebalance e) {
            throw new NoNeedToRebalance_Thrift();
        }
    }

    @Override
    public boolean discardRebalanceTask(long requestTaskId)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {

        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend");
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("request to discard a rebalance task {}", requestTaskId);
        return segmentUnitsDistributionManager.discardRebalanceTask(requestTaskId);
    }

    @Override
    public UpdateVolumeResponse updateVolume(UpdateVolumeRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, InvalidInputException_Thrift,
            VolumeNotFoundException_Thrift, VolumeNameExistedException_Thrift, TException {
        UpdateVolumeResponse response = new UpdateVolumeResponse();
        response.setRequestId(request.getRequestId());

        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("updateVolume request: {}", request);
        boolean changeVolumeName = request.isSetNewVolumeName();
        if (changeVolumeName) {
            if (request.getNewVolumeName().length() > 100) {
                logger.error("volume new name:{} is too long", request.getNewVolumeName());
                throw new InvalidInputException_Thrift();
            }

            // check new volume name valid
            if (volumeStore.getVolumeNotDeadByName(request.getNewVolumeName()) != null) {
                logger.error("some volume has been exist, name is {}", request.getNewVolumeName());
                throw new VolumeNameExistedException_Thrift();
            }

        }
        long volumeId = request.getVolumeId();

        List<VolumeMetadata> volumeMetadatas = volumeStore.listVolumesFromRoot(volumeId);
        if (volumeMetadatas.size() == 0) {
            logger.error("can not get volumeMetadata from volumeId: {}", volumeId);
            throw new VolumeNotFoundException_Thrift();
        }

        if (volumeMetadatas.get(0).getVolumeId() != volumeId) {
            logger.error("The first volume {} has different volume id {}", volumeMetadatas.get(0), volumeId);
            throw new VolumeNotFoundException_Thrift();
        }

        for (VolumeMetadata volumeMetadata : volumeMetadatas) {
            if (changeVolumeName) {
                volumeMetadata.setName(request.getNewVolumeName());
                logger.warn("volume:{} change new name:{}", volumeMetadata.getVolumeId(), request.getNewVolumeName());
            }
            if (request.isSetVolumeInAction() && volumeMetadata.isRoot()) {
                volumeMetadata.setInAction(buildVolumeInActionFrom(request.getVolumeInAction()));
            }
            volumeStore.saveVolume(volumeMetadata);
        }

        logger.warn("updateVolume response: {}", response);
        return response;
    }

    @Override
    public FixVolumeResponse_Thrift fixVolume(FixVolumeRequest_Thrift request)
            throws InternalError_Thrift, VolumeNotFoundException_Thrift, ServiceHavingBeenShutdown_Thrift,
            ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("fixVolume request: {}", request);
        boolean needFixVolume = false;
        boolean fixVolumeCompletely = true;
        VolumeMetadata volumeMetadata;

        try {
            volumeMetadata = getRootVolumeWithChildren(request.getVolumeId(), true);
        } catch (VolumeNotFoundException e) {
            logger.error("can not found volume:{}", request.getVolumeId(), e);
            throw new VolumeNotFoundException_Thrift();
        }

        if (volumeMetadata == null) {
            logger.warn("fix volume error:the volumeMetadata is null {}", request);
            Validate.notNull(volumeMetadata);
        }

        FixVolumeResponse_Thrift response = new FixVolumeResponse_Thrift();
        response.setRequestId(request.getRequestId());

        Set<Long> lostDatanodes = new HashSet<>();
        Set<Long> tmpDatanodes = new HashSet<>();

        for (int i = 0; i < volumeMetadata.getSegmentCount(); i++) {
            int needFixCount = 0;
            InstanceId aliveSegmentUnit = null;
            tmpDatanodes.clear();

            SegmentMembership membership = volumeMetadata.getMembership(i);
            if (membership == null) {
                logger.warn("fix volume error:all datanode die,the membership is null {}", request);
                fixVolumeCompletely = false;
                needFixVolume = true;
                continue;
            }
            SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(i);
            Validate.notNull(membership);
            if (segmentMetadata == null) {
                logger.warn("fix volume error:the segmentMetadata is null,volumeMetadata is: {}", volumeMetadata);
                tmpDatanodes.add(membership.getPrimary().getId());
                if (membership.getSecondaries().size() != 0) {
                    for (InstanceId entry : membership.getSecondaries()) {
                        tmpDatanodes.add(entry.getId());
                    }
                }
                if (membership.getJoiningSecondaries().size() != 0) {
                    for (InstanceId entry : membership.getJoiningSecondaries()) {
                        tmpDatanodes.add(entry.getId());
                    }
                }
                if (membership.getArbiters().size() != 0) {
                    for (InstanceId entry : membership.getArbiters()) {
                        tmpDatanodes.add(entry.getId());
                    }
                }

                lostDatanodes.addAll(tmpDatanodes);
                fixVolumeCompletely = false;
                needFixVolume = true;
                continue;
            }

            // judge this segment if need fix
            int memberCount = membership.getMembers().size();
            InstanceId alivePrimaryID = null;
            for (InstanceId instanceId : membership.getMembers()) {
                // datanode down
                InstanceMetadata datanode = storageStore.get(instanceId.getId());
                if (datanode == null || datanode.getDatanodeStatus().equals(UNKNOWN)) {
                    logger.warn(" storageStore can not get datanode {}", instanceId);
                    needFixCount++;
                    tmpDatanodes.add(instanceId.getId());
                    continue;
                } else {
                    Instance datanodeInstance = instanceStore.get(instanceId);
                    Validate.notNull(datanodeInstance);
                    if (datanodeInstance.getStatus() != InstanceStatus.OK) {
                        logger.warn(" instanceStore can not get datanode {}", instanceId);
                        needFixCount++;
                        tmpDatanodes.add(instanceId.getId());
                        continue;
                    }
                }
                // disk down
                Validate.notNull(datanode);
                SegmentUnitMetadata segmentUnitMetadata = segmentMetadata.getSegmentUnitMetadata(instanceId);
                if (segmentUnitMetadata == null) {
                    logger.warn("fix volume error:the segmentUnitMetadata is null {}", segmentMetadata);
                    needFixCount++;
                    continue;
                }
                Validate.notNull(segmentUnitMetadata);
                String diskName = segmentUnitMetadata.getDiskName();
                if (diskName == null || diskName.isEmpty()) {
                    logger.warn("fix volume error:the diskName is null {}", segmentUnitMetadata);
                }
                boolean found = false;
                for (RawArchiveMetadata archiveMetadata : datanode.getArchives()) {
                    logger.warn("segment unit disk name: {}, archive name: {}", diskName,
                            archiveMetadata.getDeviceName());
                    if (diskName.equalsIgnoreCase(archiveMetadata.getDeviceName())) {
                        found = true;
                        if (archiveMetadata.getStatus() != ArchiveStatus.GOOD) {
                            logger.warn("fix the volume, archiveMetadata Status is error {}",
                                    archiveMetadata.getStatus());
                            needFixCount++;
                        } else {
                            // found and archive is good
                            aliveSegmentUnit = instanceId;
                            if (membership.getPrimary() == instanceId) {
                                alivePrimaryID = instanceId;
                            }

                        }
                        // found, break;
                        break;
                    }
                }  // for loop to find archive info
                if (!found) {
                    logger.warn("fix the volume:archive did not found, diskName is:{}, at datanode:{}", diskName,
                            datanode.getInstanceId());
                    needFixCount++;
                }
            } // for every segment unit in segment

            int aliveCount = memberCount - needFixCount;
            if (aliveCount < volumeMetadata.getVolumeType().getVotingQuorumSize()) {
                needFixVolume = true;
                if (!tmpDatanodes.isEmpty()) {
                    lostDatanodes.addAll(tmpDatanodes);
                }
                // can we fix this segment completely
                if (!fixVolumeCompletely) {
                    continue;
                }
                if (aliveSegmentUnit == null) {
                    //no alive segmentUnit in this segment
                    fixVolumeCompletely = false;
                } else {
                    if (membership.isJoiningSecondary(aliveSegmentUnit) || membership.isArbiter(aliveSegmentUnit)) {
                        //J alive or A alive
                        fixVolumeCompletely = false;
                    } else if (membership.isSecondary(aliveSegmentUnit)) {
                        //S alive and SegmentForm is PSS
                        SegmentUnitMetadata aliveSegmentUnitMetadata = segmentMetadata
                                .getSegmentUnitMetadata(aliveSegmentUnit);
                        SegmentForm segmentForm = aliveSegmentUnitMetadata.getMembership()
                                .getSegmentForm(volumeMetadata.getVolumeType());
                        if (segmentForm == SegmentForm.PSS) {
                            fixVolumeCompletely = false;
                        }
                    }
                }
            } else if (alivePrimaryID == null) {
                //only P  is not alive when PJA
                SegmentUnitMetadata aliveSegmentUnitMetadata = segmentMetadata.getSegmentUnitMetadata(aliveSegmentUnit);
                SegmentForm segmentForm = aliveSegmentUnitMetadata.getMembership()
                        .getSegmentForm(volumeMetadata.getVolumeType());
                if (!segmentForm.canGenerateNewPrimary()) {
                    fixVolumeCompletely = false;
                    needFixVolume = true;
                    lostDatanodes.add(aliveSegmentUnitMetadata.getMembership().getPrimary().getId());
                }

            }// if judge this segment needs fix or not
        } // for every segment in volume

        response.setLostDatanodes(lostDatanodes);
        response.setFixVolumeCompletely(fixVolumeCompletely);
        response.setNeedFixVolume(needFixVolume);
        logger.warn("fixVolume response: {}", response);
        return response;
    }

    @Override
    public ChangeVolumeStatusFromFixToUnavailableResponse changeVolumeStatusFromFixToUnavailable(
            ChangeVolumeStatusFromFixToUnavailableRequest request)
            throws InternalError_Thrift, VolumeNotFoundException_Thrift, ServiceHavingBeenShutdown_Thrift,
            ServiceIsNotAvailable_Thrift, TException {
        ChangeVolumeStatusFromFixToUnavailableResponse changeVolumeStatusFromFixToUnavailableResponse = new ChangeVolumeStatusFromFixToUnavailableResponse();
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("changeVolumeStatusFromFixToUnavailable request: {}", request);
        VolumeMetadata volumeMetadata;
        try {
            volumeMetadata = getRootVolumeWithChildren(request.getVolumeId(), true);
            if (volumeMetadata == null) {
                logger.error("fix volume error:the volumeMetadata is null {}", request);
            }
        } catch (VolumeNotFoundException e) {
            logger.error("can not found volume:{}", request.getVolumeId(), e);
            throw new VolumeNotFoundException_Thrift();
        }

        if (volumeMetadata.getVolumeStatus().equals(VolumeStatus.Fixing)) {
            volumeMetadata.setVolumeStatus(VolumeStatus.Unavailable);
            volumeStore.saveVolume(volumeMetadata);
        }

        changeVolumeStatusFromFixToUnavailableResponse.setChangeSucess(true);
        logger.warn("changeVolumeStatusFromFixToUnavailable response: {}",
                changeVolumeStatusFromFixToUnavailableResponse);
        return changeVolumeStatusFromFixToUnavailableResponse;
    }

    @Override
    public InformCloneRelationshipResponse informCloneRelationship(InformCloneRelationshipRequest request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("informCloneRelationship request: {}", request);

        CloneRelationshipInformation cloneRelationshipInformation = RequestResponseHelper
                .buildCloneRelationshipFrom(request.getCloneRelationship());

        cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
        InformCloneRelationshipResponse relationshipResponse = new InformCloneRelationshipResponse(
                request.getRequestId());
        logger.warn("informCloneRelationship response: {}", relationshipResponse);
        return relationshipResponse;
    }

    @Override
    public MarkVolumeReadWriteResponse markVolumeReadWrite(MarkVolumeReadWriteRequest request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, VolumeNotFoundException_Thrift,
            TException {
        logger.warn("markVolumeReadWrite request: {}", request);
        VolumeMetadata volume = volumeStore.getVolume(request.getVolumeId());

        if (null == volume) {
            throw new VolumeNotFoundException_Thrift();
        }

        VolumeMetadata.ReadWriteType readWriteType = RequestResponseHelper
                .buildReadWriteTypeFrom(request.getReadWrite());
        if (readWriteType != volume.getReadWrite()) {
            volume.setReadWrite(readWriteType);
            volumeStore.saveVolume(volume);
            volumeStatusStore.addVolumeToStore(volume);
        }
        MarkVolumeReadWriteResponse response = new MarkVolumeReadWriteResponse(request.getRequestId());
        logger.warn("markVolumeReadWrite response: {}", response);
        return response;
    }

    @Override
    public IsVolumeReadOnlyResponse isVolumeReadOnly(IsVolumeReadOnlyRequest request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, VolumeNotFoundException_Thrift,
            VolumeIsMarkWriteException_Thrift, VolumeIsAppliedWriteAccessRuleException_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("isVolumeReadOnly request: {}", request);

        VolumeMetadata volume = volumeStore.getVolume(request.getVolumeId());

        if (volume == null) {
            throw new VolumeNotFoundException_Thrift();
        }

        // first check whether volume is marked as readonly, if not, throw exception immediately
        if (VolumeMetadata.ReadWriteType.READ_ONLY != volume.getReadWrite()) {
            VolumeIsMarkWriteException_Thrift volumeIsMarkWriteExceptionThrift = new VolumeIsMarkWriteException_Thrift();
            volumeIsMarkWriteExceptionThrift
                    .setDetail("Volume has been marked as " + volume.getReadWrite().name() + ".");
            logger.warn("{}", volumeIsMarkWriteExceptionThrift.getDetail());
            throw volumeIsMarkWriteExceptionThrift;
        }

        Long volumeId = volume.getVolumeId();
        // then check whether volume is applied access rule with write permission
        List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
                .getByVolumeId(volumeId);
        if (null != relationshipInfoList && !relationshipInfoList.isEmpty()) {
            String exceptionDetailString = "";
            boolean needThrowException = false;
            for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
                AccessRuleInformation accessRuleInformation = accessRuleStore.get(relationshipInfo.getRuleId());
                if (AccessPermissionType.READ != AccessPermissionType
                        .findByValue(accessRuleInformation.getPermission())) {
                    needThrowException = true;
                    exceptionDetailString =
                            exceptionDetailString + "IP:" + accessRuleInformation.getIpAddress() + ", Permission:"
                                    + AccessPermissionType.findByValue(accessRuleInformation.getPermission()).name()
                                    + "\n";
                }
            }
            if (needThrowException) {
                VolumeIsAppliedWriteAccessRuleException_Thrift volumeIsAppliedWriteAccessRuleExceptionThrift = new VolumeIsAppliedWriteAccessRuleException_Thrift();
                volumeIsAppliedWriteAccessRuleExceptionThrift
                        .setDetail("Volume is applied by access rule with write permission.\n" + exceptionDetailString);
                logger.warn("{}", volumeIsAppliedWriteAccessRuleExceptionThrift.getDetail());
                throw volumeIsAppliedWriteAccessRuleExceptionThrift;
            }
        }

        // if until here there is no exception thrown, can get all drivers bound with volume and return to control center
        Set<DriverMetadata_Thrift> driverMetadataThriftSet = new HashSet<>();
        List<DriverMetadata> volumeBindingDrivers = driverStore.get(volumeId);
        if (null != volumeBindingDrivers && !volumeBindingDrivers.isEmpty()) {
            for (DriverMetadata driver : volumeBindingDrivers) {
                DriverMetadata_Thrift driverMetadataThrift = RequestResponseHelper
                        .buildThriftDriverMetadataFrom(driver);
                driverMetadataThriftSet.add(driverMetadataThrift);
            }
        }

        IsVolumeReadOnlyResponse readOnlyResponse = new IsVolumeReadOnlyResponse();
        readOnlyResponse.setRequestId(request.getRequestId());
        readOnlyResponse.setVolumeId(request.getVolumeId());
        readOnlyResponse.setDrivers(driverMetadataThriftSet);

        logger.warn("isVolumeReadOnly response: {}", readOnlyResponse);
        return readOnlyResponse;
    }

    @Override
    public ReportServerNodeInfoResponse_Thrift reportServerNodeInfo(ReportServerNodeInfoRequest_Thrift request)
            throws ServiceIsNotAvailable_Thrift, ServiceHavingBeenShutdown_Thrift, TException {
        logger.warn("begin reportServerNodeInfo, request is: {}", request);

        ServerNode serverNode = new ServerNode();
        serverNode.setId(request.getServerId());
        serverNode.setCpuInfo(request.getCpuInfo());
        serverNode.setDiskInfo(request.getDiskInfo());
        serverNode.setMemoryInfo(request.getMemoryInfo());
        serverNode.setModelInfo(request.getModelInfo());
        serverNode.setNetworkCardInfo(request.getNetworkCardInfo());
        serverNode.setNetworkCardInfoName(request.getNetworkCardInfoName());
        serverNode.setGatewayIp(request.getGatewayIp());
        serverNode.setHostName(request.getHostName());
        serverNode.setManageIp(request.getManageIp());
        serverNode.setStatus("ok");

        Set<HardDiskInfo_Thrift> hardDisks = request.getHardDisks();
        Set<DiskInfo> diskInfoSet = new HashSet<>();
        for (HardDiskInfo_Thrift diskInfoThrift : hardDisks) {
            if ("".equals(diskInfoThrift.getSn())) {
                logger.warn("disk sn is empty string, can not build disk primary key, serverNode id is: {}",
                        serverNode.getId());
                continue;
            }
            String diskId = serverNode.getId() + "-" + diskInfoThrift.getSn();
            DiskInfo diskInfo = diskInfoStore.listDiskInfoById(diskId);
            if (diskInfo == null) {
                diskInfo = new DiskInfo();
                diskInfo.setId(diskId);
            }
            diskInfo.setSn(diskInfoThrift.getSn());
            diskInfo.setName(diskInfoThrift.getName());
            diskInfo.setSsdOrHdd(diskInfoThrift.getSsdOrHdd());
            diskInfo.setVendor(diskInfoThrift.getVendor());
            diskInfo.setModel(diskInfoThrift.getModel());
            diskInfo.setRate(diskInfoThrift.getRate());
            diskInfo.setSize(diskInfoThrift.getSize());
            diskInfo.setWwn(diskInfoThrift.getWwn());
            diskInfo.setControllerId(diskInfoThrift.getControllerId());
            diskInfo.setSlotNumber(diskInfoThrift.getSlotNumber());
            diskInfo.setEnclosureId(diskInfoThrift.getEnclosureId());
            diskInfo.setCardType(diskInfoThrift.getCardType());
            //            diskInfo.setSwith(diskInfoThrift.getSwith());
            diskInfo.setSerialNumber(diskInfoThrift.getSerialNumber());

            diskInfo.setReadErrorRate(diskInfoThrift.getReadErrorRate());
            diskInfo.setReallocatedSector(diskInfoThrift.getReallocatedSector());
            diskInfo.setSpinRetryCount(diskInfoThrift.getSpinRetryCount());
            diskInfo.setEndtoEndError(diskInfoThrift.getEndtoEndError());
            diskInfo.setCommandTimeout(diskInfoThrift.getCommandTimeout());
            diskInfo.setReallocationEvent_Count(diskInfoThrift.getReallocationEvent_Count());
            diskInfo.setCurrentPendingSector(diskInfoThrift.getCurrentPendingSector());
            diskInfo.setOfflineUncorrectable(diskInfoThrift.getOfflineUncorrectable());
            diskInfo.setSoftReadErrorRate(diskInfoThrift.getSoftReadErrorRate());

            diskInfoSet.add(diskInfo);
        }

        serverNode.setDiskInfoSet(diskInfoSet);

        ServerNode serverNodeStored = serverNodeStore.listServerNodeById(request.getServerId());
        if (serverNodeStored != null) {
            serverNode.setStoreIp(serverNodeStored.getStoreIp());
            serverNode.setRackNo(serverNodeStored.getRackNo());
            serverNode.setSlotNo(serverNodeStored.getSlotNo());
            serverNode.setChildFramNo(serverNodeStored.getChildFramNo());
        }

        serverNodeStore.saveOrUpdateServerNode(serverNode);
        ReportServerNodeInfoResponse_Thrift response = new ReportServerNodeInfoResponse_Thrift();
        response.setResponseId(request.getRequestId());
        return response;
    }

    @Override
    public DeleteServerNodesResponse_Thrift deleteServerNodes(DeleteServerNodesRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, ServerNodeIsUnknown_Thrift {
        logger.warn("begin deleteServerNode, request is: {}", request);
        List<String> serverIds = request.getServerIds();
        List<String> deletedHostnames = new ArrayList<>();
        for (String serverId : serverIds) {
            ServerNode serverNode = serverNodeStore.listServerNodeById(serverId);
            if ("ok".equals(serverNode.getStatus())) {
                throw new ServerNodeIsUnknown_Thrift();
            }
            deletedHostnames.add(serverNode.getHostName());
        }
        serverNodeStore.deleteServerNodes(serverIds);

        DeleteServerNodesResponse_Thrift response = new DeleteServerNodesResponse_Thrift();
        response.setResponseId(request.getRequestId());
        response.setDeletedServerNodeHostnames(deletedHostnames);
        return response;
    }

    @Override
    public UpdateServerNodeResponse_Thrift updateServerNode(UpdateServerNodeRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift,
            ServerNodePositionIsRepeatException_Thrift, TException {
        logger.warn("begin updateServerNode, request is: {}", request);

        ServerNode serverNode = serverNodeStore.listServerNodeById(request.getServerId());
        if (request.getCpuInfo() != null) {
            serverNode.setCpuInfo(request.getCpuInfo());
        }
        if (request.getDiskInfo() != null) {
            serverNode.setDiskInfo(request.getDiskInfo());
        }
        if (request.getMemoryInfo() != null) {
            serverNode.setMemoryInfo(request.getMemoryInfo());
        }
        if (request.getModelInfo() != null) {
            serverNode.setModelInfo(request.getModelInfo());
        }
        if (request.getNetworkCardInfo() != null) {
            serverNode.setNetworkCardInfo(request.getNetworkCardInfo());
            serverNode.setNetworkCardInfoName(request.getNetworkCardInfoName());
        }
        if (request.getGatewayIp() != null) {
            serverNode.setGatewayIp(request.getGatewayIp());
        }

        if (request.getManageIp() != null) {
            serverNode.setManageIp(request.getManageIp());
        }
        if (request.getStoreIp() != null) {
            serverNode.setStoreIp(request.getStoreIp());
        }
        if (request.getRackNo() != null) {
            serverNode.setRackNo(request.getRackNo());
        }
        if (request.getSlotNo() != null) {
            serverNode.setSlotNo(request.getSlotNo());
        }
        if (request.getChildFramNo() != null) {
            serverNode.setChildFramNo(request.getChildFramNo());
        }

        if (serverNode.getRackNo() != null && serverNode.getSlotNo() != null && serverNode.getChildFramNo() != null) {
            String checkInfo = serverNode.getRackNo() + serverNode.getSlotNo() + serverNode.getChildFramNo();
            List<ServerNode> serverNodeList = serverNodeStore.listAllServerNodes();
            for (ServerNode node : serverNodeList) {
                if (serverNode.getId().equals(node.getId())) {
                    continue;
                }
                String checkInfoTemp = node.getRackNo() + node.getSlotNo() + node.getChildFramNo();
                if (checkInfo.equals(checkInfoTemp)) {
                    logger.warn("rackNo, slotNo and childFramNo is repeat, rackNo:{}, slotNo:{}, childFramNo:{}");
                    throw new ServerNodePositionIsRepeatException_Thrift();
                }
            }
        }

        serverNodeStore.updateServerNode(serverNode);

        UpdateServerNodeResponse_Thrift response = new UpdateServerNodeResponse_Thrift();
        response.setResponseId(request.getRequestId());
        return response;
    }

    @Override
    public ListServerNodesResponse_Thrift listServerNodes(ListServerNodesRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        logger.warn("begin listServerNodes, request is: {}", request);

        int limit = request.getLimit();
        int offset = request.getPage() * limit;
        String sortField = request.getSortField();
        String sortDirection = request.getSortDirection();
        String hostName = request.getHostName();
        String modelInfo = request.getModelInfo();
        String cpuInfo = request.getCpuInfo();
        String memoryInfo = request.getMemoryInfo();
        String diskInfo = request.getDiskInfo();
        String networkCardInfo = request.getNetworkCardInfo();
        String manageIp = request.getManageIp();
        String gatewayIp = request.getGatewayIp();
        String storeIp = request.getStoreIp();
        String rackNo = request.getRackNo();
        String slotNo = request.getSlotNo();

        List<ServerNode> serverNodeList = serverNodeStore
                .listServerNodes(offset, limit, sortField, sortDirection, hostName, modelInfo, cpuInfo, memoryInfo,
                        diskInfo, networkCardInfo, manageIp, gatewayIp, storeIp, rackNo, slotNo);

        List<ServerNode_Thrift> serverNode_thriftList = new ArrayList<>();
        for (ServerNode serverNode : serverNodeList) {
            ServerNode_Thrift serverNode_thrift = RequestResponseHelper.buildThriftServerNodeFrom(serverNode);
            serverNode_thriftList.add(serverNode_thrift);
        }
        ListServerNodesResponse_Thrift response = new ListServerNodesResponse_Thrift();
        response.setResponseId(request.getRequestId());
        response.setRecordsTotal(serverNodeStore.getCountTotle());
        response.setRecordsAfterFilter(serverNode_thriftList.size());
        response.setServerNodesList(serverNode_thriftList);

        logger.debug("list serverNodes, response is: {}", response);
        return response;
    }

    @Override
    public ListServerNodeByIdResponse_Thrift listServerNodeById(ListServerNodeByIdRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        logger.warn("begin listServerNodeById, request is:{}", request);
        ServerNode serverNode = serverNodeStore.listServerNodeById(request.getServerId());
        ListServerNodeByIdResponse_Thrift response = new ListServerNodeByIdResponse_Thrift();
        response.setResponseId(request.getRequestId());
        response.setServerNode(RequestResponseHelper.buildThriftServerNodeFrom(serverNode));
        logger.debug("response is {}", response);
        return response;
    }

    @Override
    public UpdateDiskLightStatusByIdResponse_Thrift updateDiskLightStatusById(
            UpdateDiskLightStatusByIdRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        logger.warn("begin updateDiskLightStatusById, request is: {}", request);
        String diskId = request.getDiskId();
        String status = request.getStatus();

        diskInfoStore.updateDiskInfoLightStatusById(diskId, status);

        UpdateDiskLightStatusByIdResponse_Thrift response = new UpdateDiskLightStatusByIdResponse_Thrift();
        response.setResponseId(request.getRequestId());
        logger.warn("end updateDiskLightStatusById, response is: {}", response);
        return response;
    }

    @Override
    public TurnOffAllDiskLightByServerIdResponse_Thrift turnOffAllDiskLightByServerId(
            TurnOffAllDiskLightByServerIdRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        logger.warn("begin turn off all disk light by server id, request is: {}", request);

        ServerNode serverNode = serverNodeStore.listServerNodeById(request.getServerId());
        int count = 0;
        while (serverNode == null) {
            logger.warn("server id is not in database. server id is {}", request.getServerId());
            serverNode = serverNodeStore.listServerNodeById(request.getServerId());
            if (count++ == 10) {
                logger.warn("we have attempted 10 times, but did not get the server by server id.");
                return new TurnOffAllDiskLightByServerIdResponse_Thrift(request.getRequestId());
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.warn("caught an exception, ", e);
            }
        }

        Set<DiskInfo> diskInfoSet = serverNode.getDiskInfoSet();
        for (DiskInfo diskInfo : diskInfoSet) {
            diskInfoStore.updateDiskInfoLightStatusById(diskInfo.getId(), DiskInfoLightStatus.OFF.toString());
        }
        logger.warn("end turnOffAllDiskLightByServerId.");
        return new TurnOffAllDiskLightByServerIdResponse_Thrift(request.getRequestId());
    }

    @Override
    public GetIOLimitationResponse_Thrift getIOLimitationsInOneDriverContainer(GetIOLimitationRequest_Thrift request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            logger.error("Refuse request from remote due to I am shutting down, request: {}", request);
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn(request.toString());

        GetIOLimitationResponse_Thrift responseThrift = new GetIOLimitationResponse_Thrift();
        responseThrift.setRequestId(request.getRequestId());
        responseThrift.setDriverContainerId(request.getDriverContainerId());

        List<DriverMetadata> driverMetadataList = driverStore.list();

        Map<DriverKey_Thrift, List<IOLimitation_Thrift>> mapDriver2ItsIOLimitations = new HashMap<>();
        for (DriverMetadata driverMetadata : driverMetadataList) {
            if (driverMetadata.getDriverContainerId() == request.getDriverContainerId()) {

                List<IOLimitation_Thrift> ioLimitation_thriftList = new ArrayList<>();
                if (driverMetadata.getDynamicIOLimitationId() != 0) {
                    IOLimitationInformation limitationInformation = ioLimitationStore
                            .get(driverMetadata.getDynamicIOLimitationId());
                    ioLimitation_thriftList.add(RequestResponseHelper
                            .buildThriftIOLimitationFrom(new IOLimitation(limitationInformation)));
                }

                if (driverMetadata.getStaticIOLimitationId() != 0) {
                    IOLimitationInformation limitationInformation = ioLimitationStore
                            .get(driverMetadata.getStaticIOLimitationId());
                    ioLimitation_thriftList.add(RequestResponseHelper
                            .buildThriftIOLimitationFrom(new IOLimitation(limitationInformation)));
                }

                DriverKey driverKey = new DriverKey(driverMetadata.getDriverContainerId(), driverMetadata.getVolumeId(),
                        driverMetadata.getSnapshotId(), driverMetadata.getDriverType());
                DriverKey_Thrift driverKeyThrift = RequestResponseHelper.buildThriftDriverKeyFrom(driverKey);
                logger.warn("got driver:{} has io limitations:{}", driverKeyThrift, ioLimitation_thriftList);
                mapDriver2ItsIOLimitations.put(driverKeyThrift, ioLimitation_thriftList);
            }
        }

        responseThrift.setMapDriver2ItsIOLimitations(mapDriver2ItsIOLimitations);

        return responseThrift;
    }

    @Override
    public GetServerNodeByIpResponse getServerNodeByIp(GetServerNodeByIpRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }
        logger.warn("getServerNodeByIp request is: {}", request);

        ServerNode serverNodeByIp = serverNodeStore.getServerNodeByIp(request.getIp());
        ServerNode_Thrift serverNode_thrift = RequestResponseHelper.buildThriftServerNodeFrom(serverNodeByIp);

        GetServerNodeByIpResponse response = new GetServerNodeByIpResponse();
        response.setResponseId(request.getRequestId());
        response.setServerNode(serverNode_thrift);
        logger.warn("getServerNodeByIp response is: {}", response);
        return response;
    }

    @Override
    public PingPeriodicallyResponse pingPeriodically(PingPeriodicallyRequest request)
            throws ServiceHavingBeenShutdown_Thrift, ServiceIsNotAvailable_Thrift, TException {
        logger.debug("ping infocenter from daemoncommon periodlly.");

        long reportTime = System.currentTimeMillis();
        serverNodeReportTimeMap.put(request.getServerId(), reportTime);

        return new PingPeriodicallyResponse(request.getRequestId());
    }

    @Override
    public ConfirmFixVolumeResponse confirmFixVolume(ConfirmFixVolumeRequest_Thrift request)
            throws InternalError_Thrift, VolumeNotFoundException_Thrift, LackDatanodeException_Thrift,
            ServiceIsNotAvailable_Thrift, InvalidInputException_Thrift, NotEnoughSpaceException_Thrift,
            VolumeFixingOperationException_Thrift, ServiceHavingBeenShutdown_Thrift, TException {
        if (shutDownFlag) {
            throw new ServiceHavingBeenShutdown_Thrift();
        }
        if (InstanceStatus.SUSPEND == appContext.getStatus()) {
            logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
            throw new ServiceIsNotAvailable_Thrift().setDetail("I am suspend");
        }

        logger.warn("confirmFixVolume request: {}", request);
        ConfirmFixVolumeResponse confirmFixVolumeResponse = new ConfirmFixVolumeResponse();
        confirmFixVolumeResponse.setResponseId(request.getRequestId());
        Set<Long> fixDataNode = request.getLostDatanodes();

        boolean canFix;
        InstanceId lackDataNodeID = null;
        VolumeMetadata volumeMetadata;

        try {
            volumeMetadata = getRootVolumeWithChildren(request.getVolumeId(), true);
            if (volumeMetadata == null) {
                logger.error("fix volume error: the volumeMetadata is null {}", request);
            }
            Validate.isTrue(volumeMetadata.getVolumeType() == VolumeType.REGULAR
                    || volumeMetadata.getVolumeType() == VolumeType.SMALL);
        } catch (VolumeNotFoundException e) {
            logger.error("can not found volume: {}", request.getVolumeId(), e);
            throw new VolumeNotFoundException_Thrift();
        }

        if (volumeMetadata.getVolumeStatus().equals(VolumeStatus.Fixing)) {
            logger.error("volumeMetadata status is Fixing");
            throw new VolumeFixingOperationException_Thrift();
        }

        confirmFixVolumeResponse.setVolumeType(volumeMetadata.getVolumeType().getVolumeTypeThrift());
        confirmFixVolumeResponse.setCacheType(volumeMetadata.getCacheType().getCacheTypeThrift());
        confirmFixVolumeResponse.setStoragePoolId(volumeMetadata.getStoragePoolId());
        Map<SegId_Thrift, List<CreateSegmentUnitInfo>> createSegmentUnitInfoMap = new HashMap<>();

        for (int i = 0; i < volumeMetadata.getSegmentCount(); i++) {
            int needFixCount = 0;
            InstanceId aliveSegmentUnit = null;
            canFix = true;
            List<CreateSegmentUnitInfo> tmpSegmentUnitInfoList = new ArrayList<>();
            SegmentMembership membership = volumeMetadata.getMembership(i);
            SegmentVersion segmentVersion;

            Set<Long> exceptNodeId = new HashSet<>();
            SegId segId;
            int aliveSegmentUnitCount;
            InstanceId alivePrimaryID = null;
            SegmentMetadata segmentMetadata = null;
            if (membership == null) {
                logger.warn("fix volume error: all datanodes are dead, the membership is null {}", request);
                aliveSegmentUnitCount = 0;
                canFix = true;
                segmentVersion = new SegmentVersion(1, 0);
                segId = volumeMetadata.getActualSegIdBySegIndex(i);
            } else {
                int memberCount = membership.getMembers().size();
                segmentVersion = membership.getSegmentVersion();
                segmentVersion = segmentVersion.incEpoch();
                Validate.notEmpty(membership.getMembers());
                segmentMetadata = volumeMetadata.getSegmentByIndex(i);

                // judge this segment if need fix
                segId = segmentMetadata.getSegId();
                for (InstanceId instanceId : membership.getMembers()) {
                    // datanode down
                    InstanceMetadata datanode = storageStore.get(instanceId.getId());
                    if (datanode == null) {
                        if (fixDataNode == null || !fixDataNode.contains(instanceId.getId())) {
                            logger.error("can not fix, need datanode {}", instanceId);
                            canFix = false;
                            lackDataNodeID = instanceId;
                        }
                        needFixCount++;
                        continue;
                    } else {
                        Instance datanodeInstance = instanceStore.get(instanceId);
                        Validate.notNull(datanodeInstance);
                        if (datanodeInstance.getStatus() != InstanceStatus.OK) {
                            needFixCount++;
                            if (fixDataNode == null || !fixDataNode.contains(instanceId.getId())) {
                                logger.error("can not fix, need datanode {}", instanceId);
                                canFix = false;
                                lackDataNodeID = instanceId;
                            }
                            continue;
                        }
                    }
                    // disk down
                    Validate.notNull(datanode);
                    SegmentUnitMetadata segmentUnitMetadata = segmentMetadata.getSegmentUnitMetadata(instanceId);
                    if (segmentUnitMetadata == null) {
                        logger.error("fix volume error: the segmentUnitMetadata is null {}", segmentMetadata);
                        needFixCount++;
                        continue;
                    }
                    Validate.notNull(segmentUnitMetadata);
                    String diskName = segmentUnitMetadata.getDiskName();
                    if (diskName == null || diskName.isEmpty()) {
                        logger.error("fix volume error: the diskName is null {}", segmentUnitMetadata);
                    }
                    boolean found = false;
                    for (RawArchiveMetadata archiveMetadata : datanode.getArchives()) {
                        logger.info("segment unit disk name: {}, archive name: {}", diskName,
                                archiveMetadata.getDeviceName());
                        if (diskName.equalsIgnoreCase(archiveMetadata.getDeviceName())) {
                            found = true;
                            if (archiveMetadata.getStatus() != ArchiveStatus.GOOD) {
                                needFixCount++;
                                break;
                            } else {
                                // found and archive is good
                                exceptNodeId.add(instanceId.getId());
                                aliveSegmentUnit = instanceId;
                                if (membership.getPrimary() == instanceId) {
                                    alivePrimaryID = instanceId;
                                }
                            }
                        }
                    }  // for loop to find archive info
                    if (!found) {
                        needFixCount++;
                    }
                } // for every segment unit in segment
                aliveSegmentUnitCount = memberCount - needFixCount;
            }
            //wo need fix volume
            ReserveSegUnitsResponse reserveSegUnitsResponse;
            if (aliveSegmentUnitCount < volumeMetadata.getVolumeType().getVotingQuorumSize()) {
                logger.warn("we need fix volume,alive segment count,{}", aliveSegmentUnitCount);
                // can we fix this segment completely
                if (!canFix) {
                    logger.error(" confirmFixVolume throw error ,lack DataNode ID is {}", lackDataNodeID);
                    throw new LackDatanodeException_Thrift();
                }
                if (exceptNodeId.isEmpty()) {
                    //no segmentUnit alive in segment,need create two segmentUnit
                    Validate.isTrue(aliveSegmentUnitCount == 0);
                    CreateSegmentUnitInfo tmpPrimarySegmentUnitInfoDetail = new CreateSegmentUnitInfo();
                    CreateSegmentUnitInfo tmpSecondarySegmentUnitInfoDetail = new CreateSegmentUnitInfo();
                    CreateSegmentUnitInfo tmpArbiterSegmentUnitInfoDetail = new CreateSegmentUnitInfo();

                    Map<InstanceIdAndEndPoint_Thrift, SegmentMembership_Thrift> primarySegmentMembershipThriftMap = new HashMap<>();

                    Set<InstanceId> secondaryId = new HashSet<>();
                    Set<InstanceId> joiningSecondaryId = new HashSet<>();
                    Set<InstanceId> arbiterId = new HashSet<>();

                    //reserve unit ,get datanode
                    ReserveSegUnitsRequest reserveSegUnitsRequest = new ReserveSegUnitsRequest(request.getRequestId(),
                            volumeMetadata.getSegmentSize(), exceptNodeId, 2, request.getAccountId(),
                            request.getVolumeId(), segId.getIndex(), SegmentUnitType_Thrift.Normal);
                    try {
                        reserveSegUnitsResponse = reserveSegUnits(reserveSegUnitsRequest);
                    } catch (NotEnoughSpaceException_Thrift ne) {
                        logger.error("confirmFixVolume throw error:  NotEnoughSpaceException_Thrift");
                        throw new NotEnoughSpaceException_Thrift();

                    } catch (InvalidInputException_Thrift ii) {
                        logger.error("confirmFixVolume throw error:  InvalidInputException_Thrift");
                        throw new InvalidInputException_Thrift();
                    } catch (ServiceIsNotAvailable_Thrift sna) {
                        logger.error("confirmFixVolume throw error:  ServiceIsNotAvailable_Thrift");
                        throw new ServiceIsNotAvailable_Thrift();
                    }

                    List<InstanceMetadata_Thrift> firstList = new ArrayList<>();
                    List<InstanceMetadata_Thrift> secondList = new ArrayList<>();
                    splitDatanodesByGroupId(reserveSegUnitsResponse, firstList, secondList);

                    Validate.notEmpty(firstList);
                    Validate.notEmpty(secondList);
                    InstanceIdAndEndPoint_Thrift firstInstanceIdAndEndPoint_thrift = RequestResponseHelper
                            .buildInstanceIdAndEndPoint_ThriftList(firstList).get(0);
                    InstanceIdAndEndPoint_Thrift secondInstanceIdAndEndPointThrift = RequestResponseHelper
                            .buildInstanceIdAndEndPoint_ThriftList(secondList).get(0);

                    if (volumeMetadata.getVolumeType() == VolumeType.REGULAR) {
                        logger.warn("all segmentUnit died ,we need create P+S");
                        //create membership
                        InstanceId primaryId = new InstanceId(firstInstanceIdAndEndPoint_thrift.getInstanceId());
                        InstanceId secondaryInstanceId = new InstanceId(
                                secondInstanceIdAndEndPointThrift.getInstanceId());
                        secondaryId.add(secondaryInstanceId);
                        SegmentMembership tmpMembership = new SegmentMembership(segmentVersion, primaryId, secondaryId,
                                arbiterId, null, joiningSecondaryId);

                        SegmentMembership_Thrift tmpMemberShipThrift = RequestResponseHelper
                                .buildThriftMembershipFrom(segId, tmpMembership);
                        //create segment unit as P
                        tmpPrimarySegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRole_Thrift.Primary);
                        primarySegmentMembershipThriftMap.put(firstInstanceIdAndEndPoint_thrift, tmpMemberShipThrift);
                        tmpPrimarySegmentUnitInfoDetail.setSegmentMembershipMap(primarySegmentMembershipThriftMap);

                        tmpSegmentUnitInfoList.add(tmpPrimarySegmentUnitInfoDetail);

                        // create segment unit as S
                        Map<InstanceIdAndEndPoint_Thrift, SegmentMembership_Thrift> sendarySegmentMembershipThriftMap = new HashMap<>();
                        tmpSecondarySegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRole_Thrift.Secondary);
                        sendarySegmentMembershipThriftMap.put(secondInstanceIdAndEndPointThrift, tmpMemberShipThrift);
                        tmpSecondarySegmentUnitInfoDetail.setSegmentMembershipMap(sendarySegmentMembershipThriftMap);
                        tmpSegmentUnitInfoList.add(tmpSecondarySegmentUnitInfoDetail);
                    } else if (volumeMetadata.getVolumeType() == VolumeType.SMALL) {
                        logger.warn("all segmentUnit died ,we need create P+A");
                        //create membership
                        InstanceId primaryId = new InstanceId(firstInstanceIdAndEndPoint_thrift.getInstanceId());
                        InstanceId arbiterinstanceId = new InstanceId(
                                secondInstanceIdAndEndPointThrift.getInstanceId());
                        arbiterId.add(arbiterinstanceId);
                        SegmentMembership tmpMembership = new SegmentMembership(segmentVersion, primaryId, secondaryId,
                                arbiterId, null, joiningSecondaryId);
                        SegmentMembership_Thrift tmpMemberShipThrift = RequestResponseHelper
                                .buildThriftMembershipFrom(segId, tmpMembership);

                        //create segment unit as P
                        tmpPrimarySegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRole_Thrift.Primary);
                        primarySegmentMembershipThriftMap.put(firstInstanceIdAndEndPoint_thrift, tmpMemberShipThrift);
                        tmpPrimarySegmentUnitInfoDetail.setSegmentMembershipMap(primarySegmentMembershipThriftMap);
                        tmpSegmentUnitInfoList.add(tmpPrimarySegmentUnitInfoDetail);

                        // create segment unit as A
                        Map<InstanceIdAndEndPoint_Thrift, SegmentMembership_Thrift> arbiterSegmentMembershipThriftMap = new HashMap<>();
                        tmpArbiterSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRole_Thrift.Arbiter);
                        arbiterSegmentMembershipThriftMap.put(secondInstanceIdAndEndPointThrift, tmpMemberShipThrift);
                        tmpArbiterSegmentUnitInfoDetail.setSegmentMembershipMap(arbiterSegmentMembershipThriftMap);
                        tmpSegmentUnitInfoList.add(tmpArbiterSegmentUnitInfoDetail);
                    }
                } else {
                    //  only one segmentUnit alive in segment,need create another segment unit to vote

                    Validate.isTrue(aliveSegmentUnitCount == 1);
                    ReserveSegUnitsRequest reserveSegUnitsRequest = new ReserveSegUnitsRequest(request.getRequestId(),
                            volumeMetadata.getSegmentSize(), exceptNodeId, 1, request.getAccountId(),
                            request.getVolumeId(), segId.getIndex(), SegmentUnitType_Thrift.Normal);
                    try {
                        reserveSegUnitsResponse = reserveSegUnits(reserveSegUnitsRequest);
                    } catch (NotEnoughSpaceException_Thrift ne) {
                        logger.error("confirmFixVolume throw error:  NotEnoughSpaceException_Thrift");
                        throw new NotEnoughSpaceException_Thrift();

                    } catch (InvalidInputException_Thrift ii) {
                        logger.error("confirmFixVolume throw error:  InvalidInputException_Thrift");
                        throw new InvalidInputException_Thrift();
                    } catch (ServiceIsNotAvailable_Thrift sna) {
                        logger.error("confirmFixVolume throw error:  ServiceIsNotAvailable_Thrift");
                        throw new ServiceIsNotAvailable_Thrift();
                    }

                    CreateSegmentUnitInfo tmpSegmentUnitInfoDetail = new CreateSegmentUnitInfo();
                    Map<InstanceIdAndEndPoint_Thrift, SegmentMembership_Thrift> segmentMembershipThriftMap = new HashMap<>();
                    tmpSegmentUnitInfoList.add(tmpSegmentUnitInfoDetail);

                    Set<InstanceId> secondaryId = membership.getSecondaries();
                    Set<InstanceId> joiningSecondaryId = membership.getJoiningSecondaries();
                    Set<InstanceId> arbiterId = membership.getArbiters();

                    if (volumeMetadata.getVolumeType() == VolumeType.REGULAR) {
                        if (membership.isJoiningSecondary(aliveSegmentUnit)) {
                            // J alive, need create P
                            logger.warn("segmentUnit only J alive, volume type is PSS ,we need create P");
                            secondaryId.clear();
                            arbiterId.clear();
                            tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRole_Thrift.Primary);
                            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : RequestResponseHelper
                                    .buildInstanceIdAndEndPoint_ThriftList(reserveSegUnitsResponse.getInstances())) {
                                InstanceId primaryId = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                                SegmentMembership tmpMembership = new SegmentMembership(segmentVersion, primaryId,
                                        secondaryId, arbiterId, null, joiningSecondaryId);
                                SegmentMembership_Thrift tmpMemberShipThrift = RequestResponseHelper
                                        .buildThriftMembershipFrom(segId, tmpMembership);
                                segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
                            }
                        } else if (membership.isSecondary(aliveSegmentUnit)) {
                            //  S  alive,we need create joining secondary
                            logger.warn("segmentUnit only S alive, volume type is PSS ,we need create J");
                            tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRole_Thrift.JoiningSecondary);
                            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : RequestResponseHelper
                                    .buildInstanceIdAndEndPoint_ThriftList(reserveSegUnitsResponse.getInstances())) {
                                //we need remove secondary ID that is died from secondary set in membership
                                secondaryId.clear();
                                arbiterId.clear();
                                InstanceId insertInstanceId = new InstanceId(
                                        instanceIdAndEndPointThrift.getInstanceId());
                                joiningSecondaryId.clear();
                                joiningSecondaryId.add(insertInstanceId);
                                SegmentMembership tmpMembership = new SegmentMembership(segmentVersion,
                                        aliveSegmentUnit, secondaryId, arbiterId, null, joiningSecondaryId);
                                SegmentMembership_Thrift tmpMemberShipThrift = RequestResponseHelper
                                        .buildThriftMembershipFrom(segId, tmpMembership);
                                segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
                            }
                        } else if (membership.isPrimary(aliveSegmentUnit)) {
                            // P Alive,need create joining secondary
                            logger.warn("segmentUnit only P alive ,volume type is PSS ,we need create J");
                            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : RequestResponseHelper
                                    .buildInstanceIdAndEndPoint_ThriftList(reserveSegUnitsResponse.getInstances())) {

                                secondaryId.clear();
                                arbiterId.clear();
                                InstanceId insertInstanceId = new InstanceId(
                                        instanceIdAndEndPointThrift.getInstanceId());
                                joiningSecondaryId.clear();
                                joiningSecondaryId.add(insertInstanceId);
                                SegmentMembership tmpMembership = new SegmentMembership(segmentVersion,
                                        membership.getPrimary(), secondaryId, arbiterId, null, joiningSecondaryId);
                                SegmentMembership_Thrift tmpMemberShipThrift = RequestResponseHelper
                                        .buildThriftMembershipFrom(segId, tmpMembership);
                                segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
                            }
                            tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRole_Thrift.JoiningSecondary);
                        } else {
                            logger.error("PSS Error,aliveSegmentUnit is not in membership");
                        }
                    } else if (volumeMetadata.getVolumeType() == VolumeType.SMALL) {
                        if (membership.isJoiningSecondary(aliveSegmentUnit)) {
                            // J Alive,need create p
                            logger.warn("segmentUnit only J alive ,volume type is PSA ,we need create P");
                            tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRole_Thrift.Primary);
                            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : RequestResponseHelper
                                    .buildInstanceIdAndEndPoint_ThriftList(reserveSegUnitsResponse.getInstances())) {
                                secondaryId.clear();
                                arbiterId.clear();
                                joiningSecondaryId.clear();
                                joiningSecondaryId.add(aliveSegmentUnit);
                                InstanceId insertInstanceId = new InstanceId(
                                        instanceIdAndEndPointThrift.getInstanceId());
                                SegmentMembership tmpMembership = new SegmentMembership(segmentVersion,
                                        insertInstanceId, secondaryId, arbiterId, null, joiningSecondaryId);
                                SegmentMembership_Thrift tmpMemberShipThrift = RequestResponseHelper
                                        .buildThriftMembershipFrom(segId, tmpMembership);
                                segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
                            }
                        } else if (membership.isArbiter(aliveSegmentUnit)) {
                            // A alive,need create P
                            logger.warn("segmentUnit only A alive ,volume type is PSA ,we need create P");
                            tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRole_Thrift.Primary);

                            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : RequestResponseHelper
                                    .buildInstanceIdAndEndPoint_ThriftList(reserveSegUnitsResponse.getInstances())) {
                                secondaryId.clear();
                                joiningSecondaryId.clear();
                                InstanceId insertInstanceId = new InstanceId(
                                        instanceIdAndEndPointThrift.getInstanceId());

                                SegmentMembership tmpMembership = new SegmentMembership(segmentVersion,
                                        insertInstanceId, secondaryId, arbiterId, null, joiningSecondaryId);
                                SegmentMembership_Thrift tmpMemberShipThrift = RequestResponseHelper
                                        .buildThriftMembershipFrom(segId, tmpMembership);
                                segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
                            }
                        } else if (membership.isSecondary(aliveSegmentUnit)) {
                            //  S alive, need create A
                            logger.warn("segmentUnit only S alive ,volume type is PSA ,we need create A");
                            tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRole_Thrift.Arbiter);

                            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : RequestResponseHelper
                                    .buildInstanceIdAndEndPoint_ThriftList(reserveSegUnitsResponse.getInstances())) {
                                InstanceId insertInstanceId = new InstanceId(
                                        instanceIdAndEndPointThrift.getInstanceId());
                                secondaryId.clear();
                                arbiterId.clear();
                                arbiterId.add(insertInstanceId);
                                joiningSecondaryId.clear();
                                SegmentMembership tmpMembership = new SegmentMembership(segmentVersion,
                                        aliveSegmentUnit, secondaryId, arbiterId, null, joiningSecondaryId);
                                SegmentMembership_Thrift tmpMemberShipThrift = RequestResponseHelper
                                        .buildThriftMembershipFrom(segId, tmpMembership);
                                segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
                            }
                        } else if (membership.isPrimary(aliveSegmentUnit)) {
                            // P alive,need create Arbiter;
                            logger.warn("segmentUnit only P alive ,volume type is PSA ,we need create A");
                            tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRole_Thrift.Arbiter);

                            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : RequestResponseHelper
                                    .buildInstanceIdAndEndPoint_ThriftList(reserveSegUnitsResponse.getInstances())) {
                                InstanceId insertInstanceId = new InstanceId(
                                        instanceIdAndEndPointThrift.getInstanceId());
                                arbiterId.clear();
                                arbiterId.add(insertInstanceId);
                                secondaryId.clear();
                                joiningSecondaryId.clear();
                                SegmentMembership tmpMembership = new SegmentMembership(segmentVersion,
                                        aliveSegmentUnit, secondaryId, arbiterId, null, joiningSecondaryId);
                                SegmentMembership_Thrift tmpMemberShipThrift = RequestResponseHelper
                                        .buildThriftMembershipFrom(segId, tmpMembership);
                                segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
                            }
                        } else {
                            logger.error("PSA Error,aliveSegmentUnit is not in membership");
                        }
                    }

                    tmpSegmentUnitInfoDetail.setSegmentMembershipMap(segmentMembershipThriftMap);
                }

                if (!tmpSegmentUnitInfoList.isEmpty()) {
                    createSegmentUnitInfoMap.put(new SegId_Thrift(segId.getVolumeId().getId(), segId.getIndex()),
                            tmpSegmentUnitInfoList);
                }
            } else if (alivePrimaryID == null) {
                // P is not alive

                logger.warn("we need fix volume,alive segment count,{}", aliveSegmentUnitCount);
                // can we fix this segment completely
                if (!canFix) {
                    logger.error(" confirmFixVolume throw error ,lack DataNode ID is {}", lackDataNodeID);
                    throw new LackDatanodeException_Thrift();
                }

                Validate.notNull(segmentMetadata);
                SegmentUnitMetadata aliveSegmentUnitMetadata = segmentMetadata.getSegmentUnitMetadata(aliveSegmentUnit);
                SegmentForm segmentForm = aliveSegmentUnitMetadata.getMembership()
                        .getSegmentForm(volumeMetadata.getVolumeType());
                //PJA and P is not Alive
                if (!segmentForm.canGenerateNewPrimary()) {
                    ReserveSegUnitsRequest reserveSegUnitsRequest = new ReserveSegUnitsRequest(request.getRequestId(),
                            volumeMetadata.getSegmentSize(), exceptNodeId, 1, request.getAccountId(),
                            request.getVolumeId(), segId.getIndex(), SegmentUnitType_Thrift.Normal);
                    try {
                        reserveSegUnitsResponse = reserveSegUnits(reserveSegUnitsRequest);
                    } catch (NotEnoughSpaceException_Thrift ne) {
                        logger.error("confirmFixVolume throw error:  NotEnoughSpaceException_Thrift");
                        throw new NotEnoughSpaceException_Thrift();

                    } catch (InvalidInputException_Thrift ii) {
                        logger.error("confirmFixVolume throw error:  InvalidInputException_Thrift");
                        throw new InvalidInputException_Thrift();
                    } catch (ServiceIsNotAvailable_Thrift sna) {
                        logger.error("confirmFixVolume throw error:  ServiceIsNotAvailable_Thrift");
                        throw new ServiceIsNotAvailable_Thrift();
                    }

                    CreateSegmentUnitInfo tmpSegmentUnitInfoDetail = new CreateSegmentUnitInfo();
                    Map<InstanceIdAndEndPoint_Thrift, SegmentMembership_Thrift> segmentMembershipThriftMap = new HashMap<>();
                    tmpSegmentUnitInfoList.add(tmpSegmentUnitInfoDetail);

                    Set<InstanceId> secondaryId = membership.getSecondaries();
                    Set<InstanceId> joiningSecondaryId = membership.getJoiningSecondaries();
                    Set<InstanceId> arbiterId = membership.getArbiters();

                    tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRole_Thrift.Primary);
                    for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPointThrift : RequestResponseHelper
                            .buildInstanceIdAndEndPoint_ThriftList(reserveSegUnitsResponse.getInstances())) {
                        secondaryId.clear();
                        InstanceId insertInstanceId = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                        SegmentMembership tmpMembership = new SegmentMembership(segmentVersion, insertInstanceId,
                                secondaryId, arbiterId, null, joiningSecondaryId);
                        SegmentMembership_Thrift tmpMemberShipThrift = RequestResponseHelper
                                .buildThriftMembershipFrom(segId, tmpMembership);
                        segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
                    }
                    tmpSegmentUnitInfoDetail.setSegmentMembershipMap(segmentMembershipThriftMap);
                    if (!tmpSegmentUnitInfoList.isEmpty()) {
                        createSegmentUnitInfoMap.put(new SegId_Thrift(segId.getVolumeId().getId(), segId.getIndex()),
                                tmpSegmentUnitInfoList);
                    }
                }

            } // if judge this segment needs fix or not
        } // for every segment in volume
        confirmFixVolumeResponse.setCreateSegmentUnits(createSegmentUnitInfoMap);

        confirmFixVolumeResponse.setVolumeSource(volumeMetadata.getVolumeSource().getVolumeSource_thrift());

        volumeMetadata.setVolumeStatus(VolumeStatus.Fixing);
        volumeMetadata.markLastFixVolumeTime();
        volumeStore.saveVolume(volumeMetadata);
        logger.warn("confirmFixVolume response: {}", confirmFixVolumeResponse);
        return confirmFixVolumeResponse;
    }

    /**
     * change this method to static for unit test purpose
     *
     * @param reserverSegUnitRsp
     * @param firstList
     * @param secondList
     */
    public static void splitDatanodesByGroupId(ReserveSegUnitsResponse reserverSegUnitRsp,
            List<InstanceMetadata_Thrift> firstList, List<InstanceMetadata_Thrift> secondList) {

        // split datanodes by group id, because we want to create two segment units at one time
        // must make sure that datanodes belong to one group can not split to two list
        Multimap<Integer, InstanceMetadata_Thrift> mapGroupIdToDatanodes = HashMultimap.<Integer, InstanceMetadata_Thrift>create();
        for (InstanceMetadata_Thrift instanceMetadata_thrift : reserverSegUnitRsp.getInstances()) {
            mapGroupIdToDatanodes.put(instanceMetadata_thrift.getGroup().getGroupId(), instanceMetadata_thrift);
        }

        Multimap<Integer, Integer> sortMap = HashMultimap.create();
        for (Integer groupId : mapGroupIdToDatanodes.keySet()) {
            int size = mapGroupIdToDatanodes.get(groupId).size();
            sortMap.put(size, groupId);
        }
        List<Integer> descList = new ArrayList<>(sortMap.keySet());
        Collections.sort(descList);
        Collections.reverse(descList);
        for (Integer size : descList) {
            Collection<Integer> groupIds = sortMap.get(size);
            for (Integer groupId : groupIds) {
                if (firstList.size() >= secondList.size()) {
                    secondList.addAll(mapGroupIdToDatanodes.get(groupId));
                } else {
                    firstList.addAll(mapGroupIdToDatanodes.get(groupId));
                }
            }

        }
    }

    public boolean listContentEquals(List list1, List list2) {
        if (list1 == null && list2 == null)
            return true;

        if (list1 == null && list2 != null)
            return false;

        if (list1 != null && list2 == null)
            return false;

        if (list1.size() != list2.size()) {
            return false;
        } else {
            for (Object object : list1) {
                if (!list2.contains(object))
                    return false;
            }
        }

        return true;
    }

    public CloneRelationshipsDBStore getCloneRelationshipsStore() {
        return cloneRelationshipsStore;
    }

    public void setCloneRelationshipsStore(CloneRelationshipsDBStore cloneRelationshipsStore) {
        this.cloneRelationshipsStore = cloneRelationshipsStore;
    }

    public AccountStore getAccountStore() {
        return accountStore;
    }

    public void setAccountStore(AccountStore accountStore) {
        this.accountStore = accountStore;
    }

    public RoleStore getRoleStore() {
        return roleStore;
    }

    public void setRoleStore(RoleStore roleStore) {
        this.roleStore = roleStore;
    }

    public ResourceStore getResourceStore() {
        return resourceStore;
    }

    public void setResourceStore(ResourceStore resourceStore) {
        this.resourceStore = resourceStore;
    }

    public APIStore getApiStore() {
        return apiStore;
    }

    public void setApiStore(APIStore apiStore) {
        this.apiStore = apiStore;
    }

    public int getPageWrappCount() {
        return pageWrappCount;
    }

    public void setPageWrappCount(int pageWrappCount) {
        this.pageWrappCount = pageWrappCount;
    }

    public int getSegmentWrappCount() {
        return segmentWrappCount;
    }

    public void setSegmentWrappCount(int segmentWrappCount) {
        this.segmentWrappCount = segmentWrappCount;
    }

    public InstanceMaintenanceDBStore getInstanceMaintenanceDBStore() {
        return instanceMaintenanceDBStore;
    }

    public void setInstanceMaintenanceDBStore(InstanceMaintenanceDBStore instanceMaintenanceDBStore) {
        this.instanceMaintenanceDBStore = instanceMaintenanceDBStore;
    }

}