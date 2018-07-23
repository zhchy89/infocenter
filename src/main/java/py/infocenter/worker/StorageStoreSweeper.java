package py.infocenter.worker;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.archive.ArchiveStatus;
import py.archive.RawArchiveMetadata;
import py.common.PyService;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.common.struct.EndPoint;
import py.icshare.*;
import py.icshare.exception.NotExistedException;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceStatus;
import py.monitor.alarmbak.AlarmLevel;
import py.monitor.alarmbak.AlarmMessageData;
import py.monitor.alarmbak.AlarmMessageData.AlarmOper;
import py.monitor.jmx.server.AlarmReporter;
import py.monitor.jmx.server.JmxAgent;
import py.monitorserver.common.CounterName;
import py.monitorserver.common.OperationName;
import py.monitorserver.common.UserDefineName;
import py.periodic.Worker;
import py.querylog.EventDataUtil.EventDataWorker;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

import java.util.*;

import static py.icshare.InstanceMetadata.DatanodeStatus.OK;

/**
 * check instance metadata periodic, if it is timeout, then i will remove the instance metadata.
 *
 * @author kobofare
 */
public class StorageStoreSweeper implements Worker {
    private static final Logger logger = LoggerFactory.getLogger(StorageStoreSweeper.class);

    private StorageStore storageStore;

    private Map<Long, ArchiveStatus> archiveLastStatusMap = new HashMap<>();

    private int timeToRemove;

    private AppContext appContext;

    private StoragePoolStore storagePoolStore;

    private DomainStore domainStore;

    private VolumeStore volumeStore;

    private long segmentSize;

    private Map<Long, EventDataWorker> storagePoolId2EventDataWorker = new HashMap<>();

    private InstanceMaintenanceDBStore instanceMaintenanceDBStore;

    @Override
    public void doWork() throws Exception {
        // check the service is working right, maybe become SUSPEND
        if (appContext.getStatus() == InstanceStatus.SUSPEND) {
            logger.info("++clear all instances metadata: " + storageStore);
            // need clear the cache data
            storageStore.clearMemoryData();
            return;
        }

        List<InstanceMaintenanceInformation> instanceMaintenanceDBStoreList =  instanceMaintenanceDBStore.listAll();

        for (InstanceMaintenanceInformation info : instanceMaintenanceDBStoreList) {
            if (info != null) {
                if (System.currentTimeMillis() > info.getEndTime()) {
                    instanceMaintenanceDBStore.delete(info);
                }
            }
        }

        Multimap<Long, Integer> domainId2SimpleGroupIdMap = HashMultimap.create();
        Map<Long, InstanceMetadata> instanceId2InstanceMetadata = new HashMap<>();
        Map<Long, RawArchiveMetadata> archiveId2Archive = new HashMap<>();
        logger.debug("Trying to create & send disk alarm to monitorcenter");
        long sysTime = System.currentTimeMillis();
        for (InstanceMetadata instance : storageStore.list()) {
            if (sysTime - instance.getLastUpdated() > timeToRemove) {
                logger.warn("delete the unreport instance metadata: {}", instance.getInstanceId().getId());
                instance.setDatanodeStatus(InstanceMetadata.DatanodeStatus.UNKNOWN);
                storageStore.save(instance);
//                storageStore.delete(instance.getInstanceId().getId());
            }
//            reportAlarm(instance);

            if (instance.getDatanodeStatus().equals(OK) || instanceMaintenanceDBStore.getById(instance.getInstanceId().getId()) != null) {
                instanceId2InstanceMetadata.put(instance.getInstanceId().getId(), instance);
                for (RawArchiveMetadata archiveMetadata : instance.getArchives()) {
                    //when archives changed to not good, pool level will be changed
                    if (archiveMetadata.getStatus() == ArchiveStatus.GOOD){
                        archiveId2Archive.put(archiveMetadata.getArchiveId(), archiveMetadata);
                    }
                }
            }

            if (instance.getDatanodeType() == InstanceMetadata.DatanodeType.SIMPLE){
                domainId2SimpleGroupIdMap.put(instance.getDomainId(), instance.getGroup().getGroupId());
            }
        }

        HashMap<Long, Integer> poolId2MaxSegmentUnitPerSegmentMap = new HashMap<>();          //<poolId, max segment unit per segment>
        HashMap<Long, Integer> poolId2MaxNormalSegmentUnitPerSegmentMap = new HashMap<>();    //<poolId, max normal segment unit per segment>
        ObjectCounter<Long> storagePoolVolumeCounter = new TreeSetObjectCounter<>();
        for (VolumeMetadata volume : volumeStore.listRootVolumes()) {
            if (volume.getVolumeStatus() != VolumeStatus.Deleting && volume.getVolumeStatus() != VolumeStatus.Deleted
                    && volume.getVolumeStatus() != VolumeStatus.Dead) {
                storagePoolVolumeCounter.increment(volume.getStoragePoolId());
            }

            if (volume.getVolumeStatus() != VolumeStatus.Dead){
                //all segment unit
                int maxSegmentUnitPerSegment = volume.getVolumeType().getNumMembers();
                if (poolId2MaxSegmentUnitPerSegmentMap.containsKey(volume.getStoragePoolId())) {
                    maxSegmentUnitPerSegment = Math.max(poolId2MaxSegmentUnitPerSegmentMap.get(volume.getStoragePoolId()),
                            maxSegmentUnitPerSegment);
                }
                poolId2MaxSegmentUnitPerSegmentMap.put(volume.getStoragePoolId(), maxSegmentUnitPerSegment);

                //normal segment unit
                int maxNormalSegmentUnitPerSegment = volume.getVolumeType().getNumMembers() - volume.getVolumeType().getNumArbiters();
                if (poolId2MaxNormalSegmentUnitPerSegmentMap.containsKey(volume.getStoragePoolId())) {
                    maxNormalSegmentUnitPerSegment = Math.max(poolId2MaxNormalSegmentUnitPerSegmentMap.get(volume.getStoragePoolId()),
                            maxNormalSegmentUnitPerSegment);
                }
                poolId2MaxNormalSegmentUnitPerSegmentMap.put(volume.getStoragePoolId(), maxNormalSegmentUnitPerSegment);
            }
        }

        // calculate storage pool total space and free space, at the same time, count the group
        // number in the storage pool, write these information to event data
        for (StoragePool storagePool : storagePoolStore.listAllStoragePools()) {
            long poolId = storagePool.getPoolId();
            long logicalSpace = 0;
            long logicalFreeSpace = 0;
            long logicalPSSFreeSpace = StoragePoolSpaceCalculator.calculateFreeSpace(storagePool,
                    instanceId2InstanceMetadata, archiveId2Archive, 3, segmentSize);
            long logicalPSAFreeSpace = StoragePoolSpaceCalculator.calculateFreeSpace(storagePool,
                    instanceId2InstanceMetadata, archiveId2Archive, 2, segmentSize);
            Set<Integer> groupSet = new HashSet<>();
            Set<Integer> simpleGroupSet = new HashSet<>();
            Set<Integer> normalGroupSet = new HashSet<>();
            boolean archiveLost = false;
            boolean rebuildFail = false;
            for (Long archiveId : storagePool.getArchivesInDataNode().values()) {
                RawArchiveMetadata archiveMetadata = archiveId2Archive.get(archiveId);
                if (archiveMetadata != null) {
                    logicalSpace += archiveMetadata.getLogicalSpace();
                    logicalFreeSpace += archiveMetadata.getLogicalFreeSpace();

                    InstanceMetadata instance = instanceId2InstanceMetadata.get(archiveMetadata.getInstanceId().getId());
                    groupSet.add(instance.getGroup().getGroupId());

                    if (instance.getDatanodeType() == InstanceMetadata.DatanodeType.SIMPLE){
                        simpleGroupSet.add(instance.getGroup().getGroupId());
                    } else if (instance.getDatanodeType() == InstanceMetadata.DatanodeType.NORMAL){
                        normalGroupSet.add(instance.getGroup().getGroupId());
                    } else {
                        logger.warn("instance id:{} endpoint:{} DatanodeType not set! {}",
                                instance.getInstanceId().getId(), instance.getEndpoint());
                        //throw new DatanodeTypeNotSetException_Thrift();
                    }

                    if (!archiveMetadata.drainAllMigrateFailedSegIds().isEmpty()) {
                        rebuildFail = true;
                    }
                } else {
                    // cannot find one archive in the storage pool
                    archiveLost = true;
                }
            }
            storagePool.setTotalSpace(logicalSpace);
            storagePool.setFreeSpace(logicalFreeSpace);
            storagePool.setLogicalPSSFreeSpace(logicalPSSFreeSpace);
            storagePool.setLogicalPSAFreeSpace(logicalPSAFreeSpace);

            // if storage pool has no archive, do not generate query log
            if (groupSet.isEmpty() || storagePool.isDeleting()) {
                continue;
            }

            EventDataWorker eventDataWorker = storagePoolId2EventDataWorker.get(poolId);
            if (eventDataWorker == null) {
                Map<String, String> userDefineParams = new HashMap<>();
                userDefineParams.put(UserDefineName.StoragePoolID.name(), String.valueOf(poolId));
                userDefineParams.put(UserDefineName.StoragePoolName.name(), storagePool.getName());
                eventDataWorker = new EventDataWorker(PyService.INFOCENTER, userDefineParams);
                storagePoolId2EventDataWorker.put(poolId, eventDataWorker);
            }
            long freeSpaceRatio = logicalSpace == 0 ? 0 : (logicalFreeSpace * 100) / logicalSpace;
            long availablePSSSegmentCount = logicalPSSFreeSpace / segmentSize;
            long availablePSASegmentCount = logicalPSAFreeSpace / segmentSize;

            Map<String, Long> counters = new HashMap<>();
            counters.put(CounterName.STORAGEPOOL_FREE_SPACE_RATIO.name(), freeSpaceRatio);
            counters.put(CounterName.STORAGEPOOL_AVAILABLE_PSS_SEGMENT_COUNT.name(), availablePSSSegmentCount);
            counters.put(CounterName.STORAGEPOOL_AVAILABLE_PSA_SEGMENT_COUNT.name(), availablePSASegmentCount);
            counters.put(CounterName.STORAGEPOOL_GROUP_AMOUNT.name(), Long.valueOf(groupSet.size()));
            counters.put(CounterName.STORAGEPOOL_VOLUME_AMOUNT.name(), storagePoolVolumeCounter.get(poolId));


            //get the max number of arbiter segment unit count per volume segment in current pool,
            //when has no volume, default volume is SMALL
            int maxAllSegmentUnitPerSegment = VolumeType.SMALL.getNumMembers();
            if (poolId2MaxSegmentUnitPerSegmentMap.containsKey(storagePool.getPoolId())){
                maxAllSegmentUnitPerSegment = poolId2MaxSegmentUnitPerSegmentMap.get(storagePool.getPoolId());
            }
            //get the max number of normal segment unit count per volume segment in current pool,
            //when has no volume, default volume is SMALL
            int maxNormalSegmentUnitPerSegment = VolumeType.SMALL.getNumMembers() - VolumeType.SMALL.getNumArbiters();
            if (poolId2MaxNormalSegmentUnitPerSegmentMap.containsKey(storagePool.getPoolId())){
                maxNormalSegmentUnitPerSegment = poolId2MaxNormalSegmentUnitPerSegmentMap.get(storagePool.getPoolId());
            }

            //group counter is or not enough to create segment unit
            simpleGroupSet.addAll(domainId2SimpleGroupIdMap.get(storagePool.getDomainId()));
            boolean isGroupEnough = isGroupEnoughToCreateSegment(simpleGroupSet, normalGroupSet,
                    maxAllSegmentUnitPerSegment, maxNormalSegmentUnitPerSegment);

            logger.info("rebuildFail:{} archiveLost:{} isGroupEnough:{}",
                    rebuildFail, archiveLost, isGroupEnough);

            storagePool.setStoragePoolLevel(StoragePoolLevel.HIGH.name());

            if (archiveLost) {
                storagePool.setStoragePoolLevel(StoragePoolLevel.MIDDLE.name());
            }

            if (!isGroupEnough || rebuildFail){
                storagePool.setStoragePoolLevel(StoragePoolLevel.LOW.name());
            }

            if (storagePool.getStoragePoolLevel().equals(StoragePoolLevel.LOW.name())){
                counters.put(CounterName.STORAGEPOOL_REBUILD_FAIL.name(), 0L);
            } else if (storagePool.getStoragePoolLevel().equals(StoragePoolLevel.MIDDLE.name())){
                counters.put(CounterName.STORAGEPOOL_LOST_DISK.name(), 0L);
            }

            eventDataWorker.work(OperationName.StoragePool.name(), counters);
        }

        // calculate domain total space and free space
        for (Domain domain : domainStore.listAllDomains()) {
            long logicalSpace = 0;
            long freeSpace = 0;
            for (Long instanceId : domain.getDataNodes()) {
                InstanceMetadata instanceMetadata = instanceId2InstanceMetadata.get(instanceId);
                if (instanceMetadata != null && instanceMetadata.getDatanodeStatus().equals(OK)) {
                    logicalSpace += instanceMetadata.getLogicalCapacity();
                    freeSpace += instanceMetadata.getFreeSpace();
                }
            }
            domain.setLogicalSpace(logicalSpace);
            domain.setFreeSpace(freeSpace);
        }
    }

    /**
     * group counter is or not enough to create segment unit
     * @param simpleGroupSetInPool   simple datanode group set
     * @param normalGroupSetInPool   normal datanode group set
     * @param maxSegmentUnitPerSegment   segment unit per segment must need
     * @param maxNormalSegmentUnitPerSegment   normal segment unit per segment must need
     * @return true:when
     */
    private boolean isGroupEnoughToCreateSegment(Set<Integer> simpleGroupSetInPool, Set<Integer> normalGroupSetInPool,
                                                 int maxSegmentUnitPerSegment, int maxNormalSegmentUnitPerSegment){
        logger.info("simpleGroupSetInPool:{} normalGroupSetInPool:{} maxSegmentUnitPerSegment:{} maxNormalSegmentUnitPerSegment:{}",
                simpleGroupSetInPool, normalGroupSetInPool, maxSegmentUnitPerSegment, maxNormalSegmentUnitPerSegment);
        Set<Integer> allGroupSet = new HashSet<>();
        allGroupSet.addAll(simpleGroupSetInPool);
        allGroupSet.addAll(normalGroupSetInPool);

        if (allGroupSet.size() < maxSegmentUnitPerSegment){
            logger.warn("isGroupEnoughToCreateSegment: group is not enough! need segment unit:{} current group size:{}",
                    maxSegmentUnitPerSegment, allGroupSet.size());
            return false;
        } else if (normalGroupSetInPool.size() < maxNormalSegmentUnitPerSegment){
            logger.warn("isGroupEnoughToCreateSegment: group is not enough! need normal segment unit: {} current normal group size:{} ",
                    maxNormalSegmentUnitPerSegment, normalGroupSetInPool.size());
            return false;
        }

        return true;
    }

    private void reportAlarm(InstanceMetadata instance) throws NotExistedException, Exception {
        AlarmReporter alarmReporter = JmxAgent.getInstance().getAlarmReporter();
        List<RawArchiveMetadata> archives = instance.getArchives();
        for (RawArchiveMetadata archive : archives) {
            ArchiveStatus archiveLastStatus = archiveLastStatusMap.get(archive.getArchiveId());
            if (archiveLastStatus == null) {
                archiveLastStatusMap.put(archive.getArchiveId(), archive.getStatus());
                if (!archive.getStatus().equals(ArchiveStatus.GOOD)) {
                    AlarmMessageData alarmData = new AlarmMessageData();
                    alarmData.setAlarmLevel(AlarmLevel.MAJOR);
                    alarmData.setAlarmName("Disk alarm");
                    String alarmObject =
                            new EndPoint(instance.getEndpoint()).getHostName() + ":" + archive.getDeviceName();
                    alarmData.setAlarmObject(alarmObject);
                    alarmData.setOper(AlarmOper.APPEAR);
                    String description = "Disk is not work well. current status is " + archive.getStatus().name();
                    alarmData.setAlarmDescription(description);
                    alarmReporter.report(alarmData);
                }
                continue;
            }

            logger.warn("Current archive : {}, current status: {},last status: {}",
                    instance.getEndpoint() + archive.getDeviceName(), archive.getStatus(), archiveLastStatus);
            if (archiveLastStatus.equals(archive.getStatus())) {
                // if current status is equal with last status. do not send alarm out.
                // do nothing
            } else {
                // if current status is not equal with last status

                AlarmMessageData alarmData = new AlarmMessageData();
                alarmData.setAlarmLevel(AlarmLevel.MAJOR);
                alarmData.setAlarmName("Disk alarm");
                String alarmObject = new EndPoint(instance.getEndpoint()).getHostName() + ":" + archive.getDeviceName();
                alarmData.setAlarmObject(alarmObject);

                if (archive.getStatus().equals(ArchiveStatus.GOOD)) {
                    alarmData.setOper(AlarmOper.DISAPPEAR);
                } else {
                    logger.warn("Disk <{}><{}> is in bad status: {}", instance.getEndpoint(), archive.getDeviceName(),
                            archive.getStatus());
                    alarmData.setOper(AlarmOper.APPEAR);
                    String description = "Disk is not work well. current status is " + archive.getStatus().name();
                    alarmData.setAlarmDescription(description);
                }
                alarmReporter.report(alarmData);
            }

            // save last archive status
            archiveLastStatusMap.put(archive.getArchiveId(), archive.getStatus());
        }
    }

    public void setAppContext(AppContext appContext) {
        this.appContext = appContext;
    }

    public StorageStore getStorageStore() {
        return storageStore;
    }

    public void setInstanceMetadataStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    public int getTimeToRemove() {
        return timeToRemove;
    }

    public void setTimeToRemove(int timeToRemove) {
        this.timeToRemove = timeToRemove;
    }

    public StoragePoolStore getStoragePoolStore() {
        return storagePoolStore;
    }

    public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
        this.storagePoolStore = storagePoolStore;
    }

    public DomainStore getDomainStore() {
        return domainStore;
    }

    public void setDomainStore(DomainStore domainStore) {
        this.domainStore = domainStore;
    }

    public VolumeStore getVolumeStore() {
        return volumeStore;
    }

    public void setVolumeStore(VolumeStore volumeStore) {
        this.volumeStore = volumeStore;
    }

    public long getSegmentSize() {
        return segmentSize;
    }

    public void setSegmentSize(long segmentSize) {
        this.segmentSize = segmentSize;
    }

    public InstanceMaintenanceDBStore getInstanceMaintenanceDBStore() {
        return instanceMaintenanceDBStore;
    }

    public void setInstanceMaintenanceDBStore(InstanceMaintenanceDBStore instanceMaintenanceDBStore) {
        this.instanceMaintenanceDBStore = instanceMaintenanceDBStore;
    }

    /**
     * just for test
     * @param storagePoolId2EventDataWorker
     */
    public void setStoragePoolId2EventDataWorker(Map<Long, EventDataWorker> storagePoolId2EventDataWorker) {
        this.storagePoolId2EventDataWorker = storagePoolId2EventDataWorker;
    }
}
