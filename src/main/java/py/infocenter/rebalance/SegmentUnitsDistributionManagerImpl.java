package py.infocenter.rebalance;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.Archive;
import py.archive.RawArchiveMetadata;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.common.struct.Pair;
import py.icshare.InstanceMetadata;
import py.icshare.SegmentId;
import py.icshare.StoragePool;
import py.icshare.StoragePoolStore;
import py.infocenter.rebalance.builder.InstanceInfoCollectionBuilder;
import py.infocenter.rebalance.builder.SimulateInstanceBuilder;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.rebalance.exception.TooManyTasksException;
import py.infocenter.rebalance.exception.VolumeCreatingException;
import py.infocenter.rebalance.selector.SegmentUnitReserver;
import py.infocenter.rebalance.selector.VolumeRebalanceSelector;
import py.infocenter.rebalance.struct.*;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceId;
import py.rebalance.RebalanceTask;
import py.thrift.infocenter.service.SegmentNotFoundException_Thrift;
import py.thrift.share.*;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

import java.util.*;

public class SegmentUnitsDistributionManagerImpl implements SegmentUnitsDistributionManager {

    private static final Logger logger = LoggerFactory.getLogger(SegmentUnitsDistributionManagerImpl.class);

    private final long segmentSize;
    private final VolumeStore volumeStore;
    private final StorageStore storageStore;
    private final StoragePoolStore storagePoolStore;
    private final RebalanceConfiguration config = RebalanceConfiguration.getInstance();

    private long initTime;

    /*
     * the rebalance task, instance and archives backup,
     * if instance and archives not changed, the rebalance task not be changed
     */
    private Multimap<Long, RebalanceTask> volumeId2RebalanceTaskBacMap = HashMultimap.create();     //rebalance task backup, if instance and archives not changed, it not be changed
    private Map<Long, SimulatePool> volumeId2SimulatePoolMap = new HashMap<>();                     //pool information backup of volume at last rebalance

    @Deprecated
    private final Multimap<InstanceId, SimpleRebalanceTask> movingOutTasks;
    @Deprecated
    private final Multimap<InstanceId, SimpleRebalanceTask> movingInTasks;
    @Deprecated
    private final Multimap<InstanceId, SimpleRebalanceTask> insideTasks;

    //update at reportArchives, when any datanode's type is SIMPLE,
    // isArbiterGroupSet is set to true,
    // and put this datanode's group id into arbiterGroupIds;
    private SimpleDatanodeManager simpleDatanodeManager;

    private Map<Long, VolumeRebalanceStatus> volumeId2RebalanceStatusMap = new HashMap<>();

    public enum VolumeRebalanceStatus {
        CHECKING,       //check volume is or not need to rebalance by threshold,if not, don't do rebalance
        WORKING         //do not care threshold, and must be rebalance.
    }

    public SegmentUnitsDistributionManagerImpl(long segmentSize, VolumeStore volumeStore,
                                               StorageStore storageStore, StoragePoolStore storagePoolStore) {
        this.segmentSize = segmentSize;
        this.volumeStore = volumeStore;
        this.storageStore = storageStore;
        this.storagePoolStore = storagePoolStore;

        this.movingOutTasks = Multimaps.synchronizedListMultimap(LinkedListMultimap.create());
        this.movingInTasks = Multimaps.synchronizedListMultimap(LinkedListMultimap.create());
        this.insideTasks = Multimaps.synchronizedListMultimap(LinkedListMultimap.create());

        this.initTime = System.currentTimeMillis();

        this.simpleDatanodeManager = new SimpleDatanodeManager();
    }

    public void forceStart() {
        initTime = 0;
    }

    private boolean justStarted() {
        if (initTime != 0) {
            if (System.currentTimeMillis() - initTime < 30000) {
                return true;
            } else {
                initTime = 0;
                return false;
            }
        }
        return false;
    }

    /**
     * update simple datanode group id and instance map(groupId2InstanceIdMap),
     * when any datanode's type is SIMPLE,
     * @param instanceMetadata datanode information
     */
    @Override
    public void updateSimpleDatanodeInfo(InstanceMetadata instanceMetadata) {
        if (instanceMetadata == null){
            logger.warn("[updateSimpleDatanodeGroupIdSet] instanceMetadata is null");
            return ;
        }

        logger.debug("[updateSimpleDatanodeGroupIdSet] instanceMetadata: {}", instanceMetadata);
        boolean isSimpleDatanode = (instanceMetadata.getDatanodeType() == InstanceMetadata.DatanodeType.SIMPLE);

        simpleDatanodeManager.updateSimpleDatanodeInfo(instanceMetadata.getInstanceId().getId(),
                instanceMetadata.getGroup().getGroupId(), isSimpleDatanode);
    }

    /**
     * get simple datanode's instance id set
     * @return all simple datanode's instance id set
     */
    @Override
    public Set<Long> getSimpleDatanodeInstanceIdSet(){
        return simpleDatanodeManager.getSimpleDatanodeInstanceIdSet();
    }

    /**
     * just for test
     * @return simple datanode manager
     */
    public SimpleDatanodeManager getSimpleDatanodeManager(){
        return simpleDatanodeManager;
    }

    @Override
    public synchronized List<RebalanceTask> selectRebalanceTasks(InstanceId instanceId) throws NoNeedToRebalance {
        // MigratePrimaryRequest
        // CreateSecondaryCandidateProcessor

        if (justStarted()) { // we will throw NoNeedToRebalance when info center just started cause there might be some
            // data nodes or archives or segment units not reported yet.
            throw new NoNeedToRebalance();
        }
        List<StoragePool> storagePools;
        try {
            storagePools = storagePoolStore.listAllStoragePools();
        } catch (Exception e) {
            logger.error("can not list all storage pools", e);
            throw new NoNeedToRebalance();
        }
        Validate.notNull(storagePools);

        logger.debug("now to select rebalance tasks of instance({}) if necessary, storage pools : {}", instanceId.getId(), storagePools);

        Multimap<Long, RebalanceTask> volumeId2TaskMap = HashMultimap.create();
        for (StoragePool storagePool : storagePools) {
            try {
                volumeId2TaskMap.putAll(selectInStoragePool(storagePool, false));
                logger.warn("selectInStoragePool:{}", volumeId2TaskMap);
            } catch (NoNeedToRebalance ignored) {
            }
        }

        //select task that migrate source object is instanceId



        if (volumeId2TaskMap.isEmpty()) {
            throw new NoNeedToRebalance();
        } else {
            logger.warn("got a rebalance task(record:{}) : {}", record, volumeId2TaskMap);
            return volumeId2TaskMap;
        }
    }

    private Multimap<Long, RebalanceTask> selectInStoragePool(StoragePool storagePool, boolean record)
            throws NoNeedToRebalance {
        Multimap<Long, RebalanceTask> volumeId2TaskMap = HashMultimap.create();
        List<VolumeMetadata> volumes = volumeStore.listVolumes();
        for (VolumeMetadata volume : volumes) {
            //if volume not stable, no need to rebalance
            if ((volume.getStoragePoolId() == (long) storagePool.getPoolId()) &&
                    (volume.isStable())) {
                try {
                    List<RebalanceTask> taskList = selectForVolume(storagePool, volume, record);
                    for (RebalanceTask task : taskList){
                        volumeId2TaskMap.put(volume.getVolumeId(), task);
                    }
                } catch (NoNeedToRebalance ignore) {
                } catch (VolumeCreatingException e) {
                    logger.info("volume is being created", e);
                    throw new NoNeedToRebalance();
                } catch (TooManyTasksException e) {
                    logger.info("too many tasks processing");
                    throw new NoNeedToRebalance();
                } catch (Throwable t) {
                    logger.warn("throwable caught", t);
                    throw new NoNeedToRebalance();
                }
            }
        }
        if (volumeId2TaskMap.isEmpty()) {
            throw new NoNeedToRebalance();
        } else {
            return volumeId2TaskMap;
        }
    }

    private List<RebalanceTask> selectForVolume(StoragePool storagePool, VolumeMetadata volume, boolean record)
            throws NoNeedToRebalance, VolumeCreatingException, TooManyTasksException, SegmentNotFoundException_Thrift, InstanceNotExistsException_Thrift {
        VolumeRebalanceSelector selector;
        selector = new VolumeRebalanceSelector(new SimulateInstanceBuilder(storagePool, storageStore, segmentSize).collectionInstance(), volume,
                storageStore.list(), getSimpleDatanodeInstanceIdSet());

        //when volume in CHECKING status, check combination is or not over threshold.
        // if not, no need to rebalance exception cause
        VolumeRebalanceStatus volumeRebalanceStatus = volumeId2RebalanceStatusMap.computeIfAbsent(volume.getVolumeId(), value->VolumeRebalanceStatus.CHECKING);
//        if (volumeRebalanceStatus == VolumeRebalanceStatus.CHECKING
//                && !selector.isNeedToDoRebalance()){
//            throw new NoNeedToRebalance();
//        }

        //change volume rebalance status to WORKING, when P or PS combination over threshold
        volumeId2RebalanceStatusMap.put(volume.getVolumeId(), VolumeRebalanceStatus.WORKING);

        SimulatePool lastSimulatePool = volumeId2SimulatePoolMap.get(volume.getVolumeId());
        SimulatePool currentSimulatePool = new SimulatePool(storagePool);
        //if instance and archives of pool not changed, return last task step
        if (!lastSimulatePool.isVolumePoolChanged(volume, storagePool, storageStore, simpleDatanodeManager)){

            return ;
        }

        List<InternalRebalanceTask> internalTaskList = new LinkedList<>();

        boolean doContinue = true;
        while(doContinue){

            try {
                logger.debug("try primary rebalance");
                internalTaskList.add(selector.selectPrimaryRebalanceTask());
                continue;
            } catch (NoNeedToRebalance ne) {
                logger.debug("no need for primary rebalance", ne);
            } catch (Exception e){
                logger.error("cause a exception: {}", e);
                doContinue = false;
            }

            try {
                logger.debug("try PS rebalance");
                internalTaskList.add(selector.selectPSRebalanceTask());
            } catch (NoNeedToRebalance ne) {
                logger.debug("no need for PS rebalance", ne);
                doContinue = false;
            } catch (Exception e){
                logger.error("cause a exception: {}", e);
                doContinue = false;
            }
        }

        if (internalTaskList.isEmpty()) {
            //change volume rebalance status to CHECKING, when no need to rebalance
            volumeId2RebalanceStatusMap.put(volume.getVolumeId(), VolumeRebalanceStatus.CHECKING);
            throw new NoNeedToRebalance();
        } else {
            //remove depend on task
            return selector.selectNoDependTask(internalTaskList);
        }
    }

    @Deprecated
    @Override
    public synchronized RebalanceTask selectRebalanceTask(boolean record) throws NoNeedToRebalance {
        if (justStarted()) { // we will throw NoNeedToRebalance when info center just started cause there might be some
            // data nodes or archives or segment units not reported yet.
            throw new NoNeedToRebalance();
        }
        List<StoragePool> storagePools;
        try {
            storagePools = storagePoolStore.listAllStoragePools();
        } catch (Exception e) {
            logger.error("can not list all storage pools", e);
            throw new NoNeedToRebalance();
        }
        Validate.notNull(storagePools);

        logger.debug("now to select a rebalance task if necessary, storage pools : {}", storagePools);
        TreeSet<ComparableRebalanceTask> taskSet = new TreeSet<>();
        for (StoragePool storagePool : storagePools) {
            try {
                Collection<ComparableRebalanceTask> tasks = new LinkedList<>();
//                Collection<ComparableRebalanceTask> tasks = selectInStoragePool(storagePool, record);
                logger.warn("selectInStoragePool:{}", tasks);
                for (ComparableRebalanceTask rebalanceTask : tasks) {
                    if (!rebalanceTask.getMySourceSegmentUnit().isFaked()) {
                        taskSet.add(rebalanceTask);
                    }
                }
                //} catch (NoNeedToRebalance ignored) {
            } catch(Exception e){
            }
        }
        if (taskSet.isEmpty()) {
            throw new NoNeedToRebalance();
        } else {
            ComparableRebalanceTask task = taskSet.last();
            if (record) {
                recordProcessingTask(task);
            }
            logger.warn("got a rebalance task(record:{}) : {}", record, task);
            return task;
        }
    }

    @Deprecated
    @Override
    public synchronized boolean discardRebalanceTask(long taskId) {
        Iterator<SimpleRebalanceTask> it = movingInTasks.values().iterator();
        while (it.hasNext()) {
            SimpleRebalanceTask task = it.next();
            if (task.getTaskId() == taskId) {
                it.remove();
                movingOutTasks.values().remove(task);
                logger.warn("discarding a rebalance task {}", task);
                return true;
            }
        }

        return false;
    }

    @Deprecated
    private void recordProcessingTask(SimpleRebalanceTask processingTask) {
        movingOutTasks.put(processingTask.getInstanceToMigrateFrom(), processingTask);
        movingInTasks.put(processingTask.getDestInstanceId(), processingTask);
    }

    @Override
    public Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> reserveVolume(
            long expectedSize, VolumeType volumeType, boolean isSimpleConfiguration, int segmentWrapSize,
            Long storagePoolId) throws NotEnoughGroupException_Thrift, NotEnoughSpaceException_Thrift,
            NotEnoughNormalGroupException_Thrift, TException {
        try {
            StoragePool storagePool = storagePoolStore.getStoragePool(storagePoolId);
            Validate.notNull(storagePool);
            SegmentUnitReserver reserver = new SegmentUnitReserver(segmentSize, storageStore);
            try {
                reserver.updateInstanceInfo(new SimulateInstanceBuilder(storagePool, storageStore, segmentSize).collectionInstance());
                return reserver.reserveVolume(expectedSize, volumeType, isSimpleConfiguration, segmentWrapSize,
                        simpleDatanodeManager, true);
            } catch (NotEnoughGroupException_Thrift | NotEnoughSpaceException_Thrift | NotEnoughNormalGroupException_Thrift ne) {
                reserver.updateInstanceInfo(new SimulateInstanceBuilder(storagePool, storageStore, segmentSize).collectionInstance());
                return reserver.reserveVolume(expectedSize, volumeType, isSimpleConfiguration, segmentWrapSize,
                        simpleDatanodeManager, false);
            }
        } catch (TException e) {
            logger.warn("can not reserve volume", e);
            throw e;
        } catch (Exception e) {
            logger.warn("can not reserve volume", e);
            throw new TException(e);
        }
    }
}
