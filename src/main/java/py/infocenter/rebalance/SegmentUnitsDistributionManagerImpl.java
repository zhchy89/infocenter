package py.infocenter.rebalance;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePool;
import py.icshare.StoragePoolStore;
import py.infocenter.rebalance.builder.InstanceInfoCollectionBuilder;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.rebalance.exception.TooManyTasksException;
import py.infocenter.rebalance.exception.VolumeCreatingException;
import py.infocenter.rebalance.selector.RebalanceSelector;
import py.infocenter.rebalance.selector.SegmentUnitReserver;
import py.infocenter.rebalance.struct.ComparableRebalanceTask;
import py.infocenter.rebalance.struct.SimpleDatanodeManager;
import py.infocenter.rebalance.struct.SimpleRebalanceTask;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceId;
import py.rebalance.RebalanceTask;
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

    private final Multimap<InstanceId, SimpleRebalanceTask> movingOutTasks;
    private final Multimap<InstanceId, SimpleRebalanceTask> movingInTasks;
    private final Multimap<InstanceId, SimpleRebalanceTask> insideTasks;

    //update at reportArchives, when any datanode's type is SIMPLE,
    // isArbiterGroupSet is set to true,
    // and put this datanode's group id into arbiterGroupIds;
    private SimpleDatanodeManager simpleDatanodeManager;

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
                Collection<ComparableRebalanceTask> tasks = selectInStoragePool(storagePool, record);
                logger.warn("selectInStoragePool:{}", tasks);
                for (ComparableRebalanceTask rebalanceTask : tasks) {
                    if (!rebalanceTask.getMySourceSegmentUnit().isFaked()) {
                        taskSet.add(rebalanceTask);
                    }
                }
            } catch (NoNeedToRebalance ignored) {
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

    private void recordProcessingTask(SimpleRebalanceTask processingTask) {
        movingOutTasks.put(processingTask.getInstanceToMigrateFrom(), processingTask);
        movingInTasks.put(processingTask.getDestInstanceId(), processingTask);
    }

    private ComparableRebalanceTask selectForVolume(StoragePool storagePool, VolumeMetadata volume, boolean record)
            throws NoNeedToRebalance, VolumeCreatingException, TooManyTasksException {
        RebalanceSelector selector;
        if (record) {
            selector = new RebalanceSelector(
                    new InstanceInfoCollectionBuilder(storagePool, storageStore, Collections.singletonList(volume),
                            segmentSize).setMovingInTasks(movingInTasks).setMovingOutTasks(movingOutTasks)
                                        .setInsideTasks(insideTasks).setValidateProcessingTaskAndVolumes(true).build());
        } else {
            selector = new RebalanceSelector(
                    new InstanceInfoCollectionBuilder(storagePool, storageStore, Collections.singletonList(volume),
                            segmentSize).setValidateProcessingTaskAndVolumes(false).build());
        }
        ComparableRebalanceTask task = null;
        try {
            logger.debug("try primary rebalance");
            task = selector.selectPrimaryRebalanceTask();
        } catch (NoNeedToRebalance ne) {
            logger.debug("no need for primary rebalance", ne);
        }

        if (task == null) {
            try {
                logger.debug("try normal rebalance");
                task = selector.selectNormalRebalanceTask();
            } catch (NoNeedToRebalance ne) {
                logger.debug("no need for rebalance between instances", ne);
            }
        }

        if (task == null) { // disable inside rebalance for now
//            try {
//                logger.debug("try inside rebalance");
//                task = selector.selectRebalanceTaskInsideInstance();
//            } catch (NoNeedToRebalance ne) {
//                logger.debug("no need for inside rebalance", ne);
//            }
        }

        if (task == null) {
            throw new NoNeedToRebalance();
        } else {
            return task;
        }
    }

    private Collection<ComparableRebalanceTask> selectInStoragePool(StoragePool storagePool, boolean record)
            throws NoNeedToRebalance {
        TreeSet<ComparableRebalanceTask> tasks = new TreeSet<>();
        List<VolumeMetadata> volumes = volumeStore.listVolumes();
        Collections.shuffle(volumes);
        for (VolumeMetadata volume : volumes) {
            if (volume.getStoragePoolId() == (long) storagePool.getPoolId()) {
                try {
                    tasks.add(selectForVolume(storagePool, volume, record));
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
        if (tasks.isEmpty()) {
            throw new NoNeedToRebalance();
        } else {
            return tasks;
        }
    }

    @Override
    public Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> reserveVolume(
            long expectedSize, VolumeType volumeType, boolean isSimpleConfiguration, int segmentWrapSize,
            Long storagePoolId) throws NotEnoughGroupException_Thrift, NotEnoughSpaceException_Thrift,
            NotEnoughNormalGroupException_Thrift, TException {
        SegmentUnitReserver reserver = new SegmentUnitReserver(segmentSize, storageStore);
        try {
            StoragePool storagePool = storagePoolStore.getStoragePool(storagePoolId);
            Validate.notNull(storagePool);
            reserver.updateInstanceInfo(
                    new InstanceInfoCollectionBuilder(storagePool, storageStore, new ArrayList<>(), segmentSize)
                            .setValidateProcessingTaskAndVolumes(false).build());
            try {
                return reserver.reserveVolume(expectedSize, volumeType, isSimpleConfiguration, segmentWrapSize,
                        simpleDatanodeManager, true);
            } catch (NotEnoughGroupException_Thrift | NotEnoughSpaceException_Thrift | NotEnoughNormalGroupException_Thrift ne) {
                reserver.updateInstanceInfo(
                        new InstanceInfoCollectionBuilder(storagePool, storageStore, new ArrayList<>(), segmentSize)
                                .setValidateProcessingTaskAndVolumes(false).build());
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
