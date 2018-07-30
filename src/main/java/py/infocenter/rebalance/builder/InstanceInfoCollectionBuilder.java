package py.infocenter.rebalance.builder;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.RawArchiveMetadata;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePool;
import py.infocenter.common.InfoCenterConstants;
import py.infocenter.rebalance.exception.TooManyTasksException;
import py.infocenter.rebalance.exception.VolumeCreatingException;
import py.infocenter.rebalance.struct.*;
import py.infocenter.store.StorageStore;
import py.instance.InstanceId;
import py.rebalance.RebalanceTask;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

import java.util.*;
@Deprecated
public class InstanceInfoCollectionBuilder {
    private static final Logger logger = LoggerFactory.getLogger(InstanceInfoCollectionBuilder.class);

    private final StoragePool storagePool;
    private final StorageStore storageStore;
    private final Collection<VolumeMetadata> volumes;
    private final long segmentSize;

    private final Multimap<InstanceId, SimpleSegUnitInfo> mapInstanceToSegmentUnits = Multimaps
            .synchronizedListMultimap(LinkedListMultimap.create());
    private final Map<Long, SimpleVolumeInfo> volumeMap = new HashMap<>();
    private final Map<InstanceId, InstanceMetadata> instanceMap = new HashMap<>();
    private final TreeSet<InstanceInfoImpl> instanceInfoSet = new TreeSet<>();

    private boolean validateProcessingTasksAndVolumes;
    private Multimap<InstanceId, SimpleRebalanceTask> movingOutTasks;
    private Multimap<InstanceId, SimpleRebalanceTask> movingInTasks;
    private Multimap<InstanceId, SimpleRebalanceTask> insideTasks;

    public InstanceInfoCollectionBuilder(StoragePool storagePool, StorageStore storageStore,
            Collection<VolumeMetadata> volumes, long segmentSize) {
        this.storagePool = storagePool;
        this.storageStore = storageStore;
        this.volumes = volumes;
        this.segmentSize = segmentSize;
    }

    public Set<InstanceInfoImpl> build() throws VolumeCreatingException, TooManyTasksException {

        List<InstanceMetadata> instances = storageStore.list();
        logger.info("build instanceInfoSet, storageStore.list:{} storagePool.getArchivesInDataNode:{}",
                instances, storagePool.getArchivesInDataNode());
        for (InstanceMetadata instance : instances) {
            if (instance.getDatanodeStatus() == InstanceMetadata.DatanodeStatus.OK && !storagePool
                    .getArchivesInDataNode().get(instance.getInstanceId().getId()).isEmpty()) {
                instanceMap.put(instance.getInstanceId(), instance);
            } else {
                logger.warn("a instance:{} cannot add in instanceMap", instance.getInstanceId());
            }
        }
        logger.debug("built instanceMap:{}", instanceMap);

        if (movingOutTasks == null) {
            movingOutTasks = LinkedListMultimap.create();
        }
        if (movingInTasks == null) {
            movingInTasks = LinkedListMultimap.create();
        }
        if (insideTasks == null) {
            insideTasks = LinkedListMultimap.create();
        }

        buildSegmentUnitsMap(instanceMap);
        checkProcessingTasks(instanceMap);
        buildInstanceSet(instanceMap);
        return instanceInfoSet;
    }

    public InstanceInfoCollectionBuilder setValidateProcessingTaskAndVolumes(
            boolean validateProcessingTasksAndVolumes) {
        this.validateProcessingTasksAndVolumes = validateProcessingTasksAndVolumes;
        return this;
    }

    public InstanceInfoCollectionBuilder setMovingOutTasks(Multimap<InstanceId, SimpleRebalanceTask> movingOutTasks) {
        this.movingOutTasks = movingOutTasks;
        return this;
    }

    public InstanceInfoCollectionBuilder setMovingInTasks(Multimap<InstanceId, SimpleRebalanceTask> movingInTasks) {
        this.movingInTasks = movingInTasks;
        return this;
    }

    public InstanceInfoCollectionBuilder setInsideTasks(Multimap<InstanceId, SimpleRebalanceTask> insideTasks) {
        this.insideTasks = insideTasks;
        return this;
    }

    private void buildInstanceSet(Map<InstanceId, InstanceMetadata> instances) throws TooManyTasksException {

        instanceInfoSet.clear();

        if (validateProcessingTasksAndVolumes
                && movingOutTasks.size() + insideTasks.size() > InfoCenterConstants.maxRebalanceTaskCount) {
            throw new TooManyTasksException();
        }

        for (InstanceId instanceId : instances.keySet()) {
            InstanceMetadata instance = storageStore.get(instanceId.getId());
            if (instance == null) {
                logger.warn("instanceId:{} is not in storageStore", instanceId.getId());
                continue;
            }
            long freeSpace = 0;
            int freeFlexibleSegmentUnitCount = 0;
            List<Long> archiveIds = new ArrayList<>();
            for (Long archiveId : storagePool.getArchivesInDataNode().get(instanceId.getId())) {
                RawArchiveMetadata archive = instance.getArchiveById(archiveId);
                if (archive != null) {
                    freeSpace += archive.getLogicalFreeSpace();
                    freeFlexibleSegmentUnitCount += archive.getFreeFlexibleSegmentUnitCount();
                }
                archiveIds.add(archiveId);
            }
            InstanceInfoImpl instanceInfo = new InstanceInfoImpl(instanceId, archiveIds,
                    instance.getGroup().getGroupId(), freeSpace, freeFlexibleSegmentUnitCount, segmentSize);
            for (SimpleSegUnitInfo segUnit : mapInstanceToSegmentUnits.get(instanceId)) {
                // we will ignore arbiter when doing rebalance
                if (segUnit.getStatus() != SegmentUnitStatus.Arbiter) {
                    instanceInfo.addSegmentUnit(segUnit);
                }
            }
            instanceInfoSet.add(instanceInfo);
        }
    }

    private void checkProcessingTasks(Map<InstanceId, InstanceMetadata> instances) {
        List<SimpleRebalanceTask> tasksToRemove = new ArrayList<>();
        logger.debug("processing tasks : {} \n {}", movingOutTasks.size(), movingOutTasks);
        for (SimpleRebalanceTask task : movingOutTasks.values()) {
            if (task.expired()) {
                SegId segId = task.getMySourceSegmentUnit().getSegId();
                SimpleSegmentInfo segment = volumeMap.get(segId.getVolumeId().getId()).getSegment(segId.getIndex());
                if (segment.getHighestMembership().isSecondaryCandidate(task.getDestInstanceId())) {
                    logger.debug(
                            "though the task has been expired, but the target is already secondary candidate in membership {}",
                            segment.getHighestMembership());
                    continue;
                } else {
                    logger.debug("task expired remove it {}", tasksToRemove);
                    tasksToRemove.add(task);
                    continue;
                }
            }

            SimpleSegUnitInfo mySegUnit = task.getMySourceSegmentUnit();
            if (task.getTaskType() == RebalanceTask.RebalanceTaskType.NormalRebalance
                    || task.getTaskType() == RebalanceTask.RebalanceTaskType.InsideRebalance) {

                // remove the segment unit to remove and add a segment unit on the instance to migrate to
                if (task.getTaskType() == RebalanceTask.RebalanceTaskType.InsideRebalance) {
                    if (mapInstanceToSegmentUnits.get(mySegUnit.getInstanceId()).stream().anyMatch(
                            segUnitInfo -> (segUnitInfo.getSegId().equals(mySegUnit.getSegId())
                                    && segUnitInfo.getArchiveId() != mySegUnit.getArchiveId()))) {
                        logger.debug("task done ! {}", task);
                        tasksToRemove.add(task);
                        continue;
                    }
                } else {
                    boolean oldOneRemoved = true;
                    for (SimpleSegUnitInfo segUnit : mapInstanceToSegmentUnits.get(mySegUnit.getInstanceId())) {
                        if (segUnit.getSegId().equals(mySegUnit.getSegId()) && !segUnit.isFaked()) {
                            oldOneRemoved = false;
                        }
                    }
                    if (oldOneRemoved) {
                        if (mapInstanceToSegmentUnits.get(task.getDestInstanceId()).stream().anyMatch(
                                segUnit -> segUnit.getSegId().equals(mySegUnit.getSegId()))) {
                            logger.debug("task done ! {}", task);
                            tasksToRemove.add(task);
                            continue;
                        }
                    }
                }

                SimpleVolumeInfo volume = volumeMap.get(mySegUnit.getSegId().getVolumeId().getId());
                if (volume == null) {
                    continue;
                }
                SimpleSegmentInfo segment = volume.getSegment(mySegUnit.getSegId().getIndex());
                SimpleSegUnitInfo oldSegUnit = segment.getSegUnitsMap().get(mySegUnit.getInstanceId());
                if (oldSegUnit != null) {
                    oldSegUnit.freeMySelf();
                    mapInstanceToSegmentUnits.remove(oldSegUnit.getInstanceId(), oldSegUnit);
                } else if (segment.getSegUnits().size() == volume.getVolumeType().getNumMembers()) {
                    logger.error("something wrong with the task {}", task);
                    logger.error("and the segment {}", segment);
                    logger.error("and the unit {}", mySegUnit);
                    tasksToRemove.add(task);
                    continue;
                }
                InstanceMetadata destination = instances.get(task.getDestInstanceId());
                SimpleSegUnitInfo bogusUnit;
                if (task.getTaskType() == RebalanceTask.RebalanceTaskType.InsideRebalance) {
                    bogusUnit = new SimpleSegUnitInfo(mySegUnit.getSegId(), destination.getGroup().getGroupId(),
                            volume.getStoragePoolId(), destination.getInstanceId(), task.getTargetArchiveId());
                    bogusUnit.setStatus(mySegUnit.getStatus());
                } else {
                    bogusUnit = new SimpleSegUnitInfo(mySegUnit.getSegId(), destination.getGroup().getGroupId(),
                            volume.getStoragePoolId(), destination.getInstanceId());
                }
                segment.addSegmentUnit(bogusUnit);
                mapInstanceToSegmentUnits.put(bogusUnit.getInstanceId(), bogusUnit);
            } else if (task.getTaskType() == RebalanceTask.RebalanceTaskType.PrimaryRebalance) {
                SimpleVolumeInfo volume = volumeMap.get(mySegUnit.getSegId().getVolumeId().getId());
                if (volume == null) {
                    continue;
                }
                SimpleSegmentInfo segment = volume.getSegment(mySegUnit.getSegId().getIndex());
                SimpleSegUnitInfo oldPrimary = segment.getSegUnitsMap().get(mySegUnit.getInstanceId());
                SimpleSegUnitInfo newPrimary = segment.getSegUnitsMap().get(task.getDestInstanceId());
                if (newPrimary != null) {
                    if (oldPrimary.getStatus() == SegmentUnitStatus.Secondary
                            && newPrimary.getStatus() == SegmentUnitStatus.Primary) {
                        tasksToRemove.add(task);
                    } else {
                        oldPrimary.setStatus(SegmentUnitStatus.Secondary);
                        oldPrimary.setFaked(true);
                        newPrimary.setStatus(SegmentUnitStatus.Primary);
                        newPrimary.setFaked(true);
                    }
                }
            }
        }

        for (Iterator<SimpleRebalanceTask> it = insideTasks.values().iterator(); it.hasNext(); ) {
            SimpleRebalanceTask insideTask = it.next();
            if (insideTask.expired()) {
                it.remove();
                continue;
            }
            SimpleSegUnitInfo segUnit = insideTask.getMySourceSegmentUnit();
            Collection<SimpleSegUnitInfo> segUnits = mapInstanceToSegmentUnits
                    .get(insideTask.getInstanceToMigrateFrom());
            boolean taskDone = true;
            for (SimpleSegUnitInfo unit : segUnits) {
                if (unit.getSegId() == segUnit.getSegId()) {
                    if (unit.getArchiveId() == segUnit.getArchiveId()) {
                        taskDone = false;
                    } else {
                        taskDone = true;
                        break;
                    }
                }
            }
            if (taskDone) {
                it.remove();
            }
        }

        for (SimpleRebalanceTask taskToRemove : tasksToRemove) {
            removeProcessingTask(taskToRemove);
        }
    }

    private void removeProcessingTask(SimpleRebalanceTask processingTask) {
        movingOutTasks.remove(processingTask.getInstanceToMigrateFrom(), processingTask);
        movingInTasks.remove(processingTask.getDestInstanceId(), processingTask);
    }

    private void buildSegmentUnitsMap(Map<InstanceId, InstanceMetadata> instances) throws VolumeCreatingException {
        Map<InstanceId, Integer> mapInstanceIdToGroupId = new HashMap<>();
        for (InstanceMetadata instance : instances.values()) {
            mapInstanceIdToGroupId.put(instance.getInstanceId(), instance.getGroup().getGroupId());
        }

        for (VolumeMetadata volume : volumes) {
            logger.debug("try this volume {} with storage pool {}", volume.getVolumeId(), storagePool.getPoolId());
            if (!volume.getStoragePoolId().equals(storagePool.getPoolId())) {
                logger.debug("this volume is not in the storage pool");
                continue;
            }
            if (volume.getVolumeStatus() == VolumeStatus.Deleted || volume.getVolumeStatus() == VolumeStatus.Dead
                    || volume.getVolumeStatus() == VolumeStatus.Deleting) {
                logger.debug("this volume is being deleted");
                continue;
            }
            if (volume.getVolumeStatus() == VolumeStatus.Creating || volume.getVolumeStatus() == VolumeStatus.Recycling
                    || volume.getVolumeStatus() == VolumeStatus.ToBeCreated) {
                logger.debug("this volume is being created or recycled");
                if (validateProcessingTasksAndVolumes) {
                    throw new VolumeCreatingException();
                } else {
                    continue;
                }
            }

            SimpleVolumeInfo myVolume = new SimpleVolumeInfo(volume.getVolumeId(), volume.getVolumeType(),
                    volume.getStoragePoolId());
            volumeMap.put(myVolume.getVolumeId(), myVolume);
            for (SegmentMetadata segment : volume.getSegments()) {
                SegId segId = segment.getSegId();
                SimpleSegmentInfo mySegment = new SimpleSegmentInfo(segId, segment.getLatestMembership());
                myVolume.addSegment(mySegment);
                for (SegmentUnitMetadata segmentUnit : segment.getSegmentUnits()) {
                    if (segmentUnit.getStatus() == SegmentUnitStatus.Deleted
                            || segmentUnit.getStatus() == SegmentUnitStatus.Deleting
                         // || segmentUnit.getStatus() == SegmentUnitStatus.Arbiter || segmentUnit.isArbiter()
                            ) {
                        continue;
                    }
                    InstanceId instanceId = segmentUnit.getInstanceId();
                    if (!instanceMap.containsKey(instanceId)) {
                        continue;
                    }
                    SimpleSegUnitInfo mySegmentUnit = new SimpleSegUnitInfo(segmentUnit,
                            mapInstanceIdToGroupId.get(instanceId), volume.getStoragePoolId());
                    mySegment.addSegmentUnit(mySegmentUnit);
                    mapInstanceToSegmentUnits.put(instanceId, mySegmentUnit);
                }
            }
        }
    }
}
