package py.infocenter.rebalance.struct;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegmentUnitStatus;
import py.common.struct.Pair;
import py.infocenter.rebalance.InstanceInfo;
import py.infocenter.rebalance.QuantifiableSelector;
import py.infocenter.rebalance.RebalanceConfiguration;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.rebalance.exception.NoSuitableTask;
import py.instance.InstanceId;
import py.rebalance.RebalanceTask;

import java.util.*;

@Deprecated
public class InstanceInfoImpl extends InstanceInfo {
    private static final Logger logger = LoggerFactory.getLogger(InstanceInfoImpl.class);

    private final RebalanceConfiguration config;

    private final int groupId;
    private final int diskCount;
    private final long segmentSize;
    private final List<Long> archiveIds;
    private final Multimap<Long, SimpleSegUnitInfo> mapDiskToSegmentUnits;
    private final Multimap<Long, SimpleSegUnitInfo> mapDiskToPrimaryUnits;
    private final List<SimpleRebalanceTask> unconfirmedTasks = new ArrayList<>();
    private int bogusSegmentUnitCount;
    private int primarySegmentUnitCount;
    private long freeSpace;
    private int freeFlexibleSegmentUnitCount;

    public InstanceInfoImpl(InstanceId instanceId, Collection<Long> disks, int groupId, long freeSpace,
            int freeFlexibleSegmentUnitCount, long segmentSize) {
        super(instanceId);
        this.groupId = groupId;
        this.diskCount = disks.size();
        this.archiveIds = new ArrayList<>(disks);
        this.freeSpace = freeSpace;
        this.segmentSize = segmentSize;
        this.mapDiskToSegmentUnits = LinkedListMultimap.create();
        this.mapDiskToPrimaryUnits = LinkedListMultimap.create();
        this.config = RebalanceConfiguration.getInstance();
        this.bogusSegmentUnitCount = 0;
        this.freeFlexibleSegmentUnitCount = freeFlexibleSegmentUnitCount;
    }

    public void addSegmentUnit(SimpleSegUnitInfo segmentUnit) {
        mapDiskToSegmentUnits.put(segmentUnit.getArchiveId(), segmentUnit);
        if (segmentUnit.getStatus() == SegmentUnitStatus.Primary
                || segmentUnit.getStatus() == SegmentUnitStatus.PrePrimary) {
            primarySegmentUnitCount++;
            mapDiskToPrimaryUnits.put(segmentUnit.getArchiveId(), segmentUnit);
        }
    }

    public void addABogusFlexibleSegmentUnit() {
        freeFlexibleSegmentUnitCount --;
    }

    public void addABogusSegmentUnit() {
        bogusSegmentUnitCount++;
        freeSpace -= segmentSize;
    }

    public int getSegmentUnitCount() {
        return mapDiskToSegmentUnits.size() + bogusSegmentUnitCount;
    }

    public int getDiskCount() {
        return diskCount;
    }

    @Override
    public double calculatePressure() {
        return ((double) getSegmentUnitCount()) / (diskCount + config.getPressureAddend());
    }

    public double primaryPressure() {
        return ((double) primarySegmentUnitCount) / (diskCount + config.getPressureAddend());
    }

    private ComparableRebalanceTask selectAnInsideRebalanceTask(List<Long> sourceList, List<Long> destList,
            double urgency, boolean notInTheSameSegmentWrap) throws NoSuitableTask {
        for (Long source : sourceList) {
            for (Long destination : destList) {
                for (SimpleSegUnitInfo segUnit : mapDiskToSegmentUnits.get(source)) {
                    if (canMoveTo(segUnit, destination, notInTheSameSegmentWrap)) {
                        ComparableRebalanceTask task = new ComparableRebalanceTask(segUnit, instanceId,
                                config.getRebalanceTaskExpireTimeSeconds(), urgency,
                                RebalanceTask.RebalanceTaskType.InsideRebalance);
                        task.setTargetArchiveId(destination);
                        return task;
                    }
                }
            }
        }
        throw new NoSuitableTask();
    }

    public ComparableRebalanceTask selectAnInsideRebalanceTask() throws NoNeedToRebalance {

        for (SimpleSegUnitInfo segUnitInfo : mapDiskToSegmentUnits.values()) {
            if (segUnitInfo.isFaked() && !segUnitInfo.hasArchiveId()) {
                logger.debug("segment units moving in {}", this);
                throw new NoNeedToRebalance();
            }
        }

        Multiset<Long> diskMultiSet = mapDiskToSegmentUnits.keys();
        LinkedList<Long> diskList = new LinkedList<>(archiveIds);
        diskList.sort(Comparator.comparingInt(diskMultiSet::count));

        QuantifiableSelector<Long> diskSelector = new QuantifiableSelector<>(diskMultiSet::count);
        Pair<Pair<Long, Double>, Pair<Long, Double>> minAndMax = diskSelector
                .selectTheMinAndMax(diskList, config.getPressureThreshold());

        boolean noSuitableTaskInTheMax = false;
        boolean noSuitableTaskInTheMin = false;

        if (minAndMax.getFirst() != null) {
            Pair<Long, Double> min = minAndMax.getFirst();
            long diskWithLeastSegUnits = min.getFirst();
            try {
                return selectAnInsideRebalanceTask(diskList, Collections.singletonList(diskWithLeastSegUnits),
                        min.getSecond(), true);
            } catch (NoSuitableTask e) {
                try {
                    return selectAnInsideRebalanceTask(diskList, Collections.singletonList(diskWithLeastSegUnits),
                            min.getSecond(), false);
                } catch (NoSuitableTask eAgain) {
                    noSuitableTaskInTheMin = true;
                }
            }
        }

        if (minAndMax.getSecond() != null) {
            Pair<Long, Double> max = minAndMax.getSecond();
            long diskWithMostSegUnits = max.getFirst();

            Collections.reverse(diskList);
            try {
                return selectAnInsideRebalanceTask(Collections.singletonList(diskWithMostSegUnits), diskList,
                        max.getSecond(), true);
            } catch (NoSuitableTask e) {
                try {
                    return selectAnInsideRebalanceTask(Collections.singletonList(diskWithMostSegUnits), diskList,
                            max.getSecond(), false);
                } catch (NoSuitableTask eAgain) {
                    noSuitableTaskInTheMax = true;
                }
            }
        }

        if (noSuitableTaskInTheMin) {
            long diskWithLeastSegUnits = minAndMax.getFirst().getFirst();
            archiveIds.remove(diskWithLeastSegUnits);
            try {
                return selectAnInsideRebalanceTask();
            } catch (NoNeedToRebalance ignore) {
            } finally {
                archiveIds.add(diskWithLeastSegUnits);
            }
        }

        if (noSuitableTaskInTheMax) {
            long diskWithMostSegUnits = minAndMax.getSecond().getFirst();
            archiveIds.remove(diskWithMostSegUnits);
            try {
                return selectAnInsideRebalanceTask();
            } catch (NoNeedToRebalance ignore) {
            } finally {
                archiveIds.add(diskWithMostSegUnits);
            }
        }

        throw new NoNeedToRebalance();
    }

    private boolean canMoveTo(SimpleSegUnitInfo segUnitInfo, Long destDisk, boolean notInTheSameSegmentWrap) {
        Multiset<Long> disks = mapDiskToSegmentUnits.keys();
        int sourceCount = disks.count(segUnitInfo.getArchiveId());
        int destCount = disks.count(destDisk);
        if (sourceCount - destCount <= 1) {
            return false;
        }
        if (notInTheSameSegmentWrap) {
            for (SimpleSegUnitInfo segUnitOnDest : mapDiskToSegmentUnits.get(destDisk)) {
                if (segUnitInfo.getSegId().getIndex() / config.getSegmentWrapSize()
                        == segUnitOnDest.getSegId().getIndex() / config.getSegmentWrapSize()) {
                    return false;
                }
            }
        }
        return true;
    }

    public RebalanceTask selectAPrimaryRebalanceTask(Collection<InstanceInfo> destinations) throws NoSuitableTask {
        LinkedList<Long> diskListInOrderOfPrimaryCount = new LinkedList<>();
        diskListInOrderOfPrimaryCount.addAll(mapDiskToSegmentUnits.keySet());
        diskListInOrderOfPrimaryCount.sort(Comparator.comparingInt(o -> mapDiskToPrimaryUnits.get(o).size()));
        for (InstanceInfo destination : destinations) {
            if (conflictWith((InstanceInfoImpl) destination, RebalanceTask.RebalanceTaskType.PrimaryRebalance)) {
                logger.debug("{} conflict with {}", this, destination);
                continue;
            }
            Iterator<Long> it = diskListInOrderOfPrimaryCount.descendingIterator();
            while (it.hasNext()) {
                Long disk = it.next();
                for (SimpleSegUnitInfo segUnitInfo : mapDiskToPrimaryUnits.get(disk)) {
                    if (segUnitInfo.getSegment().getHighestMembership().isSecondary(destination.getInstanceId())) {
                        logger.debug("suitable task got from {} to {}", this, destination);
                        return new SimpleRebalanceTask(segUnitInfo, destination.getInstanceId(),
                                config.getRebalanceTaskExpireTimeSeconds(),
                                RebalanceTask.RebalanceTaskType.PrimaryRebalance);
                    }
                }
            }
        }
        throw new NoSuitableTask();
    }

    @Override
    public RebalanceTask selectARebalanceTask(Collection<InstanceInfo> destinations,
            RebalanceTask.RebalanceTaskType taskType) throws NoSuitableTask {
        if (taskType == RebalanceTask.RebalanceTaskType.NormalRebalance) {
            return selectANormalRebalanceTask(destinations);
        } else if (taskType == RebalanceTask.RebalanceTaskType.PrimaryRebalance) {
            return selectAPrimaryRebalanceTask(destinations);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private SimpleRebalanceTask selectANormalRebalanceTask(Collection<InstanceInfo> destinations)
            throws NoSuitableTask {
        LinkedList<Long> diskListInOrderOfSegmentUnitCount = new LinkedList<>();
        Multiset<Long> disks = mapDiskToSegmentUnits.keys();
        for (long disk : mapDiskToSegmentUnits.keySet()) {
            if (diskListInOrderOfSegmentUnitCount.isEmpty()) {
                diskListInOrderOfSegmentUnitCount.addLast(disk);
            } else {
                ListIterator<Long> it = diskListInOrderOfSegmentUnitCount.listIterator();
                while (it.hasNext()) {
                    long diskInOrder = it.next();
                    if (disks.count(disk) > disks.count(diskInOrder)) {
                        if (it.hasPrevious()) {
                            it.previous();
                            it.add(disk);
                        } else {
                            diskListInOrderOfSegmentUnitCount.addFirst(disk);
                        }
                        break;
                    }
                    if (!it.hasNext()) {
                        diskListInOrderOfSegmentUnitCount.addLast(disk);
                        break;
                    }
                }
            }
        }

        for (InstanceInfo destination : destinations) {
            if (conflictWith((InstanceInfoImpl) destination, RebalanceTask.RebalanceTaskType.NormalRebalance)) {
                logger.debug("{} conflict with {}", this, destination);
                continue;
            }
            long freeSpace = destination.getFreeSpace();
            if (freeSpace < segmentSize) {
                logger.debug("no enough space(only {} left) on the destination {}", freeSpace, destination);
                continue;
            }
            for (Long disk : diskListInOrderOfSegmentUnitCount) {
                logger.debug("checking if disk {} has available segment units", disk);
                for (SimpleSegUnitInfo segmentUnit : mapDiskToSegmentUnits.get(disk)) {
                    if (segmentUnit.canBeMovedTo(destination)) {
                        return new SimpleRebalanceTask(segmentUnit, destination.getInstanceId(),
                                config.getRebalanceTaskExpireTimeSeconds(),
                                RebalanceTask.RebalanceTaskType.NormalRebalance);
                    }
                }
                logger.debug("no available segment unit in {}, try next one", disk);
            }
            logger.debug("no available segment unit for destination {}, try next one", destination);
        }
        logger.debug("no available segment unit in {} for destinations {}", instanceId, destinations);
        throw new NoSuitableTask();
    }

    private boolean conflictWith(InstanceInfoImpl another, RebalanceTask.RebalanceTaskType taskType) {
        double val1;
        double val2;
        if (taskType == RebalanceTask.RebalanceTaskType.NormalRebalance) {
            val1 = calculatePressure() - another.calculatePressure();
            this.bogusSegmentUnitCount--;
            another.bogusSegmentUnitCount++;

            val2 = calculatePressure() - another.calculatePressure();
            this.bogusSegmentUnitCount++;
            another.bogusSegmentUnitCount--;
        } else if (taskType == RebalanceTask.RebalanceTaskType.PrimaryRebalance) {
            val1 = primaryPressure() - another.primaryPressure();
            this.primarySegmentUnitCount--;
            another.primarySegmentUnitCount++;

            val2 = primaryPressure() - another.primaryPressure();
            this.primarySegmentUnitCount++;
            another.primarySegmentUnitCount--;
        } else {
            throw new IllegalArgumentException();
        }

        return val1 <= 0 || val1 + val2 <= 0;

    }

    @Override
    public int getFreeFlexibleSegmentUnitCount() {
        return freeFlexibleSegmentUnitCount;
    }

    @Override
    public long getFreeSpace() {
        return freeSpace;
    }

    @Override
    public int getGroupId() {
        return groupId;
    }

    @Override
    public String toString() {
        return "InstanceInfoImpl(" + instanceId + " g" + groupId + ")" + ", pressure=" + calculatePressure()
                + ", primaryPressure=" + primaryPressure() + ", diskCount=" + diskCount + ", freeSpace=" + freeSpace
                + ", primaryCount=" + primarySegmentUnitCount + ", mapDiskToSegmentUnits=" + mapDiskToSegmentUnits;
    }

}
