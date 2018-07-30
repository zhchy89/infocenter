package py.infocenter.rebalance.selector;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.common.struct.Pair;
import py.icshare.InstanceMetadata;
import py.infocenter.rebalance.RebalanceConfiguration;
import py.infocenter.rebalance.builder.SimulateInstanceBuilder;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.rebalance.struct.*;
import py.instance.InstanceId;
import py.rebalance.RebalanceTask;
import py.thrift.infocenter.service.SegmentNotFoundException_Thrift;
import py.thrift.share.InstanceNotExistsException_Thrift;
import py.volume.VolumeMetadata;

import java.util.*;

/**
 * select rebalance task by current pool environment and volume segment unit distribution
 */
public class VolumeRebalanceSelector {
    private static final Logger logger = LoggerFactory.getLogger(RebalanceSelector.class);

    private final RebalanceConfiguration config = RebalanceConfiguration.getInstance();
    private final LinkedList<SimulateInstanceInfo> simulateInstanceInfoList;    //all instance
    private final Map<Long, InstanceMetadata> instanceId2InstanceMetadataMap = new HashMap<>(); //all instance
    private final Set<Long> allSimpleDatanodeIdSet;     //all simple datanode
    private VolumeMetadata realVolume;      //volume information
    private SimulateVolume simulateVolume;  //simple volume's information
    private SimulateInstanceBuilder simulateInstanceBuilder;

    public VolumeRebalanceSelector(SimulateInstanceBuilder simulateInstanceBuilder, VolumeMetadata volumeMetadata,
                                   List<InstanceMetadata> instanceMetadataList, Set<Long> allSimpleDatanodeIdSet) {
        this.simulateInstanceInfoList = new LinkedList<>(simulateInstanceBuilder.getInstanceId2SimulateInstanceMap().values());
        Collections.sort(this.simulateInstanceInfoList);
        for (InstanceMetadata instanceMetadata : instanceMetadataList){
            instanceId2InstanceMetadataMap.put(instanceMetadata.getInstanceId().getId(), instanceMetadata);
        }
        this.allSimpleDatanodeIdSet = new HashSet<>(allSimpleDatanodeIdSet);

        this.realVolume = volumeMetadata;
        this.simulateVolume = new SimulateVolume(volumeMetadata);
        config.setSegmentWrapSize(volumeMetadata.getSegmentWrappCount());

        removeNotUsedInstance();
    }

    /**
     * Remove all nodes that cannot be used to P/S forever
     *
     * these nodes are:
     * 1. all simple datanode
     * 2. nodes in mix group, when (mix group + simple group <= volume arbiter count(exclude candidate arbiter))
     */
    private void removeNotUsedInstance(){
        //get mix group and simple group
        Set<Long> normalIdSet = new HashSet<>();    //all normal data node in pool
        Set<Long> simpleIdSet = new HashSet<>();    //all simple data node in pool
        Set<Integer> normalGroupIdSet = new HashSet<>();    //normal datanode Group Id
        Set<Integer> simpleGroupIdSet = new HashSet<>();    //all simple group in pool
        Set<Integer> allGroupSet = new HashSet<>(); //all data node group

        long domainId = 0;
        //get instance Info in pool
        for (SimulateInstanceInfo instanceInfo : simulateInstanceInfoList){
            long instanceId = instanceInfo.getInstanceId().getId();
            InstanceMetadata instance = instanceId2InstanceMetadataMap.get(instanceId);

            domainId = instance.getDomainId();

            if (instance.getDatanodeType() == InstanceMetadata.DatanodeType.NORMAL){
                normalIdSet.add(instanceId);
                normalGroupIdSet.add(instance.getGroup().getGroupId());
            }

            allGroupSet.add(instance.getGroup().getGroupId());
        }

        //simpleDatanode may be have no archives, so it cannot add to pool
        //we will create arbiter at all simpleDatanode which be owned domain
        for (long simpleDatanodeInstanceId : allSimpleDatanodeIdSet) {
            InstanceMetadata instance = instanceId2InstanceMetadataMap.get(simpleDatanodeInstanceId);

            if (instance.getDomainId() == domainId) {
                simpleIdSet.add(simpleDatanodeInstanceId);
                simpleGroupIdSet.add(instance.getGroup().getGroupId());
                allGroupSet.add(instance.getGroup().getGroupId());
            }
        }

        Set<Integer> mixGroupIdSet = new HashSet<>(simpleGroupIdSet);    //mix Group id set
        mixGroupIdSet.retainAll(normalGroupIdSet);
        simpleGroupIdSet.retainAll(mixGroupIdSet);
        normalGroupIdSet.retainAll(mixGroupIdSet);

        /*
         * remove all normal datanode of mix group,
         * when (mix group + simple group <= volume arbiter count(exclude candidate arbiter))
         */
        if (mixGroupIdSet.size() + simpleGroupIdSet.size() <= simulateVolume.getVolumeType().getNumArbiters()){
            simulateInstanceInfoList.removeIf(instanceInfo -> mixGroupIdSet.contains(instanceInfo.getGroupId()));
        }

        /*
         * remove simple datanode
         */
        simulateInstanceInfoList.removeIf(instanceInfo -> {
            InstanceMetadata instanceMetadata = instanceId2InstanceMetadataMap.get(instanceInfo.getInstanceId().getId());
            if ((instanceMetadata == null) || (instanceMetadata.getDatanodeType() == InstanceMetadata.DatanodeType.SIMPLE)) {
                return true;
            }
            return false;
        });
    }

    /**
     * check is primary balance?
     * P分布最大值-最小值 <= 1，返回false
     * P分布最大值/平均值 > 1+阈值, 返回true
     * P分布最小值/平均值 < 1-阈值, 返回true
     * @param primaryCounter
     * @return
     */
    private boolean isPrimaryNeedToDoRebalance(ObjectCounter<Long> primaryCounter) throws NoNeedToRebalance {
        //if PMax - PMin <= 1, mean P is rebalance
        if (primaryCounter.maxValue() - primaryCounter.minValue() <= 1){
            logger.debug("primary distribution:{} max-min <= 1, no need to do rebalance", primaryCounter);
            throw new NoNeedToRebalance();
        }

        //


        /*
         * 最大值与平均值的差额超过指定阈值
         */


        /*
         * 最小值与平均值的差额超过指定阈值
         */
            return false;
        }

    private boolean isPSNeedToDoRebalance() {
        //normal segment unit counter of wrapper index in volume (used for check overload)
        Map<Long, ObjectCounter<Long>> wrapperIndex2NormalCounterMap = new HashMap<>();
        //secondary combinations of primary in volume <primary, secondary combinations>
        Map<Long, ObjectCounter<Long>> primary2SecondaryCounterMap = new HashMap<>();

        //get instance info for search instance
        Map<Long, SimulateInstanceInfo> instanceId2SimulateInstanceMap = new HashMap<>();
        for (SimulateInstanceInfo simulateInstanceInfo : simulateInstanceInfoList){
            instanceId2SimulateInstanceMap.put(simulateInstanceInfo.getInstanceId().getId(), simulateInstanceInfo);
        }

        //datanode traversal
        for (SimulateInstanceInfo simulateInstanceInfo : simulateInstanceInfoList){
            long instanceId = simulateInstanceInfo.getInstanceId().getId();

            ObjectCounter<Long> secondaryOfPrimaryCounter = primary2SecondaryCounterMap.get(instanceId);
            if (secondaryOfPrimaryCounter == null){
                continue;
            }

            //get min and max
            SimulateInstanceInfo maxInstance = instanceId2SimulateInstanceMap.get(secondaryOfPrimaryCounter.max());
            Pair<SimulateInstanceInfo, Double> max = new Pair<>(maxInstance, (double)secondaryOfPrimaryCounter.maxValue());

            SimulateInstanceInfo minInstance = instanceId2SimulateInstanceMap.get(secondaryOfPrimaryCounter.min());
            Pair<SimulateInstanceInfo, Double> min = new Pair<>(minInstance, (double)secondaryOfPrimaryCounter.minValue());

            //获取去除最大最小值之外的平均值

            //判断最大值最小值是否有超过指定阈值

            //如果最大值超过指定阈值，设置最大值结点作为src列表，设置其他结点作为destination列表

            //如果最小值超过指定阈值, 设置最小值结点作为destination列表，设置其他结点作为src列表

            //根据src和destination列表，选择secondary的迁移目标

            //所有结点均没有找到task，抛NoNeedToRebalance异常




            //        SimpleRebalanceTask rebalanceTask = (SimpleRebalanceTask) source.selectARebalanceTask(destinations, taskType);
//        return new ComparableRebalanceTask(rebalanceTask.getMySourceSegmentUnit(),
//                rebalanceTask.getDestInstanceId(), config.getRebalanceTaskExpireTimeSeconds(), max.getSecond(),
//                taskType);

            return true;
        }

        return false;
    }

    /**
     * check volume is need to do rebalance by P,S distribution
     *
     * @return true if need to do rebalance
     * @throws SegmentNotFoundException_Thrift  if not found segment in volume
     */
    public boolean isNeedToDoRebalance() throws SegmentNotFoundException_Thrift, NoNeedToRebalance {

        ObjectCounter<Long> primaryCounter = new TreeSetObjectCounter<>();      //primary combination
        ObjectCounter<Long> secondaryCounter = new TreeSetObjectCounter<>();    //secondary combination
        //normal segment unit counter of wrapper index in volume (used for check overload)
        Map<Long, ObjectCounter<Long>> wrapperIndex2NormalCounterMap = new HashMap<>();
        //secondary combinations of primary in volume <primary, secondary combinations>
        Map<Long, ObjectCounter<Long>> primary2SecondaryCounterMap = new HashMap<>();

        //parse volume metadata, and get distribute information
        parseVolumeInfo(primaryCounter, secondaryCounter, wrapperIndex2NormalCounterMap, primary2SecondaryCounterMap);

        /*
         * is primary need to do rebalance
         */
        if (isPrimaryNeedToDoRebalance(primaryCounter)){
            return true;
        }

        /*
         * is PS combinations distribute need to do rebalance
         */
        return isPSNeedToDoRebalance();
    }

    /**
     * select primary rebalance task
     *
     * 1. when primary distribute max count in datanode - min count > 1, that mean can do rebalance
     * 2. primary must migrate to secondary, so max count datanode must has P and min count datanode must has S in same segment
     *
     * @return task step
     * @throws NoNeedToRebalance Either phase 1 and phase 2 are false, will be caused this exception
     * @throws SegmentNotFoundException_Thrift  segment not found in volume
     */
    public InternalRebalanceTask selectPrimaryRebalanceTask() throws NoNeedToRebalance, SegmentNotFoundException_Thrift {
        if (simulateInstanceInfoList.isEmpty()) {
            throw new NoNeedToRebalance();
        }

        //get primary distribute count
        ObjectCounter<Long> primaryCounter = new TreeSetObjectCounter<>();

        //parse volume info to get wrapperIndex2NormalCounterMap and primary2SecondaryCounterMap
        parseVolumeInfo(primaryCounter, null, null, null);

        //PMax - PMin > 1 means need migrate
        long primaryMaxValue = primaryCounter.maxValue();
        long primaryMinValue = primaryCounter.minValue();
        if (primaryMaxValue - primaryMinValue <= 1){
            logger.info("Primary distribute very balance, no need to rebalance. primaryCount:{}", primaryCounter);
            throw new NoNeedToRebalance();
        }

        //如果最大最小是同一个group

        //get migrate object by memberships
        Map<Integer, SimulateSegment> segIndex2SimulateSegmentMap = simulateVolume.getSegIndex2SimulateSegmentMap();
        List<Integer> segIndexSet = new LinkedList<>(segIndex2SimulateSegmentMap.keySet());
        Collections.sort(segIndexSet);
        for (int segmentIndex : segIndexSet) {
            SimulateSegment simulateSegment = segIndex2SimulateSegmentMap.get(segmentIndex);

            //SegmentMembership segmentMembership = volumeMetadata.getMemberships().get(segmentIndex).getLast();

            long primaryId = simulateSegment.getPrimaryId().getId();
            if (primaryCounter.get(primaryId) != primaryMaxValue){
                continue;
            }

            Set<InstanceId> secondarySet = simulateSegment.getSecondaryIdSet();
            for (InstanceId secondaryIdObj : secondarySet){
                if (primaryCounter.get(secondaryIdObj.getId()) == primaryMinValue){
                    InternalRebalanceTask task = new InternalRebalanceTask(simulateSegment.getSegId(),
                            simulateSegment.getPrimaryId(), secondaryIdObj, 0L, RebalanceTask.RebalanceTaskType.PrimaryRebalance);

                    virtualMigrate(task);
                    return task;
                }
            }


        }
        logger.info("Primary distribute not balance, but not find suitable step to rebalance. primaryCount:{}", primaryCounter);
        throw new NoNeedToRebalance();
    }

    /**
     * rebalance PS combination, ensure a datanode down, S to be P in all datanode is balance
     * @return a rebalance task
     * @throws NoNeedToRebalance if PS combination already balance, it means no need to rebalance, then throw exception
     * @throws SegmentNotFoundException_Thrift when infocenter just start, volumeMetadata is not null because it load from DB,
     *      but segmentTable may be null because it load after datanode report
     */
    public InternalRebalanceTask selectPSRebalanceTask() throws NoNeedToRebalance, SegmentNotFoundException_Thrift, InstanceNotExistsException_Thrift {
        if (simulateInstanceInfoList.isEmpty()) {
            throw new NoNeedToRebalance();
        }

        //get secondary distribute count
        ObjectCounter<Long> secondaryCounter = new TreeSetObjectCounter<>();
        //normal segment unit counter of wrapper index in volume (used for check overload)
        Map<Long, ObjectCounter<Long>> wrapperIndex2NormalCounterMap = new HashMap<>();
        //secondary combinations of primary in volume <primary, secondary combinations>
        Map<Long, ObjectCounter<Long>> primary2SecondaryCounterMap = new HashMap<>();

        //parse volume info to get wrapperIndex2NormalCounterMap and primary2SecondaryCounterMap
        parseVolumeInfo(null, secondaryCounter, wrapperIndex2NormalCounterMap, primary2SecondaryCounterMap);

        //datanode traversal
        for (long primaryId : primary2SecondaryCounterMap.keySet()){
            ObjectCounter<Long> secondaryOfPrimaryCounter = primary2SecondaryCounterMap.get(primaryId);
            if (secondaryOfPrimaryCounter == null){
                continue;
            }

            //get min and max
            //SMax - SMin > 1 means need migrate
            long secondaryMaxValue = secondaryOfPrimaryCounter.maxValue();
            long secondaryMinValue = secondaryOfPrimaryCounter.minValue();
            if (secondaryMaxValue - secondaryMinValue <= 1){
                logger.info("Secondary distribute very balance when primary is ({}), no need to rebalance. primaryCount:{}",
                        primaryId, secondaryOfPrimaryCounter);
                continue;
            }

            //如果最大值和最小值是同一个group


            //获取max secondary的结点
            //按secondary总分布个数排序选择
            //如果总排序相同，则选取secondaryOfPrimaryCounter最前面的那个，不能随机
            InstanceId maxSecondaryId = null;
            InstanceId minSecondaryId = null;
            Iterator<Long> secondaryOfPrimaryCounterIt = secondaryOfPrimaryCounter.iterator();
            long maxCount = -1;
            long minCount = secondaryCounter.maxValue()+1;
            while (secondaryOfPrimaryCounterIt.hasNext()){
                long insId = secondaryOfPrimaryCounterIt.next();
                if (secondaryOfPrimaryCounter.get(insId) == secondaryMaxValue){
                    long currentCount = secondaryCounter.get(insId);
                    if (currentCount > maxCount){
                        maxCount = currentCount;
                        maxSecondaryId = instanceId2InstanceMetadataMap.get(insId).getInstanceId();
                    }
                } else if (secondaryOfPrimaryCounter.get(insId) == secondaryMinValue){
                    long currentCount = secondaryCounter.get(insId);
                    if (currentCount < minCount){
                        minCount = currentCount;
                        minSecondaryId = instanceId2InstanceMetadataMap.get(insId).getInstanceId();
                    }
                }
            }

            //get migrate object by memberships
            Map<Integer, SimulateSegment> segIndex2SimulateSegmentMap = simulateVolume.getSegIndex2SimulateSegmentMap();
            List<Integer> segmentIndexSet = new LinkedList<>(segIndex2SimulateSegmentMap.keySet());
            Collections.sort(segmentIndexSet);
            for (int segmentIndex : segmentIndexSet) {
                SimulateSegment simulateSegment = segIndex2SimulateSegmentMap.get(segmentIndex);
                //SegmentMembership segmentMembership = volumeMetadata.getMemberships().get(segmentIndex).getLast();

                //find segment that primary is primaryId
                if (simulateSegment.getPrimaryId().getId() != primaryId) {
                    continue;
                }

                Set<InstanceId> secondarySet = simulateSegment.getSecondaryIdSet();
                if (secondarySet.contains(maxSecondaryId) && !secondarySet.contains(minSecondaryId)){
                    InternalRebalanceTask task = new InternalRebalanceTask(simulateSegment.getSegId(),
                            maxSecondaryId, minSecondaryId, 0L, RebalanceTask.RebalanceTaskType.PSRebalance);

                    virtualMigrate(task);
                    return task;
                }
            }
        }

        logger.info("Secondary of primary distribute not balance, but not find suitable step to rebalance.");
        throw new NoNeedToRebalance();
    }

    /**
     * virtual migrate segment unit by task
     * @param task  migrate method
     */
    private void virtualMigrate(InternalRebalanceTask task){
        SegId segId = task.getSegmentId();
        InstanceId source = task.getSrcInstanceId();
        InstanceId destination = task.getDestInstanceId();

        if (task.getTaskType() == RebalanceTask.RebalanceTaskType.PrimaryRebalance){
            SimulateSegment segment = simulateVolume.getSegIndex2SimulateSegmentMap().get(segId.getIndex());
            segment.migratePrimary(source, destination);
        } else if (task.getTaskType() == RebalanceTask.RebalanceTaskType.PSRebalance){
            SimulateSegment segment = simulateVolume.getSegIndex2SimulateSegmentMap().get(segId.getIndex());
            segment.migrateSecondary(source, destination);
        }
    }

    /**
     * select task no depend tasks from task list
     *
     * traversal task list from front to back
     * if segment index is exist in task before and src instance id or dest instance id is exist in task before,
     * we think task is dependent on the previous task
     *
     * @param srcTaskList   source task list
     * @return  no depend tasks
     */
    public List<RebalanceTask> selectNoDependTask(List<InternalRebalanceTask> srcTaskList){
        List<RebalanceTask> taskList = new LinkedList<>();

        Multimap<Integer, Long> segIndex2MigrateIdMap = HashMultimap.create();
        for (InternalRebalanceTask task : srcTaskList){
            SegId segId = task.getSegmentId();
            InstanceId srcId = task.getSrcInstanceId();
            InstanceId destId = task.getDestInstanceId();

            Collection<Long> migrateIdCollection = segIndex2MigrateIdMap.get(segId.getIndex());
            //if segment index is exists in task before
            if (migrateIdCollection != null){
                Set<Long> allMigrateIdSet = new HashSet<>(migrateIdCollection);

                //if src instance id or dest instance id is exist in task before
                if (allMigrateIdSet.contains(srcId.getId()) || allMigrateIdSet.contains(destId.getId())){
                    continue;
                }
            }

            segIndex2MigrateIdMap.put(segId.getIndex(), srcId.getId());
            segIndex2MigrateIdMap.put(segId.getIndex(), destId.getId());

            RebalanceTask rebalanceTask = new RebalanceTask(
                    realVolume.getSegmentByIndex(segId.getIndex()).getSegmentUnitMetadata(srcId),
                    destId, config.getRebalanceTaskExpireTimeSeconds(), task.getTaskType());

            taskList.add(rebalanceTask);
        }
        return taskList;
    }
    /**
     * parse volume metadata, then get wrapperIndex2NormalCounterMap and primary2SecondaryCounterMap
     * @param primaryCounter    O    get primary segment unit counter in volume if primaryCounter not null
     * @param secondaryCounter    O    get secondary segment unit counter in volume if secondaryCounter not null
     * @param wrapperIndex2NormalCounterMap    O    get normal segment unit counter of wrapper index in volume if wrapperIndex2NormalCounterMap not null(used for check overload)
     * @param primary2SecondaryCounterMap   O   get secondary combinations of primary in volume if primary2SecondaryCounterMap not null <primary, secondary combinations>
     * @throws SegmentNotFoundException_Thrift when infocenter just start, volumeMetadata is not null because it load from DB,
     *      but segmentTable may be null because it load after datanode report
     */
    private void parseVolumeInfo(ObjectCounter<Long> primaryCounter, ObjectCounter<Long> secondaryCounter,
                                 Map<Long, ObjectCounter<Long>> wrapperIndex2NormalCounterMap,
                                 Map<Long, ObjectCounter<Long>> primary2SecondaryCounterMap)
            throws SegmentNotFoundException_Thrift {
        Map<Integer, SimulateSegment> segIndex2SimulateSegmentMap = simulateVolume.getSegIndex2SimulateSegmentMap();

        // when infocenter just start, volumeMetadata is not null because it load from DB,
        // but segmentTable may be null because it load after datanode report
        if (segIndex2SimulateSegmentMap == null || segIndex2SimulateSegmentMap.isEmpty()){
            logger.error("Has no segment in volume:{}.",
                    simulateVolume.getVolumeId());
            throw new SegmentNotFoundException_Thrift();
        } else if (segIndex2SimulateSegmentMap.size() != simulateVolume.getSegmentCount()){
            logger.error("segmentTable size:{} not equal with volume segment count:{}",
                    segIndex2SimulateSegmentMap.size(), simulateVolume.getSegmentCount());
            throw new SegmentNotFoundException_Thrift();
        }

        //volume segment traversal
        for (int segmentIndex : segIndex2SimulateSegmentMap.keySet()){
            SimulateSegment simulateSegment = segIndex2SimulateSegmentMap.get(segmentIndex);

            // calculate wrapper index
            // we will record normal segment unit counter of data node to used for overload
            ObjectCounter<Long> normalOfWrapperCounter;
            if (wrapperIndex2NormalCounterMap == null){
                normalOfWrapperCounter = new TreeSetObjectCounter<>();
            } else {
                long wrapperIndex = segmentIndex/config.getSegmentWrapSize();
                normalOfWrapperCounter = wrapperIndex2NormalCounterMap.computeIfAbsent(wrapperIndex, value->new TreeSetObjectCounter<>());
            }

            //segment unit traversal
            long primaryId = 0;
            Set<Long> secondarySet = new HashSet<>();
            Map<InstanceId, SimulateSegmentUnit> instanceId2SimulateSegmentUnitMap = simulateSegment.getInstanceId2SimulateSegUnitMap();
            for (InstanceId instanceId : instanceId2SimulateSegmentUnitMap.keySet()){
                if (simulateSegment.getPrimaryId() == instanceId){
                    if (primaryCounter != null){
                        primaryCounter.increment(instanceId.getId());
                    }
                    //used for overload
                    primaryId = instanceId.getId();
                    normalOfWrapperCounter.increment(primaryId);
                } else if (simulateSegment.getSecondaryIdSet().contains(instanceId)){
                    //secondary
                    if (secondaryCounter != null){
                        secondaryCounter.increment(instanceId.getId());
                    }
                    secondarySet.add(instanceId.getId());
                    //used for overload
                    normalOfWrapperCounter.increment(instanceId.getId());
                }
            }

            //save secondary combinations of primary
            if (primary2SecondaryCounterMap != null){
                ObjectCounter<Long> secondaryOfPrimaryCounter = primary2SecondaryCounterMap.computeIfAbsent(primaryId, value->new TreeSetObjectCounter<>());
                for (long secondaryId : secondarySet){
                    secondaryOfPrimaryCounter.increment(secondaryId);
                }
            }
        }

        //set not used instance count to 0
        for (SimulateInstanceInfo instanceInfo : simulateInstanceInfoList){
            long instanceId = instanceInfo.getInstanceId().getId();
            if (primaryCounter != null && primaryCounter.get(instanceId) == 0){
                primaryCounter.set(instanceId, 0);
            }

            if (secondaryCounter != null && secondaryCounter.get(instanceId) == 0){
                secondaryCounter.set(instanceId, 0);
            }

            if (primary2SecondaryCounterMap != null){
                int groupId = instanceInfo.getGroupId();
                ObjectCounter<Long> secondaryOfPrimaryCounter = primary2SecondaryCounterMap.computeIfAbsent(instanceId, value->new TreeSetObjectCounter<>());
                for (SimulateInstanceInfo instanceInfoTemp : simulateInstanceInfoList){
                    //same group with primary, cannot be secondary
                    if (groupId == instanceInfoTemp.getGroupId()){
                        continue;
                    }

                    long instanceIdTemp = instanceInfoTemp.getInstanceId().getId();
                    if (secondaryOfPrimaryCounter.get(instanceIdTemp) == 0){
                        secondaryOfPrimaryCounter.set(instanceIdTemp, 0);
                    }
                }
            }
        }

        logger.info("primaryCounter:{}; secondaryCounter:{}; wrapperIndex2NormalCounterMap:{}; primary2SecondaryCounterMap:{}",
                primaryCounter, secondaryCounter, wrapperIndex2NormalCounterMap, primary2SecondaryCounterMap);
    }

}
