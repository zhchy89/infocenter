package py.infocenter.rebalance.struct;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.infocenter.rebalance.builder.SimulateInstanceBuilder;
import py.infocenter.rebalance.selector.SegmentUnitReserver;
import py.thrift.share.NotEnoughSpaceException_Thrift;
import py.volume.VolumeType;

import java.util.*;

/**
 * combine  P、S by normal instance to generate a P/S combinationList
 */
public class ReserveVolumeCombination {
    private static final Logger logger = LoggerFactory.getLogger(ReserveVolumeCombination.class);

    private int usableMixGroupCountPerOneSegment;           //mix group max count that can be reserved to P or S per one segment
    private LinkedList<Deque<Long>> combinationList;        //result combination of P and S
    private Set<Integer> normalGroupIdSet;                  //all normal data node group
    private Set<Integer> mixGroupIdSet;                     //group which contains simple datanode and normal datanode

    private final Set<Integer> simpleGroupIdSet;            //all simple data node group
    private final LinkedList<Long> normalInstanceIdList;    //all normal data node
    private Map<Long, SimulateInstanceInfo> instanceId2SimulateInstanceMap; //all instance info (include simple and normal)
    private final int reserveSegmentCount;                  //segment count that will be reserved
    private final long segmentSize;                         //segment size
    private final boolean isSimpleConfiguration;            //is simple volume
    private final VolumeType volumeType;                    //volume type
    private final SimulateInstanceBuilder simulateInstanceBuilder;  //all instance info manager in current pool

    public ReserveVolumeCombination(SimulateInstanceBuilder simulateInstanceBuilder, boolean isSimpleConfiguration,
                                    int reserveSegmentCount, long segmentSize, VolumeType volumeType,
                                    LinkedList<Long> normalInstanceIdList, Set<Integer> simpleGroupIdSet) {
        this.simulateInstanceBuilder = simulateInstanceBuilder;
        this.reserveSegmentCount = reserveSegmentCount;
        this.normalInstanceIdList = normalInstanceIdList;
        this.instanceId2SimulateInstanceMap = simulateInstanceBuilder.getInstanceId2SimulateInstanceMap();
        this.simpleGroupIdSet = simpleGroupIdSet;
        this.segmentSize = segmentSize;
        this.isSimpleConfiguration = isSimpleConfiguration;
        this.volumeType = volumeType;

        normalGroupIdSet = new HashSet<>();
        for (long normalId : normalInstanceIdList){
            normalGroupIdSet.add(instanceId2SimulateInstanceMap.get(normalId).getGroupId());
        }

        combinationList = new LinkedList<>();

        //get MixGroup
        mixGroupIdSet = calcMixGroupIdSet();

        //calc usable max count of mix group in same segment
        usableMixGroupCountPerOneSegment = calcUsableMixGroupCountPerOneSegment();
    }

    public int getUsableMixGroupCountPerOneSegment() {
        return usableMixGroupCountPerOneSegment;
    }

    public Set<Integer> getMixGroupIdSet() {
        return mixGroupIdSet;
    }

    /**
     * get mix group set
     * The intersection of all simple group and all normal group is mix group
     * @return all mix group's id
     */
    private Set<Integer> calcMixGroupIdSet(){
        //get MixGroup
        Set<Integer> mixGroupIdSet = new HashSet<>(simpleGroupIdSet);
        mixGroupIdSet.retainAll(normalGroupIdSet);
        return mixGroupIdSet;
    }

    /**
     * get mix group max count that can be reserved to P or S per one segment
     *
     * 'remain arbiter count' = 'arbiter reserve count'(exclude redundancy arbiter) - 'simple group count';
     * if 'remain arbiter count' >= 'all mix group count', 0 number of mix group count can be reserved to P or S
     * if 0 < 'remain arbiter count' < 'all mix group count', (all mix group count - remain arbiter count) number of mix group count can be reserved to P or S
     * if 'remain arbiter count' <= 0, all mix group can be reserved to P or S
     *
     * @return count of can be reserved mix group
     */
    private int calcUsableMixGroupCountPerOneSegment(){
        Set<Integer> onlySimpleGroupIdSet = new HashSet<>(simpleGroupIdSet);
        onlySimpleGroupIdSet.removeAll(mixGroupIdSet);

        //calc usable max count of mix group in same segment
        int usableMixGroupCountPerOneSegment = mixGroupIdSet.size();
        int remainArbiterToCreate = volumeType.getNumArbiters() - onlySimpleGroupIdSet.size();
        if (remainArbiterToCreate >= mixGroupIdSet.size()) {
            usableMixGroupCountPerOneSegment = 0;
        } else if (remainArbiterToCreate > 0 && remainArbiterToCreate < mixGroupIdSet.size()){
            usableMixGroupCountPerOneSegment = mixGroupIdSet.size() - remainArbiterToCreate;
        }

        return usableMixGroupCountPerOneSegment;
    }

    /**
     * generate balance P/S combinations model list, by segment count, data node information...
     *
     * phase 1: According to the list of available nodes(with datanode weight), select the node as P in the past, so that P can allocate the balance；
     * phase 2: S is allocated according to the rules of PS combination balance. If there are multiple choices,
     *    select nodes to assign S to the least. If there are also multiple choices, a node is randomly selected as a S
     *
     * we need to guarantee the following points in phase 1 and phase 2：
     * 1. The datanode must have enough space；
     * 2. P and S must be distributed in different group；
     * 3. For the basic required A, reserve the allocation position, ensure that the A is created on Simple datanode;
     *    but the redundant A does not need to reserve the location to prevent the creation of any P or S in mix group without redundancy.
     *
     */
    public void combination() throws TException {
        LinkedList<Long> primaryList = new LinkedList<>(normalInstanceIdList);      //can be primary list
        LinkedList<Long> secondaryList = new LinkedList<>(normalInstanceIdList);    //can be secondary list
        Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();   //primary V.S. secondaryList(sort by used time)
        //Map<Long, LinkedList<Long>> primaryId2SecondaryIdListMap = new HashMap<>();   //primary V.S. secondaryList(sort by used time)
        ObjectCounter<Long> instanceIdOfReservedSegmentUnitCounter = new TreeSetObjectCounter<>();  //already reserved instance counter
        ObjectCounter<Long> secondaryDistributeCounter = new TreeSetObjectCounter<>();  //The distribution of secondary on all nodes
        ObjectCounter<Long> primaryDistributeCounter = new TreeSetObjectCounter<>();
        while (combinationList.size() < reserveSegmentCount){
            if (primaryList.size() == 0){
                logger.error("No enough space to create P/S models");
                throw new NotEnoughSpaceException_Thrift().setDetail("No enough space to create P/S models");
            }

            primaryList = simulateInstanceBuilder.getPrimaryPriorityList(primaryList, primaryDistributeCounter, reserveSegmentCount);

            Set<Integer> usedGroupIdSet = new HashSet<>();
            int currentMixGroupUsedCount = 0;

            //select primary
            long primaryId = primaryList.removeFirst();
            SimulateInstanceInfo primaryInfo = instanceId2SimulateInstanceMap.get(primaryId);

            //verify mixed group
            if (mixGroupIdSet.contains(primaryInfo.getGroupId())){
                //when segment unit in mix group, but mix group must used to be arbiter, this instance can not be used
                if (currentMixGroupUsedCount >= usableMixGroupCountPerOneSegment){
                    continue;
                }
                currentMixGroupUsedCount++;
            }

            // instance has enough space to create one more segment units
            if (!canCreateOneMoreSegmentUnits(primaryInfo, instanceIdOfReservedSegmentUnitCounter)){
                // not enough space in this instance
                //logger.info("no enough space left in {}", primaryInfo);
                continue;
            }

            usedGroupIdSet.add(primaryInfo.getGroupId());

            //get secondary select list
            ObjectCounter<Long> secondaryOfPrimaryCounter = primaryId2SecondaryIdCounterMap.get(primaryId);
            if (secondaryOfPrimaryCounter == null){
                secondaryOfPrimaryCounter = new TreeSetObjectCounter<>();
                primaryId2SecondaryIdCounterMap.put(primaryId, secondaryOfPrimaryCounter);

                for (long secondaryId : secondaryList){
                    secondaryOfPrimaryCounter.set(secondaryId, 0);
                }
            }

            //get secondary datanode priority list
            LinkedList<Long> secondaryListOfPrimary = simulateInstanceBuilder.getSecondaryPriorityList(secondaryList, primaryId,
                    secondaryOfPrimaryCounter, secondaryDistributeCounter, reserveSegmentCount, volumeType.getNumSecondaries());

            Iterator<Long> secondaryIdIt = secondaryListOfPrimary.iterator();
            ArrayList<Long> selectSecondaryList = new ArrayList<>();
            //select secondary
            for (int j = 0; j < volumeType.getNumSecondaries(); j++){
                while (secondaryIdIt.hasNext()){
                    long secondaryIdTemp = secondaryIdIt.next();
                    SimulateInstanceInfo secondaryTempInfo = instanceId2SimulateInstanceMap.get(secondaryIdTemp);

                    //group is already used
                    if (usedGroupIdSet.contains(secondaryTempInfo.getGroupId())){
                        continue;
                    }

                    //verify mixed group
                    if (mixGroupIdSet.contains(secondaryTempInfo.getGroupId())){
                        //when segment unit in mix group, but mix group must used to be arbiter, this instance can not be used
                        if (currentMixGroupUsedCount >= usableMixGroupCountPerOneSegment){
                            continue;
                        }
                        currentMixGroupUsedCount++;
                    }

                    // instance has enough space to create one more segment units
                    if (!canCreateOneMoreSegmentUnits(secondaryTempInfo, instanceIdOfReservedSegmentUnitCounter)){
                        // not enough space in this instance
                        //logger.info("no enough space left in {}", primaryInfo);
                        continue;
                    }

                    //save secondary
                    selectSecondaryList.add(secondaryIdTemp);
                    usedGroupIdSet.add(secondaryTempInfo.getGroupId());
                    break;
                }
            }

            if (selectSecondaryList.size() < volumeType.getNumSecondaries()){
                logger.error("No enough space to create P/S models");
                throw new NotEnoughSpaceException_Thrift().setDetail("No enough space to create P/S models");
            }

            Deque<Long> segmentCombination = new ArrayDeque<>();
            segmentCombination.add(primaryId);
            segmentCombination.addAll(selectSecondaryList);

            primaryDistributeCounter.increment(primaryId);
            //reserve segment space
            instanceIdOfReservedSegmentUnitCounter.increment(primaryId);
            for (long secondaryIdTemp : selectSecondaryList){
                secondaryOfPrimaryCounter.increment(secondaryIdTemp);
                instanceIdOfReservedSegmentUnitCounter.increment(secondaryIdTemp);
                secondaryDistributeCounter.increment(secondaryIdTemp);
            }

            //save combination
            combinationList.add(segmentCombination);

            primaryList.offerLast(primaryId);
        }

        if (logger.isDebugEnabled()){
            verifyBalance();
        }
    }

    /**
     *  get secondary priority list
     *  The following conditions must be followed：
     *  1. The datanode is sorted according to the PS composite distribution, with the lowest distribution in the front
     *  2. In order of S distribution, the least comes first
     *  3. A random arrangement cause when all of the above conditions are equally
     * @param secondaryOfPrimaryCounter     the distribution of secondary on not primary nodes
     * @param secondaryDistributeCounter    The distribution of secondary on all nodes
     * @return priority list
     */
    @Deprecated
    public static LinkedList<Long> getSecondaryPriorityList(ObjectCounter<Long> secondaryOfPrimaryCounter, ObjectCounter<Long> secondaryDistributeCounter){
        LinkedList<Long> selSecondaryList = new LinkedList<>();

        /*
         * The datanode is sorted according to the PS composite distribution, with the lowest distribution in the front
         */
        Multimap<Long, Long> secondaryCount2InstanceIdOfPrimaryMap = HashMultimap.create();
        Iterator<Long> secondaryOfPrimaryCounterItor = secondaryOfPrimaryCounter.iterator();
        while (secondaryOfPrimaryCounterItor.hasNext()){
            long nodeId = secondaryOfPrimaryCounterItor.next();
            long nodeCount = secondaryOfPrimaryCounter.get(nodeId);
            secondaryCount2InstanceIdOfPrimaryMap.put(nodeCount, nodeId);
        }
        LinkedList<Long> countList = new LinkedList<>(secondaryCount2InstanceIdOfPrimaryMap.keySet());
        Collections.sort(countList);

        for (long count : countList){
            /*
             * In order of S distribution, the least comes first
             */
            Multimap<Long, Long> sortMap = HashMultimap.create();
            for (long nodeId : secondaryCount2InstanceIdOfPrimaryMap.get(count)){
                sortMap.put(secondaryDistributeCounter.get(nodeId), nodeId);
            }
            LinkedList<Long> sortList = new LinkedList<>(sortMap.keySet());
            Collections.sort(sortList);

            /*
             * Random ordering of the same conditions
             */
            for (long value : sortList){
                LinkedList shuffledList = new LinkedList<>(sortMap.get(value));
                Collections.shuffle(shuffledList);
                selSecondaryList.addAll(shuffledList);
            }
        }

        return selSecondaryList;
    }

    /**
     * is instance can create one or more segment unit
     * @param instanceInfo instance information
     * @param instanceIdOfReservedSegmentUnitCounter already reserved segment unit counter of instance
     * @return true:if can
     */
    public boolean canCreateOneMoreSegmentUnits(SimulateInstanceInfo instanceInfo, ObjectCounter<Long> instanceIdOfReservedSegmentUnitCounter) {
        long reservedSegmentUnit  = instanceIdOfReservedSegmentUnitCounter.get(instanceInfo.getInstanceId().getId());
        if (isSimpleConfiguration) {
            return instanceInfo.getFreeFlexibleSegmentUnitCount() - reservedSegmentUnit >= 1;
        } else {
            return instanceInfo.getFreeSpace() - reservedSegmentUnit*segmentSize >= segmentSize;
        }
    }

    /**
     * has enough space to create this combination
     * @param combination   combination of PS
     * @param reservedSegmentUnitCounter    instance that already reserved Segment unit counter
     * @return  true:if has enough space
     */
    public boolean canCombinationCreate(Deque<Long> combination, ObjectCounter<Long> reservedSegmentUnitCounter){
        if (combination.size() == 0){
            return false;
        }
        ObjectCounter<Long> reservedSegmentUnitCounterTemp = reservedSegmentUnitCounter.deepCopy();

        for (long instanceId : combination){
            SimulateInstanceInfo instanceInfo = instanceId2SimulateInstanceMap.get(instanceId);

            //can no enough space to create segment unit
            if (!canCreateOneMoreSegmentUnits(instanceInfo, reservedSegmentUnitCounterTemp)){
                return false;
            }

            reservedSegmentUnitCounterTemp.increment(instanceId);
        }

        return true;
    }

    /**
     * poll a combination, which cannot contain reserved arbiter and has no overload instance combinations is a priority
     * @param overloadInstanceIdSet overload instance
     * @return P,S combination, it may be empty when combinationList is empty
     */
    public Deque<Long> pollSegment(Set<Long> overloadInstanceIdSet){
        Deque<Long> bestSegment = new ArrayDeque<>();

        //all instance is overload, or all instance not overload, we do not need care overload
        boolean needOverloadOprate = true;
        if(overloadInstanceIdSet.size() >= normalInstanceIdList.size() ||
                overloadInstanceIdSet.size() == 0){
            needOverloadOprate = false;
        }

        // if do not care overload, poll it
        if (!needOverloadOprate){
            if (!combinationList.isEmpty()){
                bestSegment = combinationList.removeFirst();
            }
            return bestSegment;
        }

        int maxNoOverloadInstanceNum = 0;
        for (Deque<Long> segmentUnitDeque : combinationList){
            int notOverloadCount = 0;
            for (long instanceId : segmentUnitDeque){
                //record not overload count
                if (!overloadInstanceIdSet.contains(instanceId)){
                    notOverloadCount++;
                }

                //all reserved normal segment are not overload
                if (notOverloadCount >= volumeType.getNumSecondaries()+SegmentUnitReserver.PRIMARYCOUNT){
                    combinationList.remove(segmentUnitDeque);
                    return segmentUnitDeque;
                }

                //all not overload instance in this combination
                if (notOverloadCount >= normalInstanceIdList.size()-overloadInstanceIdSet.size()) {
                    combinationList.remove(segmentUnitDeque);
                    return segmentUnitDeque;
                }


            }

            if (notOverloadCount > maxNoOverloadInstanceNum){
                //select max not overload node combination to return
                maxNoOverloadInstanceNum = notOverloadCount;
                bestSegment = segmentUnitDeque;
            }
        }

        // when normal list have some mix group, and arbiter will be reserved in this group,
        // then normal node which in this group will never to be used, so "overloadInstanceIdSet < normalInstanceIdList" forever.
        // At this time overloadInstanceIdSet contains all usable node, then will not found a combination in combinationList.
        //if not found, poll the first
        if (bestSegment.size() == 0 && combinationList.size() > 0){
            bestSegment = combinationList.getFirst();
        }

        combinationList.remove(bestSegment);

        // when combinationsList's size is 0, empty combination will be return
        return bestSegment;
    }

    /**
     * reserve a P/S combination of segment at random
     * @param overloadInstanceIdSet instance which already overload
     * @param instanceIdOfReservedSegmentUnitCounter    already reserved segment unit counter of instance
     * @return  a P/S combination that is not null or empty
     * @throws NotEnoughSpaceException_Thrift   if have no enough space
     */
    public Deque<Long> randomSegment(Set<Long> overloadInstanceIdSet, ObjectCounter<Long> instanceIdOfReservedSegmentUnitCounter)
            throws NotEnoughSpaceException_Thrift {
        Deque<Long> segmentUnitDeque = new ArrayDeque<>();
        Set<Integer> usedGroupIdSet = new HashSet<>();
        int currentMixGroupUsedCount = 0;

        //shuffle normal list
        LinkedList<Long> normalInstanceIdListBac = new LinkedList<>(normalInstanceIdList);
        Collections.shuffle(normalInstanceIdListBac);

        int normalSegmentUnitCountPerSegment = volumeType.getNumSecondaries()+SegmentUnitReserver.PRIMARYCOUNT;

        /*
         * Step1. reserve segment unit from normal instance list(not contains overload instance list)
         */
        boolean isOverloadList = false;
        Iterator<Long> normalIdIt = normalInstanceIdListBac.iterator();
        Set<Long> overloadNotUsedSet = new HashSet<>(overloadInstanceIdSet);
        for (int i = 0; i < normalSegmentUnitCountPerSegment; i++){
            boolean canBeSegmentUnit = false;
            while(normalIdIt.hasNext()){
                long normalIdTemp = normalIdIt.next();
                SimulateInstanceInfo instanceInfo = instanceId2SimulateInstanceMap.get(normalIdTemp);

                //group is already used
                if (usedGroupIdSet.contains(instanceInfo.getGroupId())){
                    continue;
                }

                //verify mixed group
                if (mixGroupIdSet.contains(instanceInfo.getGroupId())){
                    //when segment unit in mix group, but mix group must used to be arbiter, this instance can not be used
                    if (currentMixGroupUsedCount >= usableMixGroupCountPerOneSegment){
                        continue;
                    }
                    currentMixGroupUsedCount++;
                }

                // instance has enough space to create one more segment units
                if (!canCreateOneMoreSegmentUnits(instanceInfo, instanceIdOfReservedSegmentUnitCounter)){
                    // not enough space in this instance
                    //logger.info("no enough space left in {}", primaryInfo);
                    continue;
                }

                if (!isOverloadList && overloadNotUsedSet.contains(normalIdTemp)){
                    continue;
                }

                //save secondary
                segmentUnitDeque.add(normalIdTemp);
                usedGroupIdSet.add(instanceInfo.getGroupId());
                canBeSegmentUnit = true;
                break;
            } //while(normalIdIt.hasNext()){

            /*
             * Step2. if reserve segment unit from normal instance list(not contains overload instance list) failed,
             *        reserve segment unit from overload instance list
             */
            if (!canBeSegmentUnit && !isOverloadList){
                normalIdIt = overloadNotUsedSet.iterator();
                isOverloadList = true;
                i--;
            }
        } //for

        if (segmentUnitDeque.size() < volumeType.getNumSecondaries()+SegmentUnitReserver.PRIMARYCOUNT){
            logger.error("No enough data node to create P/S models");
            throw new NotEnoughSpaceException_Thrift().setDetail("No enough data node to create P/S models");
        }

        return segmentUnitDeque;
    }

    /**
     * just for unit test
     */
    private void verifyBalance(){
        ObjectCounter<Long> primaryIdCounter = new TreeSetObjectCounter<>();
        ObjectCounter<Long> secondaryIdCounter = new TreeSetObjectCounter<>();

        Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();

        for (Deque<Long> combination : combinationList){
            //count necessary primary, secondary
            ObjectCounter<Long> primarySecondaryCounter = new TreeSetObjectCounter<>();
            int first = 0;
            for (long instanceId : combination) {
                if (first == 0){
                    primaryIdCounter.increment(instanceId);

                    primarySecondaryCounter = primaryId2SecondaryIdCounterMap.computeIfAbsent(instanceId, value->new TreeSetObjectCounter<>());
                    first++;
                    continue;
                }

                secondaryIdCounter.increment(instanceId);
                primarySecondaryCounter.increment(instanceId);
            }
        }

        long maxCount, minCount;
        try {
             /*
             * verify average distribution
             */
            //verify necessary primary max and min count
            maxCount = primaryIdCounter.maxValue();
            minCount = primaryIdCounter.minValue();
            if (maxCount - minCount > 10){
                logger.error("primary average distribute failed ! maxCount:{}, minCount:{}",maxCount, minCount);
            }

            //verify necessary secondary max and min count
            maxCount = secondaryIdCounter.maxValue();
            minCount = secondaryIdCounter.minValue();
            if (maxCount - minCount > 10){
                logger.error("secondary average distribute failed ! maxCount:{}, minCount:{}",maxCount, minCount);
            }

            /*
             * verify rebalance when P down
             */
            for (Map.Entry<Long, ObjectCounter<Long>> entry : primaryId2SecondaryIdCounterMap.entrySet()){
                ObjectCounter<Long> secondaryCounterTemp = entry.getValue();
                maxCount = secondaryCounterTemp.maxValue();
                minCount = secondaryCounterTemp.minValue();

                if (maxCount - minCount > 10){
                    logger.error("PS distribution not balance! maxCount:{}, minCount:{}",maxCount, minCount);
                }
            }
        } catch (Exception e){
            logger.error("catch a exception(may be 0 segment reserved)", e);
        }
    }

}
