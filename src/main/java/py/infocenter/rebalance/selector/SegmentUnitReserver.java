package py.infocenter.rebalance.selector;

import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.client.RequestResponseHelper;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.icshare.InstanceMetadata;
import py.infocenter.rebalance.struct.InstanceInfoImpl;
import py.infocenter.rebalance.struct.ReserveVolumeCombination;
import py.infocenter.rebalance.struct.SimpleDatanodeManager;
import py.infocenter.store.StorageStore;
import py.instance.Group;
import py.instance.InstanceId;
import py.thrift.share.*;
import py.volume.VolumeType;

import java.util.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SegmentUnitReserver {
    private static final Logger logger = LoggerFactory.getLogger(SegmentUnitReserver.class);

    private final long segmentSize;
    private final StorageStore storageStore;
    private final TreeSet<InstanceInfoImpl> instanceInfoSet = new TreeSet<>();

    //fault_tolerant instance max count
    private final int FAULT_TOLERANT_NORMAL_INSTANCE_COUNT_MAX = 2;
    private final int FAULT_TOLERANT_SIMPLE_INSTANCE_COUNT_MAX = 1;

    public static final int PRIMARYCOUNT = 1;

    private enum GroupFlag {
        SIMPLE("S"),          //group which only contains simple datanode
        MIXED("M"),           //group which contains simple datanode and normal datanode at the same time
        NORMAL("N");          //group which only contains normal datanode

        private final String flag;

        GroupFlag(String flag) {
            this.flag = flag;
        }
    }

    public SegmentUnitReserver(long segmentSize, StorageStore storageStore) {
        this.segmentSize = segmentSize;
        this.storageStore = storageStore;
    }

    public void updateInstanceInfo(Collection<InstanceInfoImpl> instanceInfoCollection) {
        instanceInfoSet.clear();
        instanceInfoSet.addAll(instanceInfoCollection);
    }

    /**
     * pre-allocate segment units to ensure we have enough groups and space for the new volume.<br>
     * <p>
     * The volume has been divided into several parts by segment wrap size. We will reserve for one part at a time and
     * join the results together in the end.
     * <p>
     * <pre>
     *
     * We have 2 phases to reserve segment unit:
     *      phase 1: pre-allocating all least needed P and S segment unit, get PS combinations list;
     *      phase 2: for each reserve operations, poll one PS combination from list, then reserve other segment unit;
     *
     * For example, we are reserving a volume with 100 segments, and segment wrap size is 30.
     * There will be 4 (30 + 30 + 30 + 10) reserve operations.
     *
     * phase 1: pre-allocating 100 P and S segment unit, get 100 elements list which has allocated P and S,
     *          must consider P and S balance, and PS balance(when P down, S to be P, P also balance) in this process;
     * phase 2: for each 4 of reserve operations, do it like below:
     *      a). poll one element from PS combinations list(must consider overload), and get a PS combination set;
     *      b). reserve least needed A, and put it to PS combination set;
     *      c). reserve redundancy S, no need to consider balance and overload;
     *      d). reserve redundancy A, no need to consider be created on simple datanode priority
     *
     * But how do we make sure that each segment doesn't have a group conflicting ?
     *      not allocating more than segment count of segment units in each group in each process.
     *
     * The join process is pretty simple cause the only thing we need to do is calculating the real segment index
     * for each segment.
     *
     * </pre>
     *
     * @param expectedSize          volume size
     * @param volumeType            volume type, currently only Normal or Small
     * @param segmentWrapSize       the segment wrap size, in each segment wrapper we will try to reserve as much instances
     *                              as possible
     * @param simpleDatanodeManager simple datanode instance info
     * @param faultTolerant         if we need to reserve some more instances in each segment for fault tolerant
     */
    public Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> reserveVolume(
            long expectedSize, VolumeType volumeType, boolean isSimpleConfiguration, int segmentWrapSize,
            SimpleDatanodeManager simpleDatanodeManager, boolean faultTolerant)
            throws TException {
        logger.warn(
                "expectedSize:{}, volumeType:{}, isSimpleConfiguration:{}, segmentWrapSize:{}, simpleDatanodeManager:{}, faultTolerant:{}",
                expectedSize, volumeType, isSimpleConfiguration, segmentWrapSize, simpleDatanodeManager, faultTolerant);
        //save all instanceImpl, <instanceId, InstanceInfoImpl>
        HashMap<Long, InstanceInfoImpl> instanceId2InstanceInfoImplMap = new HashMap<>();
        //save all InstanceMetadata, <instanceId, InstanceMetadata>
        HashMap<Long, InstanceMetadata> instanceId2instanceMetadataMap = new HashMap<>();
        // these datanode will be only used to create arbiter segment units
        ObjectCounter<Long> simpleDatanodeIdCounter = new TreeSetObjectCounter<>();
        // these datanode will be used to create arbiter segment units, when simpleDatanodeIdList is not enough
        ObjectCounter<Long> normalDatanodeIdForArbiterCounter = new TreeSetObjectCounter<>();
        // these datanode will be used to create normal segment units
        ObjectCounter<Long> normalDatanodeIdCounter = new TreeSetObjectCounter<>();

        Set<Group> allGroupSet = new HashSet<>();   // all group set, to check if we have enough instances in different groups
        Set<Integer> simpleDatanodeGroupIdSet = new HashSet<>();    //simple datanode Group Id
        Set<Integer> normalDatanodeGroupIdSet = new HashSet<>();    //normal datanode Group Id

        //simpleDatanode may be have no archives, so it cannot add to pool
        //we will create arbiter at all simpleDatanode which be owned domain
        Set<Long> allSimpleDatanodeIdSet = simpleDatanodeManager.getSimpleDatanodeInstanceIdSet();

        long domainId = -1;
        //for (simpleDatanodeManager.getSimpleDatanodeGroupIdSet())
        // initialize group set and simple datanode hosts
        for (InstanceInfoImpl instanceInfo : instanceInfoSet) {
            InstanceMetadata instanceMetadata = storageStore.get(instanceInfo.getInstanceId().getId());
            domainId = instanceMetadata.getDomainId();

            allGroupSet.add(instanceMetadata.getGroup());
            instanceId2InstanceInfoImplMap.put(instanceInfo.getInstanceId().getId(), instanceInfo);
            instanceId2instanceMetadataMap.put(instanceInfo.getInstanceId().getId(), instanceMetadata);

            //            Set<Long> simpleDatanodeIdSetInOneGroup = simpleDatanodeManager.getSimpleDatanodeIdSetByGroupId(instanceMetadata.getGroup().getGroupId());
            //            if (simpleDatanodeIdSetInOneGroup.contains(instanceMetadata.getInstanceId().getId())) {
            //                //arbiter priority selection is simple datanode
            //                simpleDatanodeIdList.set(instanceInfo.getInstanceId().getId(), 0);
            //            } else {
            if (!allSimpleDatanodeIdSet.contains(instanceMetadata.getInstanceId().getId())) {
                normalDatanodeGroupIdSet.add(instanceMetadata.getGroup().getGroupId());
                //simple segment unit cannot be create at normal datanode
                normalDatanodeIdForArbiterCounter.set(instanceInfo.getInstanceId().getId(), 0);
                normalDatanodeIdCounter.set(instanceInfo.getInstanceId().getId(), 0);
            }
        }

        //simpleDatanode may be have no archives, so it cannot add to pool
        //we will create arbiter at all simpleDatanode which be owned domain
        for (long simpleDatanodeInstanceId : allSimpleDatanodeIdSet) {
            InstanceMetadata instanceMetadata = storageStore.get(simpleDatanodeInstanceId);

            if (instanceMetadata.getDomainId() == domainId) {
                allGroupSet.add(instanceMetadata.getGroup());
                //instanceId2InstanceInfoImplMap.put(instanceMetadata.getInstanceId().getId(), instanceMetadata);
                instanceId2instanceMetadataMap.put(instanceMetadata.getInstanceId().getId(), instanceMetadata);

                simpleDatanodeGroupIdSet.add(instanceMetadata.getGroup().getGroupId());
                simpleDatanodeIdCounter.set(simpleDatanodeInstanceId, 0);
            }
        }

        logger.info(
                "reserveVolume: simpleDatanodeGroupIdSet:{}; simpleDatanodeIdList:{}; normalDatanodeGroupIdSet:{}; normalDatanodeIdList:{}",
                simpleDatanodeGroupIdSet, simpleDatanodeIdCounter, normalDatanodeGroupIdSet, normalDatanodeIdCounter);
        logger.info("reserveVolume: instanceId2InstanceInfoImplMap:{}; instanceId2instanceMetadataMap:{}",
                instanceId2InstanceInfoImplMap, instanceId2instanceMetadataMap);

        //calculate how much arbiter segment unit count and normal segment unit count will be create
        Map<SegmentUnitType_Thrift, Integer> segmentUnitCountMap = calculateSegmentUnitWillBeCreateCount(
                simpleDatanodeGroupIdSet, normalDatanodeGroupIdSet, allGroupSet, volumeType, faultTolerant);
        int numberOfNormalWillReservePerSegment = segmentUnitCountMap.get(SegmentUnitType_Thrift.Normal);
        int numberOfArbiterWillReservePerSegment = segmentUnitCountMap.get(SegmentUnitType_Thrift.Arbiter);

        int numOfSegments = (int) (expectedSize / segmentSize); // segment count
        int segmentWrapperCount = numOfSegments / segmentWrapSize; // how many wrapper we have
        // maybe segment count is not a integer multiple of wrap size and we will have a remainder.
        int remainder = numOfSegments % segmentWrapSize;

        /**
         * get segment unit P/S combinations list
         */
        ReserveVolumeCombination reserveVolumeCombination = new ReserveVolumeCombination(isSimpleConfiguration,
                numOfSegments, segmentSize, volumeType, new LinkedList<>(normalDatanodeIdCounter.getAll()),
                instanceId2InstanceInfoImplMap, simpleDatanodeGroupIdSet);
        //get segment unit P/S combinations list
        reserveVolumeCombination.combination();

        Set<Integer> mixGroupSet = reserveVolumeCombination.getMixGroupIdSet();

        //for distribute arbiter
        ObjectCounter<Long> simpleGroupSimpleDatanodeCounter = new TreeSetObjectCounter<>();
        ObjectCounter<Long> mixGroupSimpleDatanodeCounter = new TreeSetObjectCounter<>();
        // get simple datanode in simple group and simple datanode in mix group
        Iterator<Long> simpleDatanodeIdListItor = simpleDatanodeIdCounter.iterator();
        while (simpleDatanodeIdListItor.hasNext()){
            long instanceId = simpleDatanodeIdListItor.next();
            InstanceMetadata instanceMetadata = instanceId2instanceMetadataMap.get(instanceId);
            if (mixGroupSet.contains(instanceMetadata.getGroup().getGroupId())){
                mixGroupSimpleDatanodeCounter.set(instanceId, 0);
            } else {
                simpleGroupSimpleDatanodeCounter.set(instanceId, 0);
            }
        }

        // loop time depends on whether we have a remainder
        int segmentCreateTimesPerSegmentWrapper = segmentWrapperCount;
        if (remainder != 0) {
            segmentCreateTimesPerSegmentWrapper += 1;
        }

        ObjectCounter<Long> reservedSegmentUnitCounter = new TreeSetObjectCounter<>();

        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> resultToReturn = new HashMap<>();
        for (int wrapperIndex = 0; wrapperIndex < segmentCreateTimesPerSegmentWrapper; wrapperIndex++) {
            // segment count is usually the segmentWrapSize, but for the round of remainder(which is smaller than segment wrap size),
            // the remainder will be enough
            int segmentCountThisTime = segmentWrapSize;
            if (wrapperIndex >= segmentWrapperCount) {
                segmentCountThisTime = remainder;
            }

            ObjectCounter<Long> instanceIdCounterForOverload = new TreeSetObjectCounter<>(); // counter of instance
            // if we keep allocate segment units on overloaded instances, this passel of segment units can't all lay on
            // different disks. So we will try not to chose from an overloaded instance.
            Set<Long> overloadInstanceIdSet = new HashSet<>();

            for (int segmentIndex = 0; segmentIndex < segmentCountThisTime; segmentIndex++) {
                //group which is used to reserve segment unit, whether arbiter or normal
                HashSet<Integer> usedGroupSet = new HashSet<>();

                /**
                 * reserve normal
                 */
                LinkedList<InstanceIdAndEndPoint_Thrift> selectedNormalList = new LinkedList<>();

                Deque<Long> combinationOfPS = reserveVolumeCombination.pollSegment(overloadInstanceIdSet);
                if (combinationOfPS.size() == 0){
                    //Choose normal segment arbitrarily
                    combinationOfPS = reserveVolumeCombination.randomSegment(overloadInstanceIdSet, reservedSegmentUnitCounter);
                }

                //is combination can be created
                boolean isThisCombinationOK = reserveVolumeCombination.canCombinationCreate(combinationOfPS,
                        reservedSegmentUnitCounter);
                if (!isThisCombinationOK){
                    segmentIndex--;
                    continue;
                }

                //update data
                for (long instanceId : combinationOfPS){
                    InstanceInfoImpl instanceInfo = instanceId2InstanceInfoImplMap.get(instanceId);

                    int instanceReservedCount = (int) instanceIdCounterForOverload.get(instanceId);
                    if (instanceReservedCount >= instanceInfo.getDiskCount()) {
                        // segment units can't lay on different disks inside a segment wrapper if more segments
                        // allocated on this instance
                        overloadInstanceIdSet.add(instanceId);
                    }

                    reservedSegmentUnitCounter.increment(instanceId);
                    instanceIdCounterForOverload.increment(instanceId);
                    usedGroupSet.add(instanceInfo.getGroupId());

                    //save select normal segment unit
                    InstanceMetadata instanceMetadata = instanceId2instanceMetadataMap.get(instanceId);
                    selectedNormalList.addLast(RequestResponseHelper.buildInstanceIdAndEndPointFrom(instanceMetadata));
                }

                /**
                 * reserve arbiter
                 * simple datanode which in simple group is first priority;
                 * simple datanode which in mix group is second priority;
                 * normal datanode is the last priority
                 */
                LinkedList<InstanceIdAndEndPoint_Thrift> selectedArbitersList = new LinkedList<>();

                //remove data node which own group is already used, to reduce the number of cycles
                LinkedList<Long> normalIdForArbiterList = new LinkedList<>(normalDatanodeIdForArbiterCounter.getAll());
                for (int usedGroupId : usedGroupSet){
                    normalIdForArbiterList.removeIf(id -> instanceId2instanceMetadataMap.get(id).getGroup().getGroupId() == usedGroupId);
                }


                Iterator<Long> simpleGroupSimpleDatanodeCounterItor = simpleGroupSimpleDatanodeCounter.iterator();
                Iterator<Long> mixGroupSimpleDatanodeCounterItor = mixGroupSimpleDatanodeCounter.iterator();
                Iterator<Long> normalDatanodeIdListForArbiterItor = normalIdForArbiterList.iterator();

                List<Long> simpleGroupSimpleDatanodeToBeArbiterList = new LinkedList<>();
                List<Long> mixGroupSimpleDatanodeToBeArbiterList = new LinkedList<>();
                List<Long> normalDatanodeToBeArbiterList = new LinkedList<>();

                while (selectedArbitersList.size() < volumeType.getNumArbiters()) {
                    if (simpleGroupSimpleDatanodeCounterItor.hasNext()) {
                        //arbiter priority selection simple datanode to be created
                        InstanceMetadata simpleDatanodeTemp = instanceId2instanceMetadataMap
                                .get(simpleGroupSimpleDatanodeCounterItor.next());
                        //logger.info("get simple datanode from storageStore: {}", simpleDatanodeTemp);
                        if (usedGroupSet.stream().noneMatch(id -> id == simpleDatanodeTemp.getGroup().getGroupId())) {
//                            logger.info("reserveVolume from simple datanode: {}", simpleDatanodeTemp);
                            selectedArbitersList
                                    .add(RequestResponseHelper.buildInstanceIdAndEndPointFrom(simpleDatanodeTemp));
                            simpleGroupSimpleDatanodeToBeArbiterList.add(simpleDatanodeTemp.getInstanceId().getId());
                            usedGroupSet.add(simpleDatanodeTemp.getGroup().getGroupId());
                        }
                    } else if (mixGroupSimpleDatanodeCounterItor.hasNext()) {
                        //arbiter priority selection simple datanode to be created
                        InstanceMetadata simpleDatanodeTemp = instanceId2instanceMetadataMap
                                .get(mixGroupSimpleDatanodeCounterItor.next());
                        //logger.info("get simple datanode from storageStore: {}", simpleDatanodeTemp);
                        if (usedGroupSet.stream().noneMatch(id -> id == simpleDatanodeTemp.getGroup().getGroupId())) {
//                            logger.info("reserveVolume from simple datanode: {}", simpleDatanodeTemp);
                            selectedArbitersList
                                    .add(RequestResponseHelper.buildInstanceIdAndEndPointFrom(simpleDatanodeTemp));
                            mixGroupSimpleDatanodeToBeArbiterList.add(simpleDatanodeTemp.getInstanceId().getId());
                            usedGroupSet.add(simpleDatanodeTemp.getGroup().getGroupId());
                        }
                    } else if (normalDatanodeIdListForArbiterItor.hasNext()) {
                        //if simple datanode is not enough , arbiter can create at normal datanode
                        InstanceMetadata normalDatanodeTemp = instanceId2instanceMetadataMap
                                .get(normalDatanodeIdListForArbiterItor.next());
//                        logger.info("reserveVolume: simple datanode is no enough!reserveVolume from simple datanode: {}", normalDatanodeTemp);
                        if (usedGroupSet.stream().noneMatch(id -> id == normalDatanodeTemp.getGroup().getGroupId())) {
                            selectedArbitersList
                                    .add(RequestResponseHelper.buildInstanceIdAndEndPointFrom(normalDatanodeTemp));
                            normalDatanodeToBeArbiterList.add(normalDatanodeTemp.getInstanceId().getId());
                            usedGroupSet.add(normalDatanodeTemp.getGroup().getGroupId());
                        }
                    }
                }

                //put necessary used arbiter to tail, ensure balance
                for (Long simpleDatanodeIdTemp : simpleGroupSimpleDatanodeToBeArbiterList) {
                    simpleGroupSimpleDatanodeCounter.increment(simpleDatanodeIdTemp);
                }
                for (Long simpleDatanodeIdTemp : mixGroupSimpleDatanodeToBeArbiterList) {
                    mixGroupSimpleDatanodeCounter.increment(simpleDatanodeIdTemp);
                }
                for (Long normalDatanodeIdTemp : normalDatanodeToBeArbiterList) {
                    normalDatanodeIdForArbiterCounter.increment(normalDatanodeIdTemp);
                }

                /**
                 * reserve fault secondary
                 */
                int faultNormalCount = numberOfNormalWillReservePerSegment - volumeType.getNumSecondaries() - PRIMARYCOUNT;
                if (faultNormalCount > 0){
                    LinkedList<InstanceIdAndEndPoint_Thrift> selectedFaultNormalList = reserveFaultSecondary(
                            faultNormalCount, usedGroupSet, normalDatanodeIdCounter,
                            instanceId2instanceMetadataMap, instanceId2InstanceInfoImplMap,
                            selectedNormalList, reserveVolumeCombination, reservedSegmentUnitCounter);

                    //put fault normal segment unit to normal segment unit list
                    selectedNormalList.addAll(selectedFaultNormalList);
                }

                /**
                 * reserve fault arbiter
                 */
                int faultArbiterCount = numberOfArbiterWillReservePerSegment - volumeType.getNumArbiters();
                if (faultArbiterCount > 0){
                    LinkedList<InstanceIdAndEndPoint_Thrift> selectedFaultArbitersList = reserveFaultArbiter(faultArbiterCount, usedGroupSet,
                            simpleGroupSimpleDatanodeCounter, mixGroupSimpleDatanodeCounter, normalDatanodeIdForArbiterCounter, instanceId2instanceMetadataMap);

                    //put fault segment unit to arbiter segment unit list
                    selectedArbitersList.addAll(selectedFaultArbitersList);
                }

                /**
                 * save result
                 */
                int segmentNo = wrapperIndex * segmentWrapSize + segmentIndex;
                Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> mapTypeToList = new HashMap<>();
                mapTypeToList
                        .put(isSimpleConfiguration ? SegmentUnitType_Thrift.Flexible : SegmentUnitType_Thrift.Normal,
                                selectedNormalList);
                mapTypeToList.put(SegmentUnitType_Thrift.Arbiter, selectedArbitersList);
                resultToReturn.put(segmentNo, mapTypeToList);
            }
        }
        logger.info("reserveVolume: resultToReturn:{}", resultToReturn);
        return resultToReturn;
    }

    /**
     * reserve fault secondary
     * fault segment unit not care overload but care space
     * @param faultNormalCount  will reserve secondary count
     * @param usedGroupSet  has already used group set
     * @param normalDatanodeIdList  normal data node list
     * @param instanceId2instanceMetadataMap    all instance info
     * @return  fault secondary list
     */
    private LinkedList<InstanceIdAndEndPoint_Thrift> reserveFaultSecondary(
            int faultNormalCount, Set<Integer> usedGroupSet, ObjectCounter<Long> normalDatanodeIdList,
            HashMap<Long, InstanceMetadata> instanceId2instanceMetadataMap, HashMap<Long, InstanceInfoImpl> instanceId2InstanceInfoImplMap,
            LinkedList<InstanceIdAndEndPoint_Thrift> selectedNormalList, ReserveVolumeCombination reserveVolumeCombination,
            ObjectCounter<Long> reservedSegmentUnitCounter) throws NotEnoughSpaceException_Thrift {
        LinkedList<InstanceIdAndEndPoint_Thrift> selectedFaultNormalList = new LinkedList<>();

        //remove data node which own group is already used, to reduce the number of cycles
        LinkedList<Long> faultNormalIdList = new LinkedList<>(normalDatanodeIdList.getAll());
        for (int usedGroupId : usedGroupSet){
            faultNormalIdList.removeIf(id -> instanceId2instanceMetadataMap.get(id).getGroup().getGroupId() == usedGroupId);
        }

        //shuffle normalIdList
        Collections.shuffle(faultNormalIdList);

        Set<Integer> mixGroupIdSet = reserveVolumeCombination.getMixGroupIdSet();
        int usableMixGroupCountInSameSegment = reserveVolumeCombination.getUsableMixGroupCountPerOneSegment();

        Set<Integer> usedMixGroupIdSet = new HashSet<>();
        for (InstanceIdAndEndPoint_Thrift instanceImpl : selectedNormalList){
            usedMixGroupIdSet.add(instanceImpl.getGroupId());
        }
        usedMixGroupIdSet.retainAll(mixGroupIdSet);
        int usableMixGroupCountThisTime = usableMixGroupCountInSameSegment - usedMixGroupIdSet.size();
        int currentMixGroupUsedCount = 0;

        for (Long faultNormalId : faultNormalIdList) {
            if (selectedFaultNormalList.size() >= faultNormalCount) {
                break;
            }
            //if simple datanode is not enough , arbiter can create at normal datanode
            InstanceMetadata normalDatanodeTemp = instanceId2instanceMetadataMap.get(faultNormalId);
            long instanceId = normalDatanodeTemp.getInstanceId().getId();

            //verify mixed group
            if (mixGroupIdSet.contains(normalDatanodeTemp.getGroup().getGroupId())) {
                //when segment unit in mix group, but mix group must used to be arbiter, this instance can not be used
                if (currentMixGroupUsedCount >= usableMixGroupCountThisTime) {
                    continue;
                }
                currentMixGroupUsedCount++;
            }

            InstanceInfoImpl instanceInfo = instanceId2InstanceInfoImplMap.get(instanceId);
            //has enough space to create segment unit
            if (!reserveVolumeCombination.canCreateOneMoreSegmentUnits(
                    instanceInfo, reservedSegmentUnitCounter)) {
                continue;
            }

            if (usedGroupSet.stream().noneMatch(id -> id == normalDatanodeTemp.getGroup().getGroupId())) {
                selectedFaultNormalList
                        .add(RequestResponseHelper.buildInstanceIdAndEndPointFrom(normalDatanodeTemp));

                usedGroupSet.add(normalDatanodeTemp.getGroup().getGroupId());
                reservedSegmentUnitCounter.increment(instanceId);
            }
        }

        if (selectedFaultNormalList.size() < faultNormalCount){
            logger.error("reserve fault normal segment unit failed! faultNormalCount:{}, selectedFaultNormalList:{}",
                    faultNormalCount, selectedFaultNormalList);
            throw new NotEnoughSpaceException_Thrift().setDetail("reserve fault normal segment unit failed!");
        }

        return selectedFaultNormalList;
    }

    /**
     *  reserve fault arbiter
     * @param faultArbiterCount will reserve arbiter count
     * @param usedGroupSet  has already used group set
     * @param simpleGroupSimpleDatanodeCounter  simple data node which in simple group
     * @param mixGroupSimpleDatanodeCounter     simple data node which in mix group
     * @param normalDatanodeIdForArbiterList    normal data node list
     * @param instanceId2instanceMetadataMap    all instance info
     * @return fault arbiter list
     */
    private LinkedList<InstanceIdAndEndPoint_Thrift> reserveFaultArbiter(int faultArbiterCount, Set<Integer> usedGroupSet,
                                ObjectCounter<Long> simpleGroupSimpleDatanodeCounter, ObjectCounter<Long> mixGroupSimpleDatanodeCounter,
                                ObjectCounter<Long> normalDatanodeIdForArbiterList, HashMap<Long, InstanceMetadata> instanceId2instanceMetadataMap){
        LinkedList<InstanceIdAndEndPoint_Thrift> selectedFaultArbitersList = new LinkedList<>();

        //remove data node which own group is already used, to reduce the number of cycles
        LinkedList<Long> simpleGroupSimpleIdForFaultArbiterList = new LinkedList<>(simpleGroupSimpleDatanodeCounter.getAll());
        for (int usedGroupId : usedGroupSet){
            simpleGroupSimpleIdForFaultArbiterList.removeIf(id -> instanceId2instanceMetadataMap.get(id).getGroup().getGroupId() == usedGroupId);
        }

        LinkedList<Long> mixGroupSimpleIdForFaultArbiterList = new LinkedList<>(mixGroupSimpleDatanodeCounter.getAll());
        for (int usedGroupId : usedGroupSet){
            mixGroupSimpleIdForFaultArbiterList.removeIf(id -> instanceId2instanceMetadataMap.get(id).getGroup().getGroupId() == usedGroupId);
        }

        LinkedList<Long> normalIdForFaultArbiterList = new LinkedList<>(normalDatanodeIdForArbiterList.getAll());
        for (int usedGroupId : usedGroupSet){
            normalIdForFaultArbiterList.removeIf(id -> instanceId2instanceMetadataMap.get(id).getGroup().getGroupId() == usedGroupId);
        }

        //shuffle simpleIdList
        Collections.shuffle(simpleGroupSimpleIdForFaultArbiterList);
        //shuffle simpleIdList
        Collections.shuffle(mixGroupSimpleIdForFaultArbiterList);
        //shuffle normalIdList
        Collections.shuffle(normalIdForFaultArbiterList);

        Iterator<Long> simpleGroupSimpleIdListForFaultItor = simpleGroupSimpleIdForFaultArbiterList.iterator();
        Iterator<Long> mixGroupSimpleIdListForFaultItor = mixGroupSimpleIdForFaultArbiterList.iterator();
        Iterator<Long> normalIdListForFaultArbiterItor = normalIdForFaultArbiterList.iterator();

        while (selectedFaultArbitersList.size() < faultArbiterCount) {
            if (simpleGroupSimpleIdListForFaultItor.hasNext()) {
                //arbiter priority selection simple datanode to be created
                InstanceMetadata simpleDatanodeTemp = instanceId2instanceMetadataMap.get(simpleGroupSimpleIdListForFaultItor.next());
                if (usedGroupSet.stream().noneMatch(id -> id == simpleDatanodeTemp.getGroup().getGroupId())) {
                    logger.info("reserveVolume from simple datanode: {}", simpleDatanodeTemp);
                    selectedFaultArbitersList
                            .add(RequestResponseHelper.buildInstanceIdAndEndPointFrom(simpleDatanodeTemp));
                    usedGroupSet.add(simpleDatanodeTemp.getGroup().getGroupId());
                }
            } else if (mixGroupSimpleIdListForFaultItor.hasNext()) {
                //arbiter priority selection simple datanode to be created
                InstanceMetadata simpleDatanodeTemp = instanceId2instanceMetadataMap.get(mixGroupSimpleIdListForFaultItor.next());
                if (usedGroupSet.stream().noneMatch(id -> id == simpleDatanodeTemp.getGroup().getGroupId())) {
                    logger.info("reserveVolume from simple datanode: {}", simpleDatanodeTemp);
                    selectedFaultArbitersList
                            .add(RequestResponseHelper.buildInstanceIdAndEndPointFrom(simpleDatanodeTemp));
                    usedGroupSet.add(simpleDatanodeTemp.getGroup().getGroupId());
                }
            } else if (normalIdListForFaultArbiterItor.hasNext()) {
                //if simple datanode is not enough , arbiter can create at normal datanode
                InstanceMetadata normalDatanodeTemp = instanceId2instanceMetadataMap.get(normalIdListForFaultArbiterItor.next());
                if (usedGroupSet.stream().noneMatch(id -> id == normalDatanodeTemp.getGroup().getGroupId())) {
                    selectedFaultArbitersList
                            .add(RequestResponseHelper.buildInstanceIdAndEndPointFrom(normalDatanodeTemp));
                    usedGroupSet.add(normalDatanodeTemp.getGroup().getGroupId());
                }
            } else {
                break;
            }
        }
        return selectedFaultArbitersList;
    }

    /**
     * Calculate segment unit count will be create, normal segment unit will be the priority. Such as:
     * if has fault:
     * PSA:
     * 1.if group like {S, S, S, M}, exception will be cause
     * 2.if has enough Group, it will create 4 normal segment unit and 2 arbiter segment unit
     * 3.if group like {S, S, M, M}, it will create 2 normal and 2 arbiter
     * 4.if group like {S, M, M, N}, it will create 3 normal and 1 arbiter
     * 5.if group like {M, M, M, N}, it will create 3 normal and 1 arbiter
     *
     * @param simpleDatanodeGroupIdSet group which has simpleDatanode
     * @param normalDatanodeGroupIdSet group which has normalDatanode
     * @param allGroupSet              all group
     * @param volumeType               volumeType
     * @param faultTolerant            is fault tolerant
     * @return arbiter counter and normal counter
     * @throws NotEnoughGroupException_Thrift exception
     */
    private Map<SegmentUnitType_Thrift, Integer> calculateSegmentUnitWillBeCreateCount(
            Set<Integer> simpleDatanodeGroupIdSet, Set<Integer> normalDatanodeGroupIdSet, Set<Group> allGroupSet,
            VolumeType volumeType, boolean faultTolerant)
            throws NotEnoughGroupException_Thrift, NotEnoughNormalGroupException_Thrift {
        int numberOfMembersPerSegment = volumeType.getNumMembers(); // including normal segment units and arbiters
        int numberOfArbitersPerSegment = volumeType.getNumArbiters(); // arbiters
        int numberOfNormalPerSegment = numberOfMembersPerSegment - numberOfArbitersPerSegment;

        //verify group count
        if (allGroupSet.size() < numberOfMembersPerSegment) {
            logger.error(
                    "Groups not enough! Groups: {} count: {} less than expected least instance number: {} for segment",
                    allGroupSet, allGroupSet.size(), numberOfMembersPerSegment);
            throw new NotEnoughGroupException_Thrift().setMinGroupsNumber(numberOfMembersPerSegment);
        } else if (normalDatanodeGroupIdSet.size() < numberOfNormalPerSegment) {
            logger.error(
                    "Normal groups not enough! Normal Groups: {} count: {} less than expected least normal instance counter: {} ",
                    normalDatanodeGroupIdSet, normalDatanodeGroupIdSet.size(), numberOfNormalPerSegment);
            throw new NotEnoughNormalGroupException_Thrift().setMinGroupsNumber(numberOfNormalPerSegment);

        }

        //calculate segment unit count will be create
        int numberOfNormalWillReservePerSegment = numberOfNormalPerSegment;
        int numberOfArbiterWillReservePerSegment = numberOfArbitersPerSegment;
        int faultNormalCountPerSegment = numberOfNormalPerSegment + FAULT_TOLERANT_NORMAL_INSTANCE_COUNT_MAX;
        int faultArbiterCountPerSegment = numberOfArbitersPerSegment + FAULT_TOLERANT_SIMPLE_INSTANCE_COUNT_MAX;
        if (numberOfArbiterWillReservePerSegment == 0) {
            faultArbiterCountPerSegment = 0;
        }

        Set<Integer> simpleDatanodeGroupIdSetBac = new HashSet<>(simpleDatanodeGroupIdSet);
        Set<Integer> normalDatanodeGroupIdSetBac = new HashSet<>(normalDatanodeGroupIdSet);

        //if has faultTolerant, calculate normal or arbiter segment unit count that will be create
        if (faultTolerant) {
            HashSet<Integer> mixedGroupIdSet = new HashSet<>();     //group which contains simple datanode and normal datanode
            mixedGroupIdSet.addAll(simpleDatanodeGroupIdSetBac);
            mixedGroupIdSet.retainAll(normalDatanodeGroupIdSetBac);
            simpleDatanodeGroupIdSetBac.removeAll(mixedGroupIdSet);    //group which only contains simple datanode
            normalDatanodeGroupIdSetBac.removeAll(mixedGroupIdSet);    //group which only contains normal datanode

            int allGroupSize = allGroupSet.size();

            //store group type in groupModelQueue, like {S, S, S, M, M, M, N, N, N}
            Deque<GroupFlag> groupModelQueue = new ArrayDeque<>();

            //order Group for calculate
            for (int i = 0; i < allGroupSize; i++) {
                if (i < simpleDatanodeGroupIdSetBac.size()) {
                    groupModelQueue.addLast(GroupFlag.SIMPLE);
                } else if (i < mixedGroupIdSet.size()) {
                    groupModelQueue.addLast(GroupFlag.MIXED);
                } else {
                    groupModelQueue.addLast(GroupFlag.NORMAL);
                }
            }

            //distribute arbiter that must be required
            for (int i = 0; i < numberOfArbiterWillReservePerSegment; i++) {
                groupModelQueue.pollFirst();
            }
            for (int i = 0; i < numberOfNormalWillReservePerSegment; i++) {
                groupModelQueue.pollLast();
            }

            while (groupModelQueue.size() > 0) {
                //fault normal segment unit and fault arbiter segment unit all be created, break;
                if (numberOfNormalWillReservePerSegment >= faultNormalCountPerSegment
                        && numberOfArbiterWillReservePerSegment >= faultArbiterCountPerSegment) {
                    break;
                }

                GroupFlag lastGroup = groupModelQueue.pollLast();
                if (lastGroup == GroupFlag.SIMPLE) {
                    //simple datanode can only be created arbiter segment unit,
                    if (numberOfArbiterWillReservePerSegment < faultArbiterCountPerSegment) {
                        numberOfArbiterWillReservePerSegment++;
                    } else {
                        break;
                    }
                } else {
                    //crate normal fault datanode priority, then create arbiter
                    if (numberOfNormalWillReservePerSegment < faultNormalCountPerSegment) {
                        numberOfNormalWillReservePerSegment++;
                    } else if (numberOfArbiterWillReservePerSegment < faultArbiterCountPerSegment) {
                        numberOfArbiterWillReservePerSegment++;
                    }
                }
            }
        }

        Map<SegmentUnitType_Thrift, Integer> returnMap = new HashMap<>();
        returnMap.put(SegmentUnitType_Thrift.Arbiter, numberOfArbiterWillReservePerSegment);
        returnMap.put(SegmentUnitType_Thrift.Normal, numberOfNormalWillReservePerSegment);
        return returnMap;
    }

    private boolean canCreateOneMoreSegmentUnits(InstanceInfoImpl instanceInfo, boolean isSimpleConfiguration) {
        if (isSimpleConfiguration) {
            return instanceInfo.getFreeFlexibleSegmentUnitCount() >= 1;
        } else {
            return instanceInfo.getFreeSpace() >= segmentSize;
        }
    }

    /**
     * reserve a passel of segment units.
     *
     * @param numOfSegmentsWeWillReserve segment count
     * @param membersCountPerSegment     segment unit count per segment
     * @return instances with count, added up to be the segment units' count
     * @throws NotEnoughSpaceException_Thrift if we don't have enough space for the request
     */
    @Deprecated
    ObjectCounter<InstanceId> reserveNormalSegmentUnits(int numOfSegmentsWeWillReserve, int membersCountPerSegment,
            boolean isSimpleConfiguration) throws NotEnoughSpaceException_Thrift {
        ObjectCounter<Integer> groupIdCounter = new TreeSetObjectCounter<>(); // counter of group
        ObjectCounter<InstanceId> instanceIdCounter = new TreeSetObjectCounter<>(); // counter of instance
        // if we keep allocate segment units on overloaded instances, this passel of segment units can't all lay on
        // different disks. So we will try not to chose from an overloaded instance.
        TreeSet<InstanceInfoImpl> overloadedInstanceSet = new TreeSet<>();
        // for the excepted instance, we will remove it temporarily. They will be added back in the end
        List<InstanceInfoImpl> exceptedInstances = new ArrayList<>();
        for (int i = 0; i < numOfSegmentsWeWillReserve; i++) {
            for (int j = 0; j < membersCountPerSegment; j++) {
                while (true) {
                    boolean overloaded = false;
                    // poll out the first element with the minimum pressure from a sorted set.
                    InstanceInfoImpl instance = instanceInfoSet.pollFirst();
                    if (instance == null) {
                        instance = overloadedInstanceSet.pollFirst();
                        overloaded = true;
                        if (instance == null) {
                            throw new NotEnoughSpaceException_Thrift();
                        }
                    }
                    if (!canCreateOneMoreSegmentUnits(instance, isSimpleConfiguration)) {
                        // not enough space in this instance
                        logger.warn("no enough space left in {}", instance);
                        continue;
                    }

                    int groupReservedCount = (int) groupIdCounter.get(instance.getGroupId());
                    if (groupReservedCount == numOfSegmentsWeWillReserve) {
                        // we have too much instance on the same group
                        logger.debug("we have too much instance in group {}", instance);
                        exceptedInstances.add(instance);
                        continue;
                    } else {
                        Validate.isTrue(groupReservedCount < numOfSegmentsWeWillReserve);
                    }

                    int instanceReservedCount = (int) instanceIdCounter.get(instance.getInstanceId());
                    if (!overloaded && instanceReservedCount > instance.getDiskCount()) {
                        // segment units can't lay on different disks inside a segment wrapper if more segments
                        // allocated on this instance
                        logger.debug("instance overloaded {}", instance);
                        overloadedInstanceSet.add(instance);
                        exceptedInstances.add(instance);
                        continue;
                    }

                    // add a bogus segment unit to the instance info, and its pressure will change.
                    // then add the instance back to the sorted set and it will be put into a proper place.
                    if (isSimpleConfiguration) {
                        instance.addABogusFlexibleSegmentUnit();
                    } else {
                        instance.addABogusSegmentUnit();
                    }
                    groupIdCounter.increment(instance.getGroupId());
                    instanceIdCounter.increment(instance.getInstanceId());
                    if (overloaded) {
                        overloadedInstanceSet.add(instance);
                    } else {
                        instanceInfoSet.add(instance);
                    }
                    break;
                }
            }
        }
        instanceInfoSet.addAll(exceptedInstances);
        return instanceIdCounter;
    }

    /**
     * distribute a passel of instances into each segment, and ensure that any two of segment units in the same segment
     * don't belong to the same group.
     */
    @Deprecated
    Map<Integer, LinkedList<InstanceIdAndEndPoint_Thrift>> distributeInstanceIntoEachSegment(
            int numOfSegmentsWeReserved, int numberOfNormalSegmentUnitsPerSegment,
            ObjectCounter<InstanceId> normalHosts) throws NotEnoughSpaceException_Thrift {

        ObjectCounter<Integer> groupCounter = new TreeSetObjectCounter<>();

        // a map from segment index to a list of instances to return to client
        Map<Integer, LinkedList<InstanceIdAndEndPoint_Thrift>> segIndex2Instances = new HashMap<>();
        for (InstanceId instanceId : normalHosts.getAll()) {
            InstanceMetadata instance = storageStore.get(instanceId.getId());
            groupCounter.increment(instance.getGroup().getGroupId(), normalHosts.get(instanceId));
        }

        for (int segmentIndex = 0; segmentIndex < numOfSegmentsWeReserved; segmentIndex++) {
            logger.debug("selecting for segment {} {}", segmentIndex, normalHosts);
            LinkedList<InstanceIdAndEndPoint_Thrift> instanceListToReturn = new LinkedList<>();
            segIndex2Instances.put(segmentIndex, instanceListToReturn);

            // we will always pick up an instance from group with most instances left
            Iterator<Integer> groupIterator = groupCounter.descendingIterator();
            // store the selected groups in a set, and decrement all of them in the end
            Set<Integer> groupIdSet = new HashSet<>();

            logger.debug("group counter {}", groupCounter);
            Set<InstanceId> segmentUnitInstanceIdSet = new HashSet<>();
            for (int j = 0; j < numberOfNormalSegmentUnitsPerSegment; j++) {
                int groupId = groupIterator.next();

                Iterator<InstanceId> instanceIterator = normalHosts.descendingIterator();
                while (instanceIterator.hasNext()) {
                    InstanceId id = instanceIterator.next();
                    // if we didn't pick up a proper instance until coming to an negative value, it means there must be
                    // some group with a count larger than segment count. And that is a bug in the previous step (most
                    // likely to be reserveSegmentUnits).
                    Validate.isTrue(normalHosts.get(id) > 0);
                    InstanceMetadata instance = storageStore.get(id.getId());
                    if (instance.getGroup().getGroupId() != groupId) {
                        logger.debug("not suitable {} g {}", id, instance.getGroup().getGroupId());
                        continue;
                    }
                    Validate.notNull(instance);
                    normalHosts.decrement(instance.getInstanceId());
                    segmentUnitInstanceIdSet.add(instance.getInstanceId());
                    groupIdSet.add(instance.getGroup().getGroupId());
                    logger.debug("got {} counter {}", instance.getInstanceId(), normalHosts);
                    break;
                }
            }
            for (Integer groupId : groupIdSet) {
                groupCounter.decrement(groupId);
            }
            logger.debug("s : {}", segmentUnitInstanceIdSet);
            for (InstanceId instanceId : segmentUnitInstanceIdSet) {
                instanceListToReturn.addLast(
                        RequestResponseHelper.buildInstanceIdAndEndPointFrom(storageStore.get(instanceId.getId())));
            }
        }
        return segIndex2Instances;
    }
}
