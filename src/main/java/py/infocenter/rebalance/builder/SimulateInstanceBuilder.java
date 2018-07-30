package py.infocenter.rebalance.builder;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.RawArchiveMetadata;
import py.common.counter.ObjectCounter;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePool;
import py.infocenter.rebalance.struct.SimulateInstanceInfo;
import py.infocenter.store.StorageStore;
import py.instance.InstanceId;

import java.util.*;

/**
 * Created by zcy on 18-7-25.
 */
public class SimulateInstanceBuilder {
    private static final Logger logger = LoggerFactory.getLogger(SimulateInstanceBuilder.class);

    private StoragePool storagePool;
    private StorageStore storageStore;
    private long segmentSize;
    private Map<Long, SimulateInstanceInfo> instanceId2SimulateInstanceMap = new HashMap<>(); //all instance info (include simple and normal)

    public SimulateInstanceBuilder(StoragePool storagePool, StorageStore storageStore, long segmentSize){
        this.storagePool = storagePool;
        this.storageStore = storageStore;
        this.segmentSize = segmentSize;
    }

    public StoragePool getStoragePool() {
        return storagePool;
    }

    public void setStoragePool(StoragePool storagePool) {
        this.storagePool = storagePool;
    }

    public StorageStore getStorageStore() {
        return storageStore;
    }

    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    public long getSegmentSize() {
        return segmentSize;
    }

    public void setSegmentSize(long segmentSize) {
        this.segmentSize = segmentSize;
    }

    public Map<Long, SimulateInstanceInfo> getInstanceId2SimulateInstanceMap() {
        return instanceId2SimulateInstanceMap;
    }

    /**
     * get simulation instance information
     * 1.datanode status must ok
     * 2.datanode has archive
     *
     * @return  list of simulation instance
     */
    public SimulateInstanceBuilder collectionInstance(){
        List<InstanceMetadata> instances = storageStore.list();
        logger.info("build instanceInfoSet, storageStore.list:{} storagePool.getArchivesInDataNode:{}",
                instances, storagePool.getArchivesInDataNode());
        for (InstanceMetadata instance : instances) {
            if (instance.getDatanodeStatus() != InstanceMetadata.DatanodeStatus.OK ||
                    storagePool.getArchivesInDataNode().get(instance.getInstanceId().getId()).isEmpty()) {
                logger.warn("a instance:{} cannot add in instanceMap", instance.getInstanceId());
                continue;
            }

            InstanceId instanceId = instance.getInstanceId();

            long freeSpace = 0;
            int datanodeWeight = 0;
            int freeFlexibleSegmentUnitCount = 0;
            List<Long> archiveIds = new ArrayList<>();
            for (Long archiveId : storagePool.getArchivesInDataNode().get(instanceId.getId())) {
                RawArchiveMetadata archive = instance.getArchiveById(archiveId);
                if (archive != null) {
                    freeSpace += archive.getLogicalFreeSpace();
                    freeFlexibleSegmentUnitCount += archive.getFreeFlexibleSegmentUnitCount();
                }
                archiveIds.add(archiveId);
                datanodeWeight += archive.getWeight();
            }

            if (datanodeWeight == 0 || freeSpace == 0){
                logger.warn("instance:{} has free space:{}, weight:{}", instanceId.getId(), freeSpace, datanodeWeight);
            }

           SimulateInstanceInfo simulateInstanceInfo = new SimulateInstanceInfo(instanceId, archiveIds,
                    instance.getGroup().getGroupId(), freeSpace, freeFlexibleSegmentUnitCount, segmentSize, datanodeWeight);

            instanceId2SimulateInstanceMap.put(instanceId.getId(), simulateInstanceInfo);
        }

        return this;
    }

    /**
     * calculate already reserved segment unit percent
     *
     * 1. calculate the max segment unit count by weight that node can reserve
     * 2. reservedPercent = already reserved count / max count
     *
     * @param instanceId    instance id
     * @param alreadyDistributeCount    the segment unit count that already reserved count
     * @param reserveSegmentCount       all segment count that will be reserved
     * @return  reserved percent
     */
    private double calcReserveProportion(long instanceId, long alreadyDistributeCount, int reserveSegmentCount, int totalWeight){
        SimulateInstanceInfo simulateInstanceInfo = instanceId2SimulateInstanceMap.get(instanceId);
        if (simulateInstanceInfo == null){
            return 0;
        }
        //1.Calculate the max count that should be allocated according to the weight(weight/totalWeight*reserveCount)
        int maxReserveCount = simulateInstanceInfo.getWeight()*reserveSegmentCount/totalWeight;
        if (simulateInstanceInfo.getWeight()*reserveSegmentCount % totalWeight != 0){
            maxReserveCount += 1;
        }

        //2.Calculate the allocated proportion
        double reserveWeightPercent = (double)(alreadyDistributeCount)/(double)maxReserveCount;
        return reserveWeightPercent;
    }

    /**
     *  get primary priority list
     *  The following conditions must be followed：
     *  1. The datanode is sorted according to P distribution with datanode wight, with the lowest distribution in the front
     *  2. A random arrangement cause when the above conditions are equally
     * @param instanceIdList all can be used datanode list
     * @param primaryDistributeCounter  datanode counter that already distribute as P
     * @param volumeSegmentCount    volume reserved segment count
     * @return  primary selection list, the highest priority is the front, and lowest is the behind
     */
    public LinkedList<Long> getPrimaryPriorityList(LinkedList<Long> instanceIdList, ObjectCounter<Long> primaryDistributeCounter,
                                                   int volumeSegmentCount){
        LinkedList<Long> priorityList = new LinkedList<>();

        if (instanceIdList.size() <= 1){
            priorityList.addAll(instanceIdList);
            return priorityList;
        }

        //calculate total weight
        int totalWeight = 0;
        for (long instanceId : instanceIdList){
            SimulateInstanceInfo simulateInstanceInfo = instanceId2SimulateInstanceMap.get(instanceId);
            totalWeight += simulateInstanceInfo.getWeight();
        }

        /*
         * in order of the unallocated proportion
         */
        Multimap<Double, Long> freeReserveWeightPercent2InsIdMap = HashMultimap.create();
        for (long instanceId : instanceIdList){
            double reserveWeightPercent = calcReserveProportion(instanceId, primaryDistributeCounter.get(instanceId),
                    volumeSegmentCount, totalWeight);
            freeReserveWeightPercent2InsIdMap.put(reserveWeightPercent, instanceId);
        }

        LinkedList<Double> freeReserveWeightPercentList = new LinkedList<>(freeReserveWeightPercent2InsIdMap.keySet());
        Collections.sort(freeReserveWeightPercentList);

        /*
         * 6. in order of weight
         */
        for (double freeReserveWeightPercent : freeReserveWeightPercentList){
            Multimap<Integer, Long> weight2InstanceIdMap = HashMultimap.create();
            for (long instanceId : freeReserveWeightPercent2InsIdMap.get(freeReserveWeightPercent)){
                weight2InstanceIdMap.put(instanceId2SimulateInstanceMap.get(instanceId).getWeight(), instanceId);
            }
            LinkedList<Integer> sortList = new LinkedList<>(weight2InstanceIdMap.keySet());
            Collections.sort(sortList);
            Collections.reverse(sortList);

            /*
             * Random ordering of the same freeReserveWeightPercent
             */
            for (int weight : sortList) {
                LinkedList<Long> shuffledList = new LinkedList<>(weight2InstanceIdMap.get(weight));
                Collections.shuffle(shuffledList);
                priorityList.addAll(shuffledList);
            }
        }

        return priorityList;
    }

    /**
     *  get secondary priority list
     *  The following conditions must be followed：
     *  1. The datanode is sorted according to the PS composite distribution with datanode wight, with the lowest distribution in the front
     *  2. In order of S distribution with datanode wight, the least comes first
     *  3. A random arrangement cause when all of the above conditions are equally
     *
     * @param instanceIdList    all can used instance, contains primary instance
     * @param primaryId     primary instance id
     * @param secondaryOfPrimaryCounter     the distribution of secondary when primary is primaryId
     * @param secondaryDistributeCounter    the distribution of secondary on all nodes
     * @param volumeSegmentCount            the volume segment count
     * @param secondaryCountPerSegment      secondary counter of segment(without redundency secondary)
     * @return  the secondary select list
     */
    public LinkedList<Long> getSecondaryPriorityList(LinkedList<Long> instanceIdList, long primaryId, ObjectCounter<Long> secondaryOfPrimaryCounter,
                                                     ObjectCounter<Long> secondaryDistributeCounter, int volumeSegmentCount, int secondaryCountPerSegment){
        LinkedList<Long> selSecondaryList = new LinkedList<>();

        if (instanceIdList.size() <= 1){
            selSecondaryList.addAll(instanceIdList);
            return selSecondaryList;
        }

        SimulateInstanceInfo primarySimulateInstanceInfo = instanceId2SimulateInstanceMap.get(primaryId);

        //calculate total weight
        int totalWeight = 0;
        for (long instanceId : instanceIdList){
            SimulateInstanceInfo simulateInstanceInfo = instanceId2SimulateInstanceMap.get(instanceId);
            if (simulateInstanceInfo == null){
                continue;
            }
            totalWeight += simulateInstanceInfo.getWeight();
        }
        int primaryWeight = primarySimulateInstanceInfo.getWeight();

        /*
         * 1.calculate the max count of primary that should be allocated according to the weight(weight/totalWeight*reserveCount)
         */
        int maxPrimaryReserveCount = primarySimulateInstanceInfo.getWeight()*volumeSegmentCount/totalWeight;
        if (primarySimulateInstanceInfo.getWeight()*volumeSegmentCount % totalWeight != 0){
            maxPrimaryReserveCount += 1;
        }

        /*
         * 2.get S max count when primary is primaryId
         */
        int maxSecondaryOfPrimaryReserveCount = maxPrimaryReserveCount * secondaryCountPerSegment;

        /*
         * 3.get S max count of volume
         */
        int maxSecondaryReserveCount = volumeSegmentCount * secondaryCountPerSegment;

        /*
         * 4.The datanode is sorted according to the PS composite distribution with datanode wight, with the lowest distribution in the front
         */
        Multimap<Double, Long> secondaryReservePercentOfPrimary2InstanceIdMap = HashMultimap.create();
        for (long instanceId : instanceIdList){
            double reserveWeightPercent = calcReserveProportion(instanceId, secondaryOfPrimaryCounter.get(instanceId),
                    maxSecondaryOfPrimaryReserveCount, totalWeight-primaryWeight);
            secondaryReservePercentOfPrimary2InstanceIdMap.put(reserveWeightPercent, instanceId);
        }
        LinkedList<Double> reserveWeightPercentList = new LinkedList<>(secondaryReservePercentOfPrimary2InstanceIdMap.keySet());
        Collections.sort(reserveWeightPercentList);

        for (double reservePercent : reserveWeightPercentList){
            /*
             * 5.In order of S distribution, the least comes first
             */
            Multimap<Double, Long> secondaryReservePercent2InstanceIdMap = HashMultimap.create();
            for (long instanceId : secondaryReservePercentOfPrimary2InstanceIdMap.get(reservePercent)){
                double reserveWeightPercent = calcReserveProportion(instanceId, secondaryDistributeCounter.get(instanceId),
                        maxSecondaryReserveCount, totalWeight);
                secondaryReservePercent2InstanceIdMap.put(reserveWeightPercent, instanceId);
            }

            LinkedList<Double> SReservePercentList = new LinkedList<>(secondaryReservePercent2InstanceIdMap.keySet());
            Collections.sort(SReservePercentList);

            /*
             * 6. in order of weight
             */
            for (double reserveOfSPercent : SReservePercentList){
                Multimap<Integer, Long> weight2InstanceIdMap = HashMultimap.create();
                for (long instanceId : secondaryReservePercent2InstanceIdMap.get(reserveOfSPercent)){
                    weight2InstanceIdMap.put(instanceId2SimulateInstanceMap.get(instanceId).getWeight(), instanceId);
                }
                LinkedList<Integer> sortList = new LinkedList<>(weight2InstanceIdMap.keySet());
                Collections.sort(sortList);
                Collections.reverse(sortList);

                /*
                 * 7.Random ordering of the same conditions
                 */
                for (int weight : sortList){
                    LinkedList<Long> shuffledList = new LinkedList<>(weight2InstanceIdMap.get(weight));
                    Collections.shuffle(shuffledList);
                    selSecondaryList.addAll(shuffledList);
                }
            }
        }

        return selSecondaryList;
    }

}
