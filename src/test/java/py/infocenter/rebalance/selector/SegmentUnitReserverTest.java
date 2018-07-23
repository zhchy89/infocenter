package py.infocenter.rebalance.selector;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.ArchiveStatus;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePool;
import py.infocenter.rebalance.builder.InstanceInfoCollectionBuilder;
import py.infocenter.rebalance.exception.TooManyTasksException;
import py.infocenter.rebalance.exception.VolumeCreatingException;
import py.infocenter.store.StorageStore;
import py.instance.Group;
import py.instance.InstanceId;
import py.test.TestBase;
import py.thrift.share.InstanceIdAndEndPoint_Thrift;
import py.thrift.share.NotEnoughGroupException_Thrift;
import py.thrift.share.NotEnoughSpaceException_Thrift;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;

public class SegmentUnitReserverTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(SegmentUnitReserverTest.class);

    private long segmentSize = 1L;
    private long archiveSize = 15L;

    private StorageStore storageStore;

    private StoragePool storagePool;
    private Map<InstanceId, Integer> mapInstanceIdToGroupId;

    private void buildStorageStoreAndStoragePool(Map<Integer, int[]> mapGroupIdToArchiveCountList) {
        storagePool = new StoragePool();
        storagePool.setPoolId(1L);
        Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, Long>create());
        mapInstanceIdToGroupId = new HashMap<>();
        storagePool.setArchivesInDataNode(archivesInDataNode);
        List<InstanceMetadata> instanceList = new ArrayList<>();
        long instanceIndex = 0;
        Map<Long, InstanceMetadata> instanceMap = new HashMap<>();
        for (Map.Entry<Integer, int[]> groupAndArchiveCountList : mapGroupIdToArchiveCountList.entrySet()) {
            Group group = new Group();
            group.setGroupId(groupAndArchiveCountList.getKey());
            for (Integer archiveCount : groupAndArchiveCountList.getValue()) {
                InstanceId instanceId = new InstanceId(instanceIndex);
                InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
                instanceMetadata.setGroup(group);
                mapInstanceIdToGroupId.put(instanceId, group.getGroupId());
                instanceMetadata.setCapacity(archiveCount * archiveSize);
                instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
                instanceMetadata.setDatanodeStatus(OK);
                List<RawArchiveMetadata> archives = new ArrayList<>();
                for (int i = 0; i < archiveCount; i++) {
                    RawArchiveMetadata archive = new RawArchiveMetadata();
                    archive.setArchiveId((long)i);
                    archive.setStatus(ArchiveStatus.GOOD);
                    archive.setStorageType(StorageType.SATA);
                    archive.setStoragePoolId(1L);
                    archive.setLogicalFreeSpace(archiveSize);
                    archives.add(archive);
                    archivesInDataNode.put(instanceIndex, (long) i);
                }
                instanceMetadata.setArchives(archives);
                instanceMetadata.setDomainId(1L);
                instanceList.add(instanceMetadata);
                instanceMap.put(instanceIndex, instanceMetadata);
                instanceIndex++;
            }
        }
        storageStore = mock(StorageStore.class);
        when(storageStore.list()).thenReturn(instanceList);
        doAnswer(invocation -> {
            long id = (long) invocation.getArguments()[0];
            return instanceMap.get(id);
        }).when(storageStore).get(anyLong());
    }

    private void testDistribution(Map<Integer, int[]> mapGroupToInstances, int numOfSegments,
            int numberOfNormalSegmentUnitsPerSegment, ObjectCounter<InstanceId> hosts)
            throws VolumeCreatingException, TooManyTasksException, NotEnoughSpaceException_Thrift {
        buildStorageStoreAndStoragePool(mapGroupToInstances);
        SegmentUnitReserver reserver = new SegmentUnitReserver(segmentSize, storageStore);
        reserver.updateInstanceInfo(
                new InstanceInfoCollectionBuilder(storagePool, storageStore, new ArrayList<>(), segmentSize)
                        .setValidateProcessingTaskAndVolumes(false).build());

        ObjectCounter<InstanceId> resultCounter = new TreeSetObjectCounter<>();
        Map<Integer, LinkedList<InstanceIdAndEndPoint_Thrift>> result = reserver
                .distributeInstanceIntoEachSegment(numOfSegments, numberOfNormalSegmentUnitsPerSegment,
                        hosts.deepCopy());
        for (List<InstanceIdAndEndPoint_Thrift> instanceIdAndEndPoint_thrifts : result.values()) {
            assertEquals(numberOfNormalSegmentUnitsPerSegment, instanceIdAndEndPoint_thrifts.size());
            Set<Integer> groupSet = new HashSet<>();
            for (InstanceIdAndEndPoint_Thrift instanceIdAndEndPoint_thrift : instanceIdAndEndPoint_thrifts) {
                assertTrue(groupSet.add(instanceIdAndEndPoint_thrift.getGroupId()));
                resultCounter.increment(new InstanceId(instanceIdAndEndPoint_thrift.getInstanceId()));
            }
        }
        for (InstanceId instanceId : resultCounter.getAll()) {
            assertEquals(hosts.get(instanceId), resultCounter.get(instanceId));
        }
    }

    @Test
    public void testDistributionCase5()
            throws TooManyTasksException, NotEnoughSpaceException_Thrift, VolumeCreatingException {
        Map<Integer, int[]> mapGroupIdToArchiveCountList = new HashMap<>();
        mapGroupIdToArchiveCountList.put(0, new int[] { 5, 5, 5 });
        mapGroupIdToArchiveCountList.put(1, new int[] { 5, 5 });
        mapGroupIdToArchiveCountList.put(2, new int[] { 5, 5 });
        mapGroupIdToArchiveCountList.put(3, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(4, new int[] { 5 });
        int numSegments = 15;
        int numSegUnitsPerSegment = 3;
        ObjectCounter<InstanceId> hosts = new TreeSetObjectCounter<>();
        hosts.set(new InstanceId(0L), 5);
        hosts.set(new InstanceId(1L), 5);
        hosts.set(new InstanceId(2L), 5);
        hosts.set(new InstanceId(3L), 5);
        hosts.set(new InstanceId(4L), 5);
        hosts.set(new InstanceId(5L), 5);
        hosts.set(new InstanceId(6L), 5);
        hosts.set(new InstanceId(7L), 5);
        hosts.set(new InstanceId(8L), 5);
        testDistribution(mapGroupIdToArchiveCountList, numSegments, numSegUnitsPerSegment, hosts);
    }

    @Test
    public void testDistributionCase4()
            throws TooManyTasksException, NotEnoughSpaceException_Thrift, VolumeCreatingException {
        Map<Integer, int[]> mapGroupIdToArchiveCountList = new HashMap<>();
        mapGroupIdToArchiveCountList.put(0, new int[] { 5, 5, 5 });
        mapGroupIdToArchiveCountList.put(1, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(2, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(3, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(4, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(5, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(6, new int[] { 5 });
        int numSegments = 15;
        int numSegUnitsPerSegment = 3;
        ObjectCounter<InstanceId> hosts = new TreeSetObjectCounter<>();
        hosts.set(new InstanceId(0L), 5);
        hosts.set(new InstanceId(1L), 5);
        hosts.set(new InstanceId(2L), 5);
        hosts.set(new InstanceId(3L), 5);
        hosts.set(new InstanceId(4L), 5);
        hosts.set(new InstanceId(5L), 5);
        hosts.set(new InstanceId(6L), 5);
        hosts.set(new InstanceId(7L), 5);
        hosts.set(new InstanceId(8L), 5);
        testDistribution(mapGroupIdToArchiveCountList, numSegments, numSegUnitsPerSegment, hosts);
    }

    @Test
    public void testDistributionCase3()
            throws TooManyTasksException, NotEnoughSpaceException_Thrift, VolumeCreatingException {
        Map<Integer, int[]> mapGroupIdToArchiveCountList = new HashMap<>();
        mapGroupIdToArchiveCountList.put(0, new int[] { 5, 5 });
        mapGroupIdToArchiveCountList.put(1, new int[] { 5, 5 });
        mapGroupIdToArchiveCountList.put(2, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(3, new int[] { 5 });
        int numSegments = 10;
        int numSegUnitsPerSegment = 3;
        ObjectCounter<InstanceId> hosts = new TreeSetObjectCounter<>();
        hosts.set(new InstanceId(0L), 5);
        hosts.set(new InstanceId(1L), 5);
        hosts.set(new InstanceId(2L), 5);
        hosts.set(new InstanceId(3L), 5);
        hosts.set(new InstanceId(4L), 5);
        hosts.set(new InstanceId(5L), 5);
        testDistribution(mapGroupIdToArchiveCountList, numSegments, numSegUnitsPerSegment, hosts);
    }

    @Test
    public void testDistributionCase2()
            throws TooManyTasksException, NotEnoughSpaceException_Thrift, VolumeCreatingException {
        Map<Integer, int[]> mapGroupIdToArchiveCountList = new HashMap<>();
        mapGroupIdToArchiveCountList.put(0, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(1, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(2, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(3, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(4, new int[] { 5 });
        int numSegments = 10;
        int numSegUnitsPerSegment = 5;
        ObjectCounter<InstanceId> hosts = new TreeSetObjectCounter<>();
        hosts.set(new InstanceId(0L), numSegments);
        hosts.set(new InstanceId(1L), numSegments);
        hosts.set(new InstanceId(2L), numSegments);
        hosts.set(new InstanceId(3L), numSegments);
        hosts.set(new InstanceId(4L), numSegments);
        testDistribution(mapGroupIdToArchiveCountList, numSegments, numSegUnitsPerSegment, hosts);
    }

    @Test
    public void testDistributionCase1()
            throws TooManyTasksException, NotEnoughSpaceException_Thrift, VolumeCreatingException {
        Map<Integer, int[]> mapGroupIdToArchiveCountList = new HashMap<>();
        mapGroupIdToArchiveCountList.put(0, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(1, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(2, new int[] { 5 });
        int numSegments = 10;
        int numSegUnitsPerSegment = 3;
        ObjectCounter<InstanceId> hosts = new TreeSetObjectCounter<>();
        hosts.set(new InstanceId(0L), numSegments);
        hosts.set(new InstanceId(1L), numSegments);
        hosts.set(new InstanceId(2L), numSegments);
        testDistribution(mapGroupIdToArchiveCountList, numSegments, numSegUnitsPerSegment, hosts);
    }

    private void testReserveSegmentUnits(Map<Integer, int[]> mapGroupToInstances, int numSegments, int segmentWrapSize,
            int numSegmentUnitsPerSegment, boolean full, boolean averageDistributionInsideGroup)
            throws VolumeCreatingException, TooManyTasksException, NotEnoughGroupException_Thrift,
            NotEnoughSpaceException_Thrift {
        buildStorageStoreAndStoragePool(mapGroupToInstances);
        Map<Integer, ObjectCounter<InstanceId>> mapGroupToInstanceCounter = new HashMap<>();
        for (Integer group : mapGroupToInstances.keySet()) {
            mapGroupToInstanceCounter.put(group, new TreeSetObjectCounter<>());
        }
        SegmentUnitReserver reserver = new SegmentUnitReserver(segmentSize, storageStore);
        reserver.updateInstanceInfo(
                new InstanceInfoCollectionBuilder(storagePool, storageStore, new ArrayList<>(), segmentSize)
                        .setValidateProcessingTaskAndVolumes(false).build());
        ObjectCounter<InstanceId> overallCounter = new TreeSetObjectCounter<>();
        for (int i = 0; i < numSegments / segmentWrapSize; i++) {
            ObjectCounter<InstanceId> normalSegmentUnits = reserver
                    .reserveNormalSegmentUnits(segmentWrapSize, numSegmentUnitsPerSegment, false);
            assertEquals(segmentWrapSize * numSegmentUnitsPerSegment, normalSegmentUnits.total());
            for (InstanceId instanceId : normalSegmentUnits.getAll()) {
                overallCounter.increment(instanceId, normalSegmentUnits.get(instanceId));
                mapGroupToInstanceCounter.get(mapInstanceIdToGroupId.get(instanceId))
                                         .increment(instanceId, normalSegmentUnits.get(instanceId));
            }
        }

        if (averageDistributionInsideGroup) {
            for (ObjectCounter<InstanceId> instanceIdObjectCounter : mapGroupToInstanceCounter.values()) {
                List<Long> counts = new ArrayList<>();
                for (InstanceId instanceId : instanceIdObjectCounter.getAll()) {
                    counts.add(instanceIdObjectCounter.get(instanceId));
                }
                logger.warn("counts {}", counts);
                assertTrue(validateAverageDistribution(counts));
            }
        }
        for (InstanceId instanceId : overallCounter.getAll()) {
            if (full) {
                assertEquals(overallCounter.get(instanceId), storageStore.get(instanceId.getId()).getFreeSpace());
            } else {
                assertTrue(overallCounter.get(instanceId) <= storageStore.get(instanceId.getId()).getFreeSpace());
            }
        }
        logger.warn("result {}", overallCounter);
    }

    private boolean validateAverageDistribution(Collection<Long> values) {
        AtomicLong sum = new AtomicLong();
        values.forEach(sum::addAndGet);
        long average = sum.get() / values.size();
        return values.stream().allMatch(val -> Math.abs(val - average) <= 1);
    }

    // case 6, 12 instances, 5 groups, and 20 archives each, reserving for max capacity, 3 normal segUnits and 2 arbiters
    @Test
    public void testReserveVolumeCase5()
            throws VolumeCreatingException, TooManyTasksException, NotEnoughGroupException_Thrift,
            NotEnoughSpaceException_Thrift {
        Map<Integer, int[]> mapGroupIdToArchiveCountList = new HashMap<>();
        mapGroupIdToArchiveCountList.put(0, new int[] { 20, 20, 20 });
        mapGroupIdToArchiveCountList.put(1, new int[] { 20, 20, 20 });
        mapGroupIdToArchiveCountList.put(2, new int[] { 20, 20 });
        mapGroupIdToArchiveCountList.put(3, new int[] { 20, 20 });
        mapGroupIdToArchiveCountList.put(4, new int[] { 20, 20 });
        int numSegments = (int) (4 * 20 * archiveSize);
        int segmentWrapSize = 10;
        testReserveSegmentUnits(mapGroupIdToArchiveCountList, numSegments, segmentWrapSize, 3, true, true);
    }

    //     case 5, 12 instances, 5 groups, and random archives each, with arbiters
    @Test
    public void testReserveVolumeCase4()
            throws VolumeCreatingException, TooManyTasksException, NotEnoughGroupException_Thrift,
            NotEnoughSpaceException_Thrift {
        Random archiveCountRandom = new Random(System.currentTimeMillis());
        Map<Integer, int[]> mapGroupIdToArchiveCountList = new HashMap<>();
        mapGroupIdToArchiveCountList.put(0,
                new int[] { archiveCountRandom.nextInt(5) + 3, archiveCountRandom.nextInt(5) + 3,
                        archiveCountRandom.nextInt(5) + 3 });
        mapGroupIdToArchiveCountList.put(1,
                new int[] { archiveCountRandom.nextInt(5) + 3, archiveCountRandom.nextInt(5) + 3,
                        archiveCountRandom.nextInt(5) + 3 });
        mapGroupIdToArchiveCountList
                .put(2, new int[] { archiveCountRandom.nextInt(5) + 3, archiveCountRandom.nextInt(5) + 3 });
        mapGroupIdToArchiveCountList
                .put(3, new int[] { archiveCountRandom.nextInt(5) + 3, archiveCountRandom.nextInt(5) + 3 });
        mapGroupIdToArchiveCountList
                .put(4, new int[] { archiveCountRandom.nextInt(5) + 3, archiveCountRandom.nextInt(5) + 3 });
        int numSegments = (int) (5 * archiveSize);
        int segmentWrapSize = 5;
        testReserveSegmentUnits(mapGroupIdToArchiveCountList, numSegments, segmentWrapSize, 3, false, false);
    }

    @Test
    public void testReserveVolumeCase3()
            throws VolumeCreatingException, TooManyTasksException, NotEnoughGroupException_Thrift,
            NotEnoughSpaceException_Thrift {
        Map<Integer, int[]> mapGroupIdToArchiveCountList = new HashMap<>();
        mapGroupIdToArchiveCountList.put(0, new int[] { 5, 5 });
        mapGroupIdToArchiveCountList.put(1, new int[] { 5, 5 });
        mapGroupIdToArchiveCountList.put(2, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(3, new int[] { 5 });
        buildStorageStoreAndStoragePool(mapGroupIdToArchiveCountList);
        int numSegments = (int) (10 * archiveSize);
        int segmentWrapSize = 5;
        testReserveSegmentUnits(mapGroupIdToArchiveCountList, numSegments, segmentWrapSize, 3, true, true);
    }

    @Test
    public void testReserveVolumeCase2()
            throws VolumeCreatingException, TooManyTasksException, NotEnoughGroupException_Thrift,
            NotEnoughSpaceException_Thrift {
        Map<Integer, int[]> mapGroupIdToArchiveCountList = new HashMap<>();
        mapGroupIdToArchiveCountList.put(0, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(1, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(2, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(3, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(4, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(5, new int[] { 5 });
        buildStorageStoreAndStoragePool(mapGroupIdToArchiveCountList);
        int numSegments = (int) (15 * archiveSize);
        int segmentWrapSize = 5;
        testReserveSegmentUnits(mapGroupIdToArchiveCountList, numSegments, segmentWrapSize, 2, true, true);
    }

    @Test
    public void testReserveVolumeCase1()
            throws VolumeCreatingException, TooManyTasksException, NotEnoughGroupException_Thrift,
            NotEnoughSpaceException_Thrift {
        Map<Integer, int[]> mapGroupIdToArchiveCountList = new HashMap<>();
        mapGroupIdToArchiveCountList.put(0, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(1, new int[] { 5 });
        mapGroupIdToArchiveCountList.put(2, new int[] { 5 });
        int numSegments = (int) (5 * archiveSize);
        int segmentWrapSize = 1;
        testReserveSegmentUnits(mapGroupIdToArchiveCountList, numSegments, segmentWrapSize, 3, true, true);
    }

}