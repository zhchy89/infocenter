package py.infocenter.rebalance;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Level;
import org.apache.thrift.TException;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.test.web.servlet.RequestBuilder;
import py.archive.RawArchiveMetadata;
import py.archive.segment.*;
import py.common.RequestIdBuilder;
import py.common.client.RequestResponseHelper;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePool;
import py.icshare.StoragePoolStore;
import py.icshare.StoragePoolStoreImpl;
import py.icshare.exception.AccessDeniedException;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.store.MemoryVolumeStoreImpl;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.infocenter.test.utils.StorageMemStore;
import py.instance.Group;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.rebalance.RebalanceTask;
import py.test.TestBase;
import py.thrift.share.*;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.icshare.InstanceMetadata.DatanodeType.NORMAL;
import static py.volume.VolumeMetadata.VolumeInAction.NULL;

public class InfoCenterRebalanceTest extends TestBase {

    private long segmentSize = 1;
    private int maxSegmentCountPerArchive = 5000;
    private StoragePool storagePool;
    private VolumeStore volumeStore;
    private StorageStore storageStore;
    private StoragePoolStore storagePoolStore;
    private SegmentUnitsDistributionManager distributionManager;
    private Map<InstanceId, ObjectCounter<Long>> mapInstanceIdToDiskCounter = new HashMap<>();
    private Random random = new Random(System.currentTimeMillis());
    private Timer timer = new Timer();
    private Semaphore semaphore = new Semaphore(0);

    private class StoragePoolMemStore extends StoragePoolStoreImpl {
        @Override
        public void saveStoragePoolToDB(StoragePool storagePool) {
            // not saving
        }

        @Override
        public StoragePool getStoragePoolFromDB(Long storagePoolId) throws SQLException, IOException {
            return null;
        }

        @Override
        public void reloadAllStoragePoolsFromDB() throws SQLException, IOException {
        }

        @Override
        public void deleteStoragePoolFromDB(Long storagePoolId) {
        }
    }

    private void buildEnv() {
        RebalanceConfiguration.getInstance().setPressureThreshold(0.01);
        RebalanceConfiguration.getInstance().setRebalanceTaskExpireTimeSeconds(10);
        volumeStore = new MemoryVolumeStoreImpl();
        storageStore = new StorageMemStore();
        storagePoolStore = new StoragePoolMemStore();
        distributionManager = new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
                storageStore, storagePoolStore);
        storagePool = new StoragePool(0L, 0L, "pool", null, null);
        storagePoolStore.saveStoragePool(storagePool);
    }

    private AtomicLong instanceIdIndex = new AtomicLong(0);

    private long generateArchiveId(long instanceId, int archiveIndex) {
        return instanceId * 1000 + archiveIndex;
    }

    private void addArchiveToExistingStorage(long instanceId, int archiveCount) {
        InstanceMetadata instance = storageStore.get(instanceId);
        List<RawArchiveMetadata> archives = instance.getArchives();
        int existingArchiveSize = archives.size();
        ObjectCounter<Long> diskCounter = mapInstanceIdToDiskCounter.get(new InstanceId(instanceId));
        for (int i = 0; i < archiveCount; i++) {
            RawArchiveMetadata archive = new RawArchiveMetadata();
            int archiveIndex = existingArchiveSize + i;
            long archiveId = generateArchiveId(instanceId, archiveIndex);
            archive.setArchiveId(archiveId);
            archive.setStoragePoolId(storagePool.getPoolId());
            archive.setLogicalFreeSpace(segmentSize * maxSegmentCountPerArchive);
            archive.setGroup(new Group((int) instanceId));
            archive.setInstanceId(new InstanceId(instanceId));
            archive.setDeviceName("sd" + archiveIndex);
            archive.setSerialNumber(instanceId + "sd" + archiveIndex);
            storagePool.addArchiveInDatanode(instanceId, archiveId);
            diskCounter.set(archiveId, 0);
            instance.getArchives().add(archive);
        }
        storageStore.save(instance);
    }

    private void addStorage(int count, int archiveCount) {
        long freeSpace = 0;
        for (int i = 0; i < count; i++) {
            InstanceId instanceId = new InstanceId(instanceIdIndex.incrementAndGet() << 32);
            ObjectCounter<Long> diskCounter = new TreeSetObjectCounter<>();
            mapInstanceIdToDiskCounter.put(instanceId, diskCounter);
            Group group = new Group((int)instanceIdIndex.get());
            InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
            instanceMetadata.setFreeSpace(freeSpace);
            instanceMetadata.setGroup(group);
            instanceMetadata.setDatanodeStatus(OK);
            instanceMetadata.setDatanodeType(NORMAL);
            List<RawArchiveMetadata> archives = new ArrayList<>();
            instanceMetadata.setArchives(archives);
            instanceMetadata.setDomainId(RequestIdBuilder.get());
            distributionManager.updateSimpleDatanodeInfo(instanceMetadata);
            storageStore.save(instanceMetadata);

            addArchiveToExistingStorage(instanceId.getId(), archiveCount);
        }
    }

    private AtomicInteger volumeIdIndexer = new AtomicInteger(0);

    private Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> createAVolume(long size, VolumeType volumeType)
            throws NotEnoughGroupException_Thrift,
            NotEnoughSpaceException_Thrift, NotEnoughNormalGroupException_Thrift, TException {
        ObjectCounter<InstanceId> diskIndexer = new TreeSetObjectCounter<>();
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> result = distributionManager
                .reserveVolume(size, volumeType, false, Integer.MAX_VALUE, storagePool.getPoolId());
        logger.warn("reserve result {}", result);
        VolumeMetadata volumeMetadata = new VolumeMetadata(volumeIdIndexer.incrementAndGet(), volumeIdIndexer.get(),
                size, segmentSize, volumeType, CacheType.MEMORY, 0L, storagePool.getPoolId());
        volumeMetadata.setSegmentWrappCount(10);
        for (Map.Entry<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> indexMapEntry : result
                .entrySet()) {
            int segIndex = indexMapEntry.getKey();
            SegId segId = new SegId(volumeIdIndexer.get(), segIndex);
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> map = indexMapEntry.getValue();
            List<InstanceIdAndEndPoint_Thrift> arbiterUnits = map.get(SegmentUnitType_Thrift.Arbiter);
            List<InstanceIdAndEndPoint_Thrift> segUnits = map.get(SegmentUnitType_Thrift.Normal);

            // build membership
            SegmentMembership membership;
            InstanceId primary = new InstanceId(segUnits.get(0).getInstanceId());
            List<InstanceId> secondaries = new ArrayList<>();
            for (int i = 1; i <= volumeType.getNumSecondaries(); i++) {
                secondaries.add(new InstanceId(segUnits.get(i).getInstanceId()));
            }
            List<InstanceId> arbiters = new ArrayList<>();
            for (int i = 0; i < volumeType.getNumArbiters(); i++) {
                arbiters.add(new InstanceId(arbiterUnits.get(i).getInstanceId()));
            }
            if (arbiters.isEmpty()) {
                membership = new SegmentMembership(primary, secondaries);
            } else {
                membership = new SegmentMembership(primary, secondaries, arbiters);
            }

            // build segment meta data
            SegmentMetadata segment = new SegmentMetadata(segId, segIndex);

            // build segment units
            for (InstanceId arbiter : membership.getArbiters()) {
                SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
                        SegmentUnitStatus.Arbiter, volumeType, SegmentUnitType.Arbiter, CacheType.MEMORY);
                InstanceId instanceId = new InstanceId(arbiter);
                InstanceMetadata instance = storageStore.get(instanceId.getId());
                diskIndexer.increment(instanceId);
                int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

                segmentUnitMetadata.setInstanceId(instanceId);
                segmentUnitMetadata.setArchiveId(instance.getArchives().get(diskIndex).getArchiveId());
                segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
            }

            Set<InstanceId> normalUnits = new HashSet<>(membership.getSecondaries());
            normalUnits.add(primary);
            for (InstanceId normalUnit : normalUnits) {
                SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
                        normalUnit.equals(primary) ? SegmentUnitStatus.Primary : SegmentUnitStatus.Secondary,
                        volumeType, SegmentUnitType.Normal, CacheType.MEMORY);
                InstanceId instanceId = new InstanceId(normalUnit);
                InstanceMetadata instance = storageStore.get(instanceId.getId());
                diskIndexer.increment(instanceId);
                int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

                segmentUnitMetadata.setInstanceId(instanceId);
                long archiveId = instance.getArchives().get(diskIndex).getArchiveId();
                segmentUnitMetadata.setArchiveId(archiveId);
                mapInstanceIdToDiskCounter.get(instanceId).increment(archiveId);
                segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
            }

            volumeMetadata.addSegmentMetadata(segment, membership);
        }
        volumeMetadata.setVolumeStatus(VolumeStatus.Available);
        volumeMetadata.setInAction(NULL);
        volumeStore.saveVolume(volumeMetadata);

        return result;
    }

    private boolean submitTask(AtomicInteger failCount, double ratioToExecute)
            throws NoNeedToRebalance, InterruptedException {
        RebalanceTask task;
        try {
            task = distributionManager.selectRebalanceTask(true);
            failCount.set(0);
        } catch (NoNeedToRebalance noNeedToRebalance) {
            if (failCount.getAndIncrement() > 20) {
                throw noNeedToRebalance;
            }
            Thread.sleep(1000);
            return submitTask(failCount, ratioToExecute);
        }

        if (random.nextInt(100) <= ratioToExecute * 100) { // task may not be executed
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try {
                        logger.warn("processing rebalance task {}", task.toSimpleString());
                        switch (task.getTaskType()) {
                        case PrimaryRebalance:
                            switchPrimary(task);
                            break;
                        case NormalRebalance:
                        case InsideRebalance:
                            migrateSegmentUnit(task);
                            break;
                        }
                    } catch (Throwable throwable) {
                        logger.error("throwable caught", throwable);
                    } finally {
                        semaphore.release();
                    }
                }
            }, 2 * 1000);
            return true;
        } else {
            return false;
        }
    }

    private void count(boolean validateBalanced, int pDiffCount, int sDiffCount, int diskDiffCount) throws AccessDeniedException {
        ObjectCounter<InstanceId> primaryCounter = new TreeSetObjectCounter<>();
        ObjectCounter<InstanceId> segUnitCounter = new TreeSetObjectCounter<>();
        ObjectCounter<Long> diskCounter = new TreeSetObjectCounter<>();
        VolumeMetadata volume = volumeStore.getVolume((long) volumeIdIndexer.get());
        for (SegmentMetadata segmentMetadata : volume.getSegments()) {
            for (SegmentUnitMetadata segmentUnitMetadata : segmentMetadata.getSegmentUnits()) {
                if (!segmentUnitMetadata.isArbiter()) {
                    segUnitCounter.increment(segmentUnitMetadata.getInstanceId());
                    diskCounter.increment(segmentUnitMetadata.getArchiveId());
                    if (segmentUnitMetadata.getStatus() == SegmentUnitStatus.Primary) {
                        primaryCounter.increment(segmentUnitMetadata.getInstanceId());
                    }
                }
            }
        }
        logger.warn("counting primary counter {}", primaryCounter);
        logger.warn("counting segment unit counter {}", segUnitCounter);
        logger.warn("counting disk counter {}", diskCounter);

        if (validateBalanced) {
            if (pDiffCount == 0) pDiffCount = 1;
            if (sDiffCount == 0) sDiffCount = 1;
            if (diskDiffCount == 0) diskDiffCount = 1;

            if (primaryCounter.size() == 0 || segUnitCounter.size() == 0 || diskCounter.size() == 0){
                logger.warn("may be 0 volume");
            } else {
                assertTrue(primaryCounter.maxValue() - primaryCounter.minValue() <= pDiffCount);
                assertTrue(segUnitCounter.maxValue() - segUnitCounter.minValue() <= sDiffCount);
                assertTrue(diskCounter.maxValue() - diskCounter.minValue() <= diskDiffCount);

                for (InstanceMetadata instanceMetadata : storageStore.list()) {
                    assertTrue(primaryCounter.get(instanceMetadata.getInstanceId()) != 0);
                    assertTrue(segUnitCounter.get(instanceMetadata.getInstanceId()) != 0);
                    for (RawArchiveMetadata rawArchiveMetadata : instanceMetadata.getArchives()) {
                        assertTrue(diskCounter.get(rawArchiveMetadata.getArchiveId()) != 0);
                    }
                }
            }
        }
    }

    private void migrateSegmentUnit(RebalanceTask task) throws AccessDeniedException {
        Validate.isTrue(task.getTaskType() == RebalanceTask.RebalanceTaskType.NormalRebalance
                || task.getTaskType() == RebalanceTask.RebalanceTaskType.InsideRebalance
                || task.getTaskType() == RebalanceTask.RebalanceTaskType.PSRebalance);
        SegmentUnitMetadata segUnit = task.getSourceSegmentUnit();
        SegId segId = segUnit.getSegId();
        InstanceId destination = task.getDestInstanceId();
        InstanceId source = task.getInstanceToMigrateFrom();

        VolumeMetadata volume = copyVolume(volumeStore.getVolume(segId.getVolumeId().getId()));
        SegmentMetadata segment = volume.getSegmentByIndex(segId.getIndex());
        SegmentMetadata newSegment = new SegmentMetadata(segId, segId.getIndex());
        SegmentMembership currentMembership = segment.getLatestMembership();

        // inside rebalance
        if (destination.equals(source)) {
            Validate.isTrue(task.getTaskType() == RebalanceTask.RebalanceTaskType.InsideRebalance);
            for (SegmentUnitMetadata segmentUnitMetadata : segment.getSegmentUnits()) {
                if (segmentUnitMetadata.getInstanceId().equals(source)) {
                    ObjectCounter<Long> archiveCounter = mapInstanceIdToDiskCounter.get(source);
                    archiveCounter.decrement(segUnit.getArchiveId());
                    long archiveId = task.getTargetArchiveId();
                    segmentUnitMetadata.setArchiveId(archiveId);
                    archiveCounter.increment(archiveId);
                }
                newSegment.putSegmentUnitMetadata(segmentUnitMetadata.getInstanceId(), segmentUnitMetadata);
            }
            volume.addSegmentMetadata(newSegment, currentMembership);
        } else {
            SegmentMembership newMembership = currentMembership.addSecondaryCandidate(destination)
                                                               .secondaryCandidateBecomesSecondaryAndRemoveTheReplacee(
                                                                       destination, source);
            for (SegmentUnitMetadata segmentUnitMetadata : segment.getSegmentUnits()) {

                //SegmentUnitMetadata newSegmentUnitMetadata = new SegmentUnitMetadata(segmentUnitMetadata);
                segmentUnitMetadata.setMembership(newMembership);
                if (segmentUnitMetadata.getInstanceId().equals(source)) {
                    segmentUnitMetadata.setInstanceId(destination);
                    ObjectCounter<Long> sourceArchiveCounter = mapInstanceIdToDiskCounter.get(source);
                    sourceArchiveCounter.decrement(segUnit.getArchiveId());
                    ObjectCounter<Long> destArchiveCounter = mapInstanceIdToDiskCounter.get(destination);
                    long archiveId = destArchiveCounter.min();
                    segmentUnitMetadata.setArchiveId(archiveId);
                    destArchiveCounter.increment(archiveId);
                }
                newSegment.putSegmentUnitMetadata(segmentUnitMetadata.getInstanceId(), segmentUnitMetadata);
            }
            volume.addSegmentMetadata(newSegment, newMembership);
        }
        volumeStore.saveVolume(volume);
    }

    private VolumeMetadata copyVolume(VolumeMetadata volume) {
        return RequestResponseHelper.buildVolumeFrom(RequestResponseHelper.buildThriftVolumeFrom(volume, true));
    }

    private void switchPrimary(RebalanceTask task) throws AccessDeniedException {
        Validate.isTrue(task.getTaskType() == RebalanceTask.RebalanceTaskType.PrimaryRebalance);
        SegmentUnitMetadata segUnit = task.getSourceSegmentUnit();
        InstanceId destinationPrimary = task.getDestInstanceId();

        VolumeMetadata volume = copyVolume(volumeStore.getVolume(segUnit.getSegId().getVolumeId().getId()));
        SegmentMetadata segment = volume.getSegmentByIndex(segUnit.getSegId().getIndex());
        SegmentMembership currentMembership = segment.getLatestMembership();
        SegmentMembership newMembership = currentMembership.secondaryBecomePrimaryCandidate(destinationPrimary)
                                                           .primaryCandidateBecomePrimary(destinationPrimary);
        for (SegmentUnitMetadata segmentUnitMetadata : segment.getSegmentUnits()) {
            segmentUnitMetadata.setMembership(newMembership);
            if (segmentUnitMetadata.getInstanceId().equals(destinationPrimary)) {
                segmentUnitMetadata.setStatus(SegmentUnitStatus.Primary);
            } else if (segmentUnitMetadata.getInstanceId().equals(segUnit.getInstanceId())) {
                segmentUnitMetadata.setStatus(SegmentUnitStatus.Secondary);
            }
        }
        volumeStore.saveVolume(volume);
    }

    @Test
    public void testSelectRebalanceTaskCase3() throws Exception {
        setLogLevel(Level.DEBUG);
        buildEnv();
        addStorage(4, 2);
        int segmentCount = random.nextInt(100);
        if (segmentCount == 0){
            segmentCount = 1;
        }
        logger.warn("segment count is {}", segmentCount);
        createAVolume(segmentSize * segmentCount, VolumeType.REGULAR);

        count(true, 5, 5, 5);

        addStorage(2, 2);

        ((SegmentUnitsDistributionManagerImpl) distributionManager).forceStart();

        AtomicInteger failCount = new AtomicInteger();

        //        for (int i = 0; i < 50; i++) {
        int count = 0;
        while (true) {
            try {
                if (submitTask(failCount, 0.9)) {
                    count++;
                }
            } catch (NoNeedToRebalance e) {
                break;
            }
        }
        semaphore.acquire(count);
        logger.warn("we got {} tasks in total, and segment count {}", count, segmentCount);

        count(true, 1, 1, 1);
    }

  //  @Test
    public void testInsideRebalance() throws Exception {
        buildEnv();
        addStorage(3, 1);
        createAVolume(segmentSize * 4, VolumeType.REGULAR);

        count(true, 5, 5, 5);

        for (InstanceMetadata instanceMetadata : storageStore.list()) {
            addArchiveToExistingStorage(instanceMetadata.getInstanceId().getId(), 1);
        }

        ((SegmentUnitsDistributionManagerImpl) distributionManager).forceStart();

        AtomicInteger failCount = new AtomicInteger();

        // we need 6 times to get balanced
        int n = 5;
        for (int i = 0; i < n; ) {
            if (submitTask(failCount, 1)) {
                i++;
            }
        }
        semaphore.acquire(n);

        try {
            failCount.set(20);
            submitTask(failCount, 1);
            fail("an exception should be thrown above");
        } catch (NoNeedToRebalance ignore) {
        }

        count(true, 1, 1, 1);
    }

    @Test
    public void testSelectRebalanceTaskCase1() throws Exception {
        buildEnv();
        addStorage(3, 2);
        createAVolume(segmentSize * 4, VolumeType.REGULAR);

        count(true, 5, 5, 5);

        addStorage(1, 2);

        ((SegmentUnitsDistributionManagerImpl) distributionManager).forceStart();

        AtomicInteger failCount = new AtomicInteger();

        // we need four times to get balanced
        int n = 4;
        for (int i = 0; i < n; ) {
            if (submitTask(failCount, 0.8)) {
                i++;
            }
        }
        semaphore.acquire(n);

        count(true, 1, 1, 1);
    }

    @Test
    @Ignore
    public void testSelectRebalanceTaskCase2() throws Exception {
        setLogLevel(Level.DEBUG);
        buildEnv();
        addStorage(3, 2);
        createAVolume(segmentSize * 4, VolumeType.SMALL);

        count(true, 5, 5, 5);

        addStorage(1, 2);

        ((SegmentUnitsDistributionManagerImpl) distributionManager).forceStart();

        AtomicInteger failCount = new AtomicInteger();

        // we need three times to get balanced
        int n = 3;
        for (int i = 0; i < n; ) {
            if (submitTask(failCount, 1)) {
                i++;
            }
        }
        semaphore.acquire(n);

        try {
            failCount.set(20);
            submitTask(failCount, 1);
            fail("we should throw an exception above");
        } catch (NoNeedToRebalance ignore) {
        }

        count(true, 1, 1, 1);
    }

    @Test
    public void checkRebalanceStepIsUnique() throws Exception {
        setLogLevel(Level.WARN);
        buildEnv();
        addStorage(3, 2);
        int segmentCount = random.nextInt(5000);
        if (segmentCount == 0){
            segmentCount = 1;
        }
        logger.warn("segment count is {}", segmentCount);
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances =
                createAVolume(segmentSize * segmentCount, VolumeType.REGULAR);

        List<VolumeMetadata> volumeList = volumeStore.listVolumes();
        VolumeMetadata volume = volumeList.get(0);

        writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
        verifyResult(volume.getVolumeType(), segIndex2Instances,3, 10, 3, 3);

        count(true, 5, 30, 5);

        addStorage(2, 2);

        ((SegmentUnitsDistributionManagerImpl) distributionManager).forceStart();

        List<RebalanceTask> bacTaskList = new LinkedList<>();
        for (int i = 0; i < 100; i++){

            List<RebalanceTask> allTaskStep = new LinkedList<>();

            Multimap<Long, RebalanceTask> volume2TaskMap = HashMultimap.create();
            while(volume2TaskMap != null){
                List<RebalanceTask> taskList;
                try {
                    volume2TaskMap = distributionManager.selectRebalanceTasks(true);
                    taskList = new LinkedList<>(volume2TaskMap.values());
                } catch (NoNeedToRebalance e) {
                    logger.info("get task over");
                    break;
                }

                //Randomly remove several tasks that can not be done correctly from taskList
                int validCount = random.nextInt(taskList.size());
                if (validCount == 0){
                    validCount = 1;
                }
                logger.warn("has task {}, only {} task will be worked", taskList.size(), validCount);
                while (taskList.size() > validCount){
                    taskList.remove(random.nextInt(taskList.size()));
                }

                boolean isRework = judgeRework(allTaskStep, taskList);
                if (isRework){
                    writeResult2File(volumeStore.getVolume(volume.getVolumeId()), storageStore.list());
                }
                judgeRework(allTaskStep, taskList);

                //do migrate to balance
                for (RebalanceTask task : taskList){
                    if (task.getTaskType() == RebalanceTask.RebalanceTaskType.PrimaryRebalance){
                        switchPrimary(task);
                    } else {
                        migrateSegmentUnit(task);
                    }
                }

                assertTrue(!judgeRework(allTaskStep, taskList));

                logger.warn("add {} task:{}", taskList.size(), taskList);
                allTaskStep.addAll(taskList);
                writeResult2File(volumeStore.getVolume(volume.getVolumeId()), storageStore.list());
            }

            writeResult2File(volumeStore.getVolume(volume.getVolumeId()), storageStore.list());

            boolean isResultDistributeOk = verifyResult(volumeStore.getVolume(volume.getVolumeId()),1, 10, 3, 1);
            if (!isResultDistributeOk){
                try {
                    ((SegmentUnitsDistributionManagerImpl) distributionManager).forceStart();
                    volume2TaskMap = distributionManager.selectRebalanceTasks(true);
                } catch (NoNeedToRebalance e) {
                    logger.info("get task over");
                }
            }

            //Compare whether task is the same
            if (bacTaskList.isEmpty()){
                bacTaskList.addAll(allTaskStep);
            } else{
                //assertTrue(taskIsEquals(bacTaskList, allTaskStep));
            }

            volumeStore.saveVolume(volume);
        }

    }

    private boolean judgeRework(List<RebalanceTask> allTaskList, List<RebalanceTask> taskList){
        for (RebalanceTask oldTask : allTaskList){
            int oldSegId = oldTask.getSourceSegmentUnit().getSegId().getIndex();
            long oldSrcId = oldTask.getSourceSegmentUnit().getInstanceId().getId();
            long oldDestId = oldTask.getDestInstanceId().getId();

            for (RebalanceTask newTask : taskList) {
                if (newTask.getSourceSegmentUnit().getSegId().getIndex() == oldSegId
                        && newTask.getSourceSegmentUnit().getInstanceId().getId() == oldSrcId
                        && newTask.getDestInstanceId().getId() == oldDestId) {
                    return true;
                }
            }
        }
        return false;
    }
    /**
     * compare tasks(not care order)
     * @param srcTaskList source tasks
     * @param destTaskList  dest tasks
     * @return  true:if tasks is same
     */
    private boolean taskIsEquals(List<RebalanceTask> srcTaskList, List<RebalanceTask> destTaskList){
        if (srcTaskList.size() != destTaskList.size()){
            return false;
        }

        boolean hasCompared = false;
        for (RebalanceTask srcTask : srcTaskList){
            for (RebalanceTask destTask :destTaskList){
                if (destTask.equals(srcTask)){
                    hasCompared = true;
                    destTaskList.remove(destTask);
                    break;
                }
            }
            if (!hasCompared){
                return false;
            }
        }

        if (destTaskList.isEmpty()){
            return true;
        }

        return false;
    }

    private void parseResultFromFile() throws IOException {
        File readFile = new File("/tmp/ReserveVolumeTest_PS.log");
        BufferedReader reader= new BufferedReader(new FileReader(readFile));

        long datanodeCount = 0;

        /**
         * parse
         */
        ObjectCounter<Long> primaryId = new TreeSetObjectCounter<>();
        ObjectCounter<Long> secondaryId = new TreeSetObjectCounter<>();
        ObjectCounter<Long> arbiterId = new TreeSetObjectCounter<>();

        LinkedList<Set<Long>> balanceSecondaryNode = new LinkedList<>();
        String lineBuf;
        while((lineBuf = reader.readLine()) != null) {
            String comb[] = lineBuf.split("\t");
            datanodeCount = comb.length - 2;
            Set<Long> sNode = new HashSet<>();
            long downNode = 3;
            boolean needBalance = false;
            for (int index = 2; index < comb.length; index++){
                if (comb[index].equals("P")){
                    primaryId.increment((long)(index-2));
                    if (downNode == index-2){
                        needBalance = true;
                    }
                } else if (comb[index].equals("S")){
                    secondaryId.increment((long)(index-2));
                    sNode.add((long)(index-2));
                } else if (comb[index].equals("A")){
                    arbiterId.increment((long)(index-2));
                }
            }

            if (needBalance){
                balanceSecondaryNode.add(sNode);
            }
        }

        /**
         * write node combination
         */
        File writeFile = new File("/tmp/ReserveVolumeTest_PS_result.log");
        OutputStream outputStream = null;
        if (!writeFile.exists()) {
            try {
                writeFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            outputStream = new FileOutputStream(writeFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String writeBuf = "";

        /**
         * primary
         */
        int pMax = 0;
        int pMin = 0xffffff;
        writeBuf += "P:\t";
        for (long i = 0; i < datanodeCount; i++){
            writeBuf += primaryId.get(i) + "\t";
            pMax = Math.max(pMax, (int)primaryId.get(i));
            if (primaryId.get(i) != 0){
                pMin = Math.min(pMin, (int)primaryId.get(i));
            }
        }
        if (pMin == 0xffffff){
            pMin = 0;
        }
        writeBuf += "\r\n";
        writeBuf += "P分配差额：\t"+pMax+"\t"+pMin+"\t"+(pMax-pMin)+"\r\n\n";


        /**
         * Secondary
         */
        pMax = 0;
        pMin = 0xffffff;
        writeBuf += "S:\t";
        for (long i = 0; i < datanodeCount; i++){
            writeBuf += secondaryId.get(i) + "\t";
            pMax = Math.max(pMax, (int)secondaryId.get(i));
            if (secondaryId.get(i) != 0){
                pMin = Math.min(pMin, (int)secondaryId.get(i));
            }
        }
        if (pMin == 0xffffff){
            pMin = 0;
        }
        writeBuf += "\r\n";
        writeBuf += "S分配差额：\t"+pMax+"\t"+pMin+"\t"+(pMax-pMin)+"\r\n\n";

        /**
         * arbiter
         */
        pMax = 0;
        pMin = 0xffffff;
        writeBuf += "A:\t";
        for (long i = 0; i < datanodeCount; i++){
            writeBuf += arbiterId.get(i) + "\t";
            pMax = Math.max(pMax, (int)arbiterId.get(i));
            if (arbiterId.get(i) != 0){
                pMin = Math.min(pMin, (int)arbiterId.get(i));
            }
        }
        if (pMin == 0xffffff){
            pMin = 0;
        }
        writeBuf += "\r\n";
        writeBuf += "A分配差额：\t"+pMax+"\t"+pMin+"\t"+(pMax-pMin)+"\r\n\n";

        /**
         * rebalance
         */
        ObjectCounter<Long> pBalanceCounter = new TreeSetObjectCounter<>();
        for (Set<Long> sNode : balanceSecondaryNode){
            for (Long i : sNode){
                pBalanceCounter.increment(i);
            }
        }

        pMax = 0;
        pMin = 0xffffff;
        writeBuf += "P再平衡:\t";
        for (long i = 0; i < datanodeCount; i++){
            writeBuf += pBalanceCounter.get(i) + "\t";
            pMax = Math.max(pMax, (int)pBalanceCounter.get(i));
            if (pBalanceCounter.get(i) != 0){
                pMin = Math.min(pMin, (int)pBalanceCounter.get(i));
            }
        }
        if (pMin == 0xffffff){
            pMin = 0;
        }
        writeBuf += "\r\n";
        writeBuf += "P再平衡差额：\t"+pMax+"\t"+pMin+"\t"+(pMax-pMin)+"\r\n\n";

        //write
        try {
            outputStream.write(writeBuf.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }

        //close
        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeResult2File(VolumeType volumeType, List<InstanceMetadata> allInstanceList,
                                  Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances){
        File file = new File("/tmp/ReserveVolumeTest_PS.log");
        OutputStream outputStream = null;
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            outputStream = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        for (int segIndex : segIndex2Instances.keySet()) {
            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            Set<Long> arbiterIdSet = new HashSet<>();
            for (int i = 0; i < volumeType.getNumArbiters(); i++) {
                arbiterIdSet.add(arbiterInstanceList.get(i).getInstanceId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);

            long primaryDatanodeId = normalInstanceList.get(0).getInstanceId();
            Set<Long> secondaryIdSet = new HashSet<>();
            for (int i = 1; i < volumeType.getNumSecondaries() + 1; i++) {
                secondaryIdSet.add(normalInstanceList.get(i).getInstanceId());
            }

            String writeBuf = segIndex + "\t";

            for (long i = 0; i < allInstanceList.size(); i++) {
                long id = allInstanceList.get((int)i).getInstanceId().getId();
                if (primaryDatanodeId == id) {
                    writeBuf += "\tP";
                } else if (secondaryIdSet.contains(id)) {
                    writeBuf += "\tS";
                } else if (arbiterIdSet.contains(id)) {
                    writeBuf += "\tA";
                } else {
                    writeBuf += "\tO";
                }
            }

            writeBuf += "\r\n";

            try {
                outputStream.write(writeBuf.getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //close
        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            parseResultFromFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void writeResult2File(VolumeMetadata volume, List<InstanceMetadata> allInstanceList){
        File file = new File("/tmp/ReserveVolumeTest_PS.log");
        OutputStream outputStream = null;
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            outputStream = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        for (int segIndex : volume.getSegmentTable().keySet()) {
            SegmentMetadata segmentMetadata = volume.getSegmentTable().get(segIndex);
            Map<InstanceId, SegmentUnitMetadata> segmentUnitMap = segmentMetadata.getSegmentUnitMetadataTable();


            Set<Long> arbiterIdSet = new HashSet<>();
            Set<Long> secondaryIdSet = new HashSet<>();
            long primaryDatanodeId = -1;
            for (InstanceId insId : segmentUnitMap.keySet()){
                SegmentUnitMetadata segmentUnitMetadata = segmentUnitMap.get(insId);
                if (segmentUnitMetadata.getMembership().isArbiter(insId)){
                    arbiterIdSet.add(insId.getId());
                } else if (segmentUnitMetadata.getMembership().isPrimary(insId)){
                    primaryDatanodeId = insId.getId();
                } else if (segmentUnitMetadata.getMembership().isSecondary(insId)){
                    secondaryIdSet.add(insId.getId());
                }
            }

            String writeBuf = segIndex + "\t";

            for (long i = 0; i < allInstanceList.size(); i++) {
                long id = allInstanceList.get((int)i).getInstanceId().getId();
                if (primaryDatanodeId == id) {
                    writeBuf += "\tP";
                } else if (secondaryIdSet.contains(id)) {
                    writeBuf += "\tS";
                } else if (arbiterIdSet.contains(id)) {
                    writeBuf += "\tA";
                } else {
                    writeBuf += "\tO";
                }
            }

            writeBuf += "\r\n";

            try {
                outputStream.write(writeBuf.getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //close
        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            parseResultFromFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void verifyResult(VolumeType volumeType, Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> segIndex2Instances,
                             long pDiffCount, long sDiffCount, long aDiffCount, long pDownDiffCount){
        ObjectCounter<Long> arbiterIdCounter = new TreeSetObjectCounter<>();
        ObjectCounter<Long> primaryIdCounter = new TreeSetObjectCounter<>();
        ObjectCounter<Long> secondaryIdCounter = new TreeSetObjectCounter<>();

        Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();
        for (int segIndex : segIndex2Instances.keySet()) {

            Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>> instanceListFromRemote = segIndex2Instances.get(segIndex);
            List<InstanceIdAndEndPoint_Thrift> arbiterInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Arbiter);

            //count necessary arbiter
            for (int i = 0; i < volumeType.getNumArbiters(); i++) {
                arbiterIdCounter.increment(arbiterInstanceList.get(i).getInstanceId());
            }

            List<InstanceIdAndEndPoint_Thrift> normalInstanceList = instanceListFromRemote.get(SegmentUnitType_Thrift.Normal);

            //count necessary primary
            long primaryId = normalInstanceList.get(0).getInstanceId();
            primaryIdCounter.increment(primaryId);

            ObjectCounter<Long> primarySecondaryCounter = primaryId2SecondaryIdCounterMap.get(primaryId);
            if (primarySecondaryCounter == null){
                primarySecondaryCounter = new TreeSetObjectCounter<>();
                primaryId2SecondaryIdCounterMap.put(primaryId, primarySecondaryCounter);
            }

            //count necessary secondary
            for (int i = 1; i < volumeType.getNumSecondaries() + 1; i++) {
                long secondaryId = normalInstanceList.get(i).getInstanceId();
                secondaryIdCounter.increment(secondaryId);

                primarySecondaryCounter.increment(secondaryId);
            }
        }

        long maxCount, minCount;
        /**
         * verify average distribution
         */
        //verify necessary primary max and min count
        maxCount = primaryIdCounter.maxValue();
        minCount = primaryIdCounter.minValue();
        if (maxCount - minCount > pDiffCount){
            logger.error("primary average distribute failed ! maxCount:{}, minCount:{}",maxCount, minCount);
        }
        assert(maxCount - minCount <= pDiffCount);

        //verify necessary secondary max and min count
        maxCount = secondaryIdCounter.maxValue();
        minCount = secondaryIdCounter.minValue();
        if (maxCount - minCount > sDiffCount){
            logger.error("secondary average distribute failed ! maxCount:{}, minCount:{}",maxCount, minCount);
        }
        assert(maxCount - minCount <= sDiffCount);

        //verify necessary arbiter max and min count
        if (arbiterIdCounter.size() > 0){
            maxCount = arbiterIdCounter.maxValue();
            minCount = arbiterIdCounter.minValue();
            if (maxCount - minCount > aDiffCount){
                logger.error("arbiter average distribute failed ! maxCount:{}, minCount:{}",maxCount, minCount);
            }
            assert(maxCount - minCount <= aDiffCount);
        }

        /**
         * verify rebalance when P down
         */
        for (Map.Entry<Long, ObjectCounter<Long>> entry : primaryId2SecondaryIdCounterMap.entrySet()){
            ObjectCounter<Long> secondaryCounterTemp = entry.getValue();
            maxCount = secondaryCounterTemp.maxValue();
            minCount = secondaryCounterTemp.minValue();

            if (maxCount - minCount > pDownDiffCount){
                logger.error("rebalance failed when P down! maxCount:{}, minCount:{}, entry:{}", maxCount, minCount, entry);
            }
            assert(maxCount - minCount <= pDownDiffCount);
        }
    }
    public boolean verifyResult(VolumeMetadata volume,long pDiffCount, long sDiffCount, long aDiffCount, long pDownDiffCount){
        ObjectCounter<Long> arbiterIdCounter = new TreeSetObjectCounter<>();
        ObjectCounter<Long> primaryIdCounter = new TreeSetObjectCounter<>();
        ObjectCounter<Long> secondaryIdCounter = new TreeSetObjectCounter<>();

        Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();

        for (int segIndex : volume.getSegmentTable().keySet()) {
            SegmentMetadata segmentMetadata = volume.getSegmentTable().get(segIndex);

            Map<InstanceId, SegmentUnitMetadata> segmentUnitMap = segmentMetadata.getSegmentUnitMetadataTable();
            Set<Long> secondaryOfPrimarySet = new HashSet<>();
            long primaryId = -1;
            for (InstanceId insId : segmentUnitMap.keySet()){
                SegmentUnitMetadata segmentUnitMetadata = segmentUnitMap.get(insId);
                if (segmentUnitMetadata.getMembership().isArbiter(insId)){
                    arbiterIdCounter.increment(insId.getId());
                } else if (segmentUnitMetadata.getMembership().isPrimary(insId)){
                    primaryId = insId.getId();
                    primaryIdCounter.increment(insId.getId());
                } else if (segmentUnitMetadata.getMembership().isSecondary(insId)){
                    secondaryIdCounter.increment(insId.getId());
                    secondaryOfPrimarySet.add(insId.getId());
                }
            }

            ObjectCounter<Long> primarySecondaryCounter = primaryId2SecondaryIdCounterMap.computeIfAbsent(primaryId, value->new TreeSetObjectCounter<>());
            //count necessary secondary
            for (long secondaryId : secondaryOfPrimarySet) {
                primarySecondaryCounter.increment(secondaryId);
            }
        }

        long maxCount, minCount;
        /**
         * verify average distribution
         */
        //verify necessary primary max and min count
        maxCount = primaryIdCounter.maxValue();
        minCount = primaryIdCounter.minValue();
        if (maxCount - minCount > pDiffCount){
            logger.error("primary average distribute failed ! maxCount:{}, minCount:{}",maxCount, minCount);
        }
        if (maxCount - minCount > pDiffCount){
            return false;
        }

        //verify necessary secondary max and min count
        maxCount = secondaryIdCounter.maxValue();
        minCount = secondaryIdCounter.minValue();
        if (maxCount - minCount > sDiffCount){
            logger.error("secondary average distribute failed ! maxCount:{}, minCount:{}",maxCount, minCount);
        }
        if (maxCount - minCount > sDiffCount){
            return false;
        }

        //verify necessary arbiter max and min count
        if (arbiterIdCounter.size() > 0){
            maxCount = arbiterIdCounter.maxValue();
            minCount = arbiterIdCounter.minValue();
            if (maxCount - minCount > aDiffCount){
                logger.error("arbiter average distribute failed ! maxCount:{}, minCount:{}",maxCount, minCount);
            }
            if (maxCount - minCount > aDiffCount){
                return false;
            }
        }

        /**
         * verify rebalance when P down
         */
        for (Map.Entry<Long, ObjectCounter<Long>> entry : primaryId2SecondaryIdCounterMap.entrySet()){
            ObjectCounter<Long> secondaryCounterTemp = entry.getValue();
            maxCount = secondaryCounterTemp.maxValue();
            minCount = secondaryCounterTemp.minValue();

            if (maxCount - minCount > pDownDiffCount){
                logger.error("rebalance failed when P down! maxCount:{}, minCount:{}, entry:{}", maxCount, minCount, entry);
            }
            if (maxCount - minCount > pDownDiffCount){
                return false;
            }
        }
        return true;
    }

}