package py.infocenter.rebalance;

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

import java.io.IOException;
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
    private int maxSegmentCountPerArchive = 100;
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

    private void createAVolume(long size, VolumeType volumeType)
            throws NotEnoughGroupException_Thrift,
            NotEnoughSpaceException_Thrift, NotEnoughNormalGroupException_Thrift, TException {
        ObjectCounter<InstanceId> diskIndexer = new TreeSetObjectCounter<>();
        Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> result = distributionManager
                .reserveVolume(size, volumeType, false, Integer.MAX_VALUE, storagePool.getPoolId());
        logger.warn("reserve result {}", result);
        VolumeMetadata volumeMetadata = new VolumeMetadata(volumeIdIndexer.incrementAndGet(), volumeIdIndexer.get(),
                size, segmentSize, volumeType, CacheType.MEMORY, 0L, storagePool.getPoolId());
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

    private void migrateSegmentUnit(RebalanceTask task) throws AccessDeniedException {
        Validate.isTrue(task.getTaskType() == RebalanceTask.RebalanceTaskType.NormalRebalance
                || task.getTaskType() == RebalanceTask.RebalanceTaskType.InsideRebalance);
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
}