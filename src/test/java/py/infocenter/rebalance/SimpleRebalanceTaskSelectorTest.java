//package py.infocenter.rebalance;
//
//import static org.junit.Assert.*;
//
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
//import org.junit.Before;
//import org.junit.Test;
//
//import py.archive.RawArchiveMetadata;
//import py.archive.segment.SegId;
//import py.archive.segment.SegmentMetadata;
//import py.archive.segment.SegmentUnitMetadata;
//import py.archive.segment.SegmentUnitStatus;
//import py.icshare.InstanceMetadata;
//import py.icshare.exception.VolumeNotFoundException;
//import py.infocenter.rebalance.simple.SimpleRebalanceTaskSelector;
//import py.infocenter.store.MemoryVolumeStoreImpl;
//import py.infocenter.store.StorageStore;
//import py.infocenter.store.VolumeStore;
//import py.infocenter.test.utils.StorageMemStore;
//import py.instance.InstanceId;
//import py.membership.SegmentMembership;
//import py.rebalance.RebalanceTask;
//import py.test.TestBase;
//import py.volume.VolumeMetadata;
//import py.volume.VolumeType;
//
//// TODO : test for storage pools
//public class SimpleRebalanceTaskSelectorTest extends TestBase {
//
//    private final VolumeStore volumeStore = new MemoryVolumeStoreImpl();
//    private final StorageStore storageStore = new StorageMemStore();
//    // private final Random random = new Random(System.currentTimeMillis());
//
//    @Before
//    public void init() {
//        volumeStore.clearData();
//        storageStore.clearMemoryData();
//    }
//
//    @Test
//    public void testSegmentUnitDeletedAfterSelection() {
//
//        double threshould = SimpleRebalanceTaskSelector.PRESSURE_THRESHOLD;
//
//        int instanceCount = 4;
//        int archiveCountPerInstance = 10;
//        int segmentUnitCountPerArchive = 10;
//        for (int i = 0; i < instanceCount; i++) {
//            InstanceMetadata instance = generateInstance(i * 10000, archiveCountPerInstance);
//            long volumeId = instance.getInstanceId().getId();
//            for (RawArchiveMetadata archive : instance.getArchives()) {
//                VolumeMetadata volume = generateVolume(volumeId++, instance.getInstanceId(), archive.getDeviceName(),
//                        segmentUnitCountPerArchive, VolumeType.REGULAR);
//                volumeStore.saveVolume(volume);
//            }
//            storageStore.save(instance);
//        }
//
//        InstanceMetadata instance = generateInstance(instanceCount * 10000, archiveCountPerInstance);
//        long volumeId = instance.getInstanceId().getId();
//        for (RawArchiveMetadata archive : instance.getArchives()) {
//            VolumeMetadata volume = generateVolume(volumeId++, instance.getInstanceId(), archive.getDeviceName(),
//                    (int) (segmentUnitCountPerArchive * (1 + threshould)) + 1, VolumeType.REGULAR);
//            volumeStore.saveVolume(volume);
//        }
//        storageStore.save(instance);
//        SimpleRebalanceTaskSelector selector = new SimpleRebalanceTaskSelector(volumeStore, storageStore);
//        try {
//            RebalanceTask rebalanceTask = selector.selectRebalanceTask();
//            logger.info("rebalance task selected {}", rebalanceTask);
//            assertEquals(instanceCount * 10000, rebalanceTask.getSegmentUnitToRemove().getInstanceId().getId());
//            SegmentUnitMetadata segmentUnit = rebalanceTask.getSegmentUnitToRemove();
//            try {
//                VolumeMetadata volume = volumeStore.getVolume(segmentUnit.getSegId().getVolumeId().getId(), null);
//                VolumeMetadata newVolume = removeSegmentUnit(segmentUnit, volume);
//                volumeStore.deleteVolume(volume);
//                volumeStore.saveVolume(newVolume);
//                logger.warn("semgent unit to remove : {}", segmentUnit);
//                logger.warn("old volume {} new volume {}", volume, newVolume);
//            } catch (Exception e) {
//                logger.error("exception caught ", e);
//                fail();
//            }
//        } catch (NoNeedToRebalance e) {
//            fail();
//        }
//        try {
//            for (int i = 0; i < 100; i++) {
//                RebalanceTask rebalanceTask = selector.selectRebalanceTask();
//                logger.info("rebalance task selected {}", rebalanceTask);
//                SegmentUnitMetadata segmentUnit = rebalanceTask.getSegmentUnitToRemove();
//                try {
//                    VolumeMetadata volume = volumeStore.getVolume(segmentUnit.getSegId().getVolumeId().getId(), null);
//                    VolumeMetadata newVolume = removeSegmentUnit(segmentUnit, volume);
//                    volumeStore.deleteVolume(volume);
//                    volumeStore.saveVolume(newVolume);
//                } catch (Exception e) {
//                    logger.error("exception caught ", e);
//                    fail();
//                }
//            }
//            fail();
//        } catch (NoNeedToRebalance e) {
//        }
//    }
//
//    @Test
//    public void testBalanceAfterSelections() {
//
//        double threshould = SimpleRebalanceTaskSelector.PRESSURE_THRESHOLD;
//
//        int instanceCount = 4;
//        int archiveCountPerInstance = 10;
//        int segmentUnitCountPerArchive = 10;
//        for (int i = 0; i < instanceCount; i++) {
//            InstanceMetadata instance = generateInstance(i * 10000, archiveCountPerInstance);
//            long volumeId = instance.getInstanceId().getId();
//            for (RawArchiveMetadata archive : instance.getArchives()) {
//                VolumeMetadata volume = generateVolume(volumeId++, instance.getInstanceId(), archive.getDeviceName(),
//                        segmentUnitCountPerArchive, VolumeType.REGULAR);
//                volumeStore.saveVolume(volume);
//            }
//            storageStore.save(instance);
//        }
//
//        InstanceMetadata instance = generateInstance(instanceCount * 10000, archiveCountPerInstance);
//        long volumeId = instance.getInstanceId().getId();
//        for (RawArchiveMetadata archive : instance.getArchives()) {
//            VolumeMetadata volume = generateVolume(volumeId++, instance.getInstanceId(), archive.getDeviceName(),
//                    (int) (segmentUnitCountPerArchive * (1 + threshould)) + 1, VolumeType.REGULAR);
//            volumeStore.saveVolume(volume);
//        }
//        storageStore.save(instance);
//        SimpleRebalanceTaskSelector selector = new SimpleRebalanceTaskSelector(volumeStore, storageStore);
//        try {
//            RebalanceTask rebalanceTask = selector.selectRebalanceTask();
//            logger.info("rebalance task selected {}", rebalanceTask);
//            assertEquals(instanceCount * 10000, rebalanceTask.getSegmentUnitToRemove().getInstanceId().getId());
//        } catch (NoNeedToRebalance e) {
//            fail();
//        }
//        try {
//            for (int i = 0; i < 100; i++) {
//                RebalanceTask rebalanceTask = selector.selectRebalanceTask();
//                logger.info("rebalance task selected {}", rebalanceTask);
//            }
//            fail();
//        } catch (NoNeedToRebalance e) {
//        }
//    }
//
//    @Test
//    public void testSelecte5TasksWithAnIdleInstance() {
//        int instanceCount = 15;
//        int archiveCountPerInstance = 100;
//        int segmentUnitCountPerArchive = 100;
//        for (int i = 0; i < instanceCount; i++) {
//            InstanceMetadata instance = generateInstance(i * 10000, archiveCountPerInstance);
//            long volumeId = instance.getInstanceId().getId();
//            for (RawArchiveMetadata archive : instance.getArchives()) {
//                VolumeMetadata volume = generateVolume(volumeId++, instance.getInstanceId(), archive.getDeviceName(),
//                        segmentUnitCountPerArchive + i, VolumeType.REGULAR);
//                volumeStore.saveVolume(volume);
//            }
//            storageStore.save(instance);
//        }
//
//        InstanceMetadata instance = generateInstance(instanceCount * 10000, archiveCountPerInstance);
//        storageStore.save(instance);
//        SimpleRebalanceTaskSelector selector = new SimpleRebalanceTaskSelector(volumeStore, storageStore);
//        try {
//            Set<SegmentUnitMetadata> segmentUnitSet = new HashSet<>();
//            for (int i = 0; i < 5; i++) {
//                RebalanceTask rebalanceTask = selector.selectRebalanceTask();
//                logger.info("rebalance task selected {}", rebalanceTask);
//                assertTrue(segmentUnitSet.add(rebalanceTask.getSegmentUnitToRemove()));
//                assertEquals(instanceCount * 10000, rebalanceTask.getDestInstanceId().getId());
//                assertEquals((instanceCount - 1) * 10000,
//                        rebalanceTask.getSegmentUnitToRemove().getInstanceId().getId());
//            }
//        } catch (NoNeedToRebalance e) {
//            fail();
//        }
//    }
//
//    @Test
//    public void testSelectionWithAnIdleInstance() {
//        int instanceCount = 15;
//        int archiveCountPerInstance = 100;
//        int segmentUnitCountPerArchive = 100;
//        for (int i = 0; i < instanceCount; i++) {
//            InstanceMetadata instance = generateInstance(i * 10000, archiveCountPerInstance);
//            long volumeId = instance.getInstanceId().getId();
//            for (RawArchiveMetadata archive : instance.getArchives()) {
//                VolumeMetadata volume = generateVolume(volumeId++, instance.getInstanceId(), archive.getDeviceName(),
//                        segmentUnitCountPerArchive + i, VolumeType.REGULAR);
//                volumeStore.saveVolume(volume);
//            }
//            storageStore.save(instance);
//        }
//
//        InstanceMetadata instance = generateInstance(instanceCount * 10000, archiveCountPerInstance);
//        storageStore.save(instance);
//        SimpleRebalanceTaskSelector selector = new SimpleRebalanceTaskSelector(volumeStore, storageStore);
//        try {
//            RebalanceTask rebalanceTask = selector.selectRebalanceTask();
//            logger.info("rebalance task selected {}", rebalanceTask);
//            assertEquals(instanceCount * 10000, rebalanceTask.getDestInstanceId().getId());
//            assertEquals((instanceCount - 1) * 10000, rebalanceTask.getSegmentUnitToRemove().getInstanceId().getId());
//        } catch (NoNeedToRebalance e) {
//            fail();
//        }
//    }
//
//    @Test
//    public void testSelectionWithAnOverloadedInstance() {
//        int instanceCount = 15;
//        int archiveCountPerInstance = 100;
//        int segmentUnitCountPerArchive = 100;
//        for (int i = 0; i < instanceCount; i++) {
//            InstanceMetadata instance = generateInstance(i * 10000, archiveCountPerInstance);
//            long volumeId = instance.getInstanceId().getId();
//            for (RawArchiveMetadata archive : instance.getArchives()) {
//                VolumeMetadata volume = generateVolume(volumeId++, instance.getInstanceId(), archive.getDeviceName(),
//                        segmentUnitCountPerArchive + i, VolumeType.REGULAR);
//                volumeStore.saveVolume(volume);
//            }
//            storageStore.save(instance);
//        }
//
//        InstanceMetadata instance = generateInstance(instanceCount * 10000, archiveCountPerInstance);
//        long volumeId = instance.getInstanceId().getId();
//        for (RawArchiveMetadata archive : instance.getArchives()) {
//            VolumeMetadata volume = generateVolume(volumeId++, instance.getInstanceId(), archive.getDeviceName(),
//                    segmentUnitCountPerArchive * 3, VolumeType.REGULAR);
//            volumeStore.saveVolume(volume);
//        }
//        storageStore.save(instance);
//        SimpleRebalanceTaskSelector selector = new SimpleRebalanceTaskSelector(volumeStore, storageStore);
//        try {
//            RebalanceTask rebalanceTask = selector.selectRebalanceTask();
//            logger.info("rebalance task selected {}", rebalanceTask);
//            assertEquals(0, rebalanceTask.getDestInstanceId().getId());
//            assertEquals(instanceCount * 10000, rebalanceTask.getSegmentUnitToRemove().getInstanceId().getId());
//        } catch (NoNeedToRebalance e) {
//            fail();
//        }
//    }
//
//    private VolumeMetadata generateVolume(long volumeId, InstanceId instanceId, String deviceName, int segmentCount,
//            VolumeType volumeType) {
//        VolumeMetadata volume = new VolumeMetadata();
//        volume.setVolumeId(volumeId);
//        for (int i = 0; i < segmentCount; i++) {
//            SegId segId = new SegId(volumeId, i);
//            SegmentMetadata segment = new SegmentMetadata(segId, i);
//
//            InstanceId primary = new InstanceId(instanceId);
//            SegmentUnitMetadata primaryUnit = new SegmentUnitMetadata(segId, 0);
//            primaryUnit.setStatus(SegmentUnitStatus.Primary);
//            segment.putSegmentUnitMetadata(primary, primaryUnit);
//
//            List<InstanceId> secondaries = new ArrayList<>();
//            for (int j = 1; j < volumeType.getNumMembers(); j++) {
//                InstanceId secondary = new InstanceId(instanceId.getId() + j);
//                secondaries.add(secondary);
//                SegmentUnitMetadata secondaryUnit = new SegmentUnitMetadata(segId, j);
//                secondaryUnit.setStatus(SegmentUnitStatus.Secondary);
//                segment.putSegmentUnitMetadata(secondary, secondaryUnit);
//            }
//            SegmentMembership membership = new SegmentMembership(primary, secondaries);
//            for (SegmentUnitMetadata segmentUnit : segment.getSegmentUnits()) {
//                segmentUnit.setMembership(membership);
//                segmentUnit.setInstanceId(instanceId);
//                segmentUnit.setDiskName(deviceName);
//            }
//            volume.addSegmentMetadata(segment, segment.getLatestMerbership());
//        }
//        return volume;
//    }
//
//    private InstanceMetadata generateInstance(long id, int archiveCount) {
//        InstanceId instanceId = new InstanceId(id);
//        InstanceMetadata instance = new InstanceMetadata(instanceId);
//        List<RawArchiveMetadata> archiveList = generateArchives(instanceId, archiveCount);
//        instance.setArchives(archiveList);
//        return instance;
//    }
//
//    private List<RawArchiveMetadata> generateArchives(InstanceId instanceId, int count) {
//        List<RawArchiveMetadata> archiveList = new ArrayList<>();
//        for (int i = 0; i < count; i++) {
//            String name = "disk" + i;
//            RawArchiveMetadata archive = new RawArchiveMetadata();
//            archive.setInstanceId(instanceId);
//            archive.setDeviceName(name);
//            archiveList.add(archive);
//        }
//        return archiveList;
//    }
//
//    private VolumeMetadata removeSegmentUnit(SegmentUnitMetadata segmentUnit, VolumeMetadata volume) throws Exception {
//        VolumeMetadata newVolume = new VolumeMetadata();
//        for (SegmentMetadata segment : volume.getSegments()) {
//            if (segment.getSegmentUnits().contains(segmentUnit)) {
//                logger.warn("contains : {}", segment);
//                SegmentMetadata newSegmentMetadata = new SegmentMetadata(segment.getSegId(), 0);
//                for (SegmentUnitMetadata oldSegmentUnit : segment.getSegmentUnits()) {
//                    if (!oldSegmentUnit.equals(segmentUnit)) {
//                        if (oldSegmentUnit.getStatus() == SegmentUnitStatus.Secondary) {
//                            newSegmentMetadata.putSegmentUnitMetadata(
//                                    new InstanceId(oldSegmentUnit.getInstanceId().getId() + 1), oldSegmentUnit);
//                        } else {
//                            newSegmentMetadata.putSegmentUnitMetadata(
//                                    new InstanceId(oldSegmentUnit.getInstanceId().getId()), oldSegmentUnit);
//                        }
//                    }
//                }
//                SegmentMembership membership = segment.getLatestMerbership();
//                membership.getSecondaries().remove(new InstanceId(segmentUnit.getInstanceId().getId() + 2));
//                logger.warn("new segment : {}", newSegmentMetadata);
//                newVolume.addSegmentMetadata(newSegmentMetadata, membership);
//            } else {
//                newVolume.addSegmentMetadata(segment, segment.getLatestMerbership());
//            }
//        }
//        return newVolume;
//    }
//}
