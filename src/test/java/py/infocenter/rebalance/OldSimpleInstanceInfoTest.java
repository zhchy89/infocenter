//package py.infocenter.rebalance;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.fail;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.ListIterator;
//import java.util.Random;
//
//import org.junit.Test;
//
//import py.archive.RawArchiveMetadata;
//import py.archive.segment.SegId;
//import py.archive.segment.SegmentUnitMetadata;
//import py.archive.segment.SegmentUnitStatus;
//import py.common.RequestIdBuilder;
//import py.icshare.InstanceMetadata;
//import py.infocenter.rebalance.simple.SimpleInstanceInfo;
//import py.instance.InstanceId;
//import py.membership.SegmentMembership;
//import py.test.TestBase;
//
//public class OldSimpleInstanceInfoTest extends TestBase {
//
//    private Random random = new Random(System.currentTimeMillis());
//
//    @Test
//    public void testInitInstanceInfo() {
//        InstanceMetadata instance = generateInstance(100);
//        List<SegmentUnitMetadata> segmentUnits = generateSegmentUnits(instance.getArchives(), 200);
//        SimpleInstanceInfo instanceInfo = new SimpleInstanceInfo(instance.getInstanceId());
//
//        instanceInfo.init(instance.getArchives().size(), segmentUnits);
//        logger.debug("pressure : {}", instanceInfo.calculatePressure());
//    }
//
//    @Test
//    public void testSelectSegmentUnitWhenAllDisksAllPrimary() {
//        InstanceMetadata instance = generateInstance(20);
//        List<SegmentUnitMetadata> segmentUnits = new ArrayList<>();
//        ListIterator<RawArchiveMetadata> it = instance.getArchives().listIterator();
//        while (it.hasNext()) {
//            long volumeId = it.nextIndex();
//            RawArchiveMetadata archive = it.next();
//            int count = random.nextInt(100) + 1;
//            boolean isPrimary = true;
//            logger.info("archive {} will have {} segment units with volumeId {}", archive.getDeviceName(), count,
//                    volumeId);
//            for (int i = 0; i < count; i++) {
//                SegmentUnitMetadata segmentUnit = generateSegmentUnit(archive.getDeviceName(), true, isPrimary,
//                        volumeId, i);
//                segmentUnits.add(segmentUnit);
//            }
//        }
//        SimpleInstanceInfo instanceInfo = new SimpleInstanceInfo(instance.getInstanceId());
//        instanceInfo.init(instance.getArchives().size(), segmentUnits);
//        try {
//            SegmentUnitMetadata segmentUnit = instanceInfo.selectASegmentUnitToRemove();
//            logger.info("segment unit selected : {}", segmentUnit);
//            fail("select segment unit successed ? ");
//        } catch (NoSegmentUnitCanBeRemoved e) {
//            logger.info("cannot select a segment unit", e);
//            return;
//        }
//        fail("select segment unit successed ? ");
//    }
//
//    @Test
//    public void testSelectSegmentUnitWhenAllDisksHasNoFullMembership() {
//        InstanceMetadata instance = generateInstance(20);
//        List<SegmentUnitMetadata> segmentUnits = new ArrayList<>();
//        ListIterator<RawArchiveMetadata> it = instance.getArchives().listIterator();
//        while (it.hasNext()) {
//            long volumeId = it.nextIndex();
//            RawArchiveMetadata archive = it.next();
//            int count = random.nextInt(100) + 1;
//            boolean membershipFull = false;
//            logger.info("archive {} will have {} segment units with volumeId {}", archive.getDeviceName(), count,
//                    volumeId);
//            for (int i = 0; i < count; i++) {
//                SegmentUnitMetadata segmentUnit = generateSegmentUnit(archive.getDeviceName(), membershipFull, false,
//                        volumeId, i);
//                segmentUnits.add(segmentUnit);
//            }
//        }
//        SimpleInstanceInfo instanceInfo = new SimpleInstanceInfo(instance.getInstanceId());
//        instanceInfo.init(instance.getArchives().size(), segmentUnits);
//        try {
//            SegmentUnitMetadata segmentUnit = instanceInfo.selectASegmentUnitToRemove();
//            logger.info("segment unit selected : {}", segmentUnit);
//            fail("select segment unit successed ? ");
//        } catch (NoSegmentUnitCanBeRemoved e) {
//            logger.info("cannot select a segment unit", e);
//            return;
//        }
//        fail("select segment unit successed ? ");
//    }
//
//    @Test
//    public void testSelectSegmentUnitWhenDisksHasNoFullMembership() {
//        InstanceMetadata instance = generateInstance(20);
//        List<SegmentUnitMetadata> segmentUnits = new ArrayList<>();
//        ListIterator<RawArchiveMetadata> it = instance.getArchives().listIterator();
//        while (it.hasNext()) {
//            long volumeId = it.nextIndex();
//            RawArchiveMetadata archive = it.next();
//            int count = random.nextInt(100) + 1;
//            boolean membershipFull = true;
//            if (it.hasNext()) {
//                count += 102;
//                membershipFull = false;
//            }
//            logger.info("archive {} will have {} segment units with volumeId {}", archive.getDeviceName(), count,
//                    volumeId);
//            for (int i = 0; i < count; i++) {
//                SegmentUnitMetadata segmentUnit = generateSegmentUnit(archive.getDeviceName(), membershipFull, false,
//                        volumeId, i);
//                segmentUnits.add(segmentUnit);
//            }
//        }
//        SimpleInstanceInfo instanceInfo = new SimpleInstanceInfo(instance.getInstanceId());
//        instanceInfo.init(instance.getArchives().size(), segmentUnits);
//        try {
//            SegmentUnitMetadata segmentUnit = instanceInfo.selectASegmentUnitToRemove();
//            logger.info("segment unit selected : {}", segmentUnit);
//            assertEquals(instance.getArchives().size() - 1, segmentUnit.getSegId().getVolumeId().getId());
//        } catch (NoSegmentUnitCanBeRemoved e) {
//            logger.error("cannot select a segment unit", e);
//            fail("select segment unit failed");
//        }
//    }
//
//    @Test
//    public void testSelectSegmentUnitWhenDisksHasNoSecondary() {
//        InstanceMetadata instance = generateInstance(20);
//        List<SegmentUnitMetadata> segmentUnits = new ArrayList<>();
//        ListIterator<RawArchiveMetadata> it = instance.getArchives().listIterator();
//        while (it.hasNext()) {
//            long volumeId = it.nextIndex();
//            RawArchiveMetadata archive = it.next();
//            int count = random.nextInt(100) + 1;
//            boolean isPrimary = false;
//            if (it.hasNext()) {
//                count += 102;
//                isPrimary = true;
//            }
//            logger.info("archive {} will have {} segment units with volumeId {}", archive.getDeviceName(), count,
//                    volumeId);
//            for (int i = 0; i < count; i++) {
//                SegmentUnitMetadata segmentUnit = generateSegmentUnit(archive.getDeviceName(), true, isPrimary,
//                        volumeId, i);
//                segmentUnits.add(segmentUnit);
//            }
//        }
//        SimpleInstanceInfo instanceInfo = new SimpleInstanceInfo(instance.getInstanceId());
//        instanceInfo.init(instance.getArchives().size(), segmentUnits);
//        try {
//            SegmentUnitMetadata segmentUnit = instanceInfo.selectASegmentUnitToRemove();
//            logger.info("segment unit selected : {}", segmentUnit);
//            assertEquals(instance.getArchives().size() - 1, segmentUnit.getSegId().getVolumeId().getId());
//        } catch (NoSegmentUnitCanBeRemoved e) {
//            logger.error("cannot select a segment unit", e);
//            fail("select segment unit failed");
//        }
//    }
//
//    @Test
//    public void testSelectSegmentUnitSuccessfully() {
//        InstanceMetadata instance = generateInstance(20);
//        List<SegmentUnitMetadata> segmentUnits = new ArrayList<>();
//        ListIterator<RawArchiveMetadata> it = instance.getArchives().listIterator();
//        while (it.hasNext()) {
//            long volumeId = it.nextIndex();
//            RawArchiveMetadata archive = it.next();
//            int count = random.nextInt(100) + 1;
//            if (!it.hasNext()) {
//                count += 102;
//            }
//            logger.info("archive {} will have {} segment units with volumeId {}", archive.getDeviceName(), count,
//                    volumeId);
//            for (int i = 0; i < count; i++) {
//                SegmentUnitMetadata segmentUnit = generateSegmentUnit(archive.getDeviceName(), true, false, volumeId,
//                        i);
//                segmentUnits.add(segmentUnit);
//            }
//        }
//        SimpleInstanceInfo instanceInfo = new SimpleInstanceInfo(instance.getInstanceId());
//        instanceInfo.init(instance.getArchives().size(), segmentUnits);
//        try {
//            SegmentUnitMetadata segmentUnit = instanceInfo.selectASegmentUnitToRemove();
//            logger.info("segment unit selected : {}", segmentUnit);
//            assertEquals(instance.getArchives().size() - 1, segmentUnit.getSegId().getVolumeId().getId());
//        } catch (NoSegmentUnitCanBeRemoved e) {
//            logger.error("cannot select a segment unit", e);
//            fail("select segment unit failed");
//        }
//    }
//
//    private InstanceMetadata generateInstance(int archiveCount) {
//        InstanceId instanceId = new InstanceId(RequestIdBuilder.get());
//        InstanceMetadata instance = new InstanceMetadata(instanceId);
//        List<RawArchiveMetadata> archiveList = generateArchives(archiveCount);
//        instance.setArchives(archiveList);
//        return instance;
//    }
//
//    private List<RawArchiveMetadata> generateArchives(int count) {
//        List<RawArchiveMetadata> archiveList = new ArrayList<>();
//        for (int i = 0; i < count; i++) {
//            String name = "disk" + i;
//            RawArchiveMetadata archive = new RawArchiveMetadata();
//            archive.setDeviceName(name);
//            archiveList.add(archive);
//        }
//        return archiveList;
//    }
//
//    private List<SegmentUnitMetadata> generateSegmentUnits(List<RawArchiveMetadata> archives, int count) {
//        List<SegmentUnitMetadata> segmentUnits = new ArrayList<>();
//
//        for (int i = 0; i < count; i++) {
//            RawArchiveMetadata archive = archives.get(random.nextInt(archives.size()));
//            SegmentUnitMetadata segmentUnit = generateSegmentUnit(archive.getDeviceName(), random.nextBoolean(),
//                    random.nextBoolean(), 1l, i);
//            segmentUnits.add(segmentUnit);
//        }
//        return segmentUnits;
//    }
//
//    private SegmentUnitMetadata generateSegmentUnit(String diskName, boolean membershipFull, boolean isPrimary,
//            long volumeId, int segmentIndex) {
//        InstanceId primary = new InstanceId(RequestIdBuilder.get());
//        InstanceId secondary1 = new InstanceId(RequestIdBuilder.get());
//        InstanceId secondary2 = new InstanceId(RequestIdBuilder.get());
//
//        List<InstanceId> twoSecondaries = new ArrayList<>();
//        twoSecondaries.add(secondary1);
//        twoSecondaries.add(secondary2);
//
//        List<InstanceId> orphanSecondaries = new ArrayList<>();
//        orphanSecondaries.add(secondary1);
//
//        SegmentMembership fullMembership = new SegmentMembership(0, 0, primary, twoSecondaries);
//        SegmentMembership orphanMembership = new SegmentMembership(0, 0, primary, orphanSecondaries);
//        SegmentUnitMetadata segmentUnit = new SegmentUnitMetadata(new SegId(volumeId, segmentIndex), 0);
//        segmentUnit.setDiskName(diskName);
//        if (membershipFull) {
//            segmentUnit.setMembership(fullMembership);
//        } else {
//            segmentUnit.setMembership(orphanMembership);
//        }
//        if (isPrimary) {
//            segmentUnit.setStatus(SegmentUnitStatus.Primary);
//        } else {
//            segmentUnit.setStatus(SegmentUnitStatus.Secondary);
//        }
//        return segmentUnit;
//    }
//
//}
