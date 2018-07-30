package py.infocenter.rebalance.struct;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.mockito.Mockito;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.infocenter.rebalance.InstanceInfo;
import py.instance.InstanceId;
import py.test.TestBase;
import py.volume.VolumeType;

public class SimpleSegUnitTest extends TestBase {

    @Test
    public void testCanBeMovedToWithOtherStatus() {
        SimpleSegmentInfo segment = generateSegment(new SimpleVolumeInfo(1l, VolumeType.REGULAR, 1l), 0);
        SimpleSegUnitInfo segUnit = segment.getSecondaries().get(0);

        for (SegmentUnitStatus status : SegmentUnitStatus.values()) {
            if (status == SegmentUnitStatus.Primary || status == SegmentUnitStatus.PrePrimary
                    || status == SegmentUnitStatus.PrimaryClone || status == SegmentUnitStatus.Deleting
                    || status == SegmentUnitStatus.Deleted) {
                segUnit.setStatus(status);
                assertTrue(!segUnit.canBeMovedTo(Mockito.mock(InstanceInfo.class)));
            }
        }
        segUnit.setStatus(SegmentUnitStatus.Secondary);

        segUnit = segment.getPrimary();
        segUnit.setStatus(SegmentUnitStatus.Secondary);
        assertTrue(!segUnit.canBeMovedTo(Mockito.mock(InstanceInfo.class)));

    }

    @Test
    public void testCanBeMovedToWithNotEnoughMembers() {
        SimpleSegmentInfo segment = generateSegment(new SimpleVolumeInfo(1l, VolumeType.REGULAR, 1l), 0);
        segment.getPrimary().freeMySelf();
        for (SimpleSegUnitInfo segUnit : segment.getSegUnits()) {
            assertTrue(!segUnit.canBeMovedTo(Mockito.mock(InstanceInfo.class)));
        }

        segment = generateSegment(new SimpleVolumeInfo(1l, VolumeType.REGULAR, 1l), 0);
        SimpleSegUnitInfo segUnit1 = segment.getSecondaries().get(0);
        segUnit1.freeMySelf();
        for (SimpleSegUnitInfo segUnit : segment.getSegUnits()) {
            assertTrue(!segUnit.canBeMovedTo(Mockito.mock(InstanceInfo.class)));
        }

    }

    @Test
    public void testCanBeMovedToWithWrongGroup() {
        SimpleSegmentInfo segment = generateSegment(new SimpleVolumeInfo(1l, VolumeType.REGULAR, 1l), 0);
        InstanceInfoImpl instanceInfo = Mockito.mock(InstanceInfoImpl.class);
        Mockito.when(instanceInfo.getGroupId()).thenReturn(segment.getPrimary().getGroupId());

        for (SimpleSegUnitInfo segUnit : segment.getSegUnits()) {
            assertTrue(!segUnit.canBeMovedTo(instanceInfo));
        }

    }

    @Test
    public void testCanBeMovedToWithAPerfectSegUnit() {

        InstanceInfoImpl instanceInfo = Mockito.mock(InstanceInfoImpl.class);
        Mockito.when(instanceInfo.getGroupId()).thenReturn(4);

        SimpleSegmentInfo segment = generateSegment(new SimpleVolumeInfo(1l, VolumeType.REGULAR, 1l), 0);
        SimpleSegUnitInfo segUnit = segment.getSecondaries().get(0);
        for (SegmentUnitStatus status : SegmentUnitStatus.values()) {
            if (status == SegmentUnitStatus.Primary || status == SegmentUnitStatus.PrePrimary
                    || status == SegmentUnitStatus.PrimaryClone || status == SegmentUnitStatus.Deleting
                    || status == SegmentUnitStatus.Deleted) {
                segUnit.setStatus(status);
                assertTrue(!segUnit.canBeMovedTo(instanceInfo));
            } else {
                segUnit.setStatus(status);
                assertTrue(segUnit.canBeMovedTo(instanceInfo));
            }
        }
    }

    private SimpleSegmentInfo generateSegment(SimpleVolumeInfo volume, int segIndex) {
        int memberCount = volume.getVolumeType().getNumMembers();
        long storagePoolId = volume.getStoragePoolId();
        SegId segId = new SegId(volume.getVolumeId(), segIndex);

        List<Integer> groups = new ArrayList<>();
        for (int i = 0; i < memberCount; i++) {
            groups.add(i);
        }
        Collections.shuffle(groups);

        SimpleSegmentInfo segment = new SimpleSegmentInfo(segId, null);
        volume.addSegment(segment);

        for (int i = 0; i < memberCount; i++) {
            int group = groups.get(i);
            SimpleSegUnitInfo segUnit = new SimpleSegUnitInfo(segId, group, storagePoolId, new InstanceId(group));
            segUnit.setArchiveId(1L);
            segUnit.setStatus(group == 0 ? SegmentUnitStatus.Primary : SegmentUnitStatus.Secondary);
            segment.addSegmentUnit(segUnit);
        }

        return segment;
    }

}
