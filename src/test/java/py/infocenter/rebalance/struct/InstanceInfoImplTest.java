package py.infocenter.rebalance.struct;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import py.archive.segment.SegmentUnitMetadata;
import py.infocenter.rebalance.InstanceInfo;
import py.infocenter.rebalance.exception.NoSegmentUnitCanBeRemoved;
import py.infocenter.rebalance.exception.NoSuitableTask;
import py.instance.InstanceId;
import py.rebalance.RebalanceTask;
import py.test.TestBase;

public class InstanceInfoImplTest extends TestBase {

    @Test
    public void testCalculatingPressureAndFreeSpace() throws Exception {
        long segmentSize = 1;
        long initFreeSpace = 10 * segmentSize;
        List<Long> archiveIds = new ArrayList<>();
        archiveIds.add(1L);
        InstanceInfoImpl instanceInfo = new InstanceInfoImpl(new InstanceId(1l), archiveIds, 0, initFreeSpace, 0,
                segmentSize);

        SimpleSegUnitInfo segUnit1 = mock(SimpleSegUnitInfo.class);
        when(segUnit1.getArchiveId()).thenReturn(1L);

        SimpleSegUnitInfo segUnit2 = mock(SimpleSegUnitInfo.class);
        when(segUnit2.getArchiveId()).thenReturn(2L);

        instanceInfo.addSegmentUnit(segUnit1);
        instanceInfo.addSegmentUnit(segUnit2);

        assertEquals(2, instanceInfo.calculatePressure(), 0);
        assertEquals(initFreeSpace, instanceInfo.getFreeSpace());

        instanceInfo.addABogusSegmentUnit();
        instanceInfo.addABogusSegmentUnit();
        assertEquals(4, instanceInfo.calculatePressure(), 0);
        assertEquals(initFreeSpace - 2 * segmentSize, instanceInfo.getFreeSpace());
    }

    @Test
    public void testSelectedSegmentUnitOnTheHeaviestDisk() throws NoSegmentUnitCanBeRemoved, NoSuitableTask {

        SegmentUnitMetadata segUnit1 = mock(SegmentUnitMetadata.class);
        when(segUnit1.getArchiveId()).thenReturn(1L);
        SimpleSegUnitInfo segUnitInfo1 = mock(SimpleSegUnitInfo.class);
        when(segUnitInfo1.canBeMovedTo(any())).thenReturn(true);
        when(segUnitInfo1.getArchiveId()).thenReturn(1L);
        when(segUnitInfo1.getSegmentUnit()).thenReturn(segUnit1);

        SegmentUnitMetadata segUnit2 = mock(SegmentUnitMetadata.class);
        when(segUnit2.getArchiveId()).thenReturn(2L);
        SimpleSegUnitInfo segUnitInfo2 = mock(SimpleSegUnitInfo.class);
        when(segUnitInfo2.canBeMovedTo(any())).thenReturn(true);
        when(segUnitInfo2.getArchiveId()).thenReturn(2L);
        when(segUnitInfo2.getSegmentUnit()).thenReturn(segUnit2);

        SegmentUnitMetadata segUnit3 = mock(SegmentUnitMetadata.class);
        when(segUnit3.getArchiveId()).thenReturn(2L);
        SimpleSegUnitInfo segUnitInfo3 = mock(SimpleSegUnitInfo.class);
        when(segUnitInfo3.canBeMovedTo(any())).thenReturn(true);
        when(segUnitInfo3.getArchiveId()).thenReturn(2L);
        when(segUnitInfo3.getSegmentUnit()).thenReturn(segUnit3);

        InstanceId instance1 = new InstanceId(1l);
        List<Long> archiveIds = new ArrayList<>();
        archiveIds.add(1L);
        archiveIds.add(2L);
        InstanceInfoImpl instanceInfo = new InstanceInfoImpl(instance1, archiveIds, 1, 5, 0, 1);
        instanceInfo.addSegmentUnit(segUnitInfo1);
        instanceInfo.addSegmentUnit(segUnitInfo2);
        instanceInfo.addSegmentUnit(segUnitInfo3);

        List<InstanceInfo> destinations = new ArrayList<>();
        InstanceInfoImpl destination = mock(InstanceInfoImpl.class);
        when(destination.getFreeSpace()).thenReturn(10l);
        when(destination.getInstanceId()).thenReturn(new InstanceId(100l));
        destinations.add(destination);
        assertEquals(2L,
                instanceInfo.selectARebalanceTask(destinations, RebalanceTask.RebalanceTaskType.NormalRebalance)
                            .getSourceSegmentUnit().getArchiveId());
    }

}
