package py.infocenter.rebalance.selector;

import org.junit.Test;
import py.archive.segment.SegmentUnitMetadata;
import py.infocenter.rebalance.InstanceInfo;
import py.infocenter.rebalance.struct.ComparableRebalanceTask;
import py.infocenter.rebalance.struct.InstanceInfoImpl;
import py.infocenter.rebalance.struct.SimpleRebalanceTask;
import py.infocenter.rebalance.struct.SimpleSegUnitInfo;
import py.instance.InstanceId;
import py.rebalance.RebalanceTask;
import py.test.TestBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class RebalanceSelectorTest extends TestBase {

    @Test
    public void selectPrimaryRebalanceTask() throws Exception {
        List<InstanceInfoImpl> instanceInfoList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            InstanceInfoImpl instanceInfo = mock(InstanceInfoImpl.class);
            when(instanceInfo.primaryPressure()).thenReturn((double) i);
            when(instanceInfo.getInstanceId()).thenReturn(new InstanceId(i));
            int finalI = i;
            doAnswer(invocation -> {
                @SuppressWarnings("unchecked") Collection<InstanceInfo> destinations = (Collection<InstanceInfo>) invocation
                        .getArguments()[0];
                SimpleSegUnitInfo segUnitInfo = mock(SimpleSegUnitInfo.class);
                SegmentUnitMetadata segUnit = mock(SegmentUnitMetadata.class);
                when(segUnitInfo.getSegmentUnit()).thenReturn(segUnit);
                when(segUnitInfo.getInstanceId()).thenReturn(new InstanceId(finalI));
                when(segUnit.getInstanceId()).thenReturn(new InstanceId(finalI));
                return new SimpleRebalanceTask(segUnitInfo, destinations.iterator().next().getInstanceId(),
                        Integer.MAX_VALUE, RebalanceTask.RebalanceTaskType.PrimaryRebalance);
            }).when(instanceInfo).selectARebalanceTask(any(), any());

            instanceInfoList.add(instanceInfo);
        }
        RebalanceSelector selector = new RebalanceSelector(
                Arrays.asList(instanceInfoList.get(3), instanceInfoList.get(6), instanceInfoList.get(8)));
        ComparableRebalanceTask task = selector.selectPrimaryRebalanceTask();
        assertEquals(task.getInstanceToMigrateFrom(), new InstanceId(8));
        assertEquals(task.getDestInstanceId(), new InstanceId(3));
    }

    @Test
    public void selectRebalanceTaskBetweenInstances() throws Exception {
        List<InstanceInfoImpl> instanceInfoList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            InstanceInfoImpl instanceInfo = mock(InstanceInfoImpl.class);
            when(instanceInfo.calculatePressure()).thenReturn((double) i);
            when(instanceInfo.getInstanceId()).thenReturn(new InstanceId(i));
            int finalI = i;
            doAnswer(invocation -> {
                @SuppressWarnings("unchecked") Collection<InstanceInfo> destinations = (Collection<InstanceInfo>) invocation
                        .getArguments()[0];
                SimpleSegUnitInfo segUnitInfo = mock(SimpleSegUnitInfo.class);
                SegmentUnitMetadata segUnit = mock(SegmentUnitMetadata.class);
                when(segUnitInfo.getSegmentUnit()).thenReturn(segUnit);
                when(segUnitInfo.getInstanceId()).thenReturn(new InstanceId(finalI));
                when(segUnit.getInstanceId()).thenReturn(new InstanceId(finalI));
                return new SimpleRebalanceTask(segUnitInfo, destinations.iterator().next().getInstanceId(),
                        Integer.MAX_VALUE, RebalanceTask.RebalanceTaskType.NormalRebalance);
            }).when(instanceInfo).selectARebalanceTask(any(), any());

            instanceInfoList.add(instanceInfo);
        }
        RebalanceSelector selector = new RebalanceSelector(
                Arrays.asList(instanceInfoList.get(3), instanceInfoList.get(6), instanceInfoList.get(8)));
        ComparableRebalanceTask task = selector.selectNormalRebalanceTask();
        assertEquals(task.getInstanceToMigrateFrom(), new InstanceId(8));
        assertEquals(task.getDestInstanceId(), new InstanceId(3));
    }

    @Test
    public void selectRebalanceTaskInsideInstance() throws Exception {
    }

}