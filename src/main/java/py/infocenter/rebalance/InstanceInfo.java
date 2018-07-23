package py.infocenter.rebalance;

import py.infocenter.rebalance.exception.NoSegmentUnitCanBeRemoved;
import py.infocenter.rebalance.exception.NoSuitableTask;
import py.instance.InstanceId;
import py.rebalance.RebalanceTask;

import javax.annotation.Nonnull;
import java.util.Collection;

public abstract class InstanceInfo implements Comparable<InstanceInfo> {

    protected InstanceId instanceId;

    public InstanceInfo(InstanceId instanceId) {
        this.instanceId = instanceId;
    }

    public abstract double calculatePressure();

    public abstract RebalanceTask selectARebalanceTask(Collection<InstanceInfo> destinations,
            RebalanceTask.RebalanceTaskType taskType) throws NoSuitableTask;

    public abstract int getGroupId();

    public abstract int getDiskCount();

    public abstract int getFreeFlexibleSegmentUnitCount();

    public abstract long getFreeSpace();

    @Override
    public final int compareTo(@Nonnull InstanceInfo other) {
        int pressureCompare = Double.compare(calculatePressure(), other.calculatePressure());
        if (pressureCompare == 0) {
            return Long.compare(getInstanceId().getId(), other.getInstanceId().getId());
        } else {
            return pressureCompare;
        }
    }

    public InstanceId getInstanceId() {
        return this.instanceId;
    }

}
