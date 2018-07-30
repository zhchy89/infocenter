package py.infocenter.rebalance.struct;

import py.instance.InstanceId;

import javax.annotation.Nonnull;
@Deprecated
public class ComparableRebalanceTask extends SimpleRebalanceTask implements Comparable<ComparableRebalanceTask> {

    private double urgency = 0;

    public ComparableRebalanceTask(SimpleSegUnitInfo segmentUnitToRemove, InstanceId instanceToMigrateTo,
            int taskExpireTimeSeconds, double urgency, RebalanceTaskType taskType) {
        super(segmentUnitToRemove, instanceToMigrateTo, taskExpireTimeSeconds, taskType);
        this.urgency = urgency;
    }

    @Override
    public int compareTo(@Nonnull ComparableRebalanceTask o) {
        int urgencyCompare = Double.compare(urgency, o.urgency);
        if (urgencyCompare == 0) {
            return Long.compare(getTaskId(), o.getTaskId());
        } else {
            return urgencyCompare;
        }
    }

    @Override
    public String toString() {
        return "RebalanceTask [super=" + super.toString() + ", urgency=" + urgency + "]";
    }

}
