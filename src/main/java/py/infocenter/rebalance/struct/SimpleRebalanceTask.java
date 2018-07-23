package py.infocenter.rebalance.struct;

import py.instance.InstanceId;
import py.rebalance.RebalanceTask;

public class SimpleRebalanceTask extends RebalanceTask {

    private SimpleSegUnitInfo mySourceSegmentUnit;

    public SimpleRebalanceTask(SimpleSegUnitInfo segmentUnitToRemove, InstanceId instanceToMigrateTo,
            int taskExpireTimeSeconds, RebalanceTaskType taskType) {
        super(segmentUnitToRemove.getSegmentUnit(), instanceToMigrateTo, taskExpireTimeSeconds, taskType);
        this.mySourceSegmentUnit = segmentUnitToRemove;
    }

    public SimpleSegUnitInfo getMySourceSegmentUnit() {
        return mySourceSegmentUnit;
    }

    @Override
    public InstanceId getInstanceToMigrateFrom() {
        return mySourceSegmentUnit.getInstanceId();
    }

}
