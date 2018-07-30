package py.infocenter.rebalance.struct;

import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.instance.InstanceId;
import py.rebalance.RebalanceTask;

/**
 * Created by zcy on 18-7-21.
 */
public class InternalRebalanceTask {
    private final InstanceId srcInstanceId;
    private final InstanceId destInstanceId;
    private final SegId segmentId;
    private final long targetArchiveId;
    private final RebalanceTask.RebalanceTaskType taskType;

    public InternalRebalanceTask(SegId segmentId, InstanceId srcInstanceId, InstanceId destInstanceId,
                          long targetArchiveId, RebalanceTask.RebalanceTaskType taskType){
        this.taskType = taskType;
        this.segmentId = segmentId;
        this.srcInstanceId = srcInstanceId;
        this.destInstanceId = destInstanceId;
        this.targetArchiveId = targetArchiveId;
    }

    public RebalanceTask.RebalanceTaskType getTaskType() {
        return taskType;
    }

    public SegId getSegmentId() {
        return segmentId;
    }

    public InstanceId getSrcInstanceId() {
        return srcInstanceId;
    }

    public InstanceId getDestInstanceId() {
        return destInstanceId;
    }

    public long getTargetArchiveId() {
        return targetArchiveId;
    }

    @Override
    public String toString() {
        return "InternalRebalanceTask{" +
                "srcInstanceId=" + srcInstanceId +
                ", destInstanceId=" + destInstanceId +
                ", segmentId=" + segmentId +
                ", targetArchiveId=" + targetArchiveId +
                ", taskType=" + taskType +
                '}';
    }
}
