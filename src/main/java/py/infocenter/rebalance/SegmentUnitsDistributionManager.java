package py.infocenter.rebalance;

import com.google.common.collect.Multimap;
import org.apache.thrift.TException;
import py.icshare.InstanceMetadata;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.instance.InstanceId;
import py.rebalance.RebalanceTask;
import py.thrift.share.*;
import py.volume.VolumeType;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manager for segment units' distribution, responsible for :
 *
 * 1. decide the init segment units' distribution when creating volume;
 *
 * 2. select rebalance tasks when new resources have been added/removed to/from the pool.
 *
 * @author tyr
 */
public interface SegmentUnitsDistributionManager {

    /**
     * select a rebalance task if needed.
     *
     * @param record whether to record this selection, this should be given according to whether the returned task will
     *               be processed.
     * @throws NoNeedToRebalance if there is no need to do rebalance
     */
    @Deprecated
    RebalanceTask selectRebalanceTask(boolean record) throws NoNeedToRebalance;

    /**
     * select rebalance tasks if needed.
     *
     * @return rebalanceTask
     *
     * @throws NoNeedToRebalance if there is no need to do rebalance
     */
    List<RebalanceTask> selectRebalanceTasks(InstanceId instanceId) throws NoNeedToRebalance;

    boolean discardRebalanceTask(long taskId);

    /**
     * return a segment units' distribution map for a new volume.
     *
     * @param expectedSize    the new volume size
     * @param volumeType      volume type
     * @param segmentWrapSize volume could be divided into several wrappers according to the segmentWrapSize, for
     *                        example, a 30G volume with segmentWrapSize 10G will be divided into 3 wrappers, we will
     *                        try to ensure any two segment units in the same wrapper doesn't lay on the same disk.
     * @param storagePoolId   storage pool id
     */
    Map<Integer, Map<SegmentUnitType_Thrift, List<InstanceIdAndEndPoint_Thrift>>> reserveVolume(long expectedSize,
            VolumeType volumeType, boolean isSimpleConfiguration, int segmentWrapSize, Long storagePoolId)
            throws NotEnoughGroupException_Thrift, NotEnoughSpaceException_Thrift,
            NotEnoughNormalGroupException_Thrift, TException;

    /**
     * update simple datanode group id and instance map(groupId2InstanceIdMap),
     * when any datanode's type is SIMPLE,
     * @param instanceMetadata datanode information
     */
    void updateSimpleDatanodeInfo(InstanceMetadata instanceMetadata);

    /**
     * get simple datanode's instance id set
     * @return all simple datanode's instance id set
     */
    Set<Long> getSimpleDatanodeInstanceIdSet();
}
