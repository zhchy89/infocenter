package py.infocenter.rebalance.struct;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePool;
import py.infocenter.store.StorageStore;
import py.volume.VolumeMetadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * simple pool information
 */
public class SimulatePool {
    private Logger logger = LoggerFactory.getLogger(SimulatePool.class);
    private long poolId;
    private long domainId;
    private Multimap<Long, Long> archivesInDataNode;    // <datanodeId, archiveId>
    private Set<Long> volumeIds;

    public SimulatePool(StoragePool storagePool){
        this.poolId = storagePool.getPoolId();
        this.domainId = storagePool.getDomainId();
        this.archivesInDataNode = HashMultimap.create(storagePool.getArchivesInDataNode());
        this.volumeIds = new HashSet<>(storagePool.getVolumeIds());
    }

    /**
     *
     * @param storagePool
     * @return
     */
    public boolean isVolumePoolChanged(VolumeMetadata volumeMetadata, StoragePool storagePool, StorageStore storageStore,
                                       SimpleDatanodeManager simpleDatanodeManager){
        if (storagePool.getPoolId() != poolId){
            logger.warn("pool id changed, it means storage pool changed!");
            return true;
        } else if (storagePool.getDomainId() != domainId){
            logger.warn("pool's domain id changed, it means storage pool changed!");
            return true;
        } else {
            //获取simple group和mix group数
            //Set<Long> normalIdSet = new HashSet<>();
            Set<Integer> normalGroupSet = new HashSet<>();
            //Set<Long> simpleIdSet = new HashSet<>();
            Set<Integer> simpleGroupIdSet = new HashSet<>();
            Set<Integer> mixGroupIdSet = new HashSet<>();
            Map<Long, InstanceMetadata> instanceId2InstanceMap = new HashMap<>();
            getDatanodeInfo(storageStore, simpleDatanodeManager,
                    null, null, normalGroupSet, simpleGroupIdSet, mixGroupIdSet, instanceId2InstanceMap);

            boolean isNeedDetailJudge = false;
            //如果simple group + mix group数 <= arbiter分配数，且变更的磁盘在mix group中，那么对P，S的分布没有任何影响
            if (mixGroupIdSet.size() > 0
                    && simpleGroupIdSet.size() + mixGroupIdSet.size() <= volumeMetadata.getVolumeType().getNumArbiters()){
                isNeedDetailJudge = true;
            }

            Set<Long> lastDatanodeSet = archivesInDataNode.keySet();
            Set<Long> poolDatanodeSet = storagePool.getArchivesInDataNode().keySet();

            Set<Long> mixDatanodeSet = new HashSet<>(lastDatanodeSet);
            mixDatanodeSet.retainAll(poolDatanodeSet);

            Set<Long> diffDatanodeSet = new HashSet<>(lastDatanodeSet);
            diffDatanodeSet.addAll(poolDatanodeSet);
            diffDatanodeSet.removeAll(mixDatanodeSet);

            /*
             * 判断结点是否有变更
             */
            //如果有不同的结点，说明pool中添加或删除了结点
            if (diffDatanodeSet.size() != 0){
                logger.warn("datanode in pool had changed, and mix group == 0 or simple group + mix group <= arbiter num, it means storage pool changed!");
                return true;
            }


            /*
             * 判断磁盘是否有变更
             */
            Multimap<Long, Long> archivesInDataNodeInStoragePool = storagePool.getArchivesInDataNode();
            for (long datanodeId : mixDatanodeSet){
                InstanceMetadata datanodeInfo = storageStore.get(datanodeId);
                //如果在当前数据库中没能找到结点，说明结点可能被删除，为了方便检测，认为是pool变动
                if (datanodeInfo == null){
                    logger.warn("cannot found instance:{} in storage store, it means storage pool changes", datanodeId);
                    return true;
                }

                if (isNeedDetailJudge){
                    //如果变动的结点在mix group中，即使是磁盘变更了，也不用关心
                    if (mixGroupIdSet.contains(datanodeInfo.getGroup().getGroupId())){
                        continue;
                    }
                }

                Set<Long> lastArchivesOfDatanodeSet = new HashSet<>(archivesInDataNode.get(datanodeId));
                Set<Long> archivesOfDatandoeInStoragePoolSet = new HashSet<>(archivesInDataNodeInStoragePool.get(datanodeId));

                //如果磁盘发生了变动，不考虑权重，都认为是发生了变动
                if (lastArchivesOfDatanodeSet.size() != archivesOfDatandoeInStoragePoolSet.size()
                        || !lastArchivesOfDatanodeSet.containsAll(archivesOfDatandoeInStoragePoolSet)){
                    return true;
                }
            }

            //
            if (isNeedDetailJudge){

            }



        }

        return false;
    }

    /**
     *
     * @param storageStore   storage store db
     * @param simpleDatanodeManager   all simple datanode manager object
     * @param normalIdSet   all normal data node in pool
     * @param simpleIdSet   all simple data node in pool
     * @param simpleGroupIdSet  all simple group in pool
     * @param normalGroupSet   all normal data node group
     * @param instanceId2InstanceMap    all instance information in  pool
     */
    public void getDatanodeInfo(StorageStore storageStore, SimpleDatanodeManager simpleDatanodeManager,
                                Set<Long> normalIdSet, Set<Long> simpleIdSet,
                                Set<Integer> normalGroupSet, Set<Integer> simpleGroupIdSet,
                                Set<Integer> mixGroupIdSet, Map<Long, InstanceMetadata> instanceId2InstanceMap){
        //simpleDatanode may be have no archives, so it cannot add to pool
        //we will create arbiter at all simpleDatanode which be owned domain
        Set<Long> allSimpleDatanodeIdSet = simpleDatanodeManager.getSimpleDatanodeInstanceIdSet();

        Set<Integer> normalGroupSetBac = new HashSet<>();
        Set<Integer> simpleGroupIdSetBac = new HashSet<>();

        long domainId = -1;
        // for (simpleDatanodeManager.getSimpleDatanodeGroupIdSet())
        // initialize group set and simple datanode hosts
        for (InstanceMetadata instanceMetadata : storageStore.list()) {
            domainId = instanceMetadata.getDomainId();

            if (instanceId2InstanceMap != null){
                instanceId2InstanceMap.put(instanceMetadata.getInstanceId().getId(), instanceMetadata);
            }

            if (!allSimpleDatanodeIdSet.contains(instanceMetadata.getInstanceId().getId())) {
                normalGroupSetBac.add(instanceMetadata.getGroup().getGroupId());

                if (normalIdSet != null){
                    normalIdSet.add(instanceMetadata.getInstanceId().getId());
                }
            }
        }

        //simpleDatanode may be have no archives, so it cannot add to pool
        //we will create arbiter at all simpleDatanode which be owned domain
        for (long simpleDatanodeInstanceId : allSimpleDatanodeIdSet) {
            InstanceMetadata instanceMetadata = storageStore.get(simpleDatanodeInstanceId);

            if (instanceMetadata.getDomainId() == domainId) {
                if (instanceId2InstanceMap != null) {
                    instanceId2InstanceMap.put(instanceMetadata.getInstanceId().getId(), instanceMetadata);
                }

                simpleGroupIdSetBac.add(instanceMetadata.getGroup().getGroupId());
                if (simpleIdSet != null){
                    simpleIdSet.add(simpleDatanodeInstanceId);
                }
            }
        }

        if (normalGroupSet == null && simpleGroupIdSet == null && mixGroupIdSet == null){
            return;
        }

        Set<Integer> mixGroupIdSetBac = new HashSet<>(normalGroupSetBac);
        mixGroupIdSetBac.retainAll(simpleGroupIdSetBac);

        normalGroupSetBac.removeAll(mixGroupIdSetBac);
        simpleGroupIdSetBac.removeAll(mixGroupIdSetBac);

        if (normalGroupSet != null){
            normalGroupSet.addAll(normalGroupSetBac);
        }
        if (simpleGroupIdSet != null){
            simpleGroupIdSet.addAll(simpleGroupIdSetBac);
        }
        if (mixGroupIdSet != null){
            mixGroupIdSet.addAll(mixGroupIdSetBac);
        }
    }

    @Override
    public String toString() {
        return "SimulatePool{" +
                "poolId=" + poolId +
                ", domainId=" + domainId +
                ", archivesInDataNode=" + archivesInDataNode +
                ", volumeIds=" + volumeIds +
                '}';
    }
}
