package py.infocenter.rebalance.struct;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePool;
import py.infocenter.store.StorageStore;
import py.volume.VolumeMetadata;

import java.util.*;

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
     *  Determine whether the volume environment is changed(For simple judge, we don't care mix group)
     * @param storagePool newest pool information
     * @return  if volume environment of pool was changed, return true;
     */
    public boolean isVolumePoolChanged(StoragePool storagePool){
        if (storagePool.getPoolId() != poolId){
            logger.warn("pool id changed, it means storage pool changed!");
            return true;
        } else if (storagePool.getDomainId() != domainId){
            logger.warn("pool's domain id changed, it means storage pool changed!");
            return true;
        } else {
            //if archives count different, we think pool was changed(we don't care mix group)
            if (storagePool.getArchivesInDataNode().size() != archivesInDataNode.size()){
                logger.warn("archives in pool had changed,it means storage pool changed!");
                return true;
            }

            //if archives of any datanode is difference, we think pool was changed(we don't care mix group)
            Multimap<Long, Long> archivesInDataNodeInStoragePool = storagePool.getArchivesInDataNode();
            for (long datanodeId : archivesInDataNode.keySet()){
                Set<Long> lastArchivesOfDatanodeSet = new HashSet<>(archivesInDataNode.get(datanodeId));
                Collection<Long> archivesOfDatanodeInStoragePool = archivesInDataNodeInStoragePool.get(datanodeId);

                if (archivesOfDatanodeInStoragePool == null
                        || lastArchivesOfDatanodeSet.size() != archivesOfDatanodeInStoragePool.size()
                        || !lastArchivesOfDatanodeSet.containsAll(archivesOfDatanodeInStoragePool)){
                    return true;
                }
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
