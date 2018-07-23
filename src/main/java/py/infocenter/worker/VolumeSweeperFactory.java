package py.infocenter.worker;

import py.app.context.AppContext;
import py.common.client.thrift.GenericThriftClientFactory;
import py.icshare.StoragePoolStore;
import py.infocenter.store.CloneRelationshipsDBStore;
import py.infocenter.store.VolumeStatusTransitionStore;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;
import py.thrift.datanode.service.DataNodeService;

public class VolumeSweeperFactory implements WorkerFactory {

    private static VolumeSweeper worker;

    private VolumeStore volumeStore;

    private VolumeStatusTransitionStore volumeStatusStore;

    private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory;

    private int timeout;

    private InstanceStore instanceStore;

    private AppContext appContext;
    private long deadVolumeToRemoveTime;

    private StoragePoolStore storagePoolStore;
    private CloneRelationshipsDBStore cloneRelationshipsStore;

    public long getDeadVolumeToRemoveTime() {
        return deadVolumeToRemoveTime;
    }

    public void setDeadVolumeToRemoveTime(long deadVolumeToRemoveTime) {
        this.deadVolumeToRemoveTime = deadVolumeToRemoveTime;
    }

    public VolumeStore getVolumeStore() {
        return volumeStore;
    }

    public void setVolumeStore(VolumeStore volumeStore) {
        this.volumeStore = volumeStore;
    }

    public GenericThriftClientFactory<DataNodeService.Iface> getDataNodeClientFactory() {
        return dataNodeClientFactory;
    }

    public void setDataNodeClientFactory(GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory) {
        this.dataNodeClientFactory = dataNodeClientFactory;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public void setAppContext(AppContext appContext) {
        this.appContext = appContext;
    }

    public AppContext getAppContext() {
        return this.appContext;
    }

    public InstanceStore getInstanceStore() {
        return instanceStore;
    }

    public void setInstanceStore(InstanceStore instanceStore) {
        this.instanceStore = instanceStore;
    }

    public void setVolumeStatusTransitionStore(VolumeStatusTransitionStore store) {
        this.volumeStatusStore = store;
    }

    @Override
    public Worker createWorker() {
        if (worker == null) {
            worker = new VolumeSweeper();
            worker.setVolumeStore(volumeStore);
            worker.setDataNodeClientFactory(dataNodeClientFactory);
            worker.setInstanceStore(instanceStore);
            worker.setTimeout(timeout);
            worker.setAppContext(appContext);
            worker.setDeadTimeToRemove(deadVolumeToRemoveTime);
            worker.setVolumeStatusTransitionStore(volumeStatusStore);
            worker.setStoragePoolStore(storagePoolStore);
            worker.setCloneRelationshipsStore(cloneRelationshipsStore);
        }

        return worker;
    }

    public StoragePoolStore getStoragePoolStore() {
        return storagePoolStore;
    }

    public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
        this.storagePoolStore = storagePoolStore;
    }

    public CloneRelationshipsDBStore getCloneRelationshipsStore() {
        return cloneRelationshipsStore;
    }

    public void setCloneRelationshipsStore(CloneRelationshipsDBStore cloneRelationshipsStore) {
        this.cloneRelationshipsStore = cloneRelationshipsStore;
    }
}
