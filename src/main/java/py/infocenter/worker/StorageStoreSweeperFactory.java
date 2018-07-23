package py.infocenter.worker;

import py.app.context.AppContext;
import py.icshare.DomainStore;
import py.icshare.InstanceMaintenanceDBStore;
import py.icshare.StoragePoolStore;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;

public class StorageStoreSweeperFactory implements WorkerFactory {

    private static StorageStoreSweeper worker;

    private StorageStore storageStore;

    private StoragePoolStore storagePoolStore;

    private DomainStore domainStore;

    private VolumeStore volumeStore;

    private long segmentSize;

    private int timeToRemove;

    private AppContext appContext;

    private InstanceMaintenanceDBStore instanceMaintenanceDBStore;

    @Override
    public Worker createWorker() {
        if (worker == null) {
            worker = new StorageStoreSweeper();
            worker.setInstanceMetadataStore(storageStore);
            worker.setTimeToRemove(timeToRemove);
            worker.setAppContext(appContext);
            worker.setStoragePoolStore(storagePoolStore);
            worker.setDomainStore(domainStore);
            worker.setVolumeStore(volumeStore);
            worker.setSegmentSize(segmentSize);
            worker.setInstanceMaintenanceDBStore(instanceMaintenanceDBStore);
        }
        return worker;
    }

    public void setAppContext(AppContext appContext) {
        this.appContext = appContext;
    }

    public AppContext getAppContext() {
        return this.appContext;
    }

    public int getTimeToRemove() {
        return timeToRemove;
    }

    public void setTimeToRemove(int timeToRemove) {
        this.timeToRemove = timeToRemove;
    }

    public StorageStore getStorageStore() {
        return storageStore;
    }

    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    public StoragePoolStore getStoragePoolStore() {
        return storagePoolStore;
    }

    public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
        this.storagePoolStore = storagePoolStore;
    }

    public DomainStore getDomainStore() {
        return domainStore;
    }

    public void setDomainStore(DomainStore domainStore) {
        this.domainStore = domainStore;
    }

    public VolumeStore getVolumeStore() {
        return volumeStore;
    }

    public void setVolumeStore(VolumeStore volumeStore) {
        this.volumeStore = volumeStore;
    }

    public long getSegmentSize() {
        return segmentSize;
    }

    public void setSegmentSize(long segmentSize) {
        this.segmentSize = segmentSize;
    }

    public InstanceMaintenanceDBStore getInstanceMaintenanceDBStore() {
        return instanceMaintenanceDBStore;
    }

    public void setInstanceMaintenanceDBStore(InstanceMaintenanceDBStore instanceMaintenanceDBStore) {
        this.instanceMaintenanceDBStore = instanceMaintenanceDBStore;
    }
}
