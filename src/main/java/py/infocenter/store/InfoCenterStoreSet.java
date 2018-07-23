package py.infocenter.store;

import py.icshare.DomainStore;
import py.icshare.StoragePoolStore;

/**
 * Created by jessie on 17-6-12.
 */
public class InfoCenterStoreSet {
    private DriverStore driverStore;
    private StorageStore storageStore;
    private DomainStore domainStore;
    private StoragePoolStore storagePoolStore;
    private VolumeStore volumeStore;

    public DriverStore getDriverStore() {
        return driverStore;
    }

    public VolumeStore getVolumeStore() {
        return volumeStore;
    }

    public void setVolumeStore(VolumeStore volumeStore) {
        this.volumeStore = volumeStore;
    }

    public void setDriverStore(DriverStore driverStore) {
        this.driverStore = driverStore;
    }

    public StorageStore getStorageStore() {
        return storageStore;
    }

    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    public DomainStore getDomainStore() {
        return domainStore;
    }

    public void setDomainStore(DomainStore domainStore) {
        this.domainStore = domainStore;
    }

    public StoragePoolStore getStoragePoolStore() {
        return storagePoolStore;
    }

    public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
        this.storagePoolStore = storagePoolStore;
    }
}
