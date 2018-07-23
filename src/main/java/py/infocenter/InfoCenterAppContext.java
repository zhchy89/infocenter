package py.infocenter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContextImpl;
import py.icshare.DomainStore;
import py.icshare.ServerNode;
import py.icshare.StoragePoolStore;
import py.infocenter.store.*;
import py.instance.InstanceStatus;

import java.util.List;
import java.util.Map;

/**
 * Created by jessie on 17-6-12.
 */
public class InfoCenterAppContext extends AppContextImpl {
    private final Logger logger = LoggerFactory.getLogger(InfoCenterAppContext.class);
    private InfoCenterStoreSet storeSet;
    private Map<String, Long> serverNodeReportTimeMap;
    private ServerNodeStore serverNodeStore;

    public InfoCenterAppContext(String name) {
        super(name, InstanceStatus.SUSPEND);
    }

    @Override
    public void setStatus(InstanceStatus status) {
        try {
            if (super.getStatus() == InstanceStatus.SUSPEND && status == InstanceStatus.OK) {
                logger.warn("Reload all database data into memory.");
                DomainStore domainStore = storeSet.getDomainStore();
                domainStore.clearMemoryMap();
                try {
                    domainStore.listAllDomains();
                } catch (Exception e) {
                    logger.error("can not list domain from database", e);
                    throw new RuntimeException();
                }

                DriverStore driverStore = storeSet.getDriverStore();
                driverStore.clearMemoryData();
                driverStore.list();

                StorageStore storageStore = storeSet.getStorageStore();
                storageStore.clearMemoryData();
                storageStore.list();

                StoragePoolStore storagePoolStore = storeSet.getStoragePoolStore();
                storagePoolStore.clearMemoryMap();
                try {
                    storagePoolStore.listAllStoragePools();
                } catch (Exception e) {
                    logger.error("can not list storage pools", e);
                    throw new RuntimeException();
                }

                VolumeStore volumeStore = storeSet.getVolumeStore();
                volumeStore.loadVolumeFromDbToMemory();

                reloadServerNodeReportTimeMap();
            }
        } catch (Exception e) {
            logger.error("caught an exception when reload all database into memory.", e);
            throw e;
        } finally {
            super.setStatus(status);
        }
    }

    private void reloadServerNodeReportTimeMap(){
        List<ServerNode> serverNodes = serverNodeStore.listAllServerNodes();
        for (ServerNode serverNode : serverNodes) {
            serverNodeReportTimeMap.putIfAbsent(serverNode.getId(), System.currentTimeMillis());
        }
    }

    public InfoCenterStoreSet getStoreSet() {
        return storeSet;
    }

    public void setStoreSet(InfoCenterStoreSet storeSet) {
        this.storeSet = storeSet;
    }

    public void setServerNodeReportTimeMap(Map<String, Long> serverNodeReportTimeMap) {
        this.serverNodeReportTimeMap = serverNodeReportTimeMap;
    }

    public void setServerNodeStore(ServerNodeStore serverNodeStore) {
        this.serverNodeStore = serverNodeStore;
    }
}
