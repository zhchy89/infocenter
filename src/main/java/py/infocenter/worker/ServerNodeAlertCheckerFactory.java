package py.infocenter.worker;

import py.icshare.InstanceMaintenanceDBStore;
import py.infocenter.store.ServerNodeStore;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;

import java.util.Map;

public class ServerNodeAlertCheckerFactory implements WorkerFactory {
    private Map<String, Long> serverNodeReportTimeMap;
    private int serverNodeReportOverTimeSecond;
    private InstanceStore instanceStore;
    private ServerNodeStore serverNodeStore;
    private InstanceMaintenanceDBStore instanceMaintenanceDBStore;

    @Override
    public Worker createWorker() {
        ServerNodeAlertChecker serverNodeAlertChecker = new ServerNodeAlertChecker(serverNodeReportTimeMap,
                serverNodeReportOverTimeSecond, instanceStore, serverNodeStore, instanceMaintenanceDBStore);
        return serverNodeAlertChecker;
    }

    public void setServerNodeReportTimeMap(Map<String, Long> serverNodeReportTimeMap) {
        this.serverNodeReportTimeMap = serverNodeReportTimeMap;
    }

    public void setServerNodeReportOverTimeSecond(int serverNodeReportOverTimeSecond) {
        this.serverNodeReportOverTimeSecond = serverNodeReportOverTimeSecond;
    }

    public void setInstanceStore(InstanceStore instanceStore) {
        this.instanceStore = instanceStore;
    }

    public void setServerNodeStore(ServerNodeStore serverNodeStore) {
        this.serverNodeStore = serverNodeStore;
    }

    public void setInstanceMaintenanceDBStore(InstanceMaintenanceDBStore instanceMaintenanceDBStore) {
        this.instanceMaintenanceDBStore = instanceMaintenanceDBStore;
    }
}
