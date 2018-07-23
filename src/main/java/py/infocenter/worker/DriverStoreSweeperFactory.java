package py.infocenter.worker;

import py.app.context.AppContext;
import py.icshare.DomainStore;
import py.infocenter.store.DriverStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;

public class DriverStoreSweeperFactory implements WorkerFactory {

    private static DriverStoreSweeper worker;

    private DriverStore driverStore;
    private DomainStore domainStore;

    private AppContext appContext;

    private long driverReportTimeout;

    public long getDriverReportTimeout() {
        return driverReportTimeout;
    }

    public void setDriverReportTimeout(long driverReportTimeout) {
        this.driverReportTimeout = driverReportTimeout;
    }

    public void setAppContext(AppContext appContext) {
        this.appContext = appContext;
    }

    public AppContext getAppContext() {
        return this.appContext;
    }

    public DriverStore getDriverStore() {
        return driverStore;
    }

    public void setDriverStore(DriverStore driverStore) {
        this.driverStore = driverStore;
    }

    @Override
    public Worker createWorker() {
        if (worker == null) {
            worker = new DriverStoreSweeper();
            worker.setDriverStore(driverStore);
            worker.setTimeout(driverReportTimeout);
            worker.setAppContext(appContext);
            worker.setDomainStore(domainStore);
        }
        return worker;
    }

    public DomainStore getDomainStore() {
        return domainStore;
    }

    public void setDomainStore(DomainStore domainStore) {
        this.domainStore = domainStore;
    }
}
