package py.infocenter.worker;

import py.app.context.AppContext;
import py.dih.client.DIHClientFactory;
import py.infocenter.store.AlarmStore;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;

public class AlarmSweeperFactory implements WorkerFactory {
    private static AlarmSweeper worker;
    private AlarmStore alarmStore;
    private AppContext appContext;
    private InstanceStore instanceStore;
    private DIHClientFactory dihClientFactory;

    @Override
    public Worker createWorker() {
        if (worker == null) {
            worker = new AlarmSweeper();
            worker.setAlarmStore(alarmStore);
            worker.setAppContext(appContext);
            worker.setInstanceStore(instanceStore);
            worker.setDihClientFactory(dihClientFactory);
        }
        return worker;
    }

    public static AlarmSweeper getWorker() {
        return worker;
    }

    public AlarmStore getAlarmStore() {
        return alarmStore;
    }

    public void setAlarmStore(AlarmStore alarmStore) {
        this.alarmStore = alarmStore;
    }

    public AppContext getAppContext() {
        return appContext;
    }

    public void setAppContext(AppContext appContext) {
        this.appContext = appContext;
    }

    public InstanceStore getInstanceStore() {
        return instanceStore;
    }

    public void setInstanceStore(InstanceStore instanceStore) {
        this.instanceStore = instanceStore;
    }

    public DIHClientFactory getDihClientFactory() {
        return dihClientFactory;
    }

    public void setDihClientFactory(DIHClientFactory dihClientFactory) {
        this.dihClientFactory = dihClientFactory;
    }

}
