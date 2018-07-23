package py.infocenter.store;

import java.util.List;

import py.common.struct.EndPoint;
import py.icshare.AlarmInfo;

public class AlarmStoreTwoLevelImpl implements AlarmStore {
    private final AlarmStore dbStore;
    private final AlarmStore memoryStore;

    public AlarmStoreTwoLevelImpl(AlarmStore memoryLevelStore, AlarmStore dbLevelStore) {
        this.dbStore = dbLevelStore;
        this.memoryStore = memoryLevelStore;
    }

    @Override
    public void save(AlarmInfo alarmInfo) {
        this.memoryStore.save(alarmInfo);
    }

    @Override
    public List<AlarmInfo> list() {
        return this.memoryStore.list();
    }

    @Override
    public void clearMemoryData() {
        this.memoryStore.clearMemoryData();
    }

    @Override
    public long getLastReportTime(EndPoint endPoint) {
        return this.memoryStore.getLastReportTime(endPoint);
    }

    @Override
    public void increaseAlarmTimes(AlarmInfo alarmInfo) {
        this.memoryStore.increaseAlarmTimes(alarmInfo);
    }

    @Override
    public void save(List<AlarmInfo> alarmInfos) {
        this.memoryStore.save(alarmInfos);
    }

    @Override
    public AlarmInfo get(EndPoint endPoint, String objectSource) {
        return this.memoryStore.get(endPoint, objectSource);
    }

    @Override
    public List<AlarmInfo> listActiveAlarm() {
        return this.memoryStore.listActiveAlarm();
    }

    @Override
    public void delete(AlarmInfo alarmInfo) throws Exception {
        this.memoryStore.delete(alarmInfo);
    }

}
