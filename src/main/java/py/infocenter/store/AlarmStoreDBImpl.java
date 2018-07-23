package py.infocenter.store;

import java.util.List;

import org.hibernate.SessionFactory;

import py.common.struct.EndPoint;
import py.icshare.AlarmInfo;

public class AlarmStoreDBImpl implements AlarmStore {
    private final SessionFactory sessionFactory;

    public AlarmStoreDBImpl(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public void save(AlarmInfo alarmInfo) {
        // TODO Auto-generated method stub

    }

    @Override
    public List<AlarmInfo> list() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clearMemoryData() {
        // TODO Auto-generated method stub

    }

    @Override
    public long getLastReportTime(EndPoint endPoint) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void increaseAlarmTimes(AlarmInfo alarmInfo) {
        // TODO Auto-generated method stub

    }

    @Override
    public void save(List<AlarmInfo> alarmInfos) {
        // TODO Auto-generated method stub

    }

    @Override
    public AlarmInfo get(EndPoint endPoint, String objectSource) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<AlarmInfo> listActiveAlarm() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void delete(AlarmInfo alarmInfo) throws Exception {
    }

}
