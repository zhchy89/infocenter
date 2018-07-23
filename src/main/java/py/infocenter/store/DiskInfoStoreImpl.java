package py.infocenter.store;

import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.DiskInfo;

import java.util.List;

@Transactional
public class DiskInfoStoreImpl implements DiskInfoStore {
    private static final Logger logger = LoggerFactory.getLogger(DiskInfoStore.class);
    private SessionFactory sessionFactory;

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public void clearDB() {
        Query query = sessionFactory.getCurrentSession().createQuery("delete DiskInfo where 1=1 ");
        query.executeUpdate();
    }

    @Override
    public List<DiskInfo> listDiskInfos() {
        List<DiskInfo> diskInfoList = sessionFactory.getCurrentSession().createQuery("from DiskInfo").list();
        return diskInfoList;
    }

    @Override
    public DiskInfo listDiskInfoById(String id){
        DiskInfo diskInfo = sessionFactory.getCurrentSession().get(DiskInfo.class, id);
        return diskInfo;
    }

    @Override
    public void updateDiskInfoLightStatusById(String id, String status) {
        DiskInfo diskInfo = sessionFactory.getCurrentSession().get(DiskInfo.class, id);
        diskInfo.setSwith(status);
    }
}
