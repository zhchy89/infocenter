package py.infocenter.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.query.Query;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import py.icshare.ArchiveInformation;
import py.icshare.InstanceMetadata;
import py.icshare.StorageInformation;

public class StorageStoreImpl implements StorageDBStore, StorageStore {
    private final static Logger logger = LoggerFactory.getLogger(StorageStoreImpl.class);
    private SessionFactory sessionFactory;
    private Map<Long, InstanceMetadata> instanceMap = new ConcurrentHashMap<>();
    private ArchiveStore archiveStore;

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public void setArchiveStore(ArchiveStore archiveStore) {
        this.archiveStore = archiveStore;
    }

    public ArchiveStore getArchiveStore() {
        return archiveStore;
    }

    @Override
    @Transactional
    public void saveToDB(StorageInformation storageInformation) {
        sessionFactory.getCurrentSession().saveOrUpdate(storageInformation);
    }

    @Override
    @Transactional
    public void updateToDB(StorageInformation storageInformation) {
        sessionFactory.getCurrentSession().update(storageInformation);
    }

    @Override
    @Transactional
    public StorageInformation getByInstanceIdFromDB(long instanceId) {
        return (StorageInformation) sessionFactory.getCurrentSession().get(StorageInformation.class, instanceId);
    }

    @SuppressWarnings("unchecked")
    @Override
    @Transactional
    public List<StorageInformation> listFromDB() {
        return sessionFactory.getCurrentSession().createQuery("from StorageInformation").list();
    }

    @Override
    @Transactional
    public int deleteFromDB(long instanceId) {
        Query query = sessionFactory.getCurrentSession().createQuery("delete StorageInformation where instanceId = :id");
        query.setParameter("id", instanceId);
        return query.executeUpdate();
    }

    @Override
    @Transactional
    public void save(InstanceMetadata instanceMetadata) {
        //logger.info("storage save getId() = {}", instanceMetadata.getInstanceId().getId());
        logger.debug("storage saved {}", instanceMetadata);
        instanceMap.put(instanceMetadata.getInstanceId().getId(), instanceMetadata);

        // save to storage DB
        saveToDB(instanceMetadata.toStorageInformation());

        // save to archive DB
        for (ArchiveInformation archive : instanceMetadata.toArchivesInformation()) {
            logger.info("archiveStore save {}", archive.getArchiveId());
            archiveStore.save(archive);
        }
    }

    @Override
    public InstanceMetadata get(long instanceId) {
        return instanceMap.get(instanceId);
    }

    @Override
    public List<InstanceMetadata> list() {
        return new ArrayList<InstanceMetadata>(instanceMap.values());
    }

    @Override
    @Transactional
    public void delete(long instanceId) {
        // delete from storage DB
        deleteFromDB(instanceId);
        // delete from archive DB
        archiveStore.deleteByInstanceId(instanceId);
        // delete from memory
        instanceMap.remove(instanceId);
    }

    @Override
    public int size() {
        return instanceMap.size();
    }

    public void clearMemoryData() {
        instanceMap.clear();
    }
}
