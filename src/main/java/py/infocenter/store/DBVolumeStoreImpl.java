package py.infocenter.store;

import java.sql.Blob;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.query.Query;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import py.icshare.VolumeInformation;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

@Transactional
public class DBVolumeStoreImpl implements VolumeStore {
    private final static Logger logger = LoggerFactory.getLogger(DBVolumeStoreImpl.class);
    private SessionFactory sessionFactory;

    public void setSegmentStore(SegmentStore segmentStore) {
    }

    @Override
    public void deleteVolume(VolumeMetadata volumeMetadata) throws HibernateException {
        VolumeInformation volume = buildVolumeInformation(volumeMetadata);
        sessionFactory.getCurrentSession().delete(volume);
        // TODO: if we don't store segmentUnit to DB, we do not need delete it
        // neither
    }

    @Override
    public void saveVolume(VolumeMetadata volumeMetadata) throws HibernateException {
        VolumeInformation volume = buildVolumeInformation(volumeMetadata);
        // save to volume DB
        logger.warn("saveVolume to database.");
        sessionFactory.getCurrentSession().saveOrUpdate(volume);
        // TODO: now don't care the segment unit, because it effects the performance
    }

    // Only volumes in memory are read,don`t read all volumes from database
    // this method has not been used right now
    @Override
    public List<VolumeMetadata> listVolumes() {
        List<VolumeMetadata> volumeMetadatas = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<VolumeInformation> volumeList = sessionFactory.getCurrentSession()
                .createQuery("from VolumeInformation").list();

        if (volumeList == null) {
            logger.warn("can`t get delete volume request from database");
            return volumeMetadatas;
        }

        for (VolumeInformation volumeInfo : volumeList) {
            VolumeMetadata volume = new VolumeMetadata();
            volume.setVolumeId(volumeInfo.getVolumeId());
            volume.setRootVolumeId(volumeInfo.getRootVolumeId());
            volume.setChildVolumeId(volumeInfo.getChildVolumeId());
            volume.setVolumeSize(volumeInfo.getVolumeSize());
            volume.setSegmentSize(volumeInfo.getSegmentSize());
            volume.setExtendingSize(volumeInfo.getExtendingSize());
            volume.setName(volumeInfo.getName());
            volume.setVolumeType(VolumeType.valueOf(volumeInfo.getVolumeType()));
            volume.setCacheType(CacheType.valueOf(volumeInfo.getCacheType()));
            volume.setVolumeStatus(VolumeStatus.findByValue(volumeInfo.getVolumeStatus()));
            volume.setAccountId(volumeInfo.getAccountId());
            volume.setSegmentSize(volumeInfo.getSegmentSize());
            volume.setStoragePoolId(volumeInfo.getStoragePoolId());
            volume.setDomainId(volumeInfo.getDomainId());
            if (volumeInfo.getDeadTime() != null) {
                volume.setDeadTime(volumeInfo.getDeadTime());
            }
            volume.setVolumeCreatedTime(volumeInfo.getVolumeCreatedTime());
            volume.setLastExtendedTime(volumeInfo.getLastExtendedTime());
            volume.setVolumeSource(VolumeMetadata.VolumeSourceType.valueOf(volumeInfo.getVolumeSource()));
            volume.setReadWrite(VolumeMetadata.ReadWriteType.valueOf(volumeInfo.getReadWrite()));
            volume.setInAction(VolumeMetadata.VolumeInAction.valueOf(volumeInfo.getInAction()));
            volume.setPageWrappCount(volumeInfo.getPageWrappCount());
            volume.setSegmentWrappCount(volumeInfo.getSegmentWrappCount());
            volume.setSimpleConfiguration(volumeInfo.getSimpleConfigured() == 1);
            volume.setEnableLaunchMultiDrivers(volumeInfo.isEnableLaunchMultiDrivers());
            volume.setCloneStatusWhenMoveOnline(volumeInfo.isCloneStatusWhenMoveOnline());
            volume.setCloneStatusWhenMoveOnline(volumeInfo.isCloneStatusWhenMoveOnline());

            volumeMetadatas.add(volume);
        }
        return volumeMetadatas;
    }

    @Override
    public VolumeMetadata getVolume(Long volumeId) {

        VolumeInformation volumeInfo = sessionFactory.getCurrentSession().get(
                VolumeInformation.class, volumeId.longValue());
        // TODO: because segment units have not been saved to database, so don`t care segment unit
        if (volumeInfo != null) {

            VolumeMetadata volume = new VolumeMetadata();
            volume.setVolumeId(volumeInfo.getVolumeId());
            volume.setRootVolumeId(volumeInfo.getRootVolumeId());

            volume.setChildVolumeId(volumeInfo.getChildVolumeId());

            volume.setVolumeSize(volumeInfo.getVolumeSize());
            volume.setSegmentSize(volumeInfo.getSegmentSize());
            volume.setExtendingSize(volumeInfo.getExtendingSize());
            volume.setName(volumeInfo.getName());
            volume.setVolumeType(VolumeType.valueOf(volumeInfo.getVolumeType()));
            volume.setCacheType(CacheType.valueOf(volumeInfo.getCacheType()));
            volume.setVolumeStatus(VolumeStatus.findByValue(volumeInfo.getVolumeStatus()));
            volume.setAccountId(volumeInfo.getAccountId());
            volume.setStoragePoolId(volumeInfo.getStoragePoolId());

            volume.setSegmentNumToCreateEachTime(volumeInfo.getSegmentNumToCreateEachTime());
            volume.setVolumeLayout(volumeInfo.getVolumeLayout());
            volume.setSimpleConfiguration(volumeInfo.getSimpleConfigured() == 1);

            if (volumeInfo.getDeadTime() != null) {
                volume.setDeadTime(volumeInfo.getDeadTime());
            }
            if (volumeInfo.getDomainId() != null) {
                volume.setDomainId(volumeInfo.getDomainId());
            }

            volume.setVolumeCreatedTime(volumeInfo.getVolumeCreatedTime());
            volume.setLastExtendedTime(volumeInfo.getLastExtendedTime());
            volume.setVolumeSource(VolumeMetadata.VolumeSourceType.valueOf(volumeInfo.getVolumeSource()));
            volume.setReadWrite(VolumeMetadata.ReadWriteType.valueOf(volumeInfo.getReadWrite()));
            volume.setInAction(VolumeMetadata.VolumeInAction.valueOf(volumeInfo.getInAction()));
            volume.setPageWrappCount(volumeInfo.getPageWrappCount());
            volume.setSegmentWrappCount(volumeInfo.getSegmentWrappCount());
            volume.setEnableLaunchMultiDrivers(volumeInfo.isEnableLaunchMultiDrivers());
            volume.setCloneStatusWhenMoveOnline(volumeInfo.isCloneStatusWhenMoveOnline());


            return volume;
        }
        return null;
    }

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public List<VolumeMetadata> listRootVolumes() {
        // throw new NotImplementedException("can not list all root volumes from database by accountId: " + accountId);
        // root volume means volume id is equals with root id
        List<VolumeMetadata> roots = new ArrayList<VolumeMetadata>();
        List<VolumeMetadata> volumes = this.listVolumes();
        for (VolumeMetadata volume : volumes) {
            // the root volume is the first volume
            if (volume.isRoot()) {
                roots.add(volume);
            }
        }
        return roots;
    }

    // TODO :this function is same to listVolumes.so we shall make it more smart.
    @Override
    public List<VolumeMetadata> listVolumesFromRoot(long rootId) {
        return listVolumesWithSameRoot(rootId);
    }

    @Override
    public List<VolumeMetadata> listVolumesWithSameRoot(long rootId) {
        List<VolumeMetadata> volumeMetadatas = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<VolumeInformation> volumeList = sessionFactory.getCurrentSession()
                .createQuery("from VolumeInformation where rootVolumeId = :rootId").setParameter("rootId", rootId).list();

        if (volumeList == null) {
            return volumeMetadatas;
        }

        for (VolumeInformation volumeInfo : volumeList) {
            VolumeMetadata volume = new VolumeMetadata();
            volume.setVolumeId(volumeInfo.getVolumeId());
            volume.setRootVolumeId(volumeInfo.getRootVolumeId());

            volume.setChildVolumeId(volumeInfo.getChildVolumeId());

            volume.setVolumeSize(volumeInfo.getVolumeSize());
            volume.setSegmentSize(volumeInfo.getSegmentSize());
            volume.setExtendingSize(volumeInfo.getExtendingSize());
            volume.setName(volumeInfo.getName());
            volume.setVolumeType(VolumeType.valueOf(volumeInfo.getVolumeType()));
            volume.setCacheType(CacheType.valueOf(volumeInfo.getCacheType()));
            volume.setVolumeStatus(VolumeStatus.findByValue(volumeInfo.getVolumeStatus()));
            volume.setAccountId(volumeInfo.getAccountId());
            volume.setSegmentSize(volumeInfo.getSegmentSize());
            volume.setStoragePoolId(volumeInfo.getStoragePoolId());

            if (volumeInfo.getDeadTime() != null) {
                volume.setDeadTime(volumeInfo.getDeadTime());
            }

            volume.setVolumeCreatedTime(volumeInfo.getVolumeCreatedTime());
            volume.setLastExtendedTime(volumeInfo.getLastExtendedTime());
            volume.setVolumeSource(VolumeMetadata.VolumeSourceType.valueOf(volumeInfo.getVolumeSource()));
            volume.setReadWrite(VolumeMetadata.ReadWriteType.valueOf(volumeInfo.getReadWrite()));
            volume.setInAction(VolumeMetadata.VolumeInAction.valueOf(volumeInfo.getInAction()));
            volume.setPageWrappCount(volumeInfo.getPageWrappCount());
            volume.setSegmentWrappCount(volumeInfo.getSegmentWrappCount());
            volume.setSimpleConfiguration(volumeInfo.getSimpleConfigured() == 1);
            volume.setEnableLaunchMultiDrivers(volumeInfo.isEnableLaunchMultiDrivers());
            volume.setCloneStatusWhenMoveOnline(volumeInfo.isCloneStatusWhenMoveOnline());

            volumeMetadatas.add(volume);
        }
        return volumeMetadatas;
    }

    @Override
    public void clearData() {
        logger.error("needn't to remove the data from databsse, only used for test");
        sessionFactory.getCurrentSession().createQuery("delete from VolumeInformation").executeUpdate();
      /*  throw new NotImplementedException("needn't to remove the data from database");*/
    }

    @Override
    public int updateDeadTime(long volumeId, long deadTime) throws HibernateException {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "update VolumeInformation set deadTime = :deadtime where volumeId = :id");
        query.setParameter("id", volumeId);
        query.setParameter("deadtime", deadTime);

        return query.executeUpdate();
    }

    @Override
    public int updateExtendingSize(long volumeId, long extendingSize) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "update VolumeInformation set extendingSize = :extendingSize where volumeId = :id");
        query.setParameter("id", volumeId);
        query.setParameter("extendingSize", extendingSize);
        return query.executeUpdate();
    }

    @Override
    public int updateStatusAndVolumeInAction(long volumeId, String volumeStatus, String volumeInAction) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "update VolumeInformation set volumeStatus = :volumeStatus, inAction = :inAction where volumeId = :id");
        query.setParameter("id", volumeId);
        query.setParameter("volumeStatus", volumeStatus);
        query.setParameter("inAction", volumeInAction);

        return query.executeUpdate();
    }

    @Override
    public int updatePersistedItems(long volumeId, long volumeSize, String name, String volumeType, String volumeLayout,
            boolean simpleConfigured, int segmentNumToCreateEachTime, long domainId, long storagePoolId,
            Date volumeCreatedTime, Date lastExtendedTime, String volumeSource, String readWrite) {
        Query query = sessionFactory
                .getCurrentSession()
                .createQuery(
                        "update VolumeInformation set volumeSize = :volumeSize, name = :name, "
                                + "volumeType = :volumeType, volumeLayout = :volumeLayout,"
                                + "simpleConfigured = :simpleConfigured, segmentNumToCreateEachTime ="
                                + ":segmentNumToCreateEachTime, domainId = :domainId, storagePoolId = :storagePoolId,"
                                + "volumeCreatedTime = :volumeCreatedTime, lastExtendedTime = :lastExtendedTime,"
                                + "volumeSource = :volumeSource, readWrite = :readWrite where volumeId = :id");
        query.setParameter("id", volumeId);
        query.setParameter("volumeSize", volumeSize);
        query.setParameter("name", name);
        query.setParameter("volumeType", volumeType);
        query.setParameter("volumeLayout", volumeLayout);
        query.setParameter("simpleConfigured", simpleConfigured ? 1 : 0);
        query.setParameter("segmentNumToCreateEachTime", segmentNumToCreateEachTime);
        query.setParameter("domainId", domainId);
        query.setParameter("storagePoolId", storagePoolId);
        query.setParameter("volumeCreatedTime", volumeCreatedTime);
        query.setParameter("lastExtendedTime", lastExtendedTime);
        query.setParameter("volumeSource", volumeSource);
        query.setParameter("readWrite", readWrite);
        return query.executeUpdate();
    }

    @Override
    public VolumeMetadata getVolumeNotDeadByName(String name) {
        @SuppressWarnings("unchecked")
        List<VolumeInformation> volumeList = sessionFactory.getCurrentSession()
                .createQuery("from VolumeInformation where name = :name " + "and volumeStatus != :status")
                .setParameter("name", name).setParameter("status", "Dead").list();
        if (0 == volumeList.size()) {
            return null;
        }
        VolumeInformation volumeInfo = volumeList.get(0);
        VolumeMetadata volume = new VolumeMetadata();
        volume.setVolumeId(volumeInfo.getVolumeId());
        volume.setRootVolumeId(volumeInfo.getRootVolumeId());
        volume.setChildVolumeId(volumeInfo.getChildVolumeId());
        volume.setVolumeSize(volumeInfo.getVolumeSize());
        volume.setSegmentSize(volumeInfo.getSegmentSize());
        volume.setExtendingSize(volumeInfo.getExtendingSize());
        volume.setName(volumeInfo.getName());
        volume.setVolumeType(VolumeType.valueOf(volumeInfo.getVolumeType()));
        volume.setCacheType(CacheType.valueOf(volumeInfo.getCacheType()));
        volume.setVolumeStatus(VolumeStatus.findByValue(volumeInfo.getVolumeStatus()));
        volume.setAccountId(volumeInfo.getAccountId());
        volume.setSegmentSize(volumeInfo.getSegmentSize());
        volume.setStoragePoolId(volumeInfo.getStoragePoolId());
        if (volumeInfo.getDeadTime() != null) {
            volume.setDeadTime(volumeInfo.getDeadTime());
        }
        volume.setSegmentNumToCreateEachTime(volumeInfo.getSegmentNumToCreateEachTime());
        volume.setVolumeLayout(volumeInfo.getVolumeLayout());
        volume.setSimpleConfiguration(volumeInfo.getSimpleConfigured() == 1);

        volume.setVolumeCreatedTime(volumeInfo.getVolumeCreatedTime());
        volume.setLastExtendedTime(volumeInfo.getLastExtendedTime());
        volume.setVolumeSource(VolumeMetadata.VolumeSourceType.valueOf(volumeInfo.getVolumeSource()));
        volume.setReadWrite(VolumeMetadata.ReadWriteType.valueOf(volumeInfo.getReadWrite()));
        volume.setInAction(VolumeMetadata.VolumeInAction.valueOf(volumeInfo.getInAction()));
        volume.setPageWrappCount(volumeInfo.getPageWrappCount());
        volume.setSegmentWrappCount(volumeInfo.getSegmentWrappCount());
        volume.setEnableLaunchMultiDrivers(volumeInfo.isEnableLaunchMultiDrivers());
        volume.setCloneStatusWhenMoveOnline(volumeInfo.isCloneStatusWhenMoveOnline());

        return volume;
    }

    @Override
    public void loadVolumeFromDbToMemory() {
    }

    @Override
    public void loadVolumeInDb() {
    }

    @Override
    public String updateVolumeLayout(long volumeId, int startSegmentIndex, int newSegmentsCount, String volumeLayout) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "update VolumeInformation set volumeLayout = :volumeLayout where volumeId = :id");
        query.setParameter("id", volumeId);
        query.setParameter("volumeLayout", volumeLayout);
        query.executeUpdate();
        return null;
    }

    private Blob createBlob(byte[] bytes) {
        if(bytes == null || bytes.length == 0) {
            return null;
        }
        return sessionFactory.getCurrentSession().getLobHelper().createBlob(bytes);
    }

    public VolumeInformation buildVolumeInformation(VolumeMetadata volumeMetadata) {

        VolumeInformation volumeInfo = new VolumeInformation();

        volumeInfo.setVolumeId(volumeMetadata.getVolumeId());
        volumeInfo.setRootVolumeId(volumeMetadata.getRootVolumeId());

        volumeInfo.setChildVolumeId(volumeMetadata.getChildVolumeId());

        volumeInfo.setVolumeSize(volumeMetadata.getVolumeSize());

        volumeInfo.setExtendingSize(volumeMetadata.getExtendingSize());

        volumeInfo.setName(volumeMetadata.getName());

        if (volumeMetadata.getVolumeType() != null) {
            volumeInfo.setVolumeType(volumeMetadata.getVolumeType().name());
        }
        volumeInfo.setCacheType(volumeMetadata.getCacheType().name());
        if (volumeMetadata.getVolumeStatus() != null) {
            volumeInfo.setVolumeStatus(volumeMetadata.getVolumeStatus().name());
        }
        volumeInfo.setAccountId(volumeMetadata.getAccountId());

        volumeInfo.setSegmentSize(volumeMetadata.getSegmentSize());

        volumeInfo.setDeadTime(volumeMetadata.getDeadTime());
        volumeInfo.setDomainId(volumeMetadata.getDomainId());
        volumeInfo.setStoragePoolId(volumeMetadata.getStoragePoolId());

        volumeInfo.setVolumeLayout(volumeMetadata.getVolumeLayout());
        volumeInfo.setSimpleConfigured(volumeMetadata.isSimpleConfiguration() ? 1 : 0);
        volumeInfo.setSegmentNumToCreateEachTime(volumeMetadata.getSegmentNumToCreateEachTime());

        volumeInfo.setVolumeCreatedTime(volumeMetadata.getVolumeCreatedTime());
        volumeInfo.setLastExtendedTime(volumeMetadata.getLastExtendedTime());

        volumeInfo.setPageWrappCount(volumeMetadata.getPageWrappCount());
        volumeInfo.setSegmentWrappCount(volumeMetadata.getSegmentWrappCount());

        if (volumeMetadata.getVolumeSource() != null) {
            volumeInfo.setVolumeSource(volumeMetadata.getVolumeSource().name());
        }

        if (volumeMetadata.getReadWrite() != null) {
            volumeInfo.setReadWrite(volumeMetadata.getReadWrite().name());
        }

        if (volumeMetadata.getInAction() != null) {
            volumeInfo.setInAction(volumeMetadata.getInAction().name());
        }
        volumeInfo.setEnableLaunchMultiDrivers(volumeMetadata.isEnableLaunchMultiDrivers());
        volumeInfo.setCloneStatusWhenMoveOnline(volumeMetadata.isCloneStatusWhenMoveOnline());
        volumeInfo.setCloningVolumeId(volumeMetadata.getCloningVolumeId());
        volumeInfo.setCloningSnapshotId(volumeMetadata.getCloningSnapshotId());

        return volumeInfo;
    }
}