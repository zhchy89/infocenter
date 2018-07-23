package py.infocenter.store;

import java.sql.Blob;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.sf.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hibernate.query.Query;
import org.hibernate.SessionFactory;
import org.springframework.transaction.annotation.Transactional;

import py.common.client.RequestResponseHelper;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.icshare.DriverInformation;
import py.icshare.DriverKey;
import py.icshare.DriverKeyInformation;
import py.thrift.share.AlreadyExistStaticLimitationException_Thrift;
import py.thrift.share.DynamicIOLimitationTimeInterleavingException_Thrift;
import py.icshare.qos.IOLimitation;

@Transactional
public class DriverStoreImpl implements DriverDBStore, DriverStore {
    private static final Logger logger = LoggerFactory.getLogger(DriverStoreImpl.class);

    private SessionFactory sessionFactory;

    private Map<DriverKey, DriverMetadata> driverMap = new ConcurrentHashMap<DriverKey, DriverMetadata>();

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<DriverInformation> getByVolumeIdFromDB(long volumeId) {
        Query query = sessionFactory.getCurrentSession()
                .createQuery("from DriverInformation where driverKeyInfo.volumeId = :id");
        query.setParameter("id", volumeId);
        return (List<DriverInformation>) query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<DriverInformation> getByDriverKeyFromDB(long volumeId, DriverType driverType, int snapshotId) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "from DriverInformation where driverKeyInfo.volumeId = :id and driverKeyInfo.driverType = :driverType and driverKeyInfo.snapshotId = :sid");
        query.setParameter("id", volumeId);
        query.setParameter("driverType", driverType.name());
        query.setParameter("sid", snapshotId);

        return (List<DriverInformation>) query.list();
    }

    @Override
    public List<DriverInformation> getByDriverContainerIdFromDB(long driverContainerId) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "from DriverInformation where driverKeyInfo.driverContainerId = :id");
        query.setParameter("id", driverContainerId);

        return (List<DriverInformation>) query.list();
    }

    @Override
    public void saveToDB(DriverInformation driverInformation) {
        sessionFactory.getCurrentSession().saveOrUpdate(driverInformation);
    }

    @Override
    public int updateStatusToDB(long volumeId, DriverType driverType, int snapshotId, String status) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "update DriverInformation set driverStatus = :status where driverKeyInfo.volumeId = :id and driverKeyInfo.driverType = :driverType and driverKeyInfo.snapshotId = :sid");
        query.setParameter("id", volumeId);
        query.setParameter("driverType", driverType.name());
        query.setParameter("sid", snapshotId);
        query.setParameter("status", status);
        return query.executeUpdate();
    }

    @Override
    public void updateToDB(DriverInformation driverInformation) {
        sessionFactory.getCurrentSession().update(driverInformation);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<DriverInformation> listFromDB() {
        return sessionFactory.getCurrentSession().createQuery("from DriverInformation").list();
    }

    @Override
    public int deleteFromDB(long volumeId) {
        Query query = sessionFactory.getCurrentSession()
                .createQuery("delete DriverInformation where driverKeyInfo.volumeId = :id");
        query.setParameter("id", volumeId);
        return query.executeUpdate();
    }

    @Override
    public int deleteFromDB(long volumeId, DriverType driverType, int snapshotId) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "delete DriverInformation where driverKeyInfo.volumeId = :id and driverKeyInfo.driverType = :driverType and driverKeyInfo.snapshotId = :sid");
        query.setParameter("id", volumeId);
        query.setParameter("driverType", driverType.name());
        query.setParameter("sid", snapshotId);
        return query.executeUpdate();
    }

    @Override
    public List<DriverMetadata> get(long volumeId) {
        List<DriverMetadata> driverMetadatas = new ArrayList<>();

        // get driver from memory firstly if exist
        for (DriverMetadata driverMetadata : driverMap.values()) {
            if (driverMetadata.getVolumeId() == volumeId) {
                driverMetadatas.add(driverMetadata);
            }
        }

        /*
         * driver binding to volume with specified id doesn't exist in memory,
         * get it from database firstly and put it in memory at the same time
         */
        if (driverMetadatas.size() == 0) {
            // get driver metadata from database
            List<DriverInformation> driverMetadataFromDB = getByVolumeIdFromDB(volumeId);
            if (driverMetadataFromDB != null && driverMetadataFromDB.size() > 0) {
                for (DriverInformation driverInformation : driverMetadataFromDB) {
                    if (driverInformation == null) {
                        continue;
                    }

                    DriverMetadata driverMetadata = driverInformation.toDriverMetadata();
                    // put driver metadata in memory
                    driverMap.put(getKey(driverMetadata), driverMetadata);
                    // return the driver metadata
                    driverMetadatas.add(driverMetadata);
                }
            }
        }

        return driverMetadatas;
    }

    @Override
    public List<DriverMetadata> getByDriverContainerId(long driverContainerId) {
        List<DriverMetadata> driverMetadatas = new ArrayList<>();

        // get driver from memory firstly if exist
        for (DriverMetadata driverMetadata : driverMap.values()) {
            if (driverMetadata.getDriverContainerId() == driverContainerId) {
                driverMetadatas.add(driverMetadata);
            }
        }

        /*
         * driver with specified driver container id doesn't exist in memory,
         * get it from database firstly and put it in memory at the same time
         */
        if (driverMetadatas.size() == 0) {
            // get driver metadata from database
            List<DriverInformation> driverMetadataFromDB = getByDriverContainerIdFromDB(driverContainerId);
            if (driverMetadataFromDB != null && driverMetadataFromDB.size() > 0) {
                for (DriverInformation driverInformation : driverMetadataFromDB) {
                    if (driverInformation == null) {
                        continue;
                    }

                    DriverMetadata driverMetadata = driverInformation.toDriverMetadata();
                    // put driver metadata in memory
                    driverMap.put(getKey(driverMetadata), driverMetadata);
                    // return the driver metadata
                    driverMetadatas.add(driverMetadata);
                }
            }
        }

        return driverMetadatas;
    }

    @Override
    public DriverMetadata get(long driverContainerId, long volumeId, DriverType driverType, int snapshotId) {
        DriverMetadata driverMetadata = driverMap.get(getKey(driverContainerId, volumeId, snapshotId, driverType));

        // request driver doesn't exist in memory and get from db
        if (driverMetadata == null) {
            List<DriverInformation> driverInformations = getByDriverKeyFromDB(volumeId, driverType, snapshotId);
            // record corresponding driver key is 1 vs 1
            if (driverInformations != null && driverInformations.size() == 1) {
                driverMetadata = driverInformations.get(0).toDriverMetadata();
                driverMap.put(getKey(driverMetadata), driverMetadata);
            }
        }

        return driverMetadata;
    }

    @Override
    public List<DriverMetadata> list() {
        List<DriverMetadata> driverMetadatas = new ArrayList<>(driverMap.values());

        // if memory is empty, get driver meta data from database
        if (driverMetadatas.size() == 0) {
            List<DriverInformation> driverMetadataFromDB = listFromDB();
            if (driverMetadataFromDB != null && driverMetadataFromDB.size() > 0) {
                for (DriverInformation driverInformation : driverMetadataFromDB) {
                    DriverMetadata driverMetadata = driverInformation.toDriverMetadata();
                    // put driver meta data in memory
                    driverMap.put(getKey(driverMetadata), driverMetadata);
                    // return the driver metadata
                    driverMetadatas.add(driverMetadata);
                }
            }

        }

        return driverMetadatas;
    }

    private Blob createBlob(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return this.sessionFactory.getCurrentSession().getLobHelper().createBlob(bytes);
    }

    @Override
    public void delete(long volumeId) {
        // delete from memory
        for (DriverMetadata driverMetadata : get(volumeId)) {
            driverMap.remove(getKey(driverMetadata));
        }

        // delete from DB
        deleteFromDB(volumeId);
    }

    @Override
    public void delete(long driverContainerId, long volumeId, DriverType driverType, int snapshotId) {
        logger.warn("driverStore before delete: {}", list(), new Throwable());
        // delete from memory
        driverMap.remove(getKey(driverContainerId, volumeId, snapshotId, driverType));

        // delete from db
        deleteFromDB(volumeId, driverType, snapshotId);
        logger.warn("driverStore after delete: {}", list(), new Throwable());
    }

    @Override
    // TODO: Upgrade here when we have time
    public void save(DriverMetadata driverMetadata) {
        // save to mem
        driverMap.put(getKey(driverMetadata), driverMetadata);

        // save to db
        saveToDB(buildDriverInformation(driverMetadata));
    }

    private DriverInformation buildDriverInformation(DriverMetadata driverMetadata) {

        DriverInformation driverInformation = new DriverInformation();
        DriverKeyInformation driverKeyInformation = new DriverKeyInformation(driverMetadata.getDriverContainerId(),
                driverMetadata.getVolumeId(), driverMetadata.getSnapshotId(), driverMetadata.getDriverType().name());
        // TODO maybe have good solution for the driver id. now we will assign a
        // unique value.
        driverInformation.setHostName(driverMetadata.getHostName());
        driverInformation.setPortalType(driverMetadata.getPortalType().name());
        driverInformation.setIpv6Addr(driverMetadata.getIpv6Addr());
        driverInformation.setNetIfaceName(driverMetadata.getNicName());
        driverInformation.setDriverName(driverMetadata.getDriverName());
        driverInformation.setDriverKeyInfo(driverKeyInformation);
        driverInformation.setPort(driverMetadata.getPort());
        driverInformation.setCoordinatorPort(driverMetadata.getCoordinatorPort());
        if (driverMetadata.getDriverStatus() != null) {
            driverInformation.setDriverStatus(driverMetadata.getDriverStatus().name());
        }
        driverInformation.setStaticIOLimitationId(driverMetadata.getStaticIOLimitationId());
        driverInformation.setDynamicIOLimitationId(driverMetadata.getDynamicIOLimitationId());
        driverInformation.setChapControl(driverMetadata.getChapControl());
        driverInformation.setVolumeName(driverMetadata.getVolumeName());
        return driverInformation;
    }

    public void clearMemoryData() {
        driverMap.clear();
    }

    private static DriverKey getKey(DriverMetadata driverMetadata) {
        return new DriverKey(driverMetadata.getDriverContainerId(), driverMetadata.getVolumeId(),
                driverMetadata.getSnapshotId(), driverMetadata.getDriverType());
    }

    private static DriverKey getKey(long driverContainerId, long volumeId, int snapshotId, DriverType driverType) {
        return new DriverKey(driverContainerId, volumeId, snapshotId, driverType);
    }

    @Override
    public List<DriverMetadata> get(long volumeId, int snapshotId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int updateIOLimit(long driverContainerId, long volumeId, DriverType driverType, int snapshotId,
            IOLimitation updateIOLimitation)
            throws AlreadyExistStaticLimitationException_Thrift, DynamicIOLimitationTimeInterleavingException_Thrift {
//        DriverMetadata driverMetadata = get(driverContainerId, volumeId, driverType, snapshotId);
//        IOLimitation limitToRemove = null;
//        boolean existStatic = false;
//        List<IOLimitation> existIOLimitations = new ArrayList<>();
//        existIOLimitations.addAll(driverMetadata.getIoLimitations());
//
//        for (IOLimitation limit : existIOLimitations) {
//            if (limit.getLimitType() == IOLimitation.LimitType.Static) {
//                existStatic = true;
//            }
//            if (limit.getId() == updateIOLimitation.getId()) {
//                limitToRemove = limit;
//                break;
//            }
//        }
//
//        if (existStatic && limitToRemove == null) {
//            logger.error("means driver has static io limitation, but want to add another static limitation:{}",
//                    updateIOLimitation);
//            throw new AlreadyExistStaticLimitationException_Thrift();
//        }
//
//        if (limitToRemove != null) {
//            existIOLimitations.remove(limitToRemove);
//        }
//
//        // judge if dynamic io limitation time interleaving
//        if (RequestResponseHelper.judgeDynamicIOLimitationTimeInterleaving(updateIOLimitation, existIOLimitations)) {
//            logger.error("try to add or update:{} time interleaving", updateIOLimitation);
//            throw new DynamicIOLimitationTimeInterleavingException_Thrift();
//        }
//
//        driverMetadata.getIoLimitations().remove(limitToRemove);
//        driverMetadata.getIoLimitations().add(updateIOLimitation);
//
//        // save to db
//        saveToDB(buildDriverInformation(driverMetadata));

        return 1;
    }

    @Override
    public int deleteIOLimit(long driverContainerId, long volumeId, DriverType driverType, int snapshotId,
            long limitId) {
//        DriverMetadata driverMetadata = get(driverContainerId, volumeId, driverType, snapshotId);
//        IOLimitation limitToDelete = null;
//        boolean found = false;
//        for (IOLimitation limit : driverMetadata.getIoLimitations()) {
//            if (limit.getId() == limitId) {
//                found = true;
//                limitToDelete = limit;
//                break;
//            }
//        }
//        if (!found) {
//            return -1;
//        } else {
//            driverMetadata.getIoLimitations().remove(limitToDelete);
//        }
//
//        // save to db
//        saveToDB(buildDriverInformation(driverMetadata));

        return 1;
    }

    @Override
    public int changeLimitType(long driverContainerId, long volumeId, DriverType driverType, int snapshotId,
            long limitId, boolean staticLimit) {
        // TODO Auto-generated method stub
        return 0;
    }
}
