package py.infocenter.store;

import org.hibernate.query.Query;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.CloneRelationshipInformation;

import java.util.List;

/**
 * @author yahuiliu
 */
@Transactional
public class CloneRelationshipsStoreImpl implements CloneRelationshipsDBStore {
    private static final Logger logger = LoggerFactory.getLogger(CloneRelationshipsStoreImpl.class);
    private SessionFactory sessionFactory;

    @Override
    public List<CloneRelationshipInformation> listBySrcVolumeId(Long srcVolumeId) {
        List<CloneRelationshipInformation> cloneRelationshipInformationList = sessionFactory.getCurrentSession()
                .createQuery("from CloneRelationshipInformation where scrVolumeId = :scrVolumeId")
                .setParameter("scrVolumeId", srcVolumeId.longValue()).list();
        return cloneRelationshipInformationList;
    }

    @Override
    public void saveToDB(CloneRelationshipInformation cloneRelationshipInformation) {
        sessionFactory.getCurrentSession().saveOrUpdate(cloneRelationshipInformation);
    }

    @Override
    public CloneRelationshipInformation getFromDB(Long destVolumeId) {
        CloneRelationshipInformation cloneRelationshipInformation = (CloneRelationshipInformation)
                sessionFactory.getCurrentSession().get(CloneRelationshipInformation.class, destVolumeId);
        return cloneRelationshipInformation;
    }

    @Override
    public List<CloneRelationshipInformation> loadFromDB() {
        List<CloneRelationshipInformation> cloneRelationshipInformationList = sessionFactory.getCurrentSession()
                .createQuery("from CloneRelationshipInformation").list();
        return cloneRelationshipInformationList;
    }

    @Override
    public void deleteFromDB(Long destVolumeId) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "delete CloneRelationshipInformation where destVolumeId = :destVolumeId");
        query.setParameter("destVolumeId", destVolumeId);
        query.executeUpdate();
    }


    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }
}