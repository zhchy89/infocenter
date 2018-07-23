package py.infocenter.store;

import java.util.List;

import org.hibernate.query.Query;
import org.hibernate.SessionFactory;
import org.springframework.transaction.annotation.Transactional;

import py.icshare.VolumeRuleRelationshipInformation;

@Transactional
public class VolumeRuleRelationshipStoreImpl implements VolumeRuleRelationshipStore {

    private SessionFactory sessionFactory;

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public void update(VolumeRuleRelationshipInformation relationshipInformation) {
        sessionFactory.getCurrentSession().update(relationshipInformation);
    }

    @Override
    public void save(VolumeRuleRelationshipInformation relationshipInformation) {
        sessionFactory.getCurrentSession().saveOrUpdate(relationshipInformation);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<VolumeRuleRelationshipInformation> getByVolumeId(long volumeId) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "from VolumeRuleRelationshipInformation where volumeId = :id");
        query.setParameter("id", volumeId);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<VolumeRuleRelationshipInformation> getByRuleId(long ruleId) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "from VolumeRuleRelationshipInformation where ruleId = :id");
        query.setParameter("id", ruleId);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<VolumeRuleRelationshipInformation> list() {
        return sessionFactory.getCurrentSession().createQuery("from VolumeRuleRelationshipInformation").list();
    }

    @Override
    public int deleteByVolumeId(long volumeId) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "delete VolumeRuleRelationshipInformation where volumeId = :id");
        query.setParameter("id", volumeId);
        return query.executeUpdate();
    }

    @Override
    public int deleteByRuleId(long ruleId) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "delete VolumeRuleRelationshipInformation where ruleId = :id");
        query.setParameter("id", ruleId);
        return query.executeUpdate();
    }

    @Override
    public int deleteByRuleIdandVolumeID(long volumeId, long ruleId) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "delete VolumeRuleRelationshipInformation where ruleId = :id and volumeId = :vid");
        query.setParameter("id", ruleId);
        query.setParameter("vid", volumeId);
        return query.executeUpdate();
    }
}
