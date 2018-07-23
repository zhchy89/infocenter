package py.infocenter.store;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.DriverKey;
import py.icshare.iscsiAccessRule.IscsiRuleRelationshipInformation;
import py.thrift.share.DriverKey_Thrift;

import java.util.List;

@Transactional
public class IscsiRuleRelationshipStoreImpl implements IscsiRuleRelationshipStore {

    private SessionFactory sessionFactory;

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public void update(IscsiRuleRelationshipInformation relationshipInformation) {
        sessionFactory.getCurrentSession().update(relationshipInformation);
    }

    @Override
    public void save(IscsiRuleRelationshipInformation relationshipInformation) {
        sessionFactory.getCurrentSession().saveOrUpdate(relationshipInformation);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<IscsiRuleRelationshipInformation> getByDriverKey(DriverKey_Thrift driverKey) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "from IscsiRuleRelationshipInformation where driverContainerId = :did and volumeId = :vid and snapshotId = :sid and driverType = :type");
        query.setLong("did", driverKey.getDriverContainerId());
        query.setLong("vid", driverKey.getVolumeId());
        query.setInteger("sid", driverKey.getSnapshotId());
        query.setString("type", driverKey.getDriverType().name());

        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<IscsiRuleRelationshipInformation> getByRuleId(long ruleId) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "from IscsiRuleRelationshipInformation where ruleId = :id");
        query.setLong("id", ruleId);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<IscsiRuleRelationshipInformation> list() {
        return sessionFactory.getCurrentSession().createQuery("from IscsiRuleRelationshipInformation").list();
    }

    @Override
    public int deleteByDriverKey(DriverKey_Thrift driverKey) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "delete IscsiRuleRelationshipInformation where driverContainerId = :did and volumeId = :vid and snapshotId = :sid and driverType = :type");

        query.setLong("did", driverKey.getDriverContainerId());
        query.setLong("vid", driverKey.getVolumeId());
        query.setInteger("sid", driverKey.getSnapshotId());
        query.setString("type", driverKey.getDriverType().name());

        return query.executeUpdate();
    }

    @Override
    public int deleteByRuleId(long ruleId) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "delete IscsiRuleRelationshipInformation where ruleId = :id");
        query.setLong("id", ruleId);
        return query.executeUpdate();
    }

    @Override
    public int deleteByRuleIdandDriverKey(DriverKey_Thrift driverKey, long ruleId) {
        Query query = sessionFactory.getCurrentSession().createQuery(
                "delete IscsiRuleRelationshipInformation where ruleId = :id and driverContainerId = :did and volumeId = :vid and snapshotId = :sid and driverType = :type");
        query.setLong("id", ruleId);
        query.setLong("did", driverKey.getDriverContainerId());
        query.setLong("vid", driverKey.getVolumeId());
        query.setInteger("sid", driverKey.getSnapshotId());
        query.setString("type", driverKey.getDriverType().name());
        return query.executeUpdate();
    }
}
