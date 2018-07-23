package py.infocenter.store;

import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.iscsiAccessRule.IscsiAccessRuleInformation;

import java.util.List;


@Transactional
public class IscsiAccessRuleStoreImpl implements IscsiAccessRuleStore {

    private SessionFactory sessionFactory;

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public void update(IscsiAccessRuleInformation accessRuleInformation) {
        sessionFactory.getCurrentSession().update(accessRuleInformation);
    }

    @Override
    public void save(IscsiAccessRuleInformation accessRuleInformation) {
        sessionFactory.getCurrentSession().saveOrUpdate(accessRuleInformation);
    }

    @Override
    public IscsiAccessRuleInformation get(long ruleId) {
        return (IscsiAccessRuleInformation)sessionFactory.getCurrentSession().get(IscsiAccessRuleInformation.class, ruleId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<IscsiAccessRuleInformation> list() {
        return sessionFactory.getCurrentSession().createQuery("from IscsiAccessRuleInformation").list();
    }

    @Override
    public int delete(long ruleId) {
        Query query = sessionFactory.getCurrentSession().createQuery("delete IscsiAccessRuleInformation where ruleId = :id");
        query.setLong("id", ruleId);
        return query.executeUpdate();
    }
}
