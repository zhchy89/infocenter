package py.infocenter.store;

import java.util.List;

import org.hibernate.query.Query;
import org.hibernate.SessionFactory;
import org.springframework.transaction.annotation.Transactional;

import py.icshare.AccessRuleInformation;

@Transactional
public class AccessRuleStoreImpl implements AccessRuleStore {

    private SessionFactory sessionFactory;

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public void update(AccessRuleInformation accessRuleInformation) {
        sessionFactory.getCurrentSession().update(accessRuleInformation);
    }

    @Override
    public void save(AccessRuleInformation accessRuleInformation) {
        sessionFactory.getCurrentSession().saveOrUpdate(accessRuleInformation);
    }

    @Override
    public AccessRuleInformation get(long ruleId) {
        return (AccessRuleInformation)sessionFactory.getCurrentSession().get(AccessRuleInformation.class, ruleId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<AccessRuleInformation> list() {
        return sessionFactory.getCurrentSession().createQuery("from AccessRuleInformation").list();
    }

    @Override
    public int delete(long ruleId) {
        Query query = sessionFactory.getCurrentSession().createQuery("delete AccessRuleInformation where ruleId = :id");
        query.setParameter("id", ruleId);
        return query.executeUpdate();
    }
}
