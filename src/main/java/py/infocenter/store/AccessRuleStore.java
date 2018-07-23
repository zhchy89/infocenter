package py.infocenter.store;

import java.util.List;

import py.icshare.AccessRuleInformation;

public interface AccessRuleStore {
    
    public void update(AccessRuleInformation accessRuleInformation);

    public void save(AccessRuleInformation accessRuleInformation);
    
    public AccessRuleInformation get(long ruleId);
    
    public List<AccessRuleInformation> list();
    
    public int delete(long ruleId);
}
