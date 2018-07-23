package py.infocenter.store;

import py.icshare.iscsiAccessRule.IscsiAccessRuleInformation;

import java.util.List;

public interface IscsiAccessRuleStore {

    public void update(IscsiAccessRuleInformation accessRuleInformation);

    public void save(IscsiAccessRuleInformation accessRuleInformation);

    public IscsiAccessRuleInformation get(long ruleId);

    public List<IscsiAccessRuleInformation> list();

    public int delete(long ruleId);
}
