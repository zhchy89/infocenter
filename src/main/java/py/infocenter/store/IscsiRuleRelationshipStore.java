package py.infocenter.store;

import py.icshare.iscsiAccessRule.IscsiRuleRelationshipInformation;
import py.thrift.share.DriverKey_Thrift;

import java.util.List;

public interface IscsiRuleRelationshipStore {

    public void update(IscsiRuleRelationshipInformation iscsiRelationshipInformation);

    public void save(IscsiRuleRelationshipInformation iscsiRelationshipInformation);

    public List<IscsiRuleRelationshipInformation> getByDriverKey(DriverKey_Thrift driverKey);

    public List<IscsiRuleRelationshipInformation> getByRuleId(long ruleId);

    public List<IscsiRuleRelationshipInformation> list();

    public int deleteByDriverKey(DriverKey_Thrift driverKey);

    public int deleteByRuleId(long ruleId);

    public int deleteByRuleIdandDriverKey(DriverKey_Thrift driverKey, long ruleId);
}
