package py.infocenter.store;

import java.util.List;

import py.icshare.VolumeRuleRelationshipInformation;

public interface VolumeRuleRelationshipStore {

    public void update(VolumeRuleRelationshipInformation relationshipInformation);

    public void save(VolumeRuleRelationshipInformation relationshipInformation);

    public List<VolumeRuleRelationshipInformation> getByVolumeId(long volumeId);

    public List<VolumeRuleRelationshipInformation> getByRuleId(long ruleId);

    public List<VolumeRuleRelationshipInformation> list();

    public int deleteByVolumeId(long volumeId);

    public int deleteByRuleId(long ruleId);

    public int deleteByRuleIdandVolumeID(long volumeId, long ruleId);
}
