package py.infocenter.store;

import py.icshare.CloneRelationshipInformation;

import java.util.List;

/**
 * @author yahuiliu
 */
public interface CloneRelationshipsDBStore {

    void saveToDB(CloneRelationshipInformation cloneRelationshipInformation);

    CloneRelationshipInformation getFromDB(Long destVolumeId);

    List<CloneRelationshipInformation> loadFromDB();

    List<CloneRelationshipInformation> listBySrcVolumeId(Long srcVolumeId);

    void deleteFromDB(Long destVolumeId);
}