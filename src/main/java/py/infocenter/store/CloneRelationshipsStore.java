package py.infocenter.store;

import py.icshare.CloneRelationshipInformation;

import java.util.List;

/**
 * @author yahuiliu
 */
public interface CloneRelationshipsStore {

    boolean save(CloneRelationshipInformation cloneRelationshipInformation);

    CloneRelationshipInformation get(Long relationshipId);

    List<CloneRelationshipInformation> list();

    void delete(Long relationshipId);

    void clearMemoryMap();
}