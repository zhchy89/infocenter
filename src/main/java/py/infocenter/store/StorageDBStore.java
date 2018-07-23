package py.infocenter.store;

import java.util.List;

import py.icshare.StorageInformation;

/**
 * store information about the storage node
 * @author kobofare
 *
 */
public interface StorageDBStore {
    
    public void saveToDB(StorageInformation storageInformation);
    
    public void updateToDB(StorageInformation storageInformation);
    
    public StorageInformation getByInstanceIdFromDB(long instanceId);
    
    public List<StorageInformation> listFromDB();
    
    public int deleteFromDB(long instanceId);
}
