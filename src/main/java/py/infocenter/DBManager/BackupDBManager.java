package py.infocenter.DBManager;


import py.thrift.share.ReportDBRequest_Thrift;
import py.thrift.share.ReportDBResponse_Thrift;

/**
 * 1, pick up datanodes should consider groupId 2, find way to process round define: if set three groups should save
 * database info, first arrived three groups and first datanode in its' group will save DB Info
 * 
 * @author zhongyuan
 *
 */
public interface BackupDBManager {

    public ReportDBResponse_Thrift process(ReportDBRequest_Thrift reportRequest);

    void backupDatabase();

    /**
     *
     * @return whether successfully recover database
     */
    boolean recoverDatabase();

    public boolean needRecoverDB();

    public void loadTablesFromDB(ReportDBResponse_Thrift response) throws Exception;

    /**
     *
     * @param newestDBInfo newest database info to store
     * @return whether successfully save newest database info to database
     */
    public boolean saveTablesToDB(ReportDBRequest_Thrift newestDBInfo);

    public boolean passedRecoveryTime();

}
