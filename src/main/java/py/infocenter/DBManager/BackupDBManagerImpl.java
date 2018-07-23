package py.infocenter.DBManager;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import edu.emory.mathcs.backport.java.util.Collections;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.client.RequestResponseHelper;
import py.common.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.icshare.*;
import py.icshare.authorization.*;
import py.icshare.iscsiAccessRule.Iscsi2AccessRuleRelationship;
import py.icshare.iscsiAccessRule.IscsiAccessRule;
import py.icshare.iscsiAccessRule.IscsiAccessRuleInformation;
import py.icshare.iscsiAccessRule.IscsiRuleRelationshipInformation;
import py.icshare.qos.*;
import py.infocenter.store.*;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.license.LicenseCryptogramInformation;
import py.license.LicenseCryptogramInformationTransition;
import py.license.LicenseJson;
import py.license.LicenseStorage;
import py.thrift.datanode.service.BackupDatabaseInfoRequest;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.share.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Don't try to understand the logic of this class, the situation of infocenter status and database status is too
 * complicated to analyse and the author cannot even make sure it works properly among all the situations. Any way
 * this class will run in few times.
 */
public class BackupDBManagerImpl implements BackupDBManager {

    private static final Logger logger = LoggerFactory.getLogger(BackupDBManagerImpl.class);
    private AtomicLong roundSequenceId = new AtomicLong(0);
    private AtomicLong startTime = null;
    private final long roundTimeInterval;
    private final int maxBackupCount;
    private Map<Group, EndPoint> roundRecordMap = new HashMap<>();
    private AtomicBoolean firstRound = new AtomicBoolean(true);
    // stores
    private VolumeRuleRelationshipStore volumeRuleRelationshipStore;
    private AccessRuleStore accessRuleStore;
    private IscsiRuleRelationshipStore iscsiRuleRelationshipStore;
    private IscsiAccessRuleStore iscsiAccessRuleStore;
    private DomainStore domainStore;
    private StoragePoolStore storagePoolStore;
    private CapacityRecordStore capacityRecordStore;
    private CloneRelationshipsDBStore cloneRelationshipsStore;
    private LicenseStorage licenseStorage;
    private AccountStore accountStore;
    private APIStore apiStore;
    private RoleStore roleStore;
    private ResourceStore resourceStore;
    private InstanceStore instanceStore;
    private IOLimitationStore ioLimitationStore;
    private MigrationRuleStore migrationRuleStore;
    private AtomicLong round = new AtomicLong(0);
    private Multimap<Long, EndPoint> roundRecord = HashMultimap.create();
    private Multimap<Long, EndPoint> lastRoundRecord = HashMultimap.create();
    private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = GenericThriftClientFactory
            .create(DataNodeService.Iface.class, 1);

    public BackupDBManagerImpl(long roundTimeInterval, int maxBackupCount,
                               VolumeRuleRelationshipStore volumeRuleRelationshipStore, AccessRuleStore accessRuleStore,
                               DomainStore domainStore, StoragePoolStore storagePoolStore, CapacityRecordStore capacityRecordStore,
                               CloneRelationshipsDBStore cloneRelationshipsStore, LicenseStorage licenseStorage, AccountStore accountStore,
                               APIStore apiStore, RoleStore roleStore, ResourceStore resourceStore, InstanceStore instanceStore,
                               IscsiRuleRelationshipStore iscsiRuleRelationshipStore, IscsiAccessRuleStore iscsiAccessRuleStore,
                               IOLimitationStore ioLimitationStore,
                               MigrationRuleStore migrationRuleStore) {
        this.roundSequenceId = new AtomicLong(0);
        this.startTime = null;
        this.roundTimeInterval = roundTimeInterval;
        this.maxBackupCount = maxBackupCount;
        this.roundRecordMap = new ConcurrentHashMap<>();
        this.firstRound = new AtomicBoolean(true);
        this.volumeRuleRelationshipStore = volumeRuleRelationshipStore;
        this.accessRuleStore = accessRuleStore;
        this.iscsiRuleRelationshipStore = iscsiRuleRelationshipStore;
        this.iscsiAccessRuleStore = iscsiAccessRuleStore;
        this.domainStore = domainStore;
        this.storagePoolStore = storagePoolStore;
        this.capacityRecordStore = capacityRecordStore;
        this.cloneRelationshipsStore = cloneRelationshipsStore;
        this.licenseStorage = licenseStorage;
        this.accountStore = accountStore;
        this.apiStore = apiStore;
        this.roleStore = roleStore;
        this.resourceStore = resourceStore;
        this.instanceStore = instanceStore;
        this.ioLimitationStore = ioLimitationStore;
        this.migrationRuleStore = migrationRuleStore;

    }

    @Override
    public synchronized ReportDBResponse_Thrift process(ReportDBRequest_Thrift reportRequest) {
        ReportDBResponse_Thrift response = new ReportDBResponse_Thrift(getRoundSequenceId());

        if (startTime == null) {
            startTime = new AtomicLong(System.currentTimeMillis());
        }
        EndPoint endPoint = EndPoint.fromString(reportRequest.getEndpoint());
        Validate.notNull(endPoint, "endPoint info can not be null, report request: " + reportRequest);
        Validate.isTrue(reportRequest.isSetSequenceId(), "report request must set sequenceId");

        Group group = RequestResponseHelper.buildGroupFrom(reportRequest.getGroup());
        if (group == null) {
            logger.warn(
                    "group is null means this datanode is a new one:{}, do not process for now, print its request:{}",
                    endPoint, reportRequest);
            return response;
        }
        // determine this round is over or not
        if (isRoundEndNow()) {
            firstRound.set(false);
            roundRefresh(true);
        }

        // record all the sequenceId in this round
        roundRecord.put(reportRequest.getSequenceId(), endPoint);
        if (firstRound.get()) {
            if (reportRequest.getSequenceId() > roundSequenceId.get()) {
                roundSequenceId.set(reportRequest.getSequenceId());
            }

            response.setSequenceId(getRoundSequenceId());
            return response;
        }

        /*
         * after first round, should check database is re-setup or new setup
         */
        if (needRecoverDB()) {
            logger.warn("need recover database: {}", lastRoundRecord);
            recoverDatabase();
        }

        if (reportRequest.getSequenceId() >= roundSequenceId.get()) {
            logger.warn("do not accept bigger sequence id:{}, current sequence id:{}", reportRequest.getSequenceId(),
                    roundSequenceId.get());
        }

        response.setSequenceId(getRoundSequenceId());
        if (this.roundRecordMap.containsKey(group) || this.roundRecordMap.size() >= this.maxBackupCount) {
            logger.debug(
                    "already save to datanode:{} at group:{} or has save count of:{} not less than max backup count:{}",
                    this.roundRecordMap.get(group), group, this.roundRecordMap.size(), this.maxBackupCount);
            return response;
        } else {
            // generate all database info here, if failed, DO NOT save into record map and logger warn
            try {
                loadTablesFromDB(response);
                logger.info("load from DB for datanode:{} at group:{}, and response:{}", endPoint, group, response);
                this.roundRecordMap.put(group, endPoint);
            } catch (Exception e) {
                logger.error("can not load tables from database, will not put into record map", e);
            }
        }
        return response;
    }

    @Override
    public synchronized void backupDatabase() {
        if (isRoundEndNow()) {
            firstRound.set(false);
        }
        /*
         * Cannot backup database at first round because biggest sequenceID are not sure yet, it my cause the error
         * that two same sequence ID has different database info.
         */
        if (firstRound.get()) {
            return;
        }
        logger.warn("Backup database to datanode.");
        roundRefresh(false);
        /*
         * If found need to recover database, then backup database is meaningless because database is empty
         * so before backup database, recover it first.
         */
        if (needRecoverDB()) {
            recoverDatabase();
        }
        Set<Integer> groups = new HashSet<>();

        BackupDatabaseInfoRequest backupDatabaseInfoRequest = new BackupDatabaseInfoRequest();
        backupDatabaseInfoRequest.setRequestId(RequestIdBuilder.get());
        ReportDBResponse_Thrift databaseInfo = new ReportDBResponse_Thrift();
        databaseInfo.setSequenceId(getRoundSequenceId());
        loadTablesFromDB(databaseInfo);
        backupDatabaseInfoRequest.setDatabaseInfo(databaseInfo);

        Set<Instance> datanodes = instanceStore.getAll(PyService.DATANODE.getServiceName(), InstanceStatus.OK);
        for (Instance datanode : datanodes) {
            if (groups.add(datanode.getGroup().getGroupId()) && groups.size() <= maxBackupCount) {
                try {
                    EndPoint endPoint = datanode.getEndPoint();
                    DataNodeService.Iface dataNodeClient = this.dataNodeClientFactory
                            .generateSyncClient(endPoint, 2000, 2000);
                    dataNodeClient.backupDatabaseInfo(backupDatabaseInfoRequest);
                    roundRecord.put(getRoundSequenceId(), endPoint);
                } catch (Exception e) {
                    logger.warn("caught an exception: ", e);
                    groups.remove(datanode.getGroup().getGroupId());
                }
            }
        }
        roundRefresh(false);
    }

    @Override
    public synchronized boolean recoverDatabase() {
        ReportDBRequest_Thrift newestDBInfo = null;

        List<Long> sequenceIdList = new ArrayList<>(lastRoundRecord.keySet());
        Collections.sort(sequenceIdList, Collections.reverseOrder());

        GetDBInfoRequest_Thrift getDBInfoRequest = new GetDBInfoRequest_Thrift();
        getDBInfoRequest.setRequestId(RequestIdBuilder.get());

        for (Long sequenceId : sequenceIdList) {
            Collection<EndPoint> datanodes = lastRoundRecord.get(sequenceId);
            for (EndPoint datanode : datanodes) {
                try {
                    DataNodeService.Iface dataNodeClient = dataNodeClientFactory
                            .generateSyncClient(datanode, 2000, 2000);
                    GetDBInfoResponse_Thrift getDBInfoResponse = dataNodeClient.getDBInfo(getDBInfoRequest);
                    newestDBInfo = getDBInfoResponse.getDbInfo();
                    if (newestDBInfo != null) {
                        break;
                    }
                } catch (Exception e) {
                    logger.warn("get datanode:{} DB info failed, try another", datanode, e);
                }
            }
            if (newestDBInfo != null) {
                break;
            }
        }
        logger.warn("going to recover DB: {}", newestDBInfo);
        // save newest info to database tables

        return saveTablesToDB(newestDBInfo);
    }

    /**
     * @param clearLastRoundRecord if active backup database happened, the round time is shorter than normal round end,
     *                             so keep at least two round records has more probability to get latest database info
     */
    private void roundRefresh(boolean clearLastRoundRecord) {
        logger.debug("this round:{} is over, and save info:{}", this.roundSequenceId.get(), this.roundRecordMap);
        this.startTime.set(System.currentTimeMillis());
        if (this.roundRecordMap.size() < this.maxBackupCount) {
            logger.warn("this round:{} save count:{} is less than maxBackupCount:{}", roundSequenceId,
                    this.roundRecordMap.size(), this.maxBackupCount);
        }
        if (clearLastRoundRecord) {
            lastRoundRecord.clear();
        }
        lastRoundRecord.putAll(roundRecord);
        roundRecord.clear();
        this.roundRecordMap.clear();
        this.roundSequenceId.incrementAndGet();
        this.round.incrementAndGet();
    }

    private boolean isRoundEndNow() {
        return System.currentTimeMillis() > (getStartTime() + roundTimeInterval);
    }

    public long getRoundTimeInterval() {
        return roundTimeInterval;
    }

    public Long getStartTime() {
        return startTime.get();
    }

    public Long getRoundSequenceId() {
        return roundSequenceId.get();
    }

    /**
     * just for unit test
     */
    public int getRecordCount() {
        return this.roundRecordMap.size();
    }

    /**
     * for unit test too
     */
    public Map<Group, EndPoint> getRoundRecordMap() {
        return this.roundRecordMap;
    }

    /**
     * for test only
     *
     * @return
     */
    public int getLastRecordCount() {
        return lastRoundRecord.size();
    }

    @Override
    public boolean needRecoverDB() {
        /* if all tables are empty, means that database is new setup or has been re-setup
         * account table is impossible to be empty, so if found account table is empty, then
         * the database is definitely broken and need to recover.
         */
        boolean licenseExist = false;
        try {
            LicenseJson licenseJson = licenseStorage.getLicense();
            if (licenseJson != null) {
                licenseExist = true;
            } else {
                logger.warn("license doesn't exist for now");
            }
        } catch (Exception e) {
            logger.warn("can not get license for now");
        }
        try {
            if (volumeRuleRelationshipStore.list().isEmpty() && accessRuleStore.list().isEmpty() && domainStore
                    .listAllDomains().isEmpty() &&
                    iscsiRuleRelationshipStore.list().isEmpty() &&
                    iscsiAccessRuleStore.list().isEmpty() &&
                    storagePoolStore.listAllStoragePools().isEmpty() &&
                    accountStore.listAccounts().isEmpty() &&
                    !licenseExist && roleStore.listRoles().isEmpty() &&
                    apiStore.listAPIs().isEmpty() &&
                    ioLimitationStore.list().isEmpty() &&
                    migrationRuleStore.list().isEmpty() &&
                    resourceStore.listResources().isEmpty()) {
                return true;
            }
        } catch (Exception e) {
            logger.warn("can not load from database", e);
        }
        return false;
    }

    @Override
    public void loadTablesFromDB(ReportDBResponse_Thrift response) {
        Validate.notNull(response, "report response can not be null");
        // domain
        List<Domain> domainList = null;
        try {
            domainList = domainStore.listAllDomains();
        } catch (Exception e) {
            logger.warn("can not get domain info from database", e);
        }
        if (domainList != null && !domainList.isEmpty()) {
            List<Domain_Thrift> domainThriftList = new ArrayList<>();
            for (Domain domain : domainList) {
                Domain_Thrift domainThrift = RequestResponseHelper.buildDomainThriftFrom(domain);
                domainThriftList.add(domainThrift);
            }
            response.setDomainThriftList(domainThriftList);
        }
        // storagePool
        List<StoragePool> storagePoolList = null;
        try {
            storagePoolList = storagePoolStore.listAllStoragePools();
        } catch (Exception e) {
            logger.warn("can not get storage pool info from database", e);
        }

        if (storagePoolList != null && !storagePoolList.isEmpty()) {
            List<StoragePool_Thrift> storagePoolThriftList = new ArrayList<>();
            for (StoragePool storagePool : storagePoolList) {
                StoragePool_Thrift storagePoolThrift = RequestResponseHelper.buildThriftStoragePoolFrom(storagePool, null);
                storagePoolThriftList.add(storagePoolThrift);
            }
            response.setStoragePoolThriftList(storagePoolThriftList);
        }

        // volumeRuleRelationship
        List<VolumeRuleRelationshipInformation> volumeRuleList = volumeRuleRelationshipStore.list();
        if (volumeRuleList != null && !volumeRuleList.isEmpty()) {
            List<VolumeRuleRelationship_Thrift> volume2RuleThriftList = new ArrayList<>();
            for (VolumeRuleRelationshipInformation volume2RuleInfo : volumeRuleList) {
                Volume2AccessRuleRelationship volume2Rule = volume2RuleInfo.toVolume2AccessRuleRelationship();
                VolumeRuleRelationship_Thrift volume2RuleThrift = RequestResponseHelper
                        .buildThriftVolumeRuleRelationship(volume2Rule);
                volume2RuleThriftList.add(volume2RuleThrift);
            }
            response.setVolume2RuleThriftList(volume2RuleThriftList);
        }
        // accessRule
        List<AccessRuleInformation> accessRuleList = accessRuleStore.list();
        if (accessRuleList != null && !accessRuleList.isEmpty()) {
            List<VolumeAccessRule_Thrift> accessRuleThriftList = new ArrayList<>();
            for (AccessRuleInformation accessRuleInfo : accessRuleList) {
                VolumeAccessRule accessRule = accessRuleInfo.toVolumeAccessRule();
                VolumeAccessRule_Thrift accessRuleThrift = RequestResponseHelper
                        .buildVolumeAccessRuleThriftFrom(accessRule);
                accessRuleThriftList.add(accessRuleThrift);
            }
            response.setAccessRuleThriftList(accessRuleThriftList);
        }

        // iscsi accessRule
        List<IscsiAccessRuleInformation> iscsiAccessRuleList = iscsiAccessRuleStore.list();
        if (iscsiAccessRuleList != null && !iscsiAccessRuleList.isEmpty()) {
            List<IscsiAccessRule_Thrift> iscsiAccessRuleThriftList = new ArrayList<>();
            for (IscsiAccessRuleInformation iscsiAccessRuleInfo : iscsiAccessRuleList) {
                IscsiAccessRule iscsiAccessRule = iscsiAccessRuleInfo.toIscsiAccessRule();
                IscsiAccessRule_Thrift accessRuleThrift = RequestResponseHelper
                        .buildIscsiAccessRuleThriftFrom(iscsiAccessRule);
                iscsiAccessRuleThriftList.add(accessRuleThrift);
            }
            response.setIscsiAccessRuleThriftList(iscsiAccessRuleThriftList);
        }

        // iscsiRuleRelationship
        List<IscsiRuleRelationshipInformation> iscsiRuleList = iscsiRuleRelationshipStore.list();
        if (iscsiRuleList != null && !iscsiRuleList.isEmpty()) {
            List<IscsiRuleRelationship_Thrift> iscsi2RuleThriftList = new ArrayList<>();
            for (IscsiRuleRelationshipInformation iscsi2RuleInfo : iscsiRuleList) {
                Iscsi2AccessRuleRelationship iscsi2Rule = iscsi2RuleInfo.toIscsi2AccessRuleRelationship();
                IscsiRuleRelationship_Thrift iscsi2RuleThrift = RequestResponseHelper
                        .buildThriftIscsiRuleRelationship(iscsi2Rule);
                iscsi2RuleThriftList.add(iscsi2RuleThrift);
            }
            response.setIscsi2RuleThriftList(iscsi2RuleThriftList);
        }

        // capacity record
        CapacityRecord capacityRecord = null;
        try {
            capacityRecord = capacityRecordStore.getCapacityRecord();
        } catch (Exception e1) {
            logger.warn("caught an exception", e1);
        }
        if (capacityRecord != null && !capacityRecord.getRecordMap().isEmpty()) {
            List<CapacityRecord_Thrift> capacityRecordThriftList = new ArrayList<>();
            CapacityRecord_Thrift capacityRecordThrift = RequestResponseHelper
                    .buildThriftCapacityRecordFrom(capacityRecord);
            capacityRecordThriftList.add(capacityRecordThrift);

            response.setCapacityRecordThriftList(capacityRecordThriftList);
        }

        // volume clone related
        List<CloneRelationshipInformation> cloneRelationshipInformationList = cloneRelationshipsStore.loadFromDB();
        if (cloneRelationshipInformationList != null && !cloneRelationshipInformationList.isEmpty()) {
            List<VolumeCloneRelationship_Thrift> cloneRelationshipThriftList = new ArrayList<>();
            for (CloneRelationshipInformation cloneRelationshipInfo : cloneRelationshipInformationList) {
                cloneRelationshipThriftList.add(RequestResponseHelper
                        .buildThriftCloneRelationship(cloneRelationshipInfo));
            }
            response.setCloneRelationshipThriftList(cloneRelationshipThriftList);
        }

        // license
        try {
            LicenseCryptogramInformation licenseCryptogramInformation = licenseStorage.getLicenseCryptogram();
            LicenseCryptogramInformation_Thrift licenseCryptogramInformationThrift = LicenseCryptogramInformationTransition
                    .buildLicenseCryptogramInformationThriftFrom(licenseCryptogramInformation);
            response.setLicenseCryptogramThrift(licenseCryptogramInformationThrift);
        } catch (Exception e) {
            logger.error("load from license store failed", e);
        }

        // role
        List<Role_Thrift> roleThriftList = new ArrayList<>();
        List<Role> roleList = roleStore.listRoles();
        if (null != roleList && !roleList.isEmpty()) {
            for (Role role : roleList) {
                roleThriftList.add(RequestResponseHelper.buildRoleThrift(role));
            }
            response.setRoleThriftList(roleThriftList);
        }

        // account
        List<AccountMetadataBackup_Thrift> accountBackupList = new ArrayList<>();
        Collection<AccountMetadata> accountMetadataList = accountStore.listAccounts();
        if (null != accountMetadataList && !accountMetadataList.isEmpty()) {
            for (AccountMetadata account : accountMetadataList) {
                accountBackupList.add(RequestResponseHelper.buildAccountMetadataBackupThrift(account));
            }
            response.setAccountMetadataBackupThriftList(accountBackupList);
        }

        // ioLimitation
        List<IOLimitationInformation>  ioLimitationList = ioLimitationStore.list();
        if (ioLimitationList != null && !ioLimitationList.isEmpty()) {
            List<IOLimitation_Thrift> ioLimitationThriftList = new ArrayList<>();
            for (IOLimitationInformation ioLimitationInfo : ioLimitationList) {
                IOLimitation ioLimitation = ioLimitationInfo.toIOLimitation();
                IOLimitation_Thrift ioLimitationThrift = RequestResponseHelper
                        .buildThriftIOLimitationFrom(ioLimitation);
                ioLimitationThriftList.add(ioLimitationThrift);
                logger.debug("loadtable ioLimitationInfo {} ioLimitationThrift {}", ioLimitationInfo,ioLimitationThrift);
            }
            response.setIoLimitationThriftList(ioLimitationThriftList);
        }

        // migrationSpeedRule
        List<MigrationRuleInformation> migrationSpeedList = migrationRuleStore.list();
        if (migrationSpeedList != null && !migrationSpeedList.isEmpty()) {
            List<MigrationRule_Thrift> migrationSpeedThriftList = new ArrayList<>();
            for (MigrationRuleInformation migrationSpeedInfo : migrationSpeedList) {
                MigrationRule migrationSpeed = migrationSpeedInfo.toMigrationRule();
                MigrationRule_Thrift migrationSpeedThrift = RequestResponseHelper
                        .buildMigrationRuleThriftFrom(migrationSpeed);
                migrationSpeedThriftList.add(migrationSpeedThrift);
            }
            response.setMigrationSpeedThriftList(migrationSpeedThriftList);
        }

        //api
        List<APIToAuthorize> apiList = null;
        try
        {
            apiList = apiStore.listAPIs();
        } catch (Exception e){
            logger.error("can't get api info from database", e);
        }
        if (apiList != null && !apiList.isEmpty())
        {
            List<APIToAuthorize_Thrift> apiThriftList = new ArrayList<>();
            for (APIToAuthorize api : apiList) {
                apiThriftList.add(RequestResponseHelper.buildAPIToAuthorizeThrift(api));
            }
            response.setApiThriftList(apiThriftList);
        }

        //resource
        List<PyResource> resourceList = null;
        try
        {
            resourceList = resourceStore.listResources();
        } catch (Exception e) {
            logger.error("can't get reource info from database", e);
        }
        if (resourceList != null && !resourceList.isEmpty())
        {
            List<Resource_Thrift> resourceThriftList = new ArrayList<>();
            for (PyResource resource : resourceList) {
                resourceThriftList.add(RequestResponseHelper.buildResourceThrift(resource));
            }
            response.setResourceThriftList(resourceThriftList);
        }

        return;
    }

    @Override
    public boolean saveTablesToDB(ReportDBRequest_Thrift newestDBInfo) {
        if (newestDBInfo == null) {
            return false;
        }
        // domain
        List<Domain_Thrift> domainThriftList = newestDBInfo.getDomainThriftList();
        if (domainThriftList != null && !domainThriftList.isEmpty()) {
            for (Domain_Thrift domainThrift : domainThriftList) {
                Domain domain = RequestResponseHelper.buildDomainFrom(domainThrift);
                domainStore.saveDomain(domain);
            }
        }
        // storagePool
        List<StoragePool_Thrift> storagePoolThriftList = newestDBInfo.getStoragePoolThriftList();
        if (storagePoolThriftList != null && !storagePoolThriftList.isEmpty()) {
            for (StoragePool_Thrift storagePoolThrift : storagePoolThriftList) {
                StoragePool storagePool = RequestResponseHelper.buildStoragePoolFromThrift(storagePoolThrift);
                storagePoolStore.saveStoragePool(storagePool);
            }
        }
        // volumeRuleRelationship
        List<VolumeRuleRelationship_Thrift> volume2RuleThriftList = newestDBInfo.getVolume2RuleThriftList();
        if (volume2RuleThriftList != null && !volume2RuleThriftList.isEmpty()) {
            for (VolumeRuleRelationship_Thrift volume2RuleThrift : volume2RuleThriftList) {
                Volume2AccessRuleRelationship volume2Rule = RequestResponseHelper
                        .buildVolume2AccessRuleRelationshipFromThrift(volume2RuleThrift);
                volumeRuleRelationshipStore.save(volume2Rule.toVolumeRuleRelationshipInformation());
            }
        }
        // accessRule
        List<VolumeAccessRule_Thrift> accessRuleThriftList = newestDBInfo.getAccessRuleThriftList();
        if (accessRuleThriftList != null && !accessRuleThriftList.isEmpty()) {
            for (VolumeAccessRule_Thrift accessRuleThrift : accessRuleThriftList) {
                VolumeAccessRule accessRule = RequestResponseHelper.buildVolumeAccessRuleFrom(accessRuleThrift);
                accessRuleStore.save(accessRule.toAccessRuleInformation());
            }
        }

        // iscsiRuleRelationship
        List<IscsiRuleRelationship_Thrift> iscsi2RuleThriftList = newestDBInfo.getIscsi2RuleThriftList();
        if (iscsi2RuleThriftList != null && !iscsi2RuleThriftList.isEmpty()) {
            for (IscsiRuleRelationship_Thrift iscsi2RuleThrift : iscsi2RuleThriftList) {
                Iscsi2AccessRuleRelationship iscsi2Rule = RequestResponseHelper
                        .buildIscsi2AccessRuleRelationshipFromThrift(iscsi2RuleThrift);
                iscsiRuleRelationshipStore.save(iscsi2Rule.toIscsiRuleRelationshipInformation());
            }
        }
        // accessRule
        List<IscsiAccessRule_Thrift> iscsiAccessRuleThriftList = newestDBInfo.getIscsiAccessRuleThriftList();
        if (iscsiAccessRuleThriftList != null && !iscsiAccessRuleThriftList.isEmpty()) {
            for (IscsiAccessRule_Thrift iscsiAccessRuleThrift : iscsiAccessRuleThriftList) {
                IscsiAccessRule iscsiAccessRule = RequestResponseHelper.buildIscsiAccessRuleFrom(iscsiAccessRuleThrift);
                iscsiAccessRuleStore.save(iscsiAccessRule.toIscsiAccessRuleInformation());
            }
        }

        // capacity record
        List<CapacityRecord_Thrift> capacityRecordThriftList = newestDBInfo.getCapacityRecordThriftList();
        if (capacityRecordThriftList != null && !capacityRecordThriftList.isEmpty()) {
            for (CapacityRecord_Thrift capacityRecordThrift : capacityRecordThriftList) {
                CapacityRecord capacityRecord = RequestResponseHelper.buildCapacityRecordFrom(capacityRecordThrift);
                capacityRecordStore.saveCapacityRecord(capacityRecord);
            }
        }

        // volume clone related
        List<VolumeCloneRelationship_Thrift> volumeCloneRelationshipThriftList = newestDBInfo
                .getCloneRelationshipThriftList();
        if (volumeCloneRelationshipThriftList != null && !volumeCloneRelationshipThriftList.isEmpty()) {
            for (VolumeCloneRelationship_Thrift volumeCloneRelationshipThrift : volumeCloneRelationshipThriftList) {
                CloneRelationshipInformation cloneRelationshipInformation = RequestResponseHelper
                        .buildCloneRelationshipFrom(volumeCloneRelationshipThrift);
                cloneRelationshipsStore.saveToDB(cloneRelationshipInformation);
            }
        }

        // license
        if (newestDBInfo.isSetLicenseCryptogramThrift()) {
            try {
                LicenseCryptogramInformation licenseCryptogramInformation = LicenseCryptogramInformationTransition
                        .buildLicenseCryptogramInformationFrom(newestDBInfo.getLicenseCryptogramThrift(),
                                licenseStorage);
                licenseStorage.saveLicenseCryptogram(licenseCryptogramInformation);
            } catch (Exception e) {
                logger.error("failed to build license:{}", newestDBInfo.getLicenseCryptogramThrift(), e);
            }
        }

        // api
        List<APIToAuthorize_Thrift> apiThriftList = newestDBInfo.getApiThriftList();
        if (apiThriftList != null && !apiThriftList.isEmpty()) {
            for (APIToAuthorize_Thrift apiThrift : apiThriftList) {
                APIToAuthorize api = RequestResponseHelper.buildAPIToAuthorizeFrom(apiThrift);
                apiStore.saveAPI(api);
            }
        }

        // resource
        List<Resource_Thrift> resourceThriftList = newestDBInfo.getResourceThriftList();
        if (resourceThriftList != null && !resourceThriftList.isEmpty()) {
            for (Resource_Thrift resourceThrift : resourceThriftList) {
                PyResource resource = RequestResponseHelper.buildResourceFrom(resourceThrift);
                resourceStore.saveResource(resource);
            }
        }

        // role
        if (newestDBInfo.isSetRoleThriftList()) {
            for (Role_Thrift roleThrift : newestDBInfo.getRoleThriftList()) {
                roleStore.saveRole(RequestResponseHelper.buildRoleFrom(roleThrift));
            }
        }

        // account
        if (newestDBInfo.isSetAccountMetadataBackupThriftList()) {
            for (AccountMetadataBackup_Thrift accountMetadataBackupThrift :
                    newestDBInfo.getAccountMetadataBackupThriftList()) {
                AccountMetadata accountBackup = RequestResponseHelper.
                        buildAccountMetadataBackupFrom(accountMetadataBackupThrift);
                accountStore.saveAccount(accountBackup);
            }
        }
        // ioLimitation
        List<IOLimitation_Thrift> ioLimitationThriftList = newestDBInfo.getIoLimitationThriftList();
        if (ioLimitationThriftList != null && !ioLimitationThriftList.isEmpty()) {
            for (IOLimitation_Thrift ioLimitationThrift : ioLimitationThriftList) {
                IOLimitation iolimition = RequestResponseHelper.buildIOLimitationFrom(ioLimitationThrift);
                ioLimitationStore.save(iolimition.toIOLimitationInformation(ioLimitationStore));
            }
        }

        // migrationRule
        List<MigrationRule_Thrift> migrationSpeedRuleThriftList = newestDBInfo.getMigrationRuleThriftList();
        if (migrationSpeedRuleThriftList != null && !migrationSpeedRuleThriftList.isEmpty()) {
            for (MigrationRule_Thrift migrationSpeedRuleThrift : migrationSpeedRuleThriftList) {
                MigrationRule migrationSpeed = RequestResponseHelper.buildMigrationRuleFrom(migrationSpeedRuleThrift);
                migrationRuleStore.save(migrationSpeed.toMigrationRuleInformation());
            }
        }
        return true;
    }

    public VolumeRuleRelationshipStore getVolumeRuleRelationshipStore() {
        return volumeRuleRelationshipStore;
    }

    public void setVolumeRuleRelationshipStore(VolumeRuleRelationshipStore volumeRuleRelationshipStore) {
        this.volumeRuleRelationshipStore = volumeRuleRelationshipStore;
    }

    public IscsiRuleRelationshipStore getIscsiRuleRelationshipStore() {
        return iscsiRuleRelationshipStore;
    }

    public void setIscsiRuleRelationshipStore(IscsiRuleRelationshipStore iscsiRuleRelationshipStore) {
        this.iscsiRuleRelationshipStore = iscsiRuleRelationshipStore;
    }

    public IscsiAccessRuleStore getIscsiAccessRuleStore() {
        return iscsiAccessRuleStore;
    }

    public void setIscsiAccessRuleStore(IscsiAccessRuleStore iscsiAccessRuleStore) {
        this.iscsiAccessRuleStore = iscsiAccessRuleStore;
    }

    public AccessRuleStore getAccessRuleStore() {
        return accessRuleStore;
    }

    public void setAccessRuleStore(AccessRuleStore accessRuleStore) {
        this.accessRuleStore = accessRuleStore;
    }

    public DomainStore getDomainStore() {
        return domainStore;
    }

    public void setDomainStore(DomainStore domainStore) {
        this.domainStore = domainStore;
    }

    public StoragePoolStore getStoragePoolStore() {
        return storagePoolStore;
    }

    public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
        this.storagePoolStore = storagePoolStore;
    }

    public CapacityRecordStore getCapacityRecordStore() {
        return capacityRecordStore;
    }

    public void setCapacityRecordStore(CapacityRecordStore capacityRecordStore) {
        this.capacityRecordStore = capacityRecordStore;
    }

    public GenericThriftClientFactory<DataNodeService.Iface> getDataNodeClientFactory() {
        return dataNodeClientFactory;
    }

    public void setDataNodeClientFactory(GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory) {
        this.dataNodeClientFactory = dataNodeClientFactory;
    }

    public CloneRelationshipsDBStore getCloneRelationshipsStore() {
        return cloneRelationshipsStore;
    }

    public void setCloneRelationshipsStore(CloneRelationshipsDBStore cloneRelationshipsStore) {
        this.cloneRelationshipsStore = cloneRelationshipsStore;
    }

    public IOLimitationStore getIoLimitationStore() {
        return ioLimitationStore;
    }

    public void setIoLimitationStore(IOLimitationStore ioLimitationStore) {
        this.ioLimitationStore = ioLimitationStore;
    }

    public MigrationRuleStore getMigrationRuleStore() {
        return migrationRuleStore;
    }

    public void setMigrationRuleStore(MigrationRuleStore migrationRuleStore) {
        this.migrationRuleStore = migrationRuleStore;
    }

    /**
     * this.round.get() > 1 means info center has start two round time, and had recovery DB if necessary
     */
    @Override
    public boolean passedRecoveryTime() {
        return this.round.get() > 1;
    }
}
