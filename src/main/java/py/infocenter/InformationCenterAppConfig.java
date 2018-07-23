package py.infocenter;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import py.app.NetworkConfiguration;
import py.app.context.InstanceIdFileStore;
import py.app.healthcheck.HealthChecker;
import py.app.healthcheck.HealthCheckerImpl;
import py.app.thrift.ThriftAppEngine;
import py.common.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.icshare.*;
import py.icshare.authorization.*;
import py.icshare.qos.*;
import py.infocenter.worker.*;
import py.infocenter.rebalance.SegmentUnitsDistributionManager;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.license.LicenseCryptogramInformationTransition;
import py.license.LicenseDBStore;
import py.license.LicenseStorage;
import py.dih.client.DIHClientFactory;
import py.dih.client.DIHInstanceStore;
import py.dih.client.worker.HeartBeatWorkerFactory;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.infocenter.common.InfoCenterConstants;
import py.infocenter.rebalance.RebalanceConfiguration;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.service.selection.BalancedDriverContainerSelectionStrategy;
import py.infocenter.service.selection.DriverContainerSelectionStrategy;
import py.infocenter.service.selection.FrugalInstanceSelectionStrategy;
import py.infocenter.service.selection.InstanceSelectionStrategy;
import py.infocenter.service.selection.RandomSelectionStrategy;
import py.infocenter.service.selection.SelectionStrategy;
import py.infocenter.store.*;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.monitor.jmx.configuration.JmxAgentConfiguration;
import py.monitor.jmx.server.JmxAgent;
import py.monitorserver.common.OperationName;
import py.periodic.UnableToStartException;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.storage.StorageConfiguration;
import py.thrift.datanode.service.DataNodeService;
import py.zookeeper.ZkClientFactory;
import py.zookeeper.ZkElectionLeader;

import java.util.Set;


@Configuration
@ImportResource({"classpath:spring-config/hibernate.xml", "classpath:spring-config/defaultMigrationRule.xml"})
@PropertySource("classpath:config/infocenter.properties")
@Import({StorageConfiguration.class, NetworkConfiguration.class, JmxAgentConfiguration.class})
public class InformationCenterAppConfig {
    @Value("${thrift.client.timeout}")
    private int thriftClientTimeout;

    @Value("${app.name}")
    private String appName;

    @Value("${driver.report.timeout.ms}")
    private long driverReportTimeout;

    @Value("${instance.metadata.to.remove:120000}")
    private int instanceMetadataTimeToRemove;

    @Value("${driver.sweeper.rate}")
    private int driverSweeperRate;

    @Value("${volume.sweeper.rate}")
    private int volumeSweeperRate;

    @Value("${timeout.sweeper.rate:1000}")
    private int timeoutSweeper;

    @Value("${alarm.sweeper.rate:5000}")
    private int alarmSweeperRate;

    @Value("${servernode.alert.checker.rate:10000}")
    private int serverNodeAlertCheckerRate;

    @Value("${instance.metadata.sweeper.rate:5000}")
    private int instanceMetadataSweeperRate;

    @Value("${app.main.endpoint}")
    private String mainEndPoint;

    @Value("${health.checker.rate}")
    private int healthCheckerRate;

    @Value("${dih.endpoint}")
    private String dihEndPoint;

    @Value("${app.location}")
    private String appLocation;

    @Value("${zookeeper.connection.string}")
    private String zookeeperConnectionString;

    @Value("${zookeeper.session.timeout.ms:10000}")
    private int zookeeperSessionTimeout;

    @Value("${zookeeper.election.switch:false}")
    private boolean zookeeperElectionSwitch;

    @Value("${zookeeper.lock.directory}")
    private String lockDirectory;

    @Value("${dead.volume.to.remove.second:15552000}")
    private int deadVolumeToRemove;

    @Value("${segment.unit.report.timeout.second:90}")
    private int segmentUnitReportTimeout;

    @Value("${volume.tobecreated.timeout.second:90}")
    private int volumeTobeCreatedTimeout;

    @Value("${volume.becreating.timeout.second:1800}")
    private int volumeBeCreatingTimeout;

    @Value("${fix.volume.timeout.second:600}")
    private int fixVolumeTimeoutSec = 600;

    @Value("${group.count:6}")
    private int groupCount;

    @Value("${next.action.time.interval.ms:150000}")
    private int nextActionTimeIntervalMs = 150000;

    @Value("${log.level}")
    private String logLevel;

    @Value("${actual.free.space.refresh.period.time}")
    private long refreshPeriodTime;

    @Value("${log.output.file}")
    private String logOutputFile;

    @Value("${volume.tobeorphan.time:600000}")
    private long volumeToBeOrphanTime;

    @Value("${factor.for.selector: 2}")
    private int factorForSelector = 2;

    @Value("${remainder.for.selector: 0}")
    private int remainderForSelector = 0;

    @Value("${store.capacity.record.count: 7}")
    private int storeCapacityRecordCount = 7;

    @Value("${take.sample.for.capacity.interval.second: 86400}")
    private int takeSampleInterValSecond = 86400;

    @Value("${zookeeper.launcher}")
    private String zooKeeperLauncher;

    @Value("${round.time.interval.ms: 30000}")
    private long roundTimeInterval = 30000;

    @Value("${max.backup.database.count: 3}")
    private int maxBackupCount = 3;

    @Value("${max.rebalance.task.count: 10}")
    private int maxRebalanceTaskCount;

    @Value("${rebalance.pressure.threshold: 0.2}")
    private double rebalancePressureThreshold;

    @Value("${rebalance.pressure.addend: 0}")
    private int rebalancePressureAddend;

    @Value("${rebalance.task.expire.time.seconds: 1200}")
    private int rebalanceTaskExpireTimeSeconds;

    @Value("${page.wrapp.count: 128}")
    private int pageWrappCount = 128;

    @Value("${segment.wrapp.count: 10}")
    private int segmentWrappCount = 10;

    @Value("${servernode.report.overtime.second: 30}")
    private int serverNodeReportOverTimeSecond;


    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private StorageConfiguration storageConfiguration;

    @Autowired
    private NetworkConfiguration networkConfiguration;

    @Autowired
    private JmxAgentConfiguration jmxAgentConfiguration;

    @Autowired
    private Set<MigrationRule> defaultMigrationRuleSet;

    //---------------network size----------------------
    //datanode max thrift network frame size
    @Value("${max.network.frame.size:17000000}")
    private int maxNetworkFrameSize = 17 * 1000 * 1000;

    public int getMaxNetworkFrameSize() {
        return maxNetworkFrameSize;
    }

    public void setMaxNetworkFrameSize(int maxNetworkFrameSize) {
        this.maxNetworkFrameSize = maxNetworkFrameSize;
    }


    public boolean getZookeeperElectionSwitch() {
        return zookeeperElectionSwitch;
    }

    public int getZookeeperSessionTimeout() {
        return zookeeperSessionTimeout;
    }

    public String getZooKeeperLauncher() {
        return zooKeeperLauncher;
    }

    public void setZooKeeperLauncher(String zooKeeperLauncher) {
        this.zooKeeperLauncher = zooKeeperLauncher;
    }

    @Bean
    public LicenseStorage licenseDBStore() {
        LicenseStorage licenseDBStore = new LicenseDBStore(sessionFactory);
        return licenseDBStore;
    }

    @Bean
    public LicenseCryptogramInformationTransition licenseCryptogramInformationTransition() {
        LicenseCryptogramInformationTransition informationTransition = new LicenseCryptogramInformationTransition(licenseDBStore());
        return informationTransition;
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public SegmentUnitsDistributionManager segmentUnitsDistributionManager() throws Exception {
        return new SegmentUnitsDistributionManagerImpl(storageConfiguration.getSegmentSizeByte(),
                twoLevelVolumeStore(), storageStore(), storagePoolStore());
    }

    @Bean
    public ThriftAppEngine appEngine() throws Exception {
        initInfoCenterConstants();
        InformationCenterAppEngine appEngine = new InformationCenterAppEngine(informationCenter());
        appEngine.setContext(appContext());
        appEngine.setHealthChecker(healthChecker());
        appEngine.setDriverStoreSweeperExecutor(driverStoreSweeperExecutor());
        appEngine.setVolumeSweeperExecutor(volumeSweeperExecutor());
        appEngine.setTimeoutSweeperExecutor(timeoutSweeperExecutor());
        appEngine.setInstanceMetadataStoreSweeperExecutor(instanceMetadataStoreSweeperExecutor());
        appEngine.setAlarmSweeperExecutor(alarmSweeperExecutor());
        appEngine.setServerNodeAlertCheckerExecutor(serverNodeAlertCheckerExecutor());
        appEngine.setZkElectionLeader(zkElectionLeader());
        appEngine.setInformationCenterAppConfig(this);
        appEngine.setMaxNetworkFrameSize(getMaxNetworkFrameSize());
        appEngine.setMigrationRuleStore(migrationRuleStore());
        appEngine.setDefaultMigrationRuleSet(defaultMigrationRuleSet);
        return appEngine;
    }

    @Bean
    public JmxAgent jmxAgent() {
        JmxAgent jmxAgent = JmxAgent.getInstance();
        jmxAgent.setAppContext(appContext());
        return jmxAgent;
    }

    @Bean
    public InformationCenterImpl informationCenter() throws Exception {
        InformationCenterImpl informationCenter = new InformationCenterImpl();
        informationCenter.setAppContext(appContext());
        informationCenter.setLicenseStorage(licenseDBStore());
        informationCenter.setVolumeStore(twoLevelVolumeStore());
        informationCenter.setVolumeStatusStore(volumeStatusTransitionStore());
        informationCenter.setSegmentUnitTimeoutStore(segmentUnitTimeoutStore());
        informationCenter.setDriverStore(driverStore());
        informationCenter.setStorageStore(storageStore());
        informationCenter.setInstanceStore(instanceStore());
        informationCenter.setAccessRuleStore(accessRuleStore());
        informationCenter.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore());
        informationCenter.setIscsiAccessRuleStore(iscsiAccessRuleStore());
        informationCenter.setIscsiRuleRelationshipStore(iscsiRuleRelationshipStore());
        informationCenter.setServerNodeStore(serverNodeStore());
        informationCenter.setDiskInfoStore(diskInfoStore());
        informationCenter.setDomainStore(domainStore());
        informationCenter.setStoragePoolStore(storagePoolStore());
        informationCenter.setDriverContainerSelectionStrategy(driverContainerSelectionStrategy());
        informationCenter.setSegmentSize(storageConfiguration.getSegmentSizeByte());
        informationCenter.setDeadVolumeToRemove(deadVolumeToRemove);
        informationCenter.setGroupCount(groupCount);
        informationCenter.setSelectionStrategy(selectionStrategy());
        informationCenter.setOrphanVolumes(orphanVolumeStore());
        informationCenter.setAlarmStore(alarmStore());
        informationCenter.setAccountStore(inMemAccountStore());
        informationCenter.setRoleStore(roleStore());
        informationCenter.setResourceStore(resourceStore());
        informationCenter.setApiStore(apiStore());
        informationCenter.setCoordinatorClientFactory(coordinatorClientFactory());
        informationCenter.setNextActionTimeIntervalMs(nextActionTimeIntervalMs);
        informationCenter.setCapacityRecordStore(capacityRecordStore());
        informationCenter.setNetworkConfiguration(networkConfiguration);
        informationCenter.setCloneRelationshipsStore(cloneRelationshipsDBStore());
        informationCenter.setIoLimitationStore(ioLimitationStore());
        informationCenter.setMigrationRuleStore(migrationRuleStore());
        informationCenter.initBackupDBManager(roundTimeInterval, maxBackupCount);
        informationCenter.setSegmentUnitsDistributionManager(segmentUnitsDistributionManager());
        informationCenter.setPageWrappCount(pageWrappCount);
        informationCenter.setSegmentWrappCount(segmentWrappCount);
        informationCenter.setInstanceMaintenanceDBStore(instanceMaintenanceStore());

        appContext().setServerNodeReportTimeMap(informationCenter.getServerNodeReportTimeMap());

        if (zookeeperElectionSwitch) {
            appContext().setStatus(InstanceStatus.SUSPEND);
        } else {
            appContext().setStatus(InstanceStatus.OK);
        }

        return informationCenter;
    }

    @Bean
    public CloneRelationshipsDBStore cloneRelationshipsDBStore() {
        CloneRelationshipsStoreImpl cloneRelationshipsDBStore = new CloneRelationshipsStoreImpl();
        cloneRelationshipsDBStore.setSessionFactory(sessionFactory);
        return cloneRelationshipsDBStore;
    }

    @Bean
    public InfoCenterStoreSet infoCenterStoreSet() {
        InfoCenterStoreSet infoCenterStoreSet = new InfoCenterStoreSet();

        infoCenterStoreSet.setDomainStore(domainStore());
        infoCenterStoreSet.setDriverStore(driverStore());
        infoCenterStoreSet.setStoragePoolStore(storagePoolStore());
        infoCenterStoreSet.setStorageStore(storageStore());
        infoCenterStoreSet.setVolumeStore(twoLevelVolumeStore());

        return infoCenterStoreSet;
    }

    @Bean
    public CoordinatorClientFactory coordinatorClientFactory() {
        CoordinatorClientFactory coordinatorClientFactory = new CoordinatorClientFactory(1);
        coordinatorClientFactory.getGenericClientFactory().setMaxNetworkFrameSize(maxNetworkFrameSize);
        return coordinatorClientFactory;
    }

    @Bean
    public OrphanVolumeStoreImpl orphanVolumeStore() {
        OrphanVolumeStoreImpl orphanVolumeStore = new OrphanVolumeStoreImpl();
        orphanVolumeStore.setVolumeToBeOrphanTime(volumeToBeOrphanTime);
        return orphanVolumeStore;
    }

    @Bean
    public DriverStore driverStore() {
        DriverStoreImpl driverStore = new DriverStoreImpl();
        driverStore.setSessionFactory(sessionFactory);
        return driverStore;
    }

    @Bean
    public DomainStore domainStore() {
        DomainStoreImpl domainStore = new DomainStoreImpl();
        domainStore.setSessionFactory(sessionFactory);
        return domainStore;
    }

    @Bean
    public StoragePoolStore storagePoolStore() {
        StoragePoolStoreImpl storagePoolStore = new StoragePoolStoreImpl();
        storagePoolStore.setSessionFactory(sessionFactory);
        return storagePoolStore;
    }

    @Bean
    public CapacityRecordStore capacityRecordStore() {
        CapacityRecordStoreImpl capacityRecordStore = new CapacityRecordStoreImpl();
        capacityRecordStore.setSessionFactory(sessionFactory);
        return capacityRecordStore;
    }

    @Bean
    public VolumeRuleRelationshipStore volumeRuleRelationshipStore() {
        VolumeRuleRelationshipStoreImpl volumeRuleRelationshipStore = new VolumeRuleRelationshipStoreImpl();
        volumeRuleRelationshipStore.setSessionFactory(sessionFactory);
        return volumeRuleRelationshipStore;
    }

    @Bean
    public AccessRuleStore accessRuleStore() {
        AccessRuleStoreImpl accessRuleStore = new AccessRuleStoreImpl();
        accessRuleStore.setSessionFactory(sessionFactory);
        return accessRuleStore;
    }

    @Bean
    public IscsiRuleRelationshipStore iscsiRuleRelationshipStore() {
        IscsiRuleRelationshipStoreImpl iscsiRuleRelationshipStore = new IscsiRuleRelationshipStoreImpl();
        iscsiRuleRelationshipStore.setSessionFactory(sessionFactory);
        return iscsiRuleRelationshipStore;
    }

    @Bean
    public IscsiAccessRuleStore iscsiAccessRuleStore() {
        IscsiAccessRuleStoreImpl iscsiAccessRuleStore = new IscsiAccessRuleStoreImpl();
        iscsiAccessRuleStore.setSessionFactory(sessionFactory);
        return iscsiAccessRuleStore;
    }

    @Bean
    public IOLimitationStore ioLimitationStore() {
        IOLimitationStoreImpl ioLimitationStore = new IOLimitationStoreImpl();
        ioLimitationStore.setSessionFactory(sessionFactory);
        return ioLimitationStore;
    }

    @Bean
    public MigrationRuleStore migrationRuleStore() {
        MigrationRuleStoreImpl migrationRuleStore = new MigrationRuleStoreImpl();
        migrationRuleStore.setSessionFactory(sessionFactory);
        return migrationRuleStore;
    }

    @Bean
    public ServerNodeStore serverNodeStore() {
        ServerNodeStoreImpl serverNodeStore = new ServerNodeStoreImpl();
        serverNodeStore.setSessionFactory(sessionFactory);
        return serverNodeStore;
    }

    @Bean
    public DiskInfoStore diskInfoStore(){
        DiskInfoStoreImpl diskInfoStore = new DiskInfoStoreImpl();
        diskInfoStore.setSessionFactory(sessionFactory);
        return diskInfoStore;
    }

    @Bean
    public StorageStore storageStore() {
        StorageStoreImpl storageStoreImpl = new StorageStoreImpl();
        storageStoreImpl.setSessionFactory(sessionFactory);
        storageStoreImpl.setArchiveStore(dbArchiveStore());
        return storageStoreImpl;
    }

    @Bean
    AlarmStore alarmStore() {
        AlarmStore dbStore = new AlarmStoreDBImpl(sessionFactory);
        AlarmStore memoryStore = new AlarmStoreMemoryImpl();
        AlarmStore alarmStore = new AlarmStoreTwoLevelImpl(memoryStore, dbStore);
        return alarmStore;
    }

    @Bean
    public ArchiveStore dbArchiveStore() {
        ArchiveStoreImpl archiveStoreImpl = new ArchiveStoreImpl();
        archiveStoreImpl.setSessionFactory(sessionFactory);
        return archiveStoreImpl;
    }

    @Bean
    public InfoCenterAppContext appContext() {
        InfoCenterAppContext appContext = new InfoCenterAppContext(appName);

        EndPoint endpointOfControlStream = EndPointParser.parseInSubnet(mainEndPoint,
                networkConfiguration.getControlFlowSubnet());
        EndPoint endpointOfMonitorStream = EndPointParser.parseInSubnet(jmxAgentConfiguration.getJmxAgentPort(),
                networkConfiguration.getMonitorFlowSubnet());
        appContext.putEndPoint(PortType.CONTROL, endpointOfControlStream);
        appContext.putEndPoint(PortType.MONITOR, endpointOfMonitorStream);

        appContext.setLocation(appLocation);
        appContext.setInstanceIdStore(new InstanceIdFileStore(appName, appName, endpointOfControlStream.getPort()));
        appContext.setStoreSet(infoCenterStoreSet());
        appContext.setServerNodeStore(serverNodeStore());

        return appContext;
    }

    @Bean
    public VolumeStore dbVolumeStore() {
        DBVolumeStoreImpl volumeStoreImpl = new DBVolumeStoreImpl();
        volumeStoreImpl.setSessionFactory(sessionFactory);
        volumeStoreImpl.setSegmentStore(dbSegmentStore());
        return volumeStoreImpl;
    }

    @Bean
    public SegmentStore dbSegmentStore() {
        SegmentStoreImpl segmentStore = new SegmentStoreImpl();
        segmentStore.setSessionFactory(sessionFactory);
        return segmentStore;
    }

    @Bean
    public AccountStore inMemAccountStore() {
        InMemoryAccountStoreImpl inMemoryAccountStore = new InMemoryAccountStoreImpl();
        inMemoryAccountStore.setAccountStore(dbAccountStore());
        return inMemoryAccountStore;
    }

    @Bean
    public AccountStore dbAccountStore() {
        AccountStoreDBImpl accountStore = new AccountStoreDBImpl();
        accountStore.setSessionFactory(sessionFactory);
        return accountStore;
    }

    @Bean
    public RoleStore roleStore() {
        RoleDBStoreImpl roleDBStore = new RoleDBStoreImpl();
        roleDBStore.setSessionFactory(sessionFactory);
        return roleDBStore;
    }

    @Bean
    public APIStore apiStore() {
        APIDBStoreImpl apiStore = new APIDBStoreImpl();
        apiStore.setSessionFactory(sessionFactory);
        return apiStore;
    }

    @Bean
    public ResourceStore resourceStore() {
        ResourceDBStoreImpl resourceDBStore = new ResourceDBStoreImpl();
        resourceDBStore.setSessionFactory(sessionFactory);
        return resourceDBStore;
    }

    @Bean
    public InstanceMaintenanceDBStore instanceMaintenanceStore() {
        InstanceMaintenanceStoreImpl instanceMaintenanceStore = new InstanceMaintenanceStoreImpl();
        instanceMaintenanceStore.setSessionFactory(sessionFactory);
        return instanceMaintenanceStore;
    }

    @Bean
    public VolumeStore inMemoryVolumeStore() {
        return new MemoryVolumeStoreImpl();
    }

    @Bean
    public VolumeStore twoLevelVolumeStore() {
        return new TwoLevelVolumeStoreImpl(inMemoryVolumeStore(), dbVolumeStore());
    }

    @Bean
    public VolumeStatusTransitionStore volumeStatusTransitionStore() {
        return new VolumeStatusTransitionStoreImpl();
    }

    @Bean
    public SegmentUnitTimeoutStore segmentUnitTimeoutStore() {
        return new SegmentUnitTimeoutStoreImpl(this.segmentUnitReportTimeout);
    }

    @Bean
    public InstanceSelectionStrategy instanceSelectionStrategy() {
        return new FrugalInstanceSelectionStrategy();
    }

    @Bean
    public HealthChecker healthChecker() {
        HealthCheckerImpl healthChecker = new HealthCheckerImpl(appContext());
        healthChecker.setCheckingRate(healthCheckerRate);
        healthChecker.setServiceClientClazz(py.thrift.infocenter.service.InformationCenter.Iface.class);
        healthChecker.setHeartBeatWorkerFactory(heartBeatWorkerFactory());
        return healthChecker;
    }

    @Bean
    public HeartBeatWorkerFactory heartBeatWorkerFactory() {
        HeartBeatWorkerFactory factory = new HeartBeatWorkerFactory();
        factory.setRequestTimeout(thriftClientTimeout);
        factory.setAppContext(appContext());
        factory.setLocalDIHEndPoint(localDIHEP());
        factory.setDihClientFactory(dihClientFactory());
        return factory;
    }

    @Bean
    public GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory() {
        GenericThriftClientFactory<DataNodeService.Iface> genericThriftClientFactory = GenericThriftClientFactory.create(DataNodeService.Iface.class, 1);
        genericThriftClientFactory.setMaxNetworkFrameSize(maxNetworkFrameSize);
        return genericThriftClientFactory;
    }

    @Bean
    public DIHClientFactory dihClientFactory() {
        DIHClientFactory dihClientFactory = new DIHClientFactory(1);
        dihClientFactory.getGenericClientFactory().setMaxNetworkFrameSize(maxNetworkFrameSize);
        return dihClientFactory;
    }

    @Bean
    public DriverContainerSelectionStrategy driverContainerSelectionStrategy() {
        BalancedDriverContainerSelectionStrategy balancedDriverContainerSelectionStrategy = new BalancedDriverContainerSelectionStrategy();
        return balancedDriverContainerSelectionStrategy;
    }

    @Bean
    public DriverStoreSweeperFactory driverStoreSweepWorkerFactory() {
        DriverStoreSweeperFactory driverStoreSweepWorkerFactory = new DriverStoreSweeperFactory();
        driverStoreSweepWorkerFactory.setDriverStore(driverStore());
        driverStoreSweepWorkerFactory.setDriverReportTimeout(driverReportTimeout);
        driverStoreSweepWorkerFactory.setDomainStore(domainStore());
        driverStoreSweepWorkerFactory.setAppContext(appContext());
        return driverStoreSweepWorkerFactory;
    }

    @Bean
    public VolumeSweeperFactory volumeSweeperFactory() throws Exception {
        VolumeSweeperFactory volumeSweeperFactory = new VolumeSweeperFactory();
        volumeSweeperFactory.setVolumeStore(twoLevelVolumeStore());
        volumeSweeperFactory.setVolumeStatusTransitionStore(volumeStatusTransitionStore());
        volumeSweeperFactory.setTimeout(thriftClientTimeout);
        volumeSweeperFactory.setInstanceStore(instanceStore());
        volumeSweeperFactory.setDataNodeClientFactory(dataNodeClientFactory());
        volumeSweeperFactory.setAppContext(appContext());
        volumeSweeperFactory.setDeadVolumeToRemoveTime(deadVolumeToRemove);
        volumeSweeperFactory.setVolumeStatusTransitionStore(volumeStatusTransitionStore());
        volumeSweeperFactory.setStoragePoolStore(storagePoolStore());
        volumeSweeperFactory.setCloneRelationshipsStore(cloneRelationshipsDBStore());
        return volumeSweeperFactory;
    }

    @Bean
    public TimeoutSweeperFactory timeoutSweeperFactory() {
        TimeoutSweeperFactory timeoutSweeperFactory = new TimeoutSweeperFactory();
        timeoutSweeperFactory.setVolumeStatusStore(volumeStatusTransitionStore());
        timeoutSweeperFactory.setVolumeStore(twoLevelVolumeStore());
        timeoutSweeperFactory.setSegUnitTimeoutStore(segmentUnitTimeoutStore());
        timeoutSweeperFactory.setAppContext(appContext());
        timeoutSweeperFactory.setNextActionTimeIntervalMs((long) nextActionTimeIntervalMs);
        timeoutSweeperFactory.setDomainStore(domainStore());
        timeoutSweeperFactory.setStoragePoolStore(storagePoolStore());
        timeoutSweeperFactory.setCapacityRecordStore(capacityRecordStore());
        timeoutSweeperFactory.setStorageStore(storageStore());
        timeoutSweeperFactory.setTakeSampleInterValSecond(takeSampleInterValSecond);
        timeoutSweeperFactory.setStoreCapacityRecordCount(storeCapacityRecordCount);
        timeoutSweeperFactory.setRoundTimeInterval(roundTimeInterval);
        return timeoutSweeperFactory;
    }

    @Bean
    public AlarmSweeperFactory alarmSweeperFactory() throws Exception {
        AlarmSweeperFactory alarmSweeperFactory = new AlarmSweeperFactory();
        alarmSweeperFactory.setAlarmStore(alarmStore());
        alarmSweeperFactory.setAppContext(appContext());
        alarmSweeperFactory.setInstanceStore(instanceStore());
        alarmSweeperFactory.setDihClientFactory(dihClientFactory());
        return alarmSweeperFactory;
    }

    @Bean
    public ServerNodeAlertCheckerFactory serverNodeAlertCheckerFactory() throws Exception {
        ServerNodeAlertCheckerFactory serverNodeAlertCheckerFactory = new ServerNodeAlertCheckerFactory();
        serverNodeAlertCheckerFactory.setInstanceStore(instanceStore());
        serverNodeAlertCheckerFactory.setServerNodeReportTimeMap(informationCenter().getServerNodeReportTimeMap());
        serverNodeAlertCheckerFactory.setServerNodeReportOverTimeSecond(serverNodeReportOverTimeSecond);
        serverNodeAlertCheckerFactory.setServerNodeStore(serverNodeStore());
        serverNodeAlertCheckerFactory.setInstanceMaintenanceDBStore(instanceMaintenanceStore());
        return serverNodeAlertCheckerFactory;
    }

    @Bean
    public ZkClientFactory zkClientFactory() {
        return new ZkClientFactory(zookeeperConnectionString, zookeeperSessionTimeout);
    }

    @Bean
    public ZkElectionLeader zkElectionLeader() {
        return new ZkElectionLeader(zkClientFactory(), lockDirectory, appContext());
    }

    @Bean
    public StorageStoreSweeperFactory instanceMetadataStoreSweeperFactory() {
        StorageStoreSweeperFactory factory = new StorageStoreSweeperFactory();
        factory.setStorageStore(storageStore());
        factory.setStoragePoolStore(storagePoolStore());
        factory.setDomainStore(domainStore());
        factory.setVolumeStore(twoLevelVolumeStore());
        factory.setSegmentSize(storageConfiguration.getSegmentSizeByte());
        factory.setTimeToRemove(instanceMetadataTimeToRemove);
        factory.setAppContext(appContext());
        factory.setInstanceMaintenanceDBStore(instanceMaintenanceStore());
        return factory;
    }

    @Bean
    public PeriodicWorkExecutorImpl driverStoreSweeperExecutor() throws UnableToStartException {
        PeriodicWorkExecutorImpl sweeperExecutor = new PeriodicWorkExecutorImpl(driverSweeperExecutionOptionsReader(),
                driverStoreSweepWorkerFactory(), "Driver-Sweeper");
        return sweeperExecutor;
    }

    @Bean
    public PeriodicWorkExecutorImpl volumeSweeperExecutor() throws Exception {
        PeriodicWorkExecutorImpl sweeperExecutor = new PeriodicWorkExecutorImpl(volumeSweeperExecutionOptionsReader(),
                volumeSweeperFactory(), "Volume-Sweeper");
        return sweeperExecutor;
    }

    @Bean
    public PeriodicWorkExecutorImpl timeoutSweeperExecutor() throws Exception {
        PeriodicWorkExecutorImpl timeSweeperExecutor = new PeriodicWorkExecutorImpl(
                timeoutSweeperExecutionOptionsReader(), timeoutSweeperFactory(), "timeout-Sweeper");
        return timeSweeperExecutor;
    }

    @Bean
    public PeriodicWorkExecutorImpl instanceMetadataStoreSweeperExecutor() throws UnableToStartException {
        PeriodicWorkExecutorImpl sweeperExecutor = new PeriodicWorkExecutorImpl(
                InstanceMetadataSweeperExecutionOptionsReader(), instanceMetadataStoreSweeperFactory(),
                "InstanceMetadata-Sweeper");
        return sweeperExecutor;
    }

    @Bean
    public PeriodicWorkExecutorImpl alarmSweeperExecutor() throws Exception {
        PeriodicWorkExecutorImpl sweeperExecutor = new PeriodicWorkExecutorImpl(alarmSweeperExecutionOptionsReader(),
                alarmSweeperFactory(), "Alarm-Sweeper");
        return sweeperExecutor;
    }

    @Bean
    public PeriodicWorkExecutorImpl serverNodeAlertCheckerExecutor() throws Exception {
        PeriodicWorkExecutorImpl serverNodeAlertCheckerExecutor = new PeriodicWorkExecutorImpl(serverNodeAlertCheckerExecutionOptionsReader(),
                serverNodeAlertCheckerFactory(), "ServerNode-Alert-Checker");
        return serverNodeAlertCheckerExecutor;
    }

    @Bean
    public ExecutionOptionsReader driverSweeperExecutionOptionsReader() {
        return new ExecutionOptionsReader(1, 1, driverSweeperRate, null);
    }

    @Bean
    public ExecutionOptionsReader InstanceMetadataSweeperExecutionOptionsReader() {
        return new ExecutionOptionsReader(1, 1, OperationName.StoragePool.getEvnetDataGeneratingPeriod(), null);
    }

    @Bean
    public ExecutionOptionsReader volumeSweeperExecutionOptionsReader() {
        return new ExecutionOptionsReader(1, 1, volumeSweeperRate, null);
    }

    @Bean
    public ExecutionOptionsReader timeoutSweeperExecutionOptionsReader() {
        return new ExecutionOptionsReader(1, 1, timeoutSweeper, null);
    }

    @Bean
    public ExecutionOptionsReader alarmSweeperExecutionOptionsReader() {
        return new ExecutionOptionsReader(1, 1, alarmSweeperRate, null);
    }

    @Bean
    public ExecutionOptionsReader serverNodeAlertCheckerExecutionOptionsReader(){
        return new ExecutionOptionsReader(1, 1, serverNodeAlertCheckerRate, null);
    }

    @Bean
    public InstanceStore instanceStore() throws Exception {
        Object instanceStore = DIHInstanceStore.getSingleton();
        ((DIHInstanceStore) instanceStore).setDihClientFactory(dihClientFactory());
        ((DIHInstanceStore) instanceStore).setDihEndPoint(localDIHEP());
        ((DIHInstanceStore) instanceStore).init();
        return (InstanceStore) instanceStore;
    }

    @Bean
    public SelectionStrategy selectionStrategy() {
        RandomSelectionStrategy selectionStrategy = new RandomSelectionStrategy();
        selectionStrategy.setFactor(factorForSelector);
        selectionStrategy.setRemainder(remainderForSelector);

        return selectionStrategy;
    }

    @Bean
    public EndPoint localDIHEP() {
        return EndPointParser.parseLocalEndPoint(dihEndPoint, appContext().getMainEndPoint().getHostName());
    }

    private void initInfoCenterConstants() {
        InfoCenterConstants.setVolumeToBeCreatedTimeout(this.volumeTobeCreatedTimeout);
        InfoCenterConstants.setVolumeBeCreatingTimeout(this.volumeBeCreatingTimeout);
        InfoCenterConstants.setSegmentUnitReportTimeout(this.segmentUnitReportTimeout);
        InfoCenterConstants.setTimeOfdeadVolumeToRemove(deadVolumeToRemove);
        InfoCenterConstants.setRefreshPeriodTime(refreshPeriodTime);
        InfoCenterConstants.setMaxRebalanceTaskCount(maxRebalanceTaskCount);
        InfoCenterConstants.setFixVolumeTimeoutSec(fixVolumeTimeoutSec);

        RebalanceConfiguration rebalanceConfiguration = RebalanceConfiguration.getInstance();
        rebalanceConfiguration.setPressureThreshold(rebalancePressureThreshold);
        rebalanceConfiguration.setPressureAddend(rebalancePressureAddend);
        rebalanceConfiguration.setRebalanceTaskExpireTimeSeconds(rebalanceTaskExpireTimeSeconds);
        rebalanceConfiguration.setSegmentWrapSize(segmentWrappCount);
    }
}
