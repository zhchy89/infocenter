package py.infocenter.qos;


import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.icshare.StoragePool;
import py.icshare.qos.*;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.DriverStore;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.*;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

public class ApplyIOLimitationTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(UpdateMigrateSpeedTest.class);

    private InformationCenterImpl icImpl;

    @Mock
    private IOLimitationStore ioLimitationStore;

    @Mock
    private DriverStore driverStore;

    @Mock
    private InfoCenterAppContext appContext;

    @Before
    public void init() throws Exception {
        super.init();
        icImpl = new InformationCenterImpl();
        List<IOLimitationInformation> ioLimitationInformationList = new ArrayList<IOLimitationInformation>();
        when(ioLimitationStore.list()).thenReturn(ioLimitationInformationList);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);
        icImpl.setIoLimitationStore(ioLimitationStore);
        icImpl.setDriverStore(driverStore);
        icImpl.setAppContext(appContext);

        List<DriverMetadata> driverMetadataList = new ArrayList<>();
        DriverMetadata driver1 = new DriverMetadata();
        driver1.setDriverContainerId(0l);
        driver1.setSnapshotId(0);
        driver1.setDriverType(DriverType.ISCSI);
        driver1.setVolumeId(0l);
        driver1.setDriverStatus(DriverStatus.LAUNCHED);
        DriverMetadata driver2 = new DriverMetadata();
        driver2.setDriverContainerId(0l);
        driver2.setSnapshotId(0);
        driver2.setDriverType(DriverType.ISCSI);
        driver2.setVolumeId(1l);
        driver2.setDynamicIOLimitationId(1l);
        driver2.setDriverStatus(DriverStatus.LAUNCHED);
        driverMetadataList.add(driver1);
        driverMetadataList.add(driver2);


        when(driverStore.get(0l, 0l, DriverType.ISCSI, 0)).thenReturn(driver1);
        when(driverStore.get(0l, 1l, DriverType.ISCSI, 0)).thenReturn(driver2);
        when(driverStore.list()).thenReturn(driverMetadataList);

    }

    /**
     * A test for Apply IOLimitation. In the case, new rules id should save to related driver db.
     */
    @Test
    public void testApplyIOLimitation() throws InvalidInputException_Thrift,
            TException {
            IOLimitation_Thrift ioLimitationFromRemote = new IOLimitation_Thrift();
            ioLimitationFromRemote.setLimitationId(100);
            ioLimitationFromRemote.setLimitationName("rule1");
            ioLimitationFromRemote.setLimitType(LimitType_Thrift.Dynamic);
            ioLimitationFromRemote.setEntries(new ArrayList<IOLimitationEntry_Thrift>());
            ApplyIOLimitationsRequest request = new ApplyIOLimitationsRequest();
            request.setRequestId(RequestIdBuilder.get());
            request.setAccountId(100000);
            request.setDriverKeys(buildThriftDriverKeys());
            request.setRuleId(1l);
            request.setCommit(true);

            when(ioLimitationStore.get(eq(1l))).thenReturn(buildIOLimitationInformation(1l, IOLimitationStatus.AVAILABLE));
            icImpl.applyIOLimitations(request);
            Mockito.verify(driverStore, Mockito.times(1)).save(any(DriverMetadata.class));
    }

    /**
     * A test for drivers which get Applied IOLimitation  . In the case, new rules id should save to related driver db.
     */
    @Test
    public void testGetAppliedIOLimitation() throws Exception {
        List<DriverKey_Thrift> driverKeys = buildThriftDriverKeys();
        GetIOLimitationAppliedDriversRequest request = new GetIOLimitationAppliedDriversRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);
        request.setRuleId(1l);
        when(ioLimitationStore.get(eq(1l))).thenReturn(buildIOLimitationInformation(1l, IOLimitationStatus.AVAILABLE));
        GetIOLimitationAppliedDriversResponse response = icImpl.getIOLimitationAppliedDrivers(request);
        assert(response.getDriverList().size() == 1);
    }


    /**
     * A test for drivers which get Applied IOLimitation but IOLimitation not found .
     */
    @Test
    public void testGetAppliedIOLimitationERROR() throws Exception {
        List<DriverKey_Thrift> driverKeys = buildThriftDriverKeys();
        GetIOLimitationAppliedDriversRequest request = new GetIOLimitationAppliedDriversRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);
        request.setRuleId(1l);

        boolean isException = false;
        try {
            GetIOLimitationAppliedDriversResponse response = icImpl.getIOLimitationAppliedDrivers(request);
        } catch(IOLimitationsNotExists e) {
            isException = true;
        }
        assert(isException == true);
    }



    public IOLimitationInformation buildIOLimitationInformation(long ruleId, IOLimitationStatus status) {
        IOLimitationInformation ioLimitationInformation = new IOLimitationInformation();
        ioLimitationInformation.setRuleId(ruleId);
        ioLimitationInformation.setLimitType("Dynamic");
        ioLimitationInformation.setStatus(status.toString());
        ioLimitationInformation.setIoLimitationName("name");

        return ioLimitationInformation;
    }

    List<DriverKey_Thrift> buildThriftDriverKeys() {
        List<DriverKey_Thrift> driverList = new ArrayList<>();
        DriverKey_Thrift driver = new DriverKey_Thrift(0l,0l,0,DriverType_Thrift.ISCSI);
        driverList.add(driver);
        return driverList;
    }
}
