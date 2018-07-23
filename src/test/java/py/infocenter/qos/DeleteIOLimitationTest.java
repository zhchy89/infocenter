package py.infocenter.qos;

import org.hibernate.SessionFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
import py.icshare.qos.IOLimitationInformation;
import py.icshare.qos.IOLimitationStatus;
import py.icshare.qos.IOLimitationStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.DriverStore;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.DeleteIOLimitationsRequest;
import py.thrift.share.DeleteIOLimitationsResponse;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

public class DeleteIOLimitationTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(DeleteIOLimitationTest.class);

    private InformationCenterImpl icImpl;

    private SessionFactory sessionFactory;

    @Mock
    private InfoCenterAppContext appContext;

    @Mock
    private DriverStore driverStore;

    @Mock
    IOLimitationStore ioLimitationStore;

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
    }

    /**
     * A test for deleting rules with false commit field. In the case, no rules could be delete.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteIOLimitations() throws Exception {
        DeleteIOLimitationsRequest request = new DeleteIOLimitationsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(1111);
        request.addToRuleIds(2l);
        request.addToRuleIds(3l);
        request.setCommit(true);

        List<DriverMetadata> drivers = new ArrayList<DriverMetadata>();
        DriverMetadata driverMetadata = new DriverMetadata();
        driverMetadata.setHostName("100.0.1.112");
        driverMetadata.setPort(9000);
        driverMetadata.setStaticIOLimitationId(2);
        driverMetadata.setStaticIOLimitationId(3);

        DriverMetadata driverMetadata2 = new DriverMetadata();
        driverMetadata2.setHostName("100.0.1.112");
        driverMetadata2.setPort(9001);
        driverMetadata.setStaticIOLimitationId(4);
        driverMetadata.setStaticIOLimitationId(5);

        drivers.add(driverMetadata);

        when(driverStore.list()).thenReturn(drivers);
        when(ioLimitationStore.get(eq(5l))).thenReturn(buildIOLimitationInformation(5l, IOLimitationStatus.DELETING));
        when(ioLimitationStore.get(eq(1l))).thenReturn(buildIOLimitationInformation(1l, IOLimitationStatus.AVAILABLE));
        when(ioLimitationStore.get(eq(2l))).thenReturn(buildIOLimitationInformation(2l, IOLimitationStatus.AVAILABLE));
        when(ioLimitationStore.get(eq(3l))).thenReturn(buildIOLimitationInformation(3l, IOLimitationStatus.AVAILABLE));
        when(ioLimitationStore.get(eq(4l))).thenReturn(buildIOLimitationInformation(4l, IOLimitationStatus.AVAILABLE));

        DeleteIOLimitationsResponse response = icImpl.deleteIOLimitations(request);

        Mockito.verify(ioLimitationStore, Mockito.times(2)).delete(anyLong());
    }


    public IOLimitationInformation buildIOLimitationInformation(long ruleId, IOLimitationStatus status) {
        IOLimitationInformation ioLimitationInformation = new IOLimitationInformation();
        ioLimitationInformation.setRuleId(ruleId);
        ioLimitationInformation.setLimitType("Dynamic");
        ioLimitationInformation.setStatus(status.toString());
        ioLimitationInformation.setIoLimitationName("name");

        return ioLimitationInformation;
    }
}
