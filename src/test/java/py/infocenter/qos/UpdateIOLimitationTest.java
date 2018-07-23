package py.infocenter.qos;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.icshare.qos.IOLimitationInformation;
import py.icshare.qos.IOLimitationStatus;
import py.icshare.qos.IOLimitationStore;
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

public class UpdateIOLimitationTest extends TestBase{

    private static final Logger logger = LoggerFactory.getLogger(CreateIOLimitationTest.class);

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
    }

    /**
     * A test for update IOLimitation. In the case, new rules should save to db.
     *
     * @throws Exception
     */
    @Test
    public void testUpdateIOLimitationTest() throws InvalidInputException_Thrift,
            TException {
        IOLimitation_Thrift ioLimitationFromRemote = new IOLimitation_Thrift();
        ioLimitationFromRemote.setLimitationId(100);
        ioLimitationFromRemote.setLimitationName("rule1");
        ioLimitationFromRemote.setLimitType(LimitType_Thrift.Dynamic);
        ioLimitationFromRemote.setEntries(new ArrayList<IOLimitationEntry_Thrift>());
        UpdateIOLimitationRulesRequest request = new UpdateIOLimitationRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);
        request.setIoLimitation(ioLimitationFromRemote);

        when(ioLimitationStore.get(eq(100l))).thenReturn(buildIOLimitationInformation(100l, IOLimitationStatus.AVAILABLE));
        icImpl.updateIOLimitations(request);
        Mockito.verify(ioLimitationStore, Mockito.times(1)).update(any(IOLimitationInformation.class));
    }


    /**
     * A test for list IOLimitation. In the case, new rules should save to db.
     *
     * @throws InvalidInputException_Thrift
     * @throws TException
     */
    @Test
    public void testListIOLimitationTest() throws InvalidInputException_Thrift, TException {
        ListIOLimitationsRequest request = new ListIOLimitationsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);

        icImpl.listIOLimitations(request);
        Mockito.verify(ioLimitationStore, Mockito.times(1)).list();
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
