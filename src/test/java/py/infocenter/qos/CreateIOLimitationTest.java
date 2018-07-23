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
import py.icshare.qos.IOLimitationStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.DriverStore;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;


/**
 * A class includes some tests for creating iscsi access rules.
 */
public class CreateIOLimitationTest extends TestBase {
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
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        icImpl.setIoLimitationStore(ioLimitationStore);
        icImpl.setDriverStore(driverStore);
        icImpl.setAppContext(appContext);
    }

    /**
     * A test for create IOLimitation. In the case, new rules should save to db.
     */
    @Test
    public void testCreateIOLimitationTest() throws InvalidInputException_Thrift,
            TException {
        IOLimitation_Thrift ioLimitationFromRemote = new IOLimitation_Thrift();
        ioLimitationFromRemote.setLimitationId(100);
        ioLimitationFromRemote.setLimitationName("rule1");
        ioLimitationFromRemote.setLimitType(LimitType_Thrift.Dynamic);
        ioLimitationFromRemote.setEntries(new ArrayList<IOLimitationEntry_Thrift>());
        CreateIOLimitationsRequest request = new CreateIOLimitationsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);
        request.setIoLimitation(ioLimitationFromRemote);

        List<IOLimitationInformation> ioLimitationInformationList = new ArrayList<IOLimitationInformation>();
        when(ioLimitationStore.list()).thenReturn(ioLimitationInformationList);

        icImpl.createIOLimitations(request);
        Mockito.verify(ioLimitationStore, Mockito.times(1)).save(any(IOLimitationInformation.class));
    }

    /**
     * A test for create IOLimitation already create. In the case, new rules should save to db.
     */
    @Test
    public void testCreateAlreadyCreatedIOLimitationTest() throws InvalidInputException_Thrift,
            TException {
        IOLimitation_Thrift ioLimitationFromRemote = new IOLimitation_Thrift();
        ioLimitationFromRemote.setLimitationId(100);
        ioLimitationFromRemote.setLimitationName("rule1");
        ioLimitationFromRemote.setLimitType(LimitType_Thrift.Dynamic);
        ioLimitationFromRemote.setStatus(IOLimitationStatus_Thrift.AVAILABLE);
        ioLimitationFromRemote.setEntries(new ArrayList<>());
        CreateIOLimitationsRequest request = new CreateIOLimitationsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);
        request.setIoLimitation(ioLimitationFromRemote);

        // already created
        List<IOLimitationInformation> ioLimitationInformationList = new ArrayList<IOLimitationInformation>();
        IOLimitationInformation ioLimitationInformation = new IOLimitationInformation();
        ioLimitationInformation.setIoLimitationName("rule1");
        ioLimitationInformation.setRuleId(100);
        ioLimitationInformation.setLimitType(LimitType_Thrift.Dynamic.name());
        //ioLimitationInformation.setLimitStatus(IOLimitationStatus.AVAILABLE.name());
        ioLimitationInformationList.add(ioLimitationInformation);

        when(ioLimitationStore.list()).thenReturn(ioLimitationInformationList);

        icImpl.createIOLimitations(request);

    }



    /**
     * A test for create IOLimitation. In the case, new rules timezone overlap.
     */
    @Test
    public void testCreateIOLimitationWithErrorEntry() throws InvalidInputException_Thrift, TException {
        IOLimitation_Thrift ioLimitationFromRemote = new IOLimitation_Thrift();
        ioLimitationFromRemote.setLimitationId(100);
        ioLimitationFromRemote.setLimitationName("rule1");
        ioLimitationFromRemote.setLimitType(LimitType_Thrift.Dynamic);
        List<IOLimitationEntry_Thrift> entryList = new ArrayList<>();
        IOLimitationEntry_Thrift entry = new IOLimitationEntry_Thrift();
        entry.setStartTime("11:11:11");
        entry.setEndTime("11:22:22");

        IOLimitationEntry_Thrift entry2 = new IOLimitationEntry_Thrift();
        entry2.setStartTime("11:11:12");
        entry2.setEndTime("11:22:23");

        entryList.add(entry);
        entryList.add(entry2);

        ioLimitationFromRemote.setEntries(entryList);


        CreateIOLimitationsRequest request = new CreateIOLimitationsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);
        request.setIoLimitation(ioLimitationFromRemote);

        boolean isException = false;
        try {
            icImpl.createIOLimitations(request);
        } catch(IOLimitationTimeInterLeaving_Thrift e) {
            isException = true;
        }

        assertTrue(isException);
    }



    /**
     * A test for list IOLimitation. In the case, new rules should save to db.
     */
    @Test
    public void testListIOLimitationTest() throws InvalidInputException_Thrift, TException {
        ListIOLimitationsRequest request = new ListIOLimitationsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);

        icImpl.listIOLimitations(request);
        Mockito.verify(ioLimitationStore, Mockito.times(1)).list();
    }
}

