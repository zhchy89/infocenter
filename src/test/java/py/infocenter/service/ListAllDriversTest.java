package py.infocenter.service;

import org.apache.thrift.TException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.junit.Assert;
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
import py.icshare.DriverInformation;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.store.DriverStore;
import py.infocenter.store.DriverStoreImpl;
import py.test.TestBase;
import py.thrift.icshare.ListAllDriversRequest;
import py.thrift.icshare.ListAllDriversResponse;
import py.thrift.share.DriverType_Thrift;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**The class use to test get drivers from driverStore by different request parameters.
 * Created by wangjing on 17-12-12.
 */
public class ListAllDriversTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(ListAllSnapshotIdTest.class);
    private InformationCenterImpl icImpl;
    @Mock
    private DriverStoreImpl driverStore;
    @Mock
    private SessionFactory sessionFactory;

    @Mock
    private InfoCenterAppContext appContext;


    @Before
    public void init() throws Exception {
        icImpl = new InformationCenterImpl();
        icImpl.setAppContext(appContext);
        driverStore.setSessionFactory(sessionFactory);
        List<DriverMetadata> driverMetadataList = new ArrayList<>();
        driverMetadataList.add(buildUniqueDriver(11111, 1, "192.168.2.101", "172.16.2.101", DriverType.ISCSI));
        driverMetadataList.add(buildUniqueDriver(11111, 2, "192.168.2.101", "172.16.2.101", DriverType.NBD));
        driverMetadataList.add(buildUniqueDriver(33333, 2, "192.168.2.103", "172.16.2.103", DriverType.ISCSI));
        driverMetadataList.add(buildUniqueDriver(44444, 2, "192.168.2.104", "172.16.2.104", DriverType.NBD));
        driverMetadataList.add(buildUniqueDriver(55555, 3, "192.168.2.105", "172.16.2.105", DriverType.ISCSI));
        driverMetadataList.add(buildUniqueDriver(66666, 3, "192.168.2.105", "172.16.2.105", DriverType.ISCSI));
        icImpl.setDriverStore(driverStore);
        when(driverStore.list()).thenReturn(driverMetadataList);
        super.init();
    }

    /**
     * If request not set any condition ,it will get all drivers from driverstore.
     * @throws TException
     */
    @Test
    public void listAllDriverWith() throws TException {
        ListAllDriversRequest request = new ListAllDriversRequest();
        request.setRequestId(RequestIdBuilder.get());
        ListAllDriversResponse response = icImpl.listAllDrivers(request);
        Assert.assertTrue(response.getDriverMetadatas_thrift().size() == 6);
    }

    /**
     * Set volumeId for request.
     * @throws TException
     */
    @Test
    public void volumeSetTest() throws TException {
        ListAllDriversRequest request = new ListAllDriversRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(11111);
        ListAllDriversResponse response = icImpl.listAllDrivers(request);
        Assert.assertTrue(response.getDriverMetadatas_thrift().size() == 2);

    }

    /**
     *Set volumeId and snapshotId fro request.
     * @throws TException
     */
    @Test
    public void volumeAndSnapshotIdTest() throws TException {
        ListAllDriversRequest request = new ListAllDriversRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(11111);
        request.setSnapshotId(2);
        ListAllDriversResponse response = icImpl.listAllDrivers(request);
        Assert.assertTrue(response.getDriverMetadatas_thrift().size() == 1);

    }

    /**
     * Set snapshotId and driverType for request.
     * @throws TException
     */
    @Test
    public void snapshotIdAndDriverTypeSetTest() throws TException {
        ListAllDriversRequest request = new ListAllDriversRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setSnapshotId(2);
        request.setDriverType(DriverType_Thrift.NBD);
        ListAllDriversResponse response = icImpl.listAllDrivers(request);
        Assert.assertTrue(response.getDriverMetadatas_thrift().size() == 2);
    }

    /**
     * Set snapshotId ,driverHost and driverType for request.
     * @throws TException
     */
    @Test
    public void snapshotIdAndDriverHostAndDriverTypeSet() throws TException {
        ListAllDriversRequest request = new ListAllDriversRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setSnapshotId(3);
        request.setDriverHost("172.16.2.105");
        request.setDriverType(DriverType_Thrift.NBD);
        Assert.assertTrue(icImpl.listAllDrivers(request).getDriverMetadatas_thrift().size() == 0);

        request.setDriverType(DriverType_Thrift.ISCSI);
        Assert.assertTrue(icImpl.listAllDrivers(request).getDriverMetadatas_thrift().size() == 2);
    }


    private DriverMetadata buildUniqueDriver(long volumeId, int snapshotId, String dcHost, String driverHost, DriverType driverType) {
        DriverMetadata driver = new DriverMetadata();
        driver.setDriverStatus(DriverStatus.LAUNCHED);
        driver.setProcessId(Integer.MAX_VALUE);
        driver.setVolumeId(volumeId);
        driver.setSnapshotId(snapshotId);
        driver.setQueryServerIp(dcHost);
        driver.setHostName(driverHost);
        driver.setDriverType(driverType);
        return driver;
    }
}
