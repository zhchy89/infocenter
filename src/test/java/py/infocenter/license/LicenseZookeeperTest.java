package py.infocenter.license;

import static junit.framework.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py.infocenter.test.utils.ZookeeperEnvironmentTool;
import py.license.LicenseJson;
import py.license.LicenseStoreFactory;
import py.license.ZookeeperStore;
import py.test.TestBase;

@Ignore
public class LicenseZookeeperTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(LicenseZookeeperTest.class);
    private ZookeeperEnvironmentTool zkEnvTool;

    @Before
    public void init() throws Exception {
        super.init();
        zkEnvTool = new ZookeeperEnvironmentTool();
        zkEnvTool.build();
        logger.debug("Zookeeper environment build OK");
    }

    @Test
    public void testZookeeperStoreEvent() throws Exception {
        LicenseStoreFactory licenseStoreClientFactory = new LicenseStoreFactory("localhost:21881", 3000);
        ZookeeperStore store = licenseStoreClientFactory.generateZookeeperStore();

        zkEnvTool.destory();
        Thread.sleep(5000);
        zkEnvTool.build();
        Thread.sleep(10000);

        try {
            LicenseJson license = store.getLicense();
            logger.debug("license is {}", license);
        } catch (Exception e) {
            fail();
        }
        assertTrue(true);
    }

    @After
    public void clean() throws Exception {
        zkEnvTool.destory();
    }
}
