package py.infocenter.service;

import static org.junit.Assert.assertNull;

import java.lang.reflect.Method;

import org.junit.Test;

import py.app.context.AppContextImpl;
import py.app.thrift.ThriftAppEngine;
import py.common.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.infocenter.service.InformationCenterImpl;
import py.instance.PortType;
import py.test.TestBase;

public class HealthCheckerTesting extends TestBase {
    public void init() throws Exception  {
        super.init();
    }
    
    @Test
    public void checkService() {
        Exception exception = null;
        ThriftAppEngine engine = null;
        try {
            AppContextImpl appContext = new AppContextImpl("InfoCenter");
            appContext.putEndPoint(PortType.CONTROL, new EndPoint(null, 45447));
            InformationCenterImpl serviceImpl = new InformationCenterImpl();
            engine = new ThriftAppEngine(serviceImpl);
            engine.setContext(appContext);
            engine.setHealthChecker(null);
            engine.start();

            // get heart beat factory.we'll use this factory to create a heart beat worker,which is a periodic thread
            // object.
            GenericThriftClientFactory<?> genericThriftClientFactory = GenericThriftClientFactory
                            .create(py.thrift.infocenter.service.InformationCenter.Iface.class);

            for (EndPoint endPoint : appContext.getEndPoints().values()) {
                Object serviceClient = genericThriftClientFactory.generateSyncClient(endPoint);
                Method method = serviceClient.getClass().getMethod("ping");
                Object obj = method.invoke(serviceClient);
            }
  
        } catch (Exception e) {
            logger.warn("caught an exception", e);
            exception = e;
        } finally {
            if (engine != null ) {
                engine.stop();
            }
        }

        assertNull(exception);
    }
}
