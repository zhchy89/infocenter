package py.infocenter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import py.common.PyService;
import py.monitor.jmx.server.JmxAgent;

public class Launcher extends py.app.Launcher {
    private static final Logger logger = LoggerFactory.getLogger(Launcher.class);

    public Launcher(String beansHolder, String serviceRunningPath) {
        super(beansHolder, serviceRunningPath);
    }

    @Override
    public void startAppEngine(ApplicationContext appContext) {
        try {
            InformationCenterAppEngine engine = appContext.getBean(InformationCenterAppEngine.class);
            logger.info("info center get Max network Frame size is {}",engine.getMaxNetworkFrameSize());
            engine.start();
        } catch (Exception e) {
            logger.error("Caught an exception when start infocenter service", e);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            logger.error("Usage: error args");
            System.exit(0);
        }

        Launcher launcher = new Launcher(InformationCenterAppConfig.class.getName() + ".class", args[0]);
        launcher.launch();
    }
}