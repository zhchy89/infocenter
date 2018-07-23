package py.infocenter.test.utils;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import py.icshare.authorization.APIDBStoreImpl;
import py.icshare.authorization.APIStore;
import py.icshare.authorization.ResourceDBStoreImpl;
import py.icshare.authorization.ResourceStore;
import py.icshare.qos.*;
import py.infocenter.store.*;

@Configuration
@ImportResource({ "classpath:spring-config/hibernate.xml" })
public class TestBeans {
    @Autowired
    private SessionFactory sessionFactory;
    
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public Logger logger() {
        Properties log4jProperties = new Properties();
        log4jProperties.put("log4j.rootLogger", "DEBUG, stdout, InfoCenter");
        log4jProperties.put("log4j.appender.InfoCenter", "org.apache.log4j.RollingFileAppender");
        log4jProperties.put("log4j.appender.InfoCenter.Threshold", "DEBUG");
        log4jProperties.put("log4j.appender.InfoCenter.File", "logs/infomation-center-test.log");
        log4jProperties.put("log4j.appender.InfoCenter.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.put("log4j.appender.InfoCenter.layout.ConversionPattern", "%-5p[%d][%t]%C(%L):%m%n");
        log4jProperties.put("log4j.appender.InfoCenter.MaxBackupIndex", "10");
        log4jProperties.put("log4j.appender.InfoCenter.MaxFileSize", "400MB");
        log4jProperties.put("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
        log4jProperties.put("log4j.appender.stdout.Threshold", "DEBUG");
        log4jProperties.put("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.put("log4j.appender.stdout.layout.ConversionPattern", "%-5p[%d][%t]%C(%L):%m%n");

        log4jProperties.put("log4j.logger.org.hibernate", "ERROR, stdout, InfoCenter");
        log4jProperties.put("log4j.logger.org.springframework", "ERROR, stdout, InfoCenter");
        log4jProperties.put("log4j.logger.com.opensymphony", "ERROR, stdout, InfoCenter");
        log4jProperties.put("log4j.logger.org.apache", "ERROR, stdout, InfoCenter");
        log4jProperties.put("log4j.logger.com.googlecode", "ERROR, stdout, InfoCenter");
        log4jProperties.put("log4j.logger.com.twitter.common.stats", "ERROR, stdout, InfoCenter");
        log4jProperties.put("log4j.logger.com.mchange", "ERROR, stdout, InfoCenter");
        PropertyConfigurator.configure(log4jProperties);

        return null;
    }

    @Bean
    public DriverStore driverStore() {
        DriverStoreImpl driverStore = new DriverStoreImpl();
        driverStore.setSessionFactory(sessionFactory);
        return driverStore;
    }

    @Bean
    public AccessRuleStore dbAccessRuleStore() {
        AccessRuleStoreImpl accessRuleStore = new AccessRuleStoreImpl();
        accessRuleStore.setSessionFactory(sessionFactory);
        return accessRuleStore;
    }

    @Bean
    public VolumeRuleRelationshipStore dbVolumeRuleRelationshipStore() {
        VolumeRuleRelationshipStoreImpl volumeRuleRelationshipStore = new VolumeRuleRelationshipStoreImpl();
        volumeRuleRelationshipStore.setSessionFactory(sessionFactory);
        return volumeRuleRelationshipStore;
    }

    @Bean
    public IscsiAccessRuleStore dbIscsiAccessRuleStore() {
        IscsiAccessRuleStoreImpl iscsiAccessRuleStore = new IscsiAccessRuleStoreImpl();
        iscsiAccessRuleStore.setSessionFactory(sessionFactory);
        return iscsiAccessRuleStore;
    }

    @Bean
    public IscsiRuleRelationshipStore dbIscsiRuleRelationshipStore() {
        IscsiRuleRelationshipStoreImpl iscsiRuleRelationshipStore = new IscsiRuleRelationshipStoreImpl();
        iscsiRuleRelationshipStore.setSessionFactory(sessionFactory);
        return iscsiRuleRelationshipStore;
    }


    @Bean
    public IOLimitationStore ioLimitationStore() {
        IOLimitationStoreImpl ioLimitationStore = new IOLimitationStoreImpl();
        ioLimitationStore.setSessionFactory(sessionFactory);
        return ioLimitationStore;
    }

    @Bean
    public IOLimitationRelationshipStore ioLimitationRelationshipStore() {
        IOLimitationRelationshipStoreImpl ioLimitationRelationshipStore = new IOLimitationRelationshipStoreImpl();
        ioLimitationRelationshipStore.setSessionFactory(sessionFactory);
        return ioLimitationRelationshipStore;
    }

    @Bean
    public MigrationRuleStore migrationSpeedRuleStore() {
        MigrationRuleStoreImpl migrationSpeedRuleStore = new MigrationRuleStoreImpl();
        migrationSpeedRuleStore.setSessionFactory(sessionFactory);
        return migrationSpeedRuleStore;
    }

    @Bean
    public CloneRelationshipsDBStore cloneRelationshipsDBStore() {
        CloneRelationshipsStoreImpl cloneRelationshipsDBStore = new CloneRelationshipsStoreImpl();
        cloneRelationshipsDBStore.setSessionFactory(sessionFactory);
        return cloneRelationshipsDBStore;
    }

    @Bean
    public APIStore apiStore() {
        APIDBStoreImpl apiStore = new APIDBStoreImpl();
        apiStore.setSessionFactory(sessionFactory);
        return apiStore;
    }

    @Bean
    public ResourceStore resourceStore() {
        ResourceDBStoreImpl resourceStore = new ResourceDBStoreImpl();
        resourceStore.setSessionFactory(sessionFactory);
        return resourceStore;
    }
}
