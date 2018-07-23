package py.infocenter.license;

import static org.junit.Assert.assertTrue;

import java.sql.Blob;
import java.util.Arrays;

import org.hibernate.SessionFactory;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

import py.license.LicenseCryptogramInformation;
import py.license.LicenseCryptogramInformationTransition;
import py.license.LicenseDBStore;
import py.license.LicenseStorage;
import py.license.Utils;
import py.test.TestBase;
import py.thrift.share.LicenseCryptogramInformation_Thrift;

@Configuration
@ImportResource({ "classpath:spring-config/hibernate.xml" })
class InfocenterLicenseTransitionTest {
    @Autowired
    private SessionFactory sessionFactory;

    @Bean
    public LicenseStorage licenseDBStore() {
        LicenseStorage licenseDBStore = new LicenseDBStore(sessionFactory);
        return licenseDBStore;
    }
    
    @Bean
    public LicenseCryptogramInformationTransition licenseCryptogramInformationTransition() {
        LicenseCryptogramInformationTransition informationTrantition = new LicenseCryptogramInformationTransition(licenseDBStore());
        return informationTrantition;
    }
}

/**
 * 
 * @author dph
 *
 */
public class LicenseCryptogramInformationTransitionTest extends TestBase {
    private LicenseStorage licenseStorage;
    
    private LicenseCryptogramInformationTransition licenseTranstition;

    @Before
    public void init() throws Exception {
        @SuppressWarnings("resource")
        ApplicationContext ctx = new AnnotationConfigApplicationContext(InfocenterLicenseTransitionTest.class);
        licenseStorage = (LicenseStorage) ctx.getBean("licenseDBStore");
        licenseTranstition = (LicenseCryptogramInformationTransition)ctx.getBean("licenseCryptogramInformationTransition");
    }
    
    @Test
    public void testTransitionWithInformationThrift() throws Exception {
        LicenseCryptogramInformation information = buildLicenseCryptogramInformation();
        LicenseCryptogramInformation_Thrift informationThrift = LicenseCryptogramInformationTransition.buildLicenseCryptogramInformationThriftFrom(information);
        LicenseCryptogramInformation informationAfterTransition = LicenseCryptogramInformationTransition.buildLicenseCryptogramInformationFrom(informationThrift, licenseStorage);
        
        byte[] licenseByte = {-112, -94, -70, -88, -64, -41, -49, 100, 94, -25, -46, 96, -122, -97, -73, -49, 5, 11, -53, 76, 120, 118, 48, -99, 15, -75, -125, -2, 25, -59, 56, 4, -46, -116, 115, -103, 123, -115, 62, 77, -76, -5, -93, -58, -57, 35, 54, 81, -84, -17, -18, 17, -128, 8, -48, 89, 27, -98, -100, 4, 86, -93, 101, 91, -88, -55, 109, 74, 80, -79, -107, -47, -28, -81, -27, 67, -79, -46, 84, -96, -12, -80, -46, 27, 121, 121, -8, -35, -109, -97, 6, 125, 109, 91, -92, -72, 127, -108, -13, 118, 61, -102, -17, 120, 48, 103, -114, 29, 57, -14, 76, 82, -26, -34, -21, 80, 127, -62, -114, -120, 111, 51, -114, 48, -124, 55, -55, -16, 56, -54, -35, 13, -124, 113, 47, -79, 12, -39, -48, 73, 115, 97, 91, 52, 87, 6, 76, -28, 98, 98, -29, -11, -38, -7, 84, 32, 82, -120, 70, -40, -55, 1, -68, -20, 88, 94, -6, -43, 3, -44, -100, -21, -21, -107, -57, 6, 60, 3, -40, -9, 78, 1, -106, -63, -29, -24, -19, -19, 43, 103, -92, -117, -76, -32, 122, -3, 41, -73, -122, 8, 104, -89, 82, 92, 67, -28, -114, 110, -26, -16, 40, 58, -17, 45, -90, -26, -9, 18, 26, 79, 44, -36, -10, -62, -72, 20, 48, -53, 88, -69, 56, -28, -71, -54, 49, 87, -15, 30, 13, -69, 32, 54, 71, 49, 54, 59, -26, 105, -10, 120, -107, 88, -55, 65, -65, 41, 10, -40, -11, -5, -88, 121, -27, -56, 109, 50, -113, 64, 59, 13, -66, 92, -125, -7, -72, 63, 39, -21, -120, 81, 23, 101, -20, -16, -77, 88, -76, -115, 91, -21, -35, -70, -122, -52, -17, 84, -87, -96, -2, -2, -31, 34, 85, -82, -81, 63, 8, 35, -38, -79, 125, -97, 66, -91, -108, -79, 50, 15, 65, 100, 91, -124, 60, 32, -39, 51, 74, 112, -7, -39, 103, -76, 38, 121, -74, -87, -74, 118, 66, 48, -90, 87, 12, 69, 36, 12, -107, -96, 120, 54, -4, 90, 7, -86, 14, -121, -70, 49, 8, -23, 98, 38, 44, 13, -40, 30, -60, -71, 77, -10, 52, 68, -14, 59, -89, 100, 110, -111, 122, -119, -39, -30, 123, 0, 3, 13, 72, 23, 102, 5, 54, 37, 110, 66, -6, -84, 121, 34, 91, 111, -108, -33, 74, -43, -1, -16, -43, 26, -77, -44, -58, -83, 72, -127, -67, 122, -29, -99, 47, 41, -87, -110, -61, -125, -65, -79, -45, 82, -27, -2, -105, -62, -56, 67, 87, -31, 94, -51, 12, -5, -13, 51, -32, 115, -69, -65, -84, -94, 89, 41, -75, 111, -78, 123, -75, -62, -30, -124, 82, -84, -29, 15, -90, 122, -97, -9, -83, 83, 18, -48, -122, -26, 103, 8, 4, -103, 106, 5, 47, 21, 111, 78, 41, 9, 68, 2, -15, -29, -27, 59, -44, -53, 119, -114, 40, 6, -26, 62, -14, -1, 74, -105, 44, -14, 61, 75, 37, -90, -72, -97, 42, -63};
        byte[] ageByte = {92, -80, 39, -80, 52, -60, 86, 45, -49, -87, -38, -39, 115, 126, -32, -3, -94, -57, 32, 57, 93, -95, 99, -78, -29, 116, -40, -96, 60, -124, 54, 90, 37, -100, 16, -92, -114, -70, 124, 2, -26, -51, -61, -22, 79, -44, -2, -126, 95, 22, -11, -90, 74, -99, -76, -107, 13, -71, -62, 19, 35, 105, -32, 106, -94, -109, -9, -103, -101, 0, 127, 65, 93, 92, -85, -126, 20, 77, -39, 120, 0, -77, -43, 30, -10, 79, 83, -101, 90, -80, 101, -32, -79, -2, -51, -1, -40, 19, 126, 89, 87, -77, 107, -2, 8, -44, -2, -116, 31, 92, -109, -101, -32, 64, -52, 30, 23, -30, 78, 55, -10, 57, 72, -82, 14, 122, -94, -39};
        byte[] signatureByte = {52, -75, -63, 114, -86, 123, 49, -90, 78, 119, -37, -79, 51, 23, 43, 61, -82, 63, 98, 11, 75, 43, 68, -67, 57, -110, 40, 80, -8, 53, -124, -123, -17, -69, 90, 46, -125, 49, 99, 32, 85, -122, 29, -111, 19, 117, 78, -12, 127, -126, -2, -35, 28, -14, 94, -82, 67, 36, -95, -32, 64, -66, -113, -54, 72, 77, 63, 95, 13, 100, 93, 36, -30, 101, -80, -16, 36, 105, 23, -68, 116, -69, 59, -4, -92, 16, 80, 116, -5, -59, -14, -4, -100, 93, 126, -90, 73, -24, -16, -70, -62, 85, 67, 54, 8, 15, 111, -60, -23, -79, 11, -119, -113, 37, 21, -17, 33, 47, 108, 55, -49, -60, 115, 93, -95, 12, 73, 42};
        
        assertTrue(Arrays.equals(Utils.readFrom(informationAfterTransition.getSignatureBlob()), signatureByte));
        assertTrue(Arrays.equals(Utils.readFrom(informationAfterTransition.getAgeBlob()), ageByte));
        assertTrue(Arrays.equals(Utils.readFrom(informationAfterTransition.getLicenseBlob()), licenseByte));
        
    }
    
    public LicenseCryptogramInformation buildLicenseCryptogramInformation() throws Exception {
        byte[] licenseByte = {-112, -94, -70, -88, -64, -41, -49, 100, 94, -25, -46, 96, -122, -97, -73, -49, 5, 11, -53, 76, 120, 118, 48, -99, 15, -75, -125, -2, 25, -59, 56, 4, -46, -116, 115, -103, 123, -115, 62, 77, -76, -5, -93, -58, -57, 35, 54, 81, -84, -17, -18, 17, -128, 8, -48, 89, 27, -98, -100, 4, 86, -93, 101, 91, -88, -55, 109, 74, 80, -79, -107, -47, -28, -81, -27, 67, -79, -46, 84, -96, -12, -80, -46, 27, 121, 121, -8, -35, -109, -97, 6, 125, 109, 91, -92, -72, 127, -108, -13, 118, 61, -102, -17, 120, 48, 103, -114, 29, 57, -14, 76, 82, -26, -34, -21, 80, 127, -62, -114, -120, 111, 51, -114, 48, -124, 55, -55, -16, 56, -54, -35, 13, -124, 113, 47, -79, 12, -39, -48, 73, 115, 97, 91, 52, 87, 6, 76, -28, 98, 98, -29, -11, -38, -7, 84, 32, 82, -120, 70, -40, -55, 1, -68, -20, 88, 94, -6, -43, 3, -44, -100, -21, -21, -107, -57, 6, 60, 3, -40, -9, 78, 1, -106, -63, -29, -24, -19, -19, 43, 103, -92, -117, -76, -32, 122, -3, 41, -73, -122, 8, 104, -89, 82, 92, 67, -28, -114, 110, -26, -16, 40, 58, -17, 45, -90, -26, -9, 18, 26, 79, 44, -36, -10, -62, -72, 20, 48, -53, 88, -69, 56, -28, -71, -54, 49, 87, -15, 30, 13, -69, 32, 54, 71, 49, 54, 59, -26, 105, -10, 120, -107, 88, -55, 65, -65, 41, 10, -40, -11, -5, -88, 121, -27, -56, 109, 50, -113, 64, 59, 13, -66, 92, -125, -7, -72, 63, 39, -21, -120, 81, 23, 101, -20, -16, -77, 88, -76, -115, 91, -21, -35, -70, -122, -52, -17, 84, -87, -96, -2, -2, -31, 34, 85, -82, -81, 63, 8, 35, -38, -79, 125, -97, 66, -91, -108, -79, 50, 15, 65, 100, 91, -124, 60, 32, -39, 51, 74, 112, -7, -39, 103, -76, 38, 121, -74, -87, -74, 118, 66, 48, -90, 87, 12, 69, 36, 12, -107, -96, 120, 54, -4, 90, 7, -86, 14, -121, -70, 49, 8, -23, 98, 38, 44, 13, -40, 30, -60, -71, 77, -10, 52, 68, -14, 59, -89, 100, 110, -111, 122, -119, -39, -30, 123, 0, 3, 13, 72, 23, 102, 5, 54, 37, 110, 66, -6, -84, 121, 34, 91, 111, -108, -33, 74, -43, -1, -16, -43, 26, -77, -44, -58, -83, 72, -127, -67, 122, -29, -99, 47, 41, -87, -110, -61, -125, -65, -79, -45, 82, -27, -2, -105, -62, -56, 67, 87, -31, 94, -51, 12, -5, -13, 51, -32, 115, -69, -65, -84, -94, 89, 41, -75, 111, -78, 123, -75, -62, -30, -124, 82, -84, -29, 15, -90, 122, -97, -9, -83, 83, 18, -48, -122, -26, 103, 8, 4, -103, 106, 5, 47, 21, 111, 78, 41, 9, 68, 2, -15, -29, -27, 59, -44, -53, 119, -114, 40, 6, -26, 62, -14, -1, 74, -105, 44, -14, 61, 75, 37, -90, -72, -97, 42, -63};
        byte[] ageByte = {92, -80, 39, -80, 52, -60, 86, 45, -49, -87, -38, -39, 115, 126, -32, -3, -94, -57, 32, 57, 93, -95, 99, -78, -29, 116, -40, -96, 60, -124, 54, 90, 37, -100, 16, -92, -114, -70, 124, 2, -26, -51, -61, -22, 79, -44, -2, -126, 95, 22, -11, -90, 74, -99, -76, -107, 13, -71, -62, 19, 35, 105, -32, 106, -94, -109, -9, -103, -101, 0, 127, 65, 93, 92, -85, -126, 20, 77, -39, 120, 0, -77, -43, 30, -10, 79, 83, -101, 90, -80, 101, -32, -79, -2, -51, -1, -40, 19, 126, 89, 87, -77, 107, -2, 8, -44, -2, -116, 31, 92, -109, -101, -32, 64, -52, 30, 23, -30, 78, 55, -10, 57, 72, -82, 14, 122, -94, -39};
        byte[] signatureByte = {52, -75, -63, 114, -86, 123, 49, -90, 78, 119, -37, -79, 51, 23, 43, 61, -82, 63, 98, 11, 75, 43, 68, -67, 57, -110, 40, 80, -8, 53, -124, -123, -17, -69, 90, 46, -125, 49, 99, 32, 85, -122, 29, -111, 19, 117, 78, -12, 127, -126, -2, -35, 28, -14, 94, -82, 67, 36, -95, -32, 64, -66, -113, -54, 72, 77, 63, 95, 13, 100, 93, 36, -30, 101, -80, -16, 36, 105, 23, -68, 116, -69, 59, -4, -92, 16, 80, 116, -5, -59, -14, -4, -100, 93, 126, -90, 73, -24, -16, -70, -62, 85, 67, 54, 8, 15, 111, -60, -23, -79, 11, -119, -113, 37, 21, -17, 33, 47, 108, 55, -49, -60, 115, 93, -95, 12, 73, 42};
     
        Blob license = licenseStorage.createBlob(licenseByte);
        Blob age = licenseStorage.createBlob(ageByte);
        Blob signature = licenseStorage.createBlob(signatureByte);
        
        assertTrue(Arrays.equals(Utils.readFrom(age), ageByte));
        LicenseCryptogramInformation information = new LicenseCryptogramInformation();
        
        information.setLicenseBlob(license);
        information.setAgeBlob(age);
        information.setSignatureBlob(signature);
        
        return information;
    }
}
