package py.infocenter.license;

import java.io.File;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import junit.framework.Assert;
import py.test.TestBase;

public class LicenseTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(LicenseTest.class);

    public enum LicenseType {
        Sample, Basic, Standard, Advanced;
    }

    /*
     * this is a json class for parsing the license file.
     */
    private static class LicenseJson {
        private LicenseType type;
        private String startTime;
        private String endTime;
        private int outOfDataBuffer;
        private List<LicenseTokenJson> resourceTokens;
        private List<LicenseTokenJson> functionTokens;
        private String cryptogram;

        public LicenseJson() {

        }

        public boolean equals(LicenseJson license) {
            if (this == license)
                return true;
            if (license == null)
                return false;
            if (getClass() != license.getClass())
                return false;

            // LicenseType
            if (type == null) {
                if (license.getType() != null) {
                    return false;
                }
            } else {
                if (!type.equals(license.getType())) {
                    return false;
                }
            }

            // sHostName
            if (startTime == null) {
                if (license.getStartTime() != null) {
                    return false;
                }
            } else {
                if (!startTime.equals(license.getStartTime())) {
                    return false;
                }
            }

            // endTime
            if (endTime == null) {
                if (license.getEndTime() != null) {
                    return false;
                }
            } else {
                if (!endTime.equals(license.getEndTime())) {
                    return false;
                }
            }

            // outOfDataBuffer
            if (outOfDataBuffer != license.getOutOfDataBuffer()) {
                return false;
            }

            // resource tokens
            // if (!resourceTokens.equals(license.getResourceTokens())){
            // return false;
            // }
            if (license.getResourceTokens() == this.resourceTokens)
                return true;
            if (!(license.getResourceTokens() instanceof List))
                return false;

            ListIterator<LicenseTokenJson> e1 = resourceTokens.listIterator(0);
            ListIterator<LicenseTokenJson> e2 = license.getResourceTokens().listIterator(0);
            while (e1.hasNext() && e2.hasNext()) {
                LicenseTokenJson o1 = e1.next();
                LicenseTokenJson o2 = e2.next();
                if (!(o1 == null ? o2 == null : o1.equals(o2)))
                    return false;
            }
            // return !(e1.hasNext() || e2.hasNext());

            // function tokens
            if (!functionTokens.equals(license.getFunctionTokens())) {
                return false;
            }

            // cryptogram
            if (cryptogram == null) {
                if (license.getCryptogram() != null) {
                    return false;
                }
            } else {
                if (!cryptogram.equals(license.getCryptogram())) {
                    return false;
                }
            }
            return true;
        }

        public LicenseType getType() {
            return type;
        }

        public void setType(LicenseType type) {
            this.type = type;
        }

        public String getStartTime() {
            return startTime;
        }

        public void setStartTime(String startTime) {
            this.startTime = startTime;
        }

        public String getEndTime() {
            return endTime;
        }

        public void setEndTime(String endTime) {
            this.endTime = endTime;
        }

        public int getOutOfDataBuffer() {
            return outOfDataBuffer;
        }

        public void setOutOfDataBuffer(int outOfDataBuffer) {
            this.outOfDataBuffer = outOfDataBuffer;
        }

        public List<LicenseTokenJson> getResourceTokens() {
            return resourceTokens;
        }

        public void setResourceTokens(List<LicenseTokenJson> tokens) {
            this.resourceTokens = tokens;
        }

        public List<LicenseTokenJson> getFunctionTokens() {
            return functionTokens;
        }

        public void setFunctionTokens(List<LicenseTokenJson> functionTokens) {
            this.functionTokens = functionTokens;
        }

        public String getCryptogram() {
            return cryptogram;
        }

        public void setCryptogram(String cryptogram) {
            this.cryptogram = cryptogram;
        }

        @Override
        public String toString() {
            return "LicenseJson [type=" + type + ", startTime=" + startTime + ", endTime=" + endTime
                    + ", outOfDataBuffer=" + outOfDataBuffer + ", resourceTokens=" + resourceTokens
                    + ", functionTokens=" + functionTokens + ", cryptogram=" + cryptogram + "]";
        }
    }

    public static class LicenseTokenJson {
        private String key;
        private String value;

        public LicenseTokenJson() {

        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            LicenseTokenJson other = (LicenseTokenJson) obj;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            if (value == null) {
                if (other.value != null)
                    return false;
            } else if (!value.equals(other.value))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "LicenseTokenJson [key=" + key + ", value=" + value + "]";
        }

    }

    @Test
    public void licenseParseTest() {
        LicenseJson license = new LicenseJson();
        LicenseTokenJson resourceToken = new LicenseTokenJson();
        LicenseTokenJson functionToken = new LicenseTokenJson();

        ArrayList<LicenseTokenJson> resourceTokens = new ArrayList<LicenseTokenJson>();
        ArrayList<LicenseTokenJson> functionTokens = new ArrayList<LicenseTokenJson>();

        // resource tokens
        resourceToken.setKey("CAPACITY");
        resourceToken.setValue("50T");
        resourceTokens.add(resourceToken);

        // function tokens
        functionToken.setKey("ALL");
        functionToken.setValue("ON");
        functionTokens.add(functionToken);

        // generic license
        license.setCryptogram("asdgqwegfsd21312wszfaszsdf");
        license.setEndTime("2016-1-9");
        license.setOutOfDataBuffer(30);
        license.setStartTime("2015-1-9");
        license.setResourceTokens(resourceTokens);
        license.setFunctionTokens(functionTokens);
        license.setType(LicenseType.Sample);

        // write test
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            objectMapper.writeValue(new File("/tmp/license"), license);
        } catch (JsonGenerationException e1) {
            logger.error("exceptions during JSON writing, such as trying to output  content in wrong context", e1);
            return;
        } catch (IOException e1) {
            logger.error("I/O exceptions during JSON writing", e1);
            return;
        }

        // read test
        LicenseJson newLicense = new LicenseJson();
        try {
            newLicense = objectMapper.readValue(new File("/tmp/license"), LicenseJson.class);
        } catch (IOException e1) {
            logger.error("I/O exceptions during JSON reading", e1);
            return;
        }
        // change value and then checkout whether it can be serialized to disk
        logger.debug("\nNew license: {}\nOld license: {}", newLicense, license);
        Assert.assertEquals(true, newLicense.equals(license));
        newLicense.setType(LicenseType.Advanced);
        try {
            objectMapper.writeValue(new File("/tmp/license"), newLicense);
        } catch (JsonProcessingException e) {
            logger.error("exceptions during JSON writing");
        } catch (IOException e) {
            logger.error("I/O exceptions during JSON writing", e);
        }
        try {
            license = objectMapper.readValue(new File("/tmp/license"), LicenseJson.class);
        } catch (IOException e1) {
            logger.error("I/O exceptions during JSON writing", e1);
            return;
        }
        Assert.assertEquals(true, license.getType() == LicenseType.Advanced);
        Assert.assertEquals(true, license.equals(newLicense));
    }


    @Test
    public void test() {
        try {
            LocalTime t1 = LocalTime.now();
            System.out.println("t1 " + t1);

            String s = t1.toString();
            System.out.println("s  " + s);

            LocalTime t2 = LocalTime.parse(s);
            System.out.println("t2 " + s);
        } catch (Exception e) {
        }
    }

}
