package py.infocenter.service;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import py.icshare.AccountMetadata;
import py.test.TestBase;
import static org.junit.Assert.*;


public class AccountMetadataTest extends TestBase {
    @Test
    public void testSerializeDeSerialize() throws Exception{
        
        AccountMetadata account = new AccountMetadata();
        Long accountId = 1l;
        String accountType = "Admin";
        String hashedPassword = "1111";
        String accountName = "aksdfa";
        
        account.setAccountId(accountId);
        account.setAccountName(accountName);
        account.setAccountType(accountType);
        account.setHashedPassword(hashedPassword);
        
        
        ObjectMapper mapper = new ObjectMapper();
        String accountJson = mapper.writeValueAsString(account);
        
        logger.debug(accountJson);
        AccountMetadata parsedVolume = mapper.readValue(accountJson, AccountMetadata.class);
        
        assertEquals(accountId, parsedVolume.getAccountId());
        assertEquals(accountType, parsedVolume.getAccountType());
        assertEquals(accountName, parsedVolume.getAccountName());
        assertEquals(hashedPassword, parsedVolume.getHashedPassword());
    }
}
