package minidatabase;

import java.math.BigDecimal;
import java.math.BigInteger;

public class DataConversion {

    static byte[] convertIntToBytes(int data) {
        BigInteger bigIntData = BigInteger.valueOf(data);
        return bigIntData.toByteArray(); 
    }
    
    static int convertBytesToInt(byte[] data) {
        return new BigInteger(data).intValue();    
    }
    
    static double convertBytesToDouble(byte[] data) {
        int intData = convertBytesToInt(data);
        return new BigDecimal(intData).doubleValue();    
    }
    
}
