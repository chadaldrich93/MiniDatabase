package minidatabase;

import java.util.Arrays;
import java.util.List;


public class NullIndicator {
    
    final static int BYTE_SIZE = 8;
    
    static int calculateNullIndicatorSize(List<Attribute> recordDescriptor) {
        return (int) Math.ceil(recordDescriptor.size() / BYTE_SIZE);
    }
    
    static byte[] getNullIndicator(List<Attribute> recordDescriptor,
                                    byte[] record) {
        int nullIndicatorSize = calculateNullIndicatorSize(recordDescriptor);
        byte[] nullIndicator = Arrays.copyOfRange(record, 0, nullIndicatorSize);
        return nullIndicator;
    }
    
    static boolean attributeIsNull(byte[] nullIndicator, 
                                    int attributeNumber) {
        int comparator = (int)Math.pow(2, attributeNumber);
        int nullIndicatorAsInt = 
                DataConversion.convertBytesToInt(nullIndicator);
        return ( (comparator & nullIndicatorAsInt) == 0 ); 
    }
}
