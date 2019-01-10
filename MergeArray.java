package minidatabase;

public class MergeArray {
    public static byte[] concatenate(byte[] first, byte[] second) 
    { 
        int index = 0;
        byte[] combination = new byte[first.length + second.length];
        for (byte b : first) {
            combination[index] = b;
            index++;
        }
        for (byte b : second) {
            combination[index] = b;
            index++;
        }
        return combination;
    } 
    
    public static byte[] concatenateAll(byte[]... pieces) {
        byte[] concatenation = null;
        for (int index = 0; index < pieces.length; index++) 
            concatenation = concatenate(concatenation, pieces[index]);
        return concatenation;
    }
    
}
