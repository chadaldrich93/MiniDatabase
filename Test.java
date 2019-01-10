package minidatabase;

import java.lang.String;
import java.math.BigInteger;

class Test{
    public static void main(String[] args) {
     Integer x = 3;
     passByRef(x);
     System.out.println(x);
    }
    
    
    static void passByRef(Integer a) {
            a = 4;
    }
}