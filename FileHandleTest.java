package minidatabase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileHandleTest{
    
    public static void main(String[] args) throws FileNotFoundException, IOException{
        File file = new File("testfile");
        file.createNewFile();
        System.out.println(file.length());
        System.out.println(file.getFreeSpace());
        System.out.println(file.getUsableSpace());
        System.out.println(file.getTotalSpace());
        FileOutputStream fos = new FileOutputStream (file);
        fos.write(new byte[64]);
        System.out.println(file.length());
        fos.close();
    }
}