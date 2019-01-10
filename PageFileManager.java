package minidatabase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;

class PageFileManager{
    
    static final boolean APPEND = true;
    static final boolean NOT_APPEND = false;
    
    static final int NO_PAGE = -1;
    static final int INT_SIZE = 4;
    static final int PAGE_SIZE = 4096;
    
    public static void readPage(FileHandle handle, int pageNumber, byte[] data){
        executePageOperation(handle, "read", pageNumber, data);
        handle.incrementReadCounter();
    }
    
    public static void writePage(FileHandle handle, int pageNumber, byte[] data){
        executePageOperation(handle, "write", pageNumber, data);
        handle.incrementWriteCounter();
    }
    
    public static void appendPage(FileHandle handle) {
        executePageOperation(handle, "append", NO_PAGE, new byte[PAGE_SIZE]);
        handle.incrementWriteCounter();
    }
    
    //static helper methods
    
    public static int calculatePageLocation(int pageNumber) {
        return pageNumber * PAGE_SIZE;
    }
    
    //private helper methods
    
    static byte[] readFromPage(FileHandle handle, int pageNumber, int start, int end)
                        throws FileNotFoundException{
        byte [] page = null;
        readPage(handle, pageNumber, page);
        byte[] dataRead = Arrays.copyOfRange(page, start, end);
        return dataRead; 
        }
    
    static int readIntFromPage(FileHandle handle, int pageNumber, int start)
                        throws FileNotFoundException{
        byte[] numberAsByteArray = readFromPage(handle, pageNumber, start,
                                                start + INT_SIZE);
        return new BigInteger(numberAsByteArray).intValue();
    }
    
    static void writeToPage(FileHandle handle, int pageNumber, 
                            int targetLocation, byte[] entity) 
                            throws FileNotFoundException{
        byte[] copy = null;
        PageFileManager.readPage(handle, pageNumber, copy);
        writeEntityIntoCopy(handle, targetLocation, entity, copy);
        PageFileManager.writePage(handle, pageNumber, copy);
    }
    
    static void writeEntityIntoCopy(FileHandle handle, 
                                            int targetLocation,
                                            byte[] entity, byte[] copy) 
                                            throws FileNotFoundException{
        System.arraycopy(entity, 0, copy, targetLocation, entity.length);
    }
    
    static boolean pageDoesNotExist(FileHandle handle, int pageNumber){
        File file = handle.getFile();
        if (pageNumberIsNegative(pageNumber))
            return true;
        return ( (file.length() / PAGE_SIZE) > pageNumber);
    }
    
    static boolean pageNumberIsNegative(int pageNumber) {
        return pageNumber < 0;
    }
    
    //private helper method
    static void executePageOperation(FileHandle handle,
                                             String operationType, 
                                             int pageNumber, byte[] data) {
        File file = handle.getFile();
        int startOfPage = pageNumber * PAGE_SIZE;
        try (FileInputStream  input  = new FileInputStream(file);
             FileOutputStream output = new FileOutputStream(file)
            ){
            if (operationType != "append" && 
                pageDoesNotExist(handle, pageNumber)){
                System.out.println("Page " + pageNumber + "does not exist" +
                                    "in file " + file + ".");
                return;
            }
            if (operationType == "read") 
                input.read(data, startOfPage, PAGE_SIZE);
            else if (operationType == "write")
                output.write(data, startOfPage, PAGE_SIZE);
            else  //operationType == "append"
                output.write(data, startOfPage, PAGE_SIZE);
        }
        catch (FileNotFoundException e) {
            System.out.println("File " + file + " does not exist.");
        }
        catch (IOException e) {
            System.out.println("Unable to write to file " + file + ".");
        }  
    }
}