package minidatabase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

class PageFileManager{
    
    static final boolean APPEND = true;
    static final boolean NOT_APPEND = false;
    static final int NO_PAGE = -1;
    static final int PAGE_SIZE = 4096;
    
    private int readPageCounter;
    private int writePageCounter;
    private int appendPageCounter;
    
    private static final PageFileManager instance = new PageFileManager();
    
    private PageFileManager() {}
    
    public static PageFileManager getPageFileManager() {
        return instance;
    }
    
    boolean createFile(String fileName){
        File newFile = new File( System.getProperty("user.dir") + '\\' + fileName);
        try{
            newFile.createNewFile();
            return true;
        }
        catch(IOException e) {
            System.out.println("Unable to create file");
            return true;
        }
    }
    
    boolean deleteFile(File file) {
        if (file.exists()) { 
            file.delete();
            return true;
        }
        else {
            System.out.println("Paged file manager does not have a file");
            return false;
        }
    }
    
    //a page represents 4096 contiguous bytes in a Java file
    void readPage(File file, int pageNumber, byte[] data){
        executePageOperation(file, "read", pageNumber, data);
        readPageCounter++;
    }
    
    void writePage(File file, int pageNumber, byte[] data){
        executePageOperation(file, "write", pageNumber, data);
        writePageCounter++;
    }
    
    void appendPage(File file) {
        executePageOperation(file, "append", NO_PAGE, new byte[PAGE_SIZE]);
        appendPageCounter++;
    }
    
    int getNumberOfPages() {
        return appendPageCounter;
    }
    
    void collectCounterValues(Integer readPageCount, Integer writePageCount,
                              Integer appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
    }
    
    //private helper methods
    
    private boolean pageDoesNotExist(File file, int pageNumber) {
        if (pageNumberIsNegative(pageNumber))
            return false;
        return ( (file.length() / PAGE_SIZE) > pageNumber);
    }
    
    private boolean pageNumberIsNegative(int pageNumber) {
        return pageNumber < 0;
    }
    
    private void executePageOperation(File file, String operationType, 
                                      int pageNumber, byte[] data) {
        int startOfPage = pageNumber * PAGE_SIZE;
        try (FileInputStream input = new FileInputStream(file);
             FileOutputStream output = new FileOutputStream(file)
            ){
            if (operationType != "append" && pageDoesNotExist(file, pageNumber
                                                             )){
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