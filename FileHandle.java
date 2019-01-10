package minidatabase;

import java.io.File;
import java.io.IOException;

class FileHandle{
    
    private File file;
    
    final boolean FAILURE = false;
    final boolean SUCCESS = true;
    
    private int readPageCounter;
    private int writePageCounter;
    private int appendPageCounter;
    
    void setFile(String pathName) {
        if (handleHasFile()) {
            System.out.println("File handler already has file " + file + ".");
            return;
        }
        file = new File(pathName);
    }
    
    File getFile() {
        return file;
    }
    
    boolean createFile(String fileName){
        File newFile = new File( System.getProperty("user.dir") + '\\' + 
                                 fileName);
        try{
            newFile.createNewFile();
            return SUCCESS;
        }
        catch(IOException e) {
            System.out.println("Unable to create file");
            return FAILURE;
        }
    }
    
    boolean deleteFile() {
        if (file.exists()) { 
            file.delete();
            return SUCCESS;
        }
        else {
            System.out.println("File was not found: " + file);
            return FAILURE;
        }
    }
    
    void incrementWriteCounter() {
        writePageCounter++;
    }
    
    void incrementReadCounter() {
        readPageCounter++;
    }
    
    void incrementAppendCounter() {
        appendPageCounter++;
    }
    
    int getWriteCounter() {
        return writePageCounter;
    }
    
    int getReadCounter() {
        return readPageCounter;
    }
    
    int getAppendCounter() {
        return appendPageCounter;
    }
    
    int getNumberOfPages() {
        return appendPageCounter;
    }
    
    //private helper
    private boolean handleHasFile() {
        return (file != null);
    }
    
}