import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

class PagedFileManager{
    
    static final int NO_PAGE = -1;
    static final int PAGE_SIZE = 4096;
    
    
    private int readPageCounter;
    private int writePageCounter;
    private int appendPageCounter;
    
    private File file;
    private FileInputStream input;
    private FileOutputStream output;
    
    private static PagedFileManager pagedFileManager = null;
    
    static PagedFileManager getInstance() {
        if (pagedFileManager == null)
            pagedFileManager = new PagedFileManager();
        return pagedFileManager;
    }
    
    void createFile(final String fileName) throws IOException{
        File newFile = new File( System.getProperty("user.dir") + '\\' + fileName);
        try{
            newFile.createNewFile();
        }
        catch(IOException e) {
            System.out.println("Unable to create file");
            System.exit(1);
        }
    }
    
    void destroyFile(File target) {
        target.delete();        
    }
    
    void setFile(String pathName) {
        if (fileExists()) {
            System.out.println("File handler already has file " + file + ".");
            return;
        }
        file = new File(pathName);
    }
    
    //a page represents 4096 contiguous bytes in a Java file
    void readPage(int pageNumber, Object data) {
        String errorMessage = getErrorMessage("read", pageNumber);
        if (errorMessage != "") {
            printErrorMessage(errorMessage, pageNumber);
            return;
        }
    }
    
    void writePage(int pageNumber, Object data) {
        String errorMessage = getErrorMessage("write", pageNumber);
        if (errorMessage != "") {
            printErrorMessage(errorMessage, pageNumber);
            return;
        }
    }
    
    void appendPage(Object data) {
        String errorMessage = getErrorMessage("append", NO_PAGE);
        if (errorMessage != "") {
            printErrorMessage(errorMessage, NO_PAGE);
            return;
        }
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
    
    private boolean fileExists() {
        return file.exists();
    }
    
    private boolean pageExists(int pageNumber) {
        if (!pageNumberIsNonNegative(pageNumber))
            return false;
        return ( (file.length() / PAGE_SIZE) > pageNumber);
    }
    
    boolean pageNumberIsNonNegative(int pageNumber) {
        return pageNumber >= 0;
    }
    
    private void openForWriting() {
        
    }
    
    private void closeForWriting() {
        
    }
    
    private void openForReading() {
        
    }
    
    private void closeForReading() {
        
    }
    
    private boolean fileIsOpen(boolean isWrite) {
        if (isWrite)
            return output != null;
        return input != null;
    }
    
    //appends dont pass a page number, they just add a new page
    //so checking for page existence is not relevant for appends
    private String getErrorMessage(String operation, int pageNumber) {
        if ( !fileExists() )
            return "no file";
        if (!fileIsOpen(operation == "write"))
            return "closed file";
        if ( (operation == "append") && !pageExists(pageNumber))
            return "page dne";
        return "";
    }
    
    private void printErrorMessage(String errorMessage, int pageNumber) {
        switch (errorMessage) {
            case "no file":
                System.out.println("File " + file + " does not exist.");
            case "closed file":
                System.out.println("File " + file + " is closed.");
            case "page dne":
                System.out.println("Page " + pageNumber + "does not exist.");
        }
    }
}