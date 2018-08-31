package minidatabase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

//UPDATE WITH RECORD DESCRIPTOR
class RecordFileManager{
    
    //first int in recordId is pageNumber
    //second number is slotNumber within page
    
    private static final RecordFileManager instance = 
                                                new RecordFileManager(); 
    
    //file header consists of 2 ints. page number, then slot number
    //each slot in directory also consists of 2 ints, 
    //offset of record within page, and length of record
    
    final boolean NO_UPDATE = false;
    final boolean IS_UPDATE = true;
    final int PAGE_NOT_FOUND = -1;
    final int RECORD_NOT_FOUND = -1;
    final int NO_SLOT = -1;
    final int PAGE_START = 0;
    final int DEAD_RECORD = 0;
    final int INT_SIZE = 4;
    final int RECORD_END_POSITION = 4;
    final int FILE_HEADER_SIZE = 8;
    final int RECORD_START = 8;
    final int SLOT_SIZE = 8;
    final int BYTE_SIZE = 8;
    final int END_OF_PAGE = 4095;
    final int PAGE_SIZE = 4096;
    
    //client methods
    
    //eliminate default constructor, to allow singleton pattern
    private RecordFileManager() {}
    
    public static RecordFileManager getInstance() {
        return instance;
    }
    
    boolean createFile(final String fileName) throws IOException{
        PageFileManager pageFileManager = PageFileManager.getPageFileManager();
        return pageFileManager.createFile(fileName);
    }
    
   boolean deleteFile(File file){
        PageFileManager pageFileManager = PageFileManager.getPageFileManager();
        return pageFileManager.deleteFile(file);
    }
    
    void insertRecord(File file, List<Attribute> recordDescriptor, 
                      RecordID rid, byte[] record, boolean isUpdate) 
                      throws FileNotFoundException, IOException{
        if ( !isUpdate && recordIsInFile(file, rid)) {
            printActiveRecordMessage(rid);
            return;
        }
        int recordPage = findPageWithSpace(file, record);
        int slotNumber = readNumberOfRecords(file, recordPage);
        if (!isUpdate)
            setRecordID(rid, recordPage, slotNumber);
        writeRecord(file, rid.pageNumber, record);
        byte[] pageNumberAsBytes = convertIntToBytes(recordPage);
        byte[] slotNumberAsBytes = convertIntToBytes(slotNumber);
        writeSlotHeader(file, recordPage, pageNumberAsBytes, slotNumberAsBytes,
                        slotNumber);
        updateRecordNumber(file, recordPage);
    }
    
    //fix this
    void readRecord(File file, List<Attribute> recordDescriptor, 
                    RecordID rid, byte[] record) throws FileNotFoundException{
        if (recordIsNotInFile(file, rid)) {
            printMissingRecordMessage(rid);
            return;
        }
        if (recordIsNotAlive(file, rid)) {
            printDeadRecordMessage(rid);
            return;
        }
        int recordPage = findRecordPage(file, rid);
        readRecordFromPage(file, recordPage, rid.slotNumber);
    }
    
    //finish this
    void printRecord(List<Attribute> recordDescriptor, byte[] record) {
        int attributeIndex = 0;
        int index = 0;
        int currentAttribute = 0;
        int nullIndicatorSize = calculateNullIndicatorSize(recordDescriptor);
        byte[] nullIndicator = Arrays.copyOfRange(record, 0, nullIndicatorSize);
        int attributeNumber = recordDescriptor.size();
        index += nullIndicatorSize;
        while (attributeIndex < record.length) {
            if (attributeIsNull(nullIndicator, attributeIndex))
                ;
            else {
                
            }
            attributeIndex++;
        }
    }
    
    void deleteRecord(File file, RecordID rid) 
                      throws FileNotFoundException, IOException{
        int recordLocation = findRecord(file, rid);
        int recordPage = findRecordPage(file, rid);
        if (recordLocation == RECORD_NOT_FOUND) {
            printMissingRecordMessage(rid);
            return;
        }
        if (recordLocation == DEAD_RECORD) {
            printDeadRecordMessage(rid);
            return;
        }
        byte[] newFileOffset = convertIntToBytes(DEAD_RECORD);
        writeFileOffset(file, recordPage, newFileOffset, INT_SIZE);
    }
    
    void updateRecord(File file, List<Attribute> recordDescriptor, 
                      RecordID rid, byte[] newRecord)
                      throws FileNotFoundException, IOException{
        int oldRecordLocation = findRecord(file, rid);
        int oldRecordPage = findRecordPage(file, rid);
        if (oldRecordLocation == RECORD_NOT_FOUND) {
            printMissingRecordMessage(rid);
            return;
        }
        if (oldRecordLocation == DEAD_RECORD) {
            printDeadRecordMessage(rid);
            return;
        }
        int newRecordPage = findRecordPage(file, rid);
        int newRecordSlot = readNumberOfRecords(file, newRecordPage);
        byte[] newRecordPageBytes = convertIntToBytes(-1 * newRecordPage);
        byte[] newRecordSlotBytes = convertIntToBytes( -1 * newRecordSlot);
        writeSlotHeader(file, oldRecordPage, newRecordPageBytes, 
                        newRecordSlotBytes, rid.slotNumber);
        insertRecord(file, recordDescriptor, rid, newRecord, IS_UPDATE);
    }
    
    
    void readAttribute(File file, List<Attribute> recordDescriptor,
                       RecordID rid, String attributeName, byte[] attribute) 
                       throws FileNotFoundException{
        byte[] record = null;
        readRecord(file, recordDescriptor, rid, record);
        byte[] nullIndicator = getNullIndicatorFromRecord(recordDescriptor, 
                                                          record);
        int attributeIndex = recordDescriptor.indexOf(attributeName);
        if (attributeIsNull(nullIndicator, attributeIndex)) {
            printNullAttributeMessage(recordDescriptor, attributeIndex);
            return;
        }
        int attributeOffset = getAttributeOffsetFromRecord(recordDescriptor, 
                                                           attributeIndex);
        Attribute target = recordDescriptor.get(attributeIndex);
        int recordPage = findRecordPage(file, rid);
        attribute = readAttributeFromRecord(file, recordDescriptor, record,
                                            attributeIndex);
    }
    
    /*void setFileIterator(File file, List<Attribute> recordDescriptor, 
              Attribute conditionAttribute, String operator, byte[] value,
              List<String> attributeNames
              ) throws UnsupportedEncodingException{
      
        //checkScanCondition();
        //projectSelectedAttributes();
        //
    }*/
    
    //setters
    
    private void setRecordID(RecordID rid, int pageNumber, int slotNumber){
        rid.pageNumber = pageNumber;
        rid.slotNumber = slotNumber;
    }
    
    //private helper methods
    
    byte[] convertIntToBytes(int data) {
        BigInteger bigIntData = BigInteger.valueOf(data);
        return bigIntData.toByteArray(); 
    }
    
    int convertBytesToInt(byte[] data) {
        return new BigInteger(data).intValue();    
    }
    
    double convertBytesToDouble(byte[] data) {
        int intData = convertBytesToInt(data);
        return new BigDecimal(intData).doubleValue();    
    }
    
    //helpers that read the page
    byte[] readFromPage(File file, int pageNumber, String entityType, 
                                int slotNumber) throws FileNotFoundException{
        byte[] page = null;
        PageFileManager pageFileManager = PageFileManager.getPageFileManager();
        pageFileManager.readPage(file, pageNumber, page);
        int start, end;
        int recordLength = 0;
        int pageLocation = calculatePageLocation(pageNumber);
        if (entityType == "Record Number")
            start = pageLocation;
        else if (entityType == "Record End")
            start = pageLocation + INT_SIZE;
        else if (entityType == "Record Offset") 
            start = calculateSlotLocation(file, pageNumber, slotNumber);
        else if (entityType == "Record Length") {
            start = calculateSlotLocation(file, pageNumber, slotNumber) + 
                                          INT_SIZE;
        }
        else {
            int recordOffset = readRecordOffset(file, pageNumber, slotNumber);
            recordLength = readRecordLength(file, pageNumber, slotNumber);
            start = recordOffset;
        }
        if (entityType == "Record")
            end = start + recordLength; 
        else
            end = start + INT_SIZE;
        byte[] dataRead = Arrays.copyOfRange(page, start, end);
        return dataRead; 
    }
    
    int readIntFromPage(File file, int pageNumber, String entityType, 
                                int slotNumber) throws FileNotFoundException{
        byte[] numberAsByteArray = readFromPage(file, pageNumber, entityType, 
                                                slotNumber);
        return new BigInteger(numberAsByteArray).intValue();
    }
    
    int readNumberOfRecords(File file, int pageNumber) 
                                    throws FileNotFoundException{
        return readIntFromPage(file, pageNumber, "Record Number", NO_SLOT);
    }
    
    int readEndOfRecords(File file, int pageNumber) 
                                 throws FileNotFoundException{
        return readIntFromPage(file, pageNumber, "Record End", NO_SLOT);
    }
    
    int readRecordOffset(File file, int pageNumber, int slotNumber) 
                                 throws FileNotFoundException{
        return readIntFromPage(file, pageNumber, "Record Offset", slotNumber);
    }
    
    int readRecordLength(File file, int pageNumber, int slotNumber) 
                                 throws FileNotFoundException{
        return readIntFromPage(file, pageNumber, "Record Length", slotNumber);
    }
    
    byte[] readRecordFromPage(File file, int pageNumber, 
                                      int slotNumber) 
                                      throws FileNotFoundException{
        return readFromPage(file, pageNumber, "Record", NO_SLOT);
    }
    
    byte[] readAttributeFromRecord(File file, 
                                           List<Attribute> recordDescriptor, 
                                           byte[] record, int attributeNumber){
        int recordOffset = getAttributeOffsetFromRecord(recordDescriptor, 
                                                        attributeNumber);
        Attribute target = recordDescriptor.get(attributeNumber);
        byte[] attribute = Arrays.copyOfRange(record, recordOffset, 
                                              recordOffset + target.length);
        return attribute;
    }
    
    // helpers that calculate values 
    
    private int calculatePageLocation(int pageNumber) {
        return pageNumber * PAGE_SIZE;
    }
    
    private int calculateEndOfSlotEntries(File file, int pageNumber) 
                                          throws FileNotFoundException{
        int pageLocation = calculatePageLocation(pageNumber);
        int recordNumber = readNumberOfRecords(file, pageNumber);
        int endOfSlotEntries = pageLocation + END_OF_PAGE - 
                               (recordNumber * SLOT_SIZE);
        return endOfSlotEntries;
    }
    
    private int calculateSlotLocation(File file, int pageNumber, 
                                      int slotNumber){
        int pageLocation = calculatePageLocation(pageNumber);
        return pageLocation + END_OF_PAGE - (slotNumber * SLOT_SIZE);
    }
    
    private int calculatePageFreeSpace(File file, int pageNumber) 
                                 throws FileNotFoundException{
        return calculateEndOfSlotEntries(file, pageNumber) - 
               readEndOfRecords(file, pageNumber);
    }
    
    /*private int calculateNewEndOfRecords(File file, int pageNumber, 
                                         byte[] newRecord) 
                                         throws FileNotFoundException{
        int currentRecordEnd = readEndOfRecords(file, pageNumber);
        int newRecordEnd = currentRecordEnd + (newRecord.length);
        return newRecordEnd;
    }*/

    private boolean pageHasSpaceForRecord(File file, int pageNumber, 
                                         byte[] record) 
                                         throws FileNotFoundException{
        int freeSpace = calculatePageFreeSpace(file, pageNumber);
        return ( (freeSpace - SLOT_SIZE) > record.length );
    }
    
    //returns page number of page with enough space to hold record
    private int findPageWithSpace(File file, byte[] record) 
                                  throws FileNotFoundException{
        PageFileManager pageFileManager = PageFileManager.getPageFileManager();
        int currentPage = 0;
        while (currentPage < ( pageFileManager.getNumberOfPages() ) ) {
            if (pageHasSpaceForRecord(file, currentPage, record))
                return currentPage;
            currentPage +=1;
        }
        return PAGE_NOT_FOUND;
    }
    
    //helpers that write to page
    
    private void writeToPage(File file, int pageNumber, byte[] entity, 
                             String entityType, int slotNumber)
                             throws FileNotFoundException, IOException{
        //PageFileManager pageFileManager = PageFileManager.getPageFileManager();
        int pageLocation = calculatePageLocation(pageNumber);
        int endOfRecords = readEndOfRecords(file, pageNumber);
        int slotLocation = END_OF_PAGE - (slotNumber * SLOT_SIZE);
        try (FileOutputStream output = new FileOutputStream(file)){
            if (entityType == "Record")
                output.write(entity, endOfRecords, entity.length);
            else if (entityType == "File Offset")
                output.write(entity, pageLocation, INT_SIZE);
            else if (entityType == "File Length")
                output.write(entity, pageLocation, INT_SIZE);
            else if (entityType == "Slot Header")
                output.write(entity, slotLocation, INT_SIZE);
            else // (entityType == "Record End")
                output.write(entity, RECORD_END_POSITION, INT_SIZE);
        }
    }
    
    private void writeRecordNumber(File file, int pageNumber, 
                                   byte[] recordNumber) 
                                   throws FileNotFoundException, IOException{
        writeToPage(file, pageNumber, recordNumber, "Record Number", NO_SLOT);
    }
    
    private void updateRecordNumber(File file, int pageNumber) 
                                    throws FileNotFoundException, IOException{
        int recordNumber = readNumberOfRecords(file, pageNumber);
        byte[] newRecordNumberAsBytes = convertIntToBytes(recordNumber++);
        writeRecordNumber(file, pageNumber, newRecordNumberAsBytes);
    }
    
    //private void writeEndOfRecords(File file, int pageNumber, byte[] recordEnd)
     //                              throws IOException{
     //   writeToPage(file, pageNumber, recordEnd, "Record End", NO_SLOT);
    //}
    
    private void writeRecord(File file, int pageNumber, byte[] record) 
                             throws FileNotFoundException, IOException{
        writeToPage(file, pageNumber, record, "Record", NO_SLOT);
    }
    
    private void writeFileOffset(File file, int pageNumber, byte[] offset, 
                                 int slotNumber) 
                             throws FileNotFoundException, IOException{
        writeToPage(file, pageNumber, offset, "File Offset", slotNumber);
    }
    
    private void writeFileLength(File file, int pageNumber, byte[] length, 
                                 int slotNumber) 
                              throws FileNotFoundException, IOException{
        writeToPage(file, pageNumber, length, "File Length", slotNumber);
    }
    
    private void writeSlotHeader(File file, int pageNumber, byte[] offset, 
                                 byte[] length, int slotNumber) 
                                 throws FileNotFoundException, IOException{
        writeFileOffset(file, pageNumber, offset, slotNumber);
        writeFileLength(file, pageNumber, length, slotNumber);
    }
    
    private int findRecord(File file, RecordID rid) 
                           throws FileNotFoundException{
        byte[] page = new byte[PAGE_SIZE];
        int recordOffset = -1;
        int recordLength;
        int pageNumber = rid.pageNumber;
        int slotNumber = rid.slotNumber;
        PageFileManager pageFileManager = PageFileManager.getPageFileManager();
        while ( !(recordOffset > 0) ) {
            pageFileManager.readPage(file, pageNumber, page);
            recordOffset = readRecordOffset(file, pageNumber, slotNumber);
            recordLength = readRecordLength(file, pageNumber, slotNumber);
            if (recordOffset == 0) {
                printDeadRecordMessage(rid);
                return DEAD_RECORD;
            }
            else if (recordOffset < 0) {
                pageNumber = -1 * recordOffset;
                slotNumber = -1 * recordLength;
                continue;
            }
            else  //(recordOffset > 0)
                break;
        }
        return recordOffset;
    }
    
    private int findRecordPage(File file, RecordID rid) 
                               throws FileNotFoundException{
        int recordOffset = findRecord(file, rid);
        return recordOffset / PAGE_SIZE;
    }
    
    private void printDeadRecordMessage(RecordID rid) {
        System.out.println("Record " + rid.pageNumber + " " + 
                            rid.slotNumber + " is dead"); 
    }
    
    private void printActiveRecordMessage(RecordID rid) {
        System.out.println("Record " + rid.pageNumber + " " + 
                           rid.slotNumber + " is already in file");
    }
    
    private void printMissingRecordMessage(RecordID rid) {
        System.out.println("Record " + rid.pageNumber + " " + 
                rid.slotNumber + " does not exist in file");
    }
    
    private void printNullAttributeMessage(List<Attribute> recordDescriptor,
                                           int attributeNumber) {
        System.out.println("Attribute number " + attributeNumber + 
                           " in record with descriptor " + recordDescriptor +
                           " is null");
    }
    
    //helpers which check status of record in file
    private boolean recordIsNotAlive(File file, RecordID rid) 
                                     throws FileNotFoundException{
        if (findRecord(file, rid) == DEAD_RECORD) 
            return true;
        return false;
    }
    
    private boolean recordIsInFile(File file, RecordID rid)
                                   throws FileNotFoundException{
        if ( findRecord(file, rid) > 0 ) 
            return true;
        return false;
    }
    
    private boolean recordIsNotInFile(File file, RecordID rid)
                                      throws FileNotFoundException{
        if ( findRecord(file, rid) < 0)
            return true;
        return false;
    }
    
    //helpers for handling null attribute values
    private int calculateNullIndicatorSize(List<Attribute> recordDescriptor) {
        return (int) Math.ceil(recordDescriptor.size() / BYTE_SIZE);
    }
    
    private byte[] getNullIndicatorFromRecord(List<Attribute> recordDescriptor,
                                              byte[] record) {
        int nullIndicatorSize = calculateNullIndicatorSize(recordDescriptor);
        byte[] nullIndicator = Arrays.copyOfRange(record, 0, nullIndicatorSize);
        return nullIndicator;
    }
    
    private boolean attributeIsNull(byte[] nullIndicator, 
                                    int attributeNumber) {
        int comparator = (int)Math.pow(2, attributeNumber);
        int nullIndicatorAsInt = convertBytesToInt(nullIndicator);
        return ( (comparator & nullIndicatorAsInt) == 0 ); 
    }
    
    private int getAttributeOffsetFromRecord(List<Attribute> recordDescriptor,
                                         int attributeIndex) {
        int indexCount = 0;
        int offset = 0;
        for (Attribute attribute : recordDescriptor) {
            if (indexCount == attributeIndex)
                break;
            offset += attribute.length;
            indexCount++;
        }
        return offset;
    }
}