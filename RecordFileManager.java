package minidatabase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;

//UPDATE WITH RECORD DESCRIPTOR
class RecordFileManager{
        
    //page header consists of 2 ints. page number, then slot number
    
    //each slot in directory also consists of 2 ints, 
    //offset of record within page, and length of record
    
    static final boolean NO_UPDATE = false;
    static final boolean IS_UPDATE = true;
    static final boolean WANT_OFFSET = false;
    static final boolean WANT_SLOT = true;
    
    static final int PAGE_NOT_FOUND = -1;
    static final int RECORD_NOT_FOUND = -1;
    static final int NO_SLOT = -1;
    static final int SCAN_OVER = 0;
    static final int PAGE_START = 0;
    static final int DEAD_RECORD = 0;
    static final int UNUSED = 0;
    static final int FILE_HEADER_SIZE = 8;
    static final int INT_SIZE = 4;
    static final int RECORD_START = 8;
    static final int SLOT_SIZE = 8;
    static final int BYTE_SIZE = 8;
    static final int END_OF_PAGE = 4095;
    static final int PAGE_SIZE = 4096;
    
    //client methods
    
    static void insertRecord(FileHandle handle, 
                             Vector<Attribute> recordDescriptor, 
                             RecordID rid, byte[] record, boolean isUpdate) 
                             throws FileNotFoundException, IOException{
        int recordPage = findPageWithSpace(handle, record);
        int recordSlot = readNumberOfRecords(handle, recordPage);
        if (recordIsInFile(handle, rid)) {
            if (!isUpdate) {
                printActiveRecordMessage(rid);
                return;
            }
            else {
                int oldRecordPage = findRecordPage(handle, rid);
                int oldSlotNumber = readNumberOfRecords(handle, recordPage);
                writeSlotHeader(handle, oldRecordPage, oldSlotNumber,
                                -1 * recordPage, -1 * recordSlot); 
            }
        }
        if (!isUpdate)
            setRecordID(rid, recordPage, recordSlot);
        processRecordEntry(handle, recordPage, recordSlot, record);
    }
    
    static void readRecord(FileHandle handle, Vector<Attribute> recordDescriptor,
                    RecordID rid, byte[] record) throws FileNotFoundException{
        if (recordIsInaccessible(handle, rid))
            return;
        int recordPage = findRecordPage(handle, rid);
        record = readRecordFromPage(handle, recordPage, rid.slotNumber);
    }
    
    static void deleteRecord(FileHandle handle, RecordID rid) 
                      throws FileNotFoundException, IOException{
        if (recordIsInaccessible(handle, rid))
            return;
        int recordPage = findRecordPage(handle, rid);
        int recordSlot = findRecord(handle, rid, WANT_SLOT);
        writeFileOffset(handle, recordPage, recordSlot, DEAD_RECORD);
    }
    
    static void updateRecord(FileHandle handle, Vector<Attribute> recordDescriptor, 
                      RecordID rid, byte[] newRecord)
                      throws FileNotFoundException, IOException{
        if (recordIsInaccessible(handle, rid))
            return;
        insertRecord(handle, recordDescriptor, rid, newRecord, IS_UPDATE);
    }
    
    static void printRecord(FileHandle handle,
                            Vector<Attribute> recordDescriptor, RecordID rid,
                            byte[] record) throws FileNotFoundException{
        int attributeNumber = 0;
        String attributeName = "";
        for (Attribute a: recordDescriptor) {
            byte[] attributeData = null;
            attributeName = a.name;
            readAttribute(handle, recordDescriptor, rid, attributeName,
                          attributeData);
            printAttribute(recordDescriptor, attributeData, attributeNumber);
            attributeNumber++;
            }
        }
    
    static void readAttribute(FileHandle handle, Vector<Attribute> recordDescriptor,
                       RecordID rid, String attributeName, byte[] attribute) 
                       throws FileNotFoundException{
        byte[] record = null;
        readRecord(handle, recordDescriptor, rid, record);
        byte[] nullIndicator = 
                NullIndicator.getNullIndicator(recordDescriptor, record);
        int attributeIndex = getAttributeIndex(recordDescriptor, attributeName);
        if (NullIndicator.attributeIsNull(nullIndicator, attributeIndex)) 
            attribute = null;
        attribute = parseAttribute(handle, recordDescriptor, record,
                                   attributeIndex);
    }
    
    static int scan(FileHandle handle, Vector<Attribute> recordDescriptor, 
              Attribute conditionAttribute, String operator, byte[] threshold,
              int currentPage, int currentIndex, boolean wantsSlot) 
              throws FileNotFoundException{
        byte[] record = null;
        while (currentPage != handle.getNumberOfPages()) {
            int currentSlot = 0;
            byte[] attributeValue = null;
            while (currentSlot < PAGE_SIZE) {
                RecordID rid = new RecordID(currentPage, currentSlot);
                readRecord(handle, recordDescriptor, rid, record);
                readAttribute(handle, recordDescriptor, rid, 
                              conditionAttribute.name, attributeValue);
                if(
                  ScanCondition.attributeValidatesCondition(conditionAttribute,
                                                      attributeValue, operator,
                                                      threshold)) {
                    if (wantsSlot)
                        return (currentPage * PAGE_SIZE) + currentSlot;
                    return (currentPage * PAGE_SIZE) + currentIndex;
                }
                    currentSlot++;
            }
            currentPage++;
        }
        return SCAN_OVER;
    }
    
    //setters
    
    static void setRecordID(RecordID rid, int pageNumber, int slotNumber){
        rid.pageNumber = pageNumber;
        rid.slotNumber = slotNumber;
    }
    
    //private helper methods
    
    //helpers that read the page
    
    static int getEntityStart(FileHandle handle, int entityPage, 
                              String entityType, int slotNumber) 
                              throws FileNotFoundException{
        int start = 0;
        int pageLocation = PageFileManager.calculatePageLocation(entityPage);
        if (entityType == "Record Number")
            start = pageLocation;
        else if (entityType == "Record End")
            start = pageLocation + INT_SIZE;
        else if (entityType == "Record Offset") 
            start = calculateSlotLocation(entityPage, slotNumber);
        else if (entityType == "Record Length") 
            start = calculateSlotLocation(entityPage, slotNumber) + INT_SIZE;
        else   //entityType == Record
            start = readRecordOffset(handle, entityPage, slotNumber);
        return start;
    }
    
    static int getEntityEnd(FileHandle handle, int start, String entityType, 
                     int entityPage, int slotNumber) 
                     throws FileNotFoundException{
        int end = 0;
        int recordLength = readRecordLength(handle, entityPage, slotNumber);
        if (entityType == "Record")
            end = start + recordLength;
        else
            end = start + INT_SIZE;
        return end;
    }
    
    static byte[] readFromPage(FileHandle handle, int pageNumber, 
                               String entityType, int slotNumber)
                               throws FileNotFoundException{
        int start = getEntityStart(handle, pageNumber, entityType, slotNumber);
        int end = getEntityEnd(handle, start, entityType, pageNumber, 
                               slotNumber);
        return PageFileManager.readFromPage(handle, pageNumber, start, end);
    }
    
    static int readIntFromPage(FileHandle handle, int pageNumber,
                               String entityType, int slotNumber)
                        throws FileNotFoundException{
        int start = getEntityStart(handle, pageNumber, entityType, slotNumber);
        return PageFileManager.readIntFromPage(handle, pageNumber, start);
    }
    
    static int readNumberOfRecords(FileHandle handle, int pageNumber) 
                                   throws FileNotFoundException{
        return readIntFromPage(handle, pageNumber, "Record Number", NO_SLOT);
    }
    
    static int readEndOfRecords(FileHandle handle, int pageNumber) 
                                throws FileNotFoundException{
        return readIntFromPage(handle, pageNumber, "Record End", NO_SLOT);
    }
    
    static int readRecordOffset(FileHandle handle, int pageNumber, 
                                int slotNumber) 
                                throws FileNotFoundException{
        return readIntFromPage(handle, pageNumber, "Record Offset", slotNumber);
    }
    
    static int readRecordLength(FileHandle handle, int pageNumber, 
                                int slotNumber) 
                                throws FileNotFoundException{
        return readIntFromPage(handle, pageNumber, "Record Length", slotNumber);
    }
    
    static byte[] readRecordFromPage(FileHandle handle, int pageNumber, 
                                     int slotNumber) 
                                     throws FileNotFoundException{
        return readFromPage(handle, pageNumber, "Record", NO_SLOT);
    }
    
    static byte[] parseAttribute(FileHandle handle, 
                                 Vector<Attribute> recordDescriptor,
                                 byte[] record, int attributeNumber){
        int recordOffset = getAttributeOffsetFromRecord(recordDescriptor, 
                                                        attributeNumber);
        Attribute target = recordDescriptor.get(attributeNumber);
        byte[] attribute = Arrays.copyOfRange(record, recordOffset, 
                                              recordOffset + target.length);
        return attribute;
    }
    
    // helpers that calculate values 
    
    private static int calculateEndOfSlotEntries(FileHandle handle,
                                                 int pageNumber) 
                                                 throws FileNotFoundException{
        int pageLocation = PageFileManager.calculatePageLocation(pageNumber);
        int recordNumber = readNumberOfRecords(handle, pageNumber);
        int endOfSlotEntries = pageLocation + END_OF_PAGE - 
                               (recordNumber * SLOT_SIZE);
        return endOfSlotEntries;
    }
    
    //slot numbering starts at 0
    private static int calculateSlotLocation(int pageNumber, int slotNumber){
        int pageLocation = PageFileManager.calculatePageLocation(pageNumber);
        return pageLocation + END_OF_PAGE - (slotNumber * SLOT_SIZE);
    }
    
    private static int calculatePageFreeSpace(FileHandle handle, 
                                              int pageNumber) 
                                              throws FileNotFoundException{
        return calculateEndOfSlotEntries(handle, pageNumber) - 
               readEndOfRecords(handle, pageNumber);
    }
    
    private static int calculateNewEndOfRecords(FileHandle handle, 
                                                int pageNumber, 
                                                byte[] newRecord) 
                                                throws FileNotFoundException{
        int currentRecordEnd = readEndOfRecords(handle, pageNumber);
        int newRecordEnd = currentRecordEnd + (newRecord.length);
        return newRecordEnd;
    }

    private static boolean pageHasSpaceForRecord(FileHandle handle, 
                                                 int pageNumber, 
                                                 byte[] record) 
                                                 throws FileNotFoundException{
        int freeSpace = calculatePageFreeSpace(handle, pageNumber);
        return ( (freeSpace - SLOT_SIZE) >= record.length );
    }
    
    //returns page number of page with enough space to hold record
    private static int findPageWithSpace(FileHandle handle, byte[] record) 
                                  throws FileNotFoundException{
        int currentPage = 0;
        while (currentPage < ( handle.getNumberOfPages() ) ) {
            if (pageHasSpaceForRecord(handle, currentPage, record))
                return currentPage;
            currentPage +=1;
        }
        return PAGE_NOT_FOUND;
    }
    
    //helpers that write to page
    
    private static void writeToPage(FileHandle handle, int pageNumber, 
                                    int slotNumber, byte[] entity,
                                    String entityType) 
                                    throws FileNotFoundException{
        byte[] copy = null;
        PageFileManager.readPage(handle, pageNumber, copy);
        writeEntityIntoCopy(handle, pageNumber, slotNumber, entityType, 
                            entity, copy);
        PageFileManager.writePage(handle, pageNumber, copy);
    }
    
    private static byte[] writeEntityIntoCopy(FileHandle handle, 
                                     int pageNumber, 
                                     int slotNumber, String entityType, 
                                     byte[] entity, byte[] copy) 
                                     throws FileNotFoundException{
        int pageLocation = PageFileManager.calculatePageLocation(pageNumber);
        int endOfRecords = readEndOfRecords(handle, pageNumber);
        int slotLocation = END_OF_PAGE - (slotNumber * SLOT_SIZE);
        int target;
        if (entityType == "Record") 
            target = endOfRecords;
        else if (entityType == "Record Offset")
            target = slotLocation;
        else if (entityType == "Record Length")
            target = slotLocation + INT_SIZE;
        else if (entityType == "Record Number")
            target = pageLocation;
        else // (entityType == "Record End")
            target = pageLocation + INT_SIZE;
        PageFileManager.writeEntityIntoCopy(handle, target, entity, copy);
        return entity;
    }
    
    private static void writeRecordNumber(FileHandle handle, int pageNumber, 
                                          byte[] recordNumber) 
                                    throws FileNotFoundException, IOException{
        writeToPage(handle, pageNumber, NO_SLOT, recordNumber, "Record Number");
    }
    
    private static void incrementRecordNumber(FileHandle handle, 
                                              int pageNumber)
                                    throws FileNotFoundException, IOException{
        int recordNumber = readNumberOfRecords(handle, pageNumber);
        byte[] newRecordNumberAsBytes = 
                DataConversion.convertIntToBytes(recordNumber++);
        writeRecordNumber(handle, pageNumber, newRecordNumberAsBytes);
    }
    
    private static void writeEndOfRecords(FileHandle handle, int pageNumber, 
                                          byte[] recordEnd)
                                          throws IOException{
        writeToPage(handle, pageNumber, NO_SLOT, recordEnd, "Record End");
    }
    
    private static void updateEndOfRecords(FileHandle handle, int pageNumber, 
                                           byte[] newRecord)
                                    throws FileNotFoundException, IOException{
        int endOfRecords = calculateNewEndOfRecords(handle, pageNumber, newRecord);
        byte[] endOfRecordsAsByteArray =
                DataConversion.convertIntToBytes(endOfRecords);
        writeEndOfRecords(handle, pageNumber, endOfRecordsAsByteArray);
    }
    
    private static void writeRecord(FileHandle handle, int pageNumber, 
                                    byte[] record) 
                                    throws FileNotFoundException, IOException{
        writeToPage(handle, pageNumber, NO_SLOT, record, "Record");
    }
    
    private static void writeFileOffset(FileHandle handle, int pageNumber, 
                                        int slotNumber, int offset) 
                                 throws FileNotFoundException, IOException{
        byte[] offsetAsBytes = 
                DataConversion.convertIntToBytes(offset);
        writeToPage(handle, pageNumber, slotNumber, offsetAsBytes,
                    "Record Offset");
    }
    
    private static void writeFileLength(FileHandle handle, int pageNumber, 
                                        int slotNumber, int length) 
                                 throws FileNotFoundException, IOException{
        byte[] lengthAsBytes = 
                DataConversion.convertIntToBytes(length);
        writeToPage(handle, pageNumber, slotNumber, lengthAsBytes,
                    "Record Length");
    }
    
    private static void writeSlotHeader(FileHandle handle, int pageNumber, 
                                        int slotNumber, int offset, int length) 
                                 throws FileNotFoundException, IOException{
        writeFileOffset(handle, pageNumber, slotNumber, offset);
        writeFileLength(handle, pageNumber, slotNumber, length);
    }
    
    private static int findRecord(FileHandle handle, RecordID rid, 
                                  boolean wantsSlot)
                                  throws FileNotFoundException{
        int pageNumber = rid.pageNumber;
        int slotNumber = rid.slotNumber;
        while (true) {
            int recordOffset = readRecordOffset(handle, pageNumber, slotNumber);
            int recordLength = readRecordLength(handle, pageNumber, slotNumber);
            if (recordOffset == 0) {
                printDeadRecordMessage(rid);
                return DEAD_RECORD;
            }
            else if (recordOffset < 0) {
                pageNumber = -1 * recordOffset;
                slotNumber = -1 * recordLength;
                continue;
            }
            else { 
                int recordLocation;
                recordLocation = (wantsSlot)? slotNumber : recordOffset;
                return recordLocation;
            }
        }
    }
    
    private static int findRecordPage(FileHandle handle, RecordID rid) 
                                      throws FileNotFoundException{
        int recordOffset = findRecord(handle, rid, WANT_OFFSET);
        return recordOffset / PAGE_SIZE;
    }
    
    //helpers which check status of record in file
    private static boolean recordIsNotAlive(FileHandle handle, RecordID rid) 
                                     throws FileNotFoundException{
        if (findRecord(handle, rid, WANT_OFFSET) == DEAD_RECORD) 
            return true;
        return false;
    }
    
    private static boolean recordIsInFile(FileHandle handle, RecordID rid)
                                   throws FileNotFoundException{
        if ( findRecord(handle, rid, WANT_OFFSET) > 0 ) 
            return true;
        return false;
    }
    
    private static boolean recordIsInaccessible(FileHandle handle, 
                                                RecordID rid) 
                                         throws FileNotFoundException{
        if (!recordIsInFile(handle, rid)) {
            printMissingRecordMessage(rid);
            return false;
        }
        if (recordIsNotAlive(handle, rid)) {
            printDeadRecordMessage(rid);
            return false;
        }
        return true;
    }
    
    private static int getAttributeIndex(Vector<Attribute> recordDescriptor, 
                                         String recordName) {
        int index = 0;
        for (Attribute a : recordDescriptor) {
            if (a.name == recordName)
                break;
            index++;
        }
        return index;
    }
    
    private static int getAttributeOffsetFromRecord(
                                             Vector<Attribute> recordDescriptor,
                                             int attributeIndex
                                             ){
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
    
    private static void processRecordEntry(FileHandle handle, int recordPage, 
                                           int slotNumber, byte[] record)
                                           throws IOException{
        writeRecord(handle, recordPage, record);
        int recordOffset = readEndOfRecords(handle, recordPage);
        writeSlotHeader(handle, recordPage, slotNumber, recordOffset, 
                        record.length);
        incrementRecordNumber(handle, recordPage);
        updateEndOfRecords(handle, recordPage, record);
    }
    
    private static void printAttribute(Vector<Attribute> recordDescriptor, 
                                byte[] attributeData, int attributeNumber) {
        if (attributeData == null)
            System.out.println("NULL");
        if (recordDescriptor.get(attributeNumber).type ==
            Attribute.AttributeType.INT) {
            int attributeInt =
                    DataConversion.convertBytesToInt(attributeData);
            System.out.println("INT : " + attributeInt);
        }
        else if (recordDescriptor.get(attributeNumber).type == 
                Attribute.AttributeType.REAL) {
            double attributeDouble =
                    DataConversion.convertBytesToDouble(attributeData);
            System.out.println("REAL : " + attributeDouble);
        }
        else { //AttributeType is VARCHAR
            String attributeString = new String(attributeData);
            System.out.println("VARCHAR : " + attributeString);
        }
    }
    
    private static void printDeadRecordMessage(RecordID rid) {
        System.out.println("Record " + rid.pageNumber + " " + 
                            rid.slotNumber + " is dead"); 
    }
    
    private static void printActiveRecordMessage(RecordID rid) {
        System.out.println("Record " + rid.pageNumber + " " + 
                           rid.slotNumber + " is already in file");
    }
    
    private static void printMissingRecordMessage(RecordID rid) {
        System.out.println("Record " + rid.pageNumber + " " + 
                rid.slotNumber + " does not exist in file");
    }
}