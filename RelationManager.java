package minidatabase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;

class RelationManager{
    
    static final boolean NO_UPDATE = false;
    static final boolean IS_UPDATE = true;
    static final boolean WANT_OFFSET = false;
    static final boolean WANT_SLOT = true;
    static final boolean CREATION = true;
    static final boolean DELETION = false;
    static final boolean IS_CATALOG = true;
    static final boolean NO_CATALOG = false;
    
    static final int PAGE_NOT_FOUND = -1;
    static final int TUPLE_NOT_FOUND = -1;
    static final int NO_SLOT = -1;
    static final int NO_ID = -1;
    static final int FIRST_PAGE = 0;
    static final int PAGE_START = 0;
    static final int UNUSED = 0;
    static final int TABLES_TABLE_ID = 0;
    static final int COLUMNS_TABLE_ID = 1;
    static final int FILE_HEADER_SIZE = 8;
    static final int INT_SIZE = 4;
    static final int VARCHAR_SIZE = 50;
    static final int TUPLE_START = 8;
    static final int SLOT_SIZE = 8;
    static final int BYTE_SIZE = 8;
    static final int END_OF_PAGE = 4095;
    static final int PAGE_SIZE = 4096;
    
    FileHandle tables;
    FileHandle columns;
    
    private int tableCount = 0;
    private int nextTableID = 0;
    
    private Attribute tableID = new Attribute(Attribute.AttributeType.INT, 
                                              INT_SIZE, "tableID");
    
    private Attribute tableName = new Attribute(
                                               Attribute.AttributeType.VARCHAR,
                                               VARCHAR_SIZE, "tableName");
    
    private Attribute columnName = new Attribute(
                                              Attribute.AttributeType.VARCHAR,
                                              VARCHAR_SIZE, "rowName");
    
    private Attribute columnType = new Attribute(Attribute.AttributeType.VARCHAR, 
                                              VARCHAR_SIZE, "rowType");
    
    private Attribute columnSize = new Attribute(Attribute.AttributeType.INT,
                                                 INT_SIZE, "columnSize");
    
    private Attribute rowID = new Attribute(Attribute.AttributeType.INT,
                                            INT_SIZE, "rowID");
    
    private Vector<Attribute> tablesTupleDescriptor = new Vector<Attribute>();
    
    private Vector<Attribute> columnsTupleDescriptor = new Vector<Attribute>();
    
    private static final RelationManager instance = new RelationManager();
    
    public static RelationManager getInstance() {
        return instance;
    }
    
    private RelationManager() {}
    
    void prepareCatalogDescriptors() {
        tablesTupleDescriptor.add(tableID);
        tablesTupleDescriptor.add(tableName);
        columnsTupleDescriptor.add(tableID);
        columnsTupleDescriptor.add(columnName);
        columnsTupleDescriptor.add(columnType);
        columnsTupleDescriptor.add(columnSize);
        columnsTupleDescriptor.add(rowID);
    }
    
    void createCatalog() throws IOException{
        createTable("Tables", tablesTupleDescriptor);
        createTable("Columns", columnsTupleDescriptor);     
    }
    
    void deleteCatalog() {
        deleteTable("Tables"); 
        deleteTable("Columns");
    }
    
    void createTable(String tableName, Vector<Attribute> tupleDescriptor)
                     throws IOException{
        RecordID tupleID = new RecordID();
        FileHandle handle = new FileHandle();
        handle.createFile(tableName + ".tbl");
        byte[] tableIDData = DataConversion.convertIntToBytes(nextTableID);
        byte[] tableNameData = tableName.getBytes();
        byte[] tupleData = MergeArray.concatenate(tableIDData, tableNameData);
        insertTuple(tableName, handle, tupleDescriptor, tupleID, tupleData,
                    IS_CATALOG, NO_UPDATE);
        insertIntoColumnsTable(handle, tupleDescriptor, tableIDData);
        tableCount++;
        nextTableID++;
    }
    
    void deleteTable(String tableName){
        FileHandle handle;
        Attribute conditionAttribute = new Attribute(
                                              Attribute.AttributeType.VARCHAR, 
                                              tableName.length(), 
                                              "conditionAttribute");
        handle.setFile(tableName + ".tbl");
        handle.deleteFile();
        byte[] conditionValue = tableName.getBytes();
        int location = findTableID(tableName);
        int page = location / PAGE_SIZE;
        int slot = location % PAGE_SIZE;
        RecordID tableID = new RecordID(page, slot);
        deleteTuple("Tables", handle, tableID, IS_CATALOG);
        deleteTuplesFromColumnsTable(tableID);
        tableCount--;
    }
    
    void getAttributes(String tableName, Vector<Attribute> tupleDescriptor){
        FileHandle handle = new FileHandle();
        handle.setFile(tableName + ".tbl");
        //getTableID(tableName)
        //scan
        byte[] attributeData;
        for (Attribute a: tupleDescriptor) {
            readAttribute(tableName, a.name, attributeData);
            
        }
    }
    
    void insertTuple(String tableName, FileHandle handle, 
                     Vector<Attribute> tupleDescriptor, 
                     RecordID tupleID, byte[] tupleData, 
                     boolean isCatalog, boolean isUpdate) throws IOException{
        if (tableIsCatalog(tableName) && !isCatalog){
            printUnmodifiableTable(tableName);
            return;
        }
        RecordFileManager.insertRecord(handle, tupleDescriptor, tupleID, 
                                       tupleData, NO_UPDATE);
        byte[] idData = convertRecordIDToBytes(tupleID);
        if (!isCatalog)
            insertIntoColumnsTable(handle, tupleDescriptor, idData);
    }
    
    void deleteTuple(String tableName, FileHandle handle, RecordID tupleID, 
                     boolean isCatalog) throws IOException{
        if (tableIsCatalog(tableName) && !isCatalog){
            printUnmodifiableTable(tableName);
            return;
        }
        RecordFileManager.deleteRecord(handle, tupleID);
        Vector<Attribute> tupleDescriptor;
        getAttributes(tableName, tupleDescriptor);
        if (!isCatalog)
            deleteFromColumnsTable(handle, tupleDescriptor, tupleID);
    }
    
    void updateTuple(String tableName, FileHandle handle, 
                     Vector<Attribute> tupleDescriptor, byte[] newTuple,
                     RecordID rowID) throws IOException{
        handle.setFile(tableName);
        insertTuple(tableName, handle, tupleDescriptor, rowID, newTuple, 
                    NO_CATALOG, IS_UPDATE);
    }
    
    void readTuple(String tableName, FileHandle handle, 
                   Vector<Attribute> tupleDescriptor, byte[] data,
                   RecordID tupleID) throws IOException{
        RecordFileManager.readRecord(handle, tupleDescriptor, tupleID, data);
    }
    
    void printTuple(FileHandle handle, Vector<Attribute> tupleDescriptor, 
                    byte[] tuple, RecordID tupleID) 
                    throws FileNotFoundException{
        RecordFileManager.printRecord(handle, tupleDescriptor, tupleID, tuple);
    }
    
    void readAttribute(String tableName, FileHandle handle,
                       Vector<Attribute> tupleDescriptor, RecordID tupleID,
                       String attributeName, byte[] data) 
                       throws FileNotFoundException{
        RecordFileManager.readAttribute(handle, tupleDescriptor, tupleID,
                                        attributeName, data);
    }
    
    int scan(Vector<Attribute> recordDescriptor, Attribute conditionAttribute,
             String operator, byte[] value, int currentPage, int currentIndex,
             boolean wantsSlot, String tableName) throws FileNotFoundException{
        FileHandle handle = new FileHandle();
        handle.setFile(tableName + ".tbl");
        return RecordFileManager.scan(handle, recordDescriptor,
                                      conditionAttribute, operator, value, 
                                      currentPage, currentIndex, wantsSlot);
    }
  
    // private helpers
    
    private int findTableID(String tableName) throws FileNotFoundException{
        Attribute condition = new Attribute(Attribute.AttributeType.VARCHAR, 
                                            tableName.length(), "tableName");
        byte[] targetValue = tableName.getBytes();
        return scan(tablesTupleDescriptor, condition, "EQ", targetValue,  
                    FIRST_PAGE, PAGE_START, WANT_SLOT, tableName);
    }
    
    private void insertIntoColumnsTable(FileHandle handle, 
                                        Vector<Attribute> tupleDescriptor, 
                                        byte[] tableIDData) throws IOException{
        byte[] tupleData;
        byte[] nameData;
        byte[] typeData;
        byte[] lengthData;
        byte[] rowCountData;
        int rowCount = 0;
        for (Attribute a: tupleDescriptor) {
            nameData = a.name.getBytes();
            typeData = (a.type.name()).getBytes();
            lengthData = DataConversion.convertIntToBytes(a.length);
            rowCountData = DataConversion.convertIntToBytes(rowCount);
            tupleData = MergeArray.concatenateAll(tableIDData, nameData, 
                                                  typeData, lengthData, 
                                                  rowCountData);
            insertTuple("Columns", handle, tupleDescriptor, new RecordID(), tupleData, 
                        IS_CATALOG, NO_UPDATE);
            rowCount++;
        }
    }
    
    private void deleteFromColumnsTable(FileHandle handle, 
                                        Vector<Attribute> tupleDescriptor) {
        
    }
    
    private byte[] convertRecordIDToBytes(RecordID id) {
        byte[] pageData = DataConversion.convertIntToBytes(id.pageNumber);
        byte[] slotData = DataConversion.convertIntToBytes(id.slotNumber);
        byte[] recordIDData = MergeArray.concatenate(pageData, slotData);
        return recordIDData;
    }
    
    private boolean tableIsCatalog(String tableName) {
        return (tableName == "Tables") && (tableName == "Columns");
    }
    
    private void printTableExists(String tableName){
        System.out.println("Error:" + tableName + " already exists");
    }
    
    private void printTableDoesNotExist(String tableName){
        System.out.println("Error:" + tableName + " does not exist");
    }
    
    private void printTupleDoesNotExist(String tupleName){
        System.out.println("Error:" + tupleName + " does not exist");
    }
    
   private void printUnmodifiableTable(String tableName) {
       System.out.println("Error: Catalog table " + tableName + 
                          "can't be modified");
   }
   
}