package minidatabase;

import java.io.File;
import java.io.IOException;
import java.util.List;

class RelationManager{
    
    final boolean NO_UPDATE = false;
    final boolean IS_UPDATE = true;
    
    final int TABLES_TABLE_ID = 0;
    final int COLUMNS_TABLE_ID = 1;
    
    final File tablesFile = new File("Tables.tbl");
    final File columnsFile = new File("Columns.tbl");
    
    private int tableCount = 0;
    
    void createCatalog() throws IOException{
        RecordFileManager recordFileManager = RecordFileManager.getInstance();
        if (recordFileManager.createFile("Tables.tbl") &&
            recordFileManager.createFile("Columns.tbl")) 
            ;
        else {
            System.out.println("Unable to create RelationManager Catalog");
            System.out.println("Catalog may already exist");
        }
    }
    
    void deleteCatalog() {
        RecordFileManager recordFileManager = RecordFileManager.getInstance();
        if (recordFileManager.deleteFile(tablesFile) &&
            recordFileManager.deleteFile(columnsFile)) {
            return;
        }
        else {
            System.out.println("Unable to delete RelationManager Catalog");
            System.out.println("Catalog may already exist");
        }
    }
    
    void createTable(String tableName){
        
        tableCount++;
    }
    
    //void deleteTable(String tableName)
    
    //void getAttributes(String tableName, List<Attribute> recordDescriptor)
    
    void insertTuple(String tableName, byte[] data, RecordID rid,
                     List<Attribute> recordDescriptor) throws IOException{
        RecordFileManager recordFileManager = RecordFileManager.getInstance();
        File tableFile = new File(tableName);
        recordFileManager.insertRecord(tableFile, recordDescriptor, rid, NO_UPDATE);
    }
    
    void deleteTuple(String tableName, RecordID rid) throws IOException{
        RecordFileManager recordFileManager = RecordFileManager.getInstance();
        File tableFile = new File(tableName);
        recordFileManager.deleteRecord(tableFile, rid);
    }
    
    void updateTuple(String tableName, byte[] newTuple, RecordID rid) 
                     throws IOException{
        RecordFileManager recordFileManager = RecordFileManager.getInstance();
        File tableFile = new File(tableName);
        recordFileManager.updateRecord(tableFile, rid, newTuple);
    }
    
    void readTuple(String tableName, byte[] data, RecordID rid) 
                   throws IOException{
        RecordFileManager recordFileManager = RecordFileManager.getInstance();
        File tableFile = new File(tableName);
        recordFileManager.readRecord(tableFile, rid);
    }
    
    void printTuple(List<Attribute> attributes, byte[] tuple) {
        RecordFileManager recordFileManager = RecordFileManager.getInstance();
        recordFileManager.printRecord(attributes, tuple);
    }
    
    //void readAttribute(String tableName, RecordID rid, String attributeName,
                         //byte[] data)
    
    //void scan(String tableName, String conditionAttribute, String operator, 
                //byte[] value, List<String> attributeNames, RMSI)
    
    //setters
    
    
    // private helpers
    
    private void insertTable(String tableName) throws IOException{
        RecordFileManager recordFileManager = RecordFileManager.getInstance();
        RecordID newTableID = new RecordID();
        String dataAsString = tableCount + tableName;
        byte[] dataAsBytes = dataAsString.getBytes();
        recordFileManager.insertRecord(tablesFile, newTableID, 
                                       dataAsBytes, NO_UPDATE);
    }
    
    private void insertColumn (int tableID, String columnName, 
                              Attribute columnDescriptor, int columnPosition) 
                              throws IOException{
        RecordFileManager recordFileManager = RecordFileManager.getInstance();
        RecordID newColumnID = new RecordID();
        String dataAsString = tableID                 + 
                              columnDescriptor.name   + 
                              columnDescriptor.type   +
                              columnDescriptor.length +
                              columnPosition;
        byte[] dataAsBytes = dataAsString.getBytes();
        recordFileManager.insertRecord(columnsFile, newColumnID, 
                                       dataAsBytes, NO_UPDATE);
    }
    
    //getTableID(
    //ensureTableIsNotCatalog
    //printTableExists
   //printUnmodifiableTable
    
}