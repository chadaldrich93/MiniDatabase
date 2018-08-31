package minidatabase;

import java.io.File;
import java.util.List;

class RFM_ScanIterator{
    
    private RecordFileManager instance = RecordFileManager.getInstance();
    
    private int attributeIndex;
    private int currentPage;
    private int currentSlot;
    private int totalPages;
    private int totalSlots;
    
    private byte[] value;
    
    private Attribute.AttributeType type;
    private File file;
    private String conditionAttribute;
    private String comparisonOperator;
    private List<Attribute> recordDescriptor;
    private List<RecordID> skippedRecords;
    private List<String> attributeNames;
    
    void scanInitialization(File file, List<Attribute> recordDescriptor,
                            String conditionAttribute, 
                            String comparisonOperator, byte[] value,
                            List<String> attributeNames) {
        currentPage = 0;
        currentSlot = 0;
        totalPages = 0;
        totalSlots = 0;
        this.file = file;
        this.recordDescriptor = recordDescriptor;
        this.conditionAttribute = conditionAttribute;
        this.comparisonOperator = comparisonOperator;
        this.value = value;
        this.attributeNames = attributeNames;
    }
    
    
}