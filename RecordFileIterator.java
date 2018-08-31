package minidatabase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.List;

class RecordFileIterator{
    
    public RecordFileManager recordFileManager  = 
                             RecordFileManager.getInstance();
    
    final int PAGE_SIZE = 4096; 
    
    private int currentIndex;
    private int currentPage;
    private int currentSlot;
    private int totalPages;
    private int slotsOnLastPage;
    
    private byte[] value;
    
    private Attribute.AttributeType type;
    private File file;
    private Attribute conditionAttribute;
    private String comparisonOperator;
    private List<Attribute> recordDescriptor;
    private List<String> attributeNames;
    private byte[] currentTuple;
    
    void initializeScan(File file, List<Attribute> recordDescriptor,
                            Attribute conditionAttribute, 
                            String comparisonOperator, byte[] value,
                            List<String> attributeNames) 
                            throws FileNotFoundException, 
                                   UnsupportedEncodingException{
        currentIndex = 0;
        currentPage = 0;
        currentSlot = 0;
        String valueAsString = "";
        int valueAsInt = 0;
        double valueAsDouble = 0;
        totalPages = (int) file.length() / PAGE_SIZE;
        slotsOnLastPage = recordFileManager.readNumberOfRecords(file, 
                                            totalPages) - 1;
        this.file = file;
        this.recordDescriptor = recordDescriptor;
        this.conditionAttribute = conditionAttribute;
        this.comparisonOperator = comparisonOperator;
        this.value = value;
        this.attributeNames = attributeNames;
        if (conditionAttribute.type == Attribute.AttributeType.VARCHAR)
            valueAsString = new String(value, "UTF-8");
        else if (conditionAttribute.type == Attribute.AttributeType.INT)
            valueAsInt = recordFileManager.convertBytesToInt(value);
        else //conditionAttribute.type == Attribute.AttributeType.REAL
            valueAsDouble = recordFileManager.convertBytesToDouble(value);
    }
    
    byte[] getNextTuple(RecordID rid) throws FileNotFoundException{
        byte[] nextTuple = null;
        byte[] nextAttribute = null;
        int currentAttribute = 0;
        int attributeNumber = recordDescriptor.size();
        while (currentAttribute < attributeNumber) {
            recordFileManager.readAttribute(file, recordDescriptor, rid, 
                          attributeNames.get(currentAttribute), 
                          nextAttribute);
            currentAttribute++;
        }
        
        return nextTuple;
    }
    
    private boolean checkScanCondition(byte[] tuple, String valueAsString) {
        switch (comparisonOperator) {
        case "EQ":
        case "LT":
        case "LE":
        case "GT":
        case "GE":
        case "NE":
        }
    }
    
    //valueAsInt will be autopromoted
    private boolean checkScanCondition(byte[] tuple, float valueAsDouble) {
        switch (comparisonOperator) {
        case "EQ":
        case "LT":
        case "LE":
        case "GT":
        case "GE":
        case "NE":
        }
    }
    
    private byte[] projectAttributes(byte[] tuple, List<String> attributeNames)
    {
        byte[] projectedAttributes;
    }
    
}