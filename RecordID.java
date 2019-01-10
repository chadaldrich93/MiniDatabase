package minidatabase;

class RecordID{
    int pageNumber;
    int slotNumber;
    
    RecordID(int pageNumber, int slotNumber){
        this.pageNumber = pageNumber;
        this.slotNumber = slotNumber;
    }
    
    RecordID(){
        this.pageNumber = 0;
        this.slotNumber = 0;
    }
    
}