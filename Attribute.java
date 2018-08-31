package minidatabase;

class Attribute{
    
    static enum AttributeType{VARCHAR, INT, REAL};
    
    AttributeType type;
    int length;
    String name;
}