package minidatabase;

class Attribute{
    
    static enum AttributeType{VARCHAR, INT, REAL};
    
    AttributeType type;
    int length;
    String name;
    
    Attribute(AttributeType t, int l, String name){
        this.type = t;
        this.length = l;
        this.name = name;
    }
}