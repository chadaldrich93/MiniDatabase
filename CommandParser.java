package minidatabase;

import java.io.IOException;
import java.io.PushbackReader;
import java.util.List;

import java.util.Vector;

class CommandParser{
    
    static final boolean BY_COUNT = false;
    static final boolean TO_SPACE = true;
    
    static final int NO_LENGTH = 0;
    static final int NO_OP = 0;
    static final int MIN_OP_SIZE = 1;
    static final int MAX_OP_SIZE = 2;
    static final int BACKSPACE = 10;
    static final int TAB = 11;
    static final int LINEFEED = 12;
    static final int VERTICAL_TAB = 13;
    static final int NEW_PAGE = 14;
    static final int CARRIAGE_RETURN = 15;
    static final int WHITESPACE = 32;
    static final int REC_LINE_LENGTH = 200;
    static final int TERMINAL_LENGTH = 500;
    
    private int currentColumn;
    private int currentLine;
    private char currentChar;
    
    private PushbackReader targetFile; 
   
    private static final Vector<String> keywords = new Vector();
    private static final Vector<String> logics = new Vector();
    private static final Vector<String> types = new Vector();
    
    this.keywords.add("ALTER");
    this.keywords.add("AND");
    this.keywords.add("ASC");
    this.keywords.add("AVG");
    this.keywords.add("COUNT");
    this.keywords.add("CREATE");
    this.keywords.add("DESC");
    this.keywords.add("DROP");
    this.keywords.add("FROM");
    this.keywords.add("INSERT INTO");
    this.keywords.add("IS");
    this.keywords.add("MAX");
    this.keywords.add("MIN");
    this.keywords.add("NOT");
    this.keywords.add("NULL");
    this.keywords.add("ORDER BY");
    this.keywords.add("SELECT");
    this.keywords.add("SUM");
    this.keywords.add("TABLE");
    this.keywords.add("TRUNCATE");
    this.keywords.add("VALUES");
    this.keywords.add("WHERE");
    
    this.logics.add("AND");
    this.logics.add("OR");
    this.logics.add("NOT");
    
    this.type.add("INT");
    this.type.add("REAL");
    this.type.add("VARCHAR");
    
    void setTargetFile(PushbackReader file) {
        targetFile = file;
    }
    
    Vector<String> parseQuery(String command){
        Vector<String> result = new Vector<String>();
        readKeyword("SELECT");
        readValuesIntoResult(result, "VARCHAR");
        result.add("-");
        readKeyword("FROM");
        String tableName = readValue("VARCHAR");
        result.add(tableName);
        if (checkNextChar() == ';') {
            readNextChar();
            return result;
        }
        else if  (pageHasWord("WHERE") ){
            readKeyword("WHERE");
            readConditionsIntoResult(result);
        }
        readChar(';');
        return result;
    }
    
    Vector<String> parseTableCreation(String command){
        Vector<String> result = new Vector<String>();
        readKeyword("CREATE");
        readKeyword("TABLE");
        readChar('(');
        String tableName = readValue("VARCHAR");
        result.add(tableName);
        String type = readType();
        result.add(type);
        readChar(')');
        readChar(';');
        return result;
    }
   
    Vector<String> parseInsertion(String command){
        Vector<String> result = new Vector<String>();
        readKeyword("INSERT INTO");
        readChar('(');
        readValuesIntoResult(result, "VARCHAR");
        readChar(')');
        result.add("-");
        readChar('(');
        readKeyword("VALUES");
        String value = checkPageValue();
        readValuesIntoResult(result, value);
        readChar(')');
        readChar(';');
        return result;
    }
   
    Vector<String> parseTableDrop(String command){
        Vector<String> result = new Vector<String>();
        readKeyword("DROP");
        readKeyword("TABLE");
        String tableName = readValue("VARCHAR");
        result.add(tableName);
        readChar(';');
        return result;
    }
   
    Vector<String> parseTableTruncation(String command){
        Vector<String> result = new Vector<String>();
        readKeyword("TRUNCATE");
        readKeyword("TABLE");
        String tableName = readValue("VARCHAR");
        result.add(tableName);
        readChar(';');
        return result;
    }
    
    //helper functions
    
    boolean isNameChar(char targetChar, boolean isFirstChar){
        if (isAlphabetical(targetChar))
            return true;
        if (!isFirstChar){
            if ('0' <= targetChar && targetChar <= '9')
                return true;
        }
        return isAlphabetical(targetChar);
    }
    
    boolean isAlphabetical(char targetChar) {
        return  ('A' <= targetChar && targetChar <= 'Z') ||
                ('a' <= targetChar && targetChar <= 'z');
    }
    
    boolean isIntChar(char targetChar) {
        return ('0' <= targetChar && targetChar <= '9');
    }
    
    boolean isRealChar(char targetChar, boolean decimalReached) {
        if (!decimalReached)
            return targetChar == '.';
        return isIntChar(targetChar);
    }
    
    char readNextChar() throws IOException{
        char nextChar = (char)targetFile.read();
        currentChar = nextChar;
        currentColumn += 1;
        return currentChar;
    }
    
    void readChar(char target) {
        if (checkNextChar() != target)
            exitWithErrorMessage("unexpected char");
        readNextChar();
    }
    
    char checkNextChar() throws IOException{
        char nextChar = (char)targetFile.read();
        targetFile.unread(nextChar);
        return nextChar;
    }
    
    String readWhiteSpace() throws IOException{
        String whiteSpace = "";
        while ( ( isWhiteSpace(checkNextChar()) ) ) {
            whiteSpace += readNextChar();
            if ( currentChar == '\n'){
                currentColumn = 0;
                currentLine += 1;
            }
        }
        return whiteSpace;
    }
    
    boolean isWhiteSpace(char target) {
        return( 
        (target == WHITESPACE)      || 
        (target == BACKSPACE)       ||
        (target == TAB)             ||
        (target == LINEFEED)        ||
        (target == VERTICAL_TAB)    ||
        (target == NEW_PAGE)        ||
        (target == CARRIAGE_RETURN) ||
        (target == '\n')
        );
    }
    
    String readValue(String valueType) throws IOException{
        String value = "";
        boolean isFirstChar = true;
        boolean decimalReached = false;
        readWhiteSpace();
        while (true){
            char nextChar = checkNextChar();
            boolean validChar;
            if (valueType == "NAME")
                validChar = isNameChar(nextChar, isFirstChar);
            if (valueType == "VARCHAR")
                validChar = isAlphabetical(nextChar);
            if (valueType == "INT")
                validChar = isIntChar(nextChar);
            if (valueType == "REAL")
                validChar = isRealChar(nextChar, decimalReached);
            if (validChar){
                if (nextChar == '.')
                    decimalReached = true;
                value += readNextChar();
                isFirstChar = false;
                continue;
            }
            else if ( ( (nextChar == ' ') )) {
                readWhiteSpace();
                return value;
            }
            else
                exitWithErrorMessage("invalid name character");
        }
    }
    
    String checkNextValue() {
        String valueType;
        int startColumn = currentColumn;
        
    }
    
    String readCondition() {
        String condition;
        condition += readValue("NAME");
        condition += "-";
        condition += readOperator();
        condition += "-";
        String value = checkPageValue();
        condition += readValue(value);
        return condition;
    }
    
    void readConditionsIntoResult(Vector<String> result) {
        int terminalCount = 0;
        String nextCondition = "";
        readWhiteSpace();
        while ( (terminalCount < TERMINAL_LENGTH) && !(checkNextChar() == ';'))
        {
            if (pageHasWord("NOT")) {
                readKeyword("NOT");
                result.add("NOT");
                result.add("-");
                terminalCount += 3;
            }
            nextCondition += readCondition();
            result.add(nextCondition);
            terminalCount += nextCondition.length();
            if (pageHasWord("OR")) {
                readKeyword("OR");
                result.add("OR");
                result.add("-");
                terminalCount += 2;
                continue;
            }
            if (pageHasWord("AND")) {
                readKeyword("AND");
                result.add("AND");
                result.add("-");
                terminalCount += 3;
                continue;
            }
        }
        if (checkNextChar() == ';')
            return;
        else
            exitWithErrorMessage("command length");
    }
    
    void readValuesIntoResult(Vector<String> result, String valueType) {
        int terminalCount = 0;
        String temp;
        while ( (terminalCount < TERMINAL_LENGTH) && !pageHasWord("FROM") ){
            if (valueType == "VARCHAR")
                temp = readValue("VARCHAR");
            else if (valueType == "INT")
                temp = readValue("INT");
            else //valueType == "REAL"
                temp = readValue("REAL");
            terminalCount += temp.length();
        }
        if (pageHasWord("FROM"))
            return;
        else
            exitWithErrorMessage("command length");
    }
    
    String readAhead(int desiredLength, boolean toWhitespace) {
        char[] target = new char[desiredLength];
        int start = currentColumn;
        int index = 0;
        if (toWhitespace) {
            while (!isWhiteSpace(checkNextChar())) {
                target[index] = readNextChar();
                index++;
            }
        }
        else{
            while (index < desiredLength) { 
                target[index] = readNextChar();
                index++;
            }
        }
        targetFile.unread(target);
        currentColumn = start;
        return target.toString();
    }
    
    boolean pageHasWord(String known) {
        String unknown = readAhead(known.length(), BY_COUNT);
        return unknown.equals(known);
    }
    
    String checkPageValue() {
        String unknown = readAhead(NO_LENGTH, TO_SPACE);
        char target;
        boolean hasAlphabetical = false;
        boolean hasNumerical = false;
        int periodCount = 0;
        boolean hasOther = false;
        for (int index = 0; index < unknown.length(); index++) {
            target = unknown.charAt(index);
            if (isAlphabetical(target))
                hasAlphabetical = true;
            else if (isIntChar(target))
                hasNumerical = true;
            else if (target == '.')
                periodCount += 1;
            else
                hasOther = true;
        }
        if (hasAlphabetical && hasNumerical)
            return "INVALID";
        else if (hasAlphabetical && (periodCount > 0))
            return "INVALID";
        else if (hasNumerical && (periodCount > 1))
            return "INVALID";
        else if (hasOther)
            return "INVALID";
        else if (hasAlphabetical)
            return "VARCHAR";
        else if (hasNumerical && (periodCount == 1))
            return "REAL";
        else //hasNumerical && periodCount == 0
            return "INT";
    }
    
    boolean nameIsKeyword(String name) {
        String upperCaseName = name.toUpperCase();
        return (keywords.contains(upperCaseName));
    }
    
    void readKeyword(String keyword){
        if (pageHasWord(keyword))
            readValue("VARCHAR");
        else
            exitWithErrorMessage("");
    }
    
    boolean nameIsType(String name) {
        String upperCaseName = name.toUpperCase();
        return (types.contains(upperCaseName));
    }
    
    boolean stringIsOperator(String candidate) {
        return (candidate == "="  ||
                candidate == "!=" ||
                candidate == ">"  ||
                candidate == ">=" ||
                candidate == "<"  ||
                candidate == "<="
                );
    }
    
    boolean pageHasType() {
        return (pageHasWord("VARCHAR") || pageHasWord("INT") || pageHasWord("REAL"));
    }
    
    int pageHasOperator() {
        String candidateOne = readAhead(MIN_OP_SIZE, BY_COUNT);
        String candidateTwo = readAhead(MAX_OP_SIZE, BY_COUNT);
        if (stringIsOperator(candidateOne))
            return MIN_OP_SIZE;
        else if (stringIsOperator(candidateTwo))
            return MAX_OP_SIZE;
        else
            return NO_OP;
    }
    
    String readType(){
        String type;
        if (pageHasType()) 
            type = readValue("VARCHAR");
        else
            exitWithErrorMessage("");
        return type;
    }
    
    String readOperator() {
        String operator;
        int operatorSize = pageHasOperator();
        if (operatorSize == MAX_OP_SIZE) {
            operator += readNextChar();
            operator += readNextChar();
        }
        else if (operatorSize == MIN_OP_SIZE) {
            operator += readNextChar();
        }
        else
            exitWithErrorMessage("no op");
    }
    
    void printErrorLocation(){
        System.out.println("At line " + currentLine + " " + 
                            "column " + currentColumn + " :");
    }
    
    void exitWithErrorMessage(String errorSymbol){
        printErrorLocation();
        switch (errorSymbol){
        case "unexpected char":
            System.out.println("Unexpected character was encountered");
        case "invalid name character":
            System.out.println("Invalid character encountered in name");
        case "command length":
            System.out.println("Command's length was too long (500 char max)");
        case "no op":
            System.out.println("failed read attempt, operator not found");
        }
        System.exit(1);
    }
} 