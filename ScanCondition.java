package minidatabase;

public class ScanCondition {

    static boolean attributeValidatesCondition(Attribute conditionAttribute,
                                                byte[] attribute, 
                                                String operator, 
                                                byte[] threshold) {
    if (conditionAttribute.type == Attribute.AttributeType.VARCHAR) {
        String attributeString = new String(attribute);
        String thresholdString = new String(threshold);
        return compareStrings(attributeString, operator, thresholdString);
        }
    else{
        double attributeNumber = 
        DataConversion.convertBytesToDouble(attribute);
        double thresholdNumber = 
        DataConversion.convertBytesToDouble(threshold);
        return compareNumbers(attributeNumber, operator, thresholdNumber);
        }
    }

    static boolean compareNumbers(double attribute, String operator, double threshold) {
        if (operator == "EQ")
            return attribute == threshold;
        else if (operator == "NE")
            return attribute != threshold;
        else if (operator == "LT")
            return attribute < threshold;
        else if (operator == "LE")
            return attribute <= threshold;
        else if (operator == "GT")
            return attribute > threshold;
        else //operator == "GE"
            return attribute >= threshold;
    }

    static boolean compareStrings(String attribute, String operator, String threshold) {
        int comparisonValue = attribute.compareTo(threshold);
        if (operator == "EQ")
            return comparisonValue == 0;
        else if (operator == "NE")
            return comparisonValue != 0;
        else if (operator == "LT")
            return comparisonValue < 0;
        else if (operator == "LE")
            return comparisonValue <= 0;
        else if (operator == "GT")
            return comparisonValue > 0;
        else //operator == "GE"
            return comparisonValue >= 0;
    }

}
