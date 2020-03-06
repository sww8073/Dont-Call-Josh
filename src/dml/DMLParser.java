package dml;

public class DMLParser implements IDMLParser {

    /**
     * This will create an instance of this parser and return it.
     * @return an instance of a IDMLParser
     */
    public static IDMLParser createParser(){
        return null;
    }

    @Override
    public void parseDMLStatement(String statement) throws DMLParserException {}
    
    @Override
    public Object[][] parseDMLQuery(String statement) throws DMLParserException{
        return null;
    }

}
