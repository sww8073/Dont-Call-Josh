package ddl;

import java.io.Console;

public class DDLTester {
    public static void main(String[] args) {
        DDLParser parser = new DDLParser();
        try {
            parser.parseDDLstatement("create table foo(" +
                    "baz integer," +
                    "bar Double notnull," +
                    "baz Integer," +
                    "unique (bar baz )," +
                    "primarykey( bar baz )," +
                    "foreignkey( bar ) references bazzle( baz )" +
                    ");");
            parser.parseDDLstatement("drop table foo;");
        }
        catch (DDLParserException e){
            System.out.println(e.toString());
        }
    }
}
