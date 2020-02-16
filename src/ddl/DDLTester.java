package ddl;

import java.io.Console;

public class DDLTester {
    public static void main(String[] args) {
        DDLParser parser = new DDLParser();
        try {
            parser.parseDDLstatement("create table foo(" +
                    "baz integer," +
                    "bar Double notnull," +
                    "foo char(5) primarykey," +
                    "foo2 varchar(10) unique," +
                    "primarykey( bar baz )," +
                    "foreignkey( bar ) references bazzle( baz )" +
                    ");");
        }
        catch (DDLParserException e){
            System.out.println(e.toString());
        }
    }
}
