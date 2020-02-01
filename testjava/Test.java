public class Test {
    public static void main(String[] args) {
        String varChar = "varchar(40)";
        int beginIndex = varChar.indexOf("(") + 1;
        int endIndex = varChar.indexOf(")") - 1;
        int num = Integer.parseInt(varChar.substring(beginIndex, endIndex));

        System.out.println(num);

    }
}