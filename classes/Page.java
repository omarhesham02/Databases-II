package classes;

import java.io.File;

public class Page {
    int intPageNum;
    int intTupleCount;
    String strTableName;
    String[] strarrTuples;

    public Page(String strTableName, int intPageNum) {
        File PAGE_PATH = new File("./tables/" + strTableName + "/" + intPageNum);
    }
}
