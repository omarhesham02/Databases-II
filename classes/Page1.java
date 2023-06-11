package classes;

import java.io.File;
import java.util.Hashtable;

import exceptions.DBAppException;

public class Page1 {
    int intPageNum;
    int intTupleCount;
    String strTableName;
    String[] strarrTuples;

    public Page1(String strTableName, int intPageNum) {
        File PAGE_PATH = new File("./tables/" + strTableName + "/" + intPageNum);
    }

    public boolean isFull() {
        return true;
    }

    /**
     * Following method inserts one row only.
     * @param htblColNameValue must include a value for the primary key.
     * @throws DBAppException
     */
    public void insertIntoTable(Hashtable<String,Object> htblColNameValue) throws DBAppException {

    }
}
