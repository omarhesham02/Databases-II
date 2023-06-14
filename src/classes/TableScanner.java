package src.classes;

import java.io.FileNotFoundException;
import java.nio.file.FileSystemException;
import java.util.Hashtable;

public class TableScanner {
    private Page currPage;
    private Table parentTable;
    private int currTupleIndex = 0;

    public TableScanner(Table parentTable) {
        this.parentTable = parentTable;
        try {
            this.currPage = new Page(parentTable.getName(), currTupleIndex);
        } catch (FileSystemException | FileNotFoundException e) {
            e.printStackTrace();

        }
    }

    public Hashtable<String, String> getNext() {
        // if (currTupleIndex >= )
    }
}
