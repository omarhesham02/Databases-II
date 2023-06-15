package src.classes;

import java.util.ArrayList;
import java.util.Hashtable;

public class TableScanner {
    private Page currPage;
    private Table parentTable;
    private int currTupleIndex = -1;

    public TableScanner(Table parentTable) {
        this.parentTable = parentTable;
        this.currPage = new Page(parentTable, 0);
    }

    public Boolean hasNext() {
        Boolean lastPage = currPage.getNum() >= (parentTable.numPages() - 1);
        Boolean lastTuple = currTupleIndex + 1 >= currPage.size();

        return !(lastPage && lastTuple);
    }

    /**
     * Gets the next tuple linearly in said table
     * @return
     */
    public Hashtable<String, String> next() {
        if (!hasNext()) return null;

        // Increment to next tuple
        currTupleIndex++;

        // If reached end of page, cycle to next page
        if (currTupleIndex >= currPage.size()) {
            currTupleIndex = 0;
            int pageNum = currPage.getNum() + 1;
            currPage = new Page(parentTable, pageNum);
        }

        return currPage.getTuple(currTupleIndex);
    }

    public int getIndex() {
        return currTupleIndex;
    }

    public int getPageNum() {
        return this.currPage.getNum();
    }

    public String buildTuple(Hashtable<String, String> htblColNameValue) {
        ArrayList<String> OrderedColumns = parentTable.getColNames();
        String[] temp = new String[OrderedColumns.size()];

        int i = 0;
        for (String s: OrderedColumns) {
            if (htblColNameValue.get(s) != null) {
            temp[i++] = htblColNameValue.get(s);
            } else {
                temp[i++] = "";
            }
        }

        return String.join(",", temp);
    }
}
