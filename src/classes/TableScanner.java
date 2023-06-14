package src.classes;

import java.util.Hashtable;

public class TableScanner {
    private Page currPage;
    private Table parentTable;
    private int currTupleIndex = -1;

    public TableScanner(Table parentTable) {
        this.parentTable = parentTable;
        this.currPage = new Page(parentTable, currTupleIndex);
    }

    public Boolean hasNext() {
        Boolean lastPage = currPage.getNum() >= (parentTable.numPages() - 1);
        Boolean lastTuple = currTupleIndex >= currPage.size();

        return !(lastPage && lastTuple);
    }

    /**
     * Gets the next tuple linearly in said table
     * @return
     */
    public Hashtable<String, String> getNext() {
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
}
