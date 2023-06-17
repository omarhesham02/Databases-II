package src.classes;

public class GridPoint {
    private int page;
    private int index;
    public GridPoint next;

    public GridPoint(int page, int index) {
        this.page = page;
        this.index = index;
    }

    public int getPage() {
        return this.page;
    }

    public int getIndex() {
        return this.index;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String toString() {
        if (this.next == null) {
            return this.page + "|" + this.index;
        }

        return this.page + "|" + this.index + ";" + this.next.toString();
    }
}


