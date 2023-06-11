import java.util.ArrayList;
import java.util.TreeMap;

public class GridPoint {
    // Each point is a (page, occurences) pair 
    // The page is the page containing the record falling in this grid area based on the indexing keys, and the occurunces (i.e number) of records in this page

    TreeMap<Page, Integer> Point;
    ArrayList<Point> points;
}


