package src.classes;

import java.io.File;
import java.util.Date;
import java.util.Hashtable;

import src.exceptions.DBAppException;

public class GridIndex {
    final static int NUM_BOUNDS = 3;

    private Object[] xBounds = new Object[NUM_BOUNDS + 1];
    private Object[] yBounds = new Object[NUM_BOUNDS + 1];
    private String strColNamex;
    private String strColNamey;
    private GridPoint[][] gridPoints = new GridPoint[NUM_BOUNDS][NUM_BOUNDS];
    private Table parentTable;
    private Boolean isUpdated = false;
    private String indexName;
    private File INDEX_PATH;
    
    public GridIndex(Table parentTable, String strColNamex, String strColNamey) throws DBAppException {
        this.parentTable = parentTable;
        this.strColNamex = strColNamex;
        this.strColNamey = strColNamey;

        // Ensure columns exist
        if (!parentTable.colExists(strColNamex) || !parentTable.colExists(strColNamey)) {
            throw new DBAppException("Cannot create grid index.\nOne or more of the passed columns don't exist!");
        }

        initialize();
    }

    /**
     * Load an existing grid index
     * @param indexFile
     */
    public GridIndex(Table parentTable, String indexName) {
        this.parentTable = parentTable;
        this.indexName = indexName;
        this.INDEX_PATH = new File("./src/indices/" + parentTable.getName() + "/" + indexName + ".txt");

        // Get column names from index
    }

    // TODO:
    private void initialize() throws DBAppException {
        this.isUpdated = true;
        
        this.indexName = Integer.toString(new Date().hashCode());
        this.INDEX_PATH = new File("./src/indices/" + parentTable.getName() + "/" + indexName + ".txt");

        // Create boundaries
        xBounds = Functions.getBounds(parentTable.getColMin(strColNamex), parentTable.getColMin(strColNamex), NUM_BOUNDS, parentTable.getColType(strColNamex));
        yBounds = Functions.getBounds(parentTable.getColMin(strColNamey), parentTable.getColMin(strColNamey), NUM_BOUNDS, parentTable.getColType(strColNamey));

        // Sift through table linearly and get all the tuples inserted properly
        

        // Update metadata index columns
    }

    // TODO
    public void save() {
        if (!isUpdated) return;

        // Save x boundary

        // Save y boundary
        
        // Loop through rows, then columns

        // Separate rows with new line, columns with commas, gridpoints with |
        

    }

    public void update(int pageNum) {
        isUpdated = true;

    }

    private int[] indexOf(Hashtable<String, String> tuple) throws DBAppException {
        int[] index = {-1, -1};

        // Iterate over boundaries


        // Ensure there was space
        if (index[0] == -1 || index[1] == -1) {
            throw new DBAppException("Cannot find index of a tuple");
        }

        return index;
    }
}
