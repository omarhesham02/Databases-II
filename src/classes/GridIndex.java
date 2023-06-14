package src.classes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
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
        System.out.println("Creating new index for table " + parentTable.getName() + " on columns: " + strColNamex + ", " + strColNamey);
        
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
        System.out.println("Reading index " + indexName + " for table " + parentTable.getName());
        
        this.parentTable = parentTable;
        this.indexName = indexName;
        this.INDEX_PATH = new File("./src/indices/" + parentTable.getName() + "/" + indexName + ".txt");

        // Get column names from index
    }

    // TODO:
    private void initialize() throws DBAppException {
        this.isUpdated = true;
        
        this.indexName = Integer.toString(new Date().hashCode() * -1);
        this.INDEX_PATH = new File("./src/indices/" + parentTable.getName() + "/" + indexName + ".txt");

        // Create boundaries
        String xColType = parentTable.getColType(strColNamex);
        String yColType = parentTable.getColType(strColNamey);

        xBounds = Functions.getBounds(parentTable.getColMin(strColNamex), parentTable.getColMax(strColNamex), NUM_BOUNDS, xColType);
        yBounds = Functions.getBounds(parentTable.getColMin(strColNamey), parentTable.getColMax(strColNamey), NUM_BOUNDS, yColType);

        System.out.println(strColNamex + ": " + Arrays.toString(xBounds));
        System.out.println(strColNamey + ": " + Arrays.toString(yBounds));
        // Sift through table linearly and get all the tuples inserted properly
        TableScanner scanner = new TableScanner(parentTable);

        while (scanner.hasNext()) {
            Hashtable<String, String> tuple = scanner.next();
            
            int[] indices = indexOf(tuple);

            // Add tuple to grid
            if (gridPoints[indices[0]][indices[1]] == null) {
                gridPoints[indices[0]][indices[1]] = new GridPoint(scanner.getPageNum(), scanner.getIndex());
                continue;
            }

            GridPoint curr = gridPoints[indices[0]][indices[1]];
            while (curr.next != null) {
                curr = curr.next;
            }
            curr.next = new GridPoint(scanner.getPageNum(), scanner.getIndex());
        }

        // Update metadata index columns
        BufferedReader metaR;
        FileWriter metaW;
        try {
            String line = "";
            StringBuilder build = new StringBuilder();
            metaR = new BufferedReader(new FileReader("./src/metadata.csv"));

            while ((line = metaR.readLine()) != null) {
                String[] row = line.split(",");

                // If not table or column, continue
                if (!row[0].equals(parentTable.getName()) || !(row[1].equals(strColNamex) || row[1].equals(strColNamey))) {
                    build.append(line + "\n");
                    continue;
                }

                row[4] = this.indexName;
                row[5] = "grid";

                build.append(String.join(",", row) + "\n");
            }
        
            // Close metadata file
            metaW = new FileWriter("./src/metadata.csv", false);
            metaW.append(build.toString());
            metaW.flush();
            metaW.close();
            metaR.close();
        } catch (IOException e) {
            throw new DBAppException(e.getMessage());
        }
    }

    // TODO
    public void save() {
        if (!isUpdated) return;

        // Save x boundary

        // Save y boundary
        
        // Loop through rows, then columns

        // Separate rows with new line, columns with commas, gridpoints with |
        

    }

    // TODO:
    public void update(int pageNum) {
        isUpdated = true;

    }

    public String toString() {
        String result = strColNamex + "," + strColNamey + "\n";


        for (int i = 0; i < NUM_BOUNDS; i++) {
            String[] row = new String[NUM_BOUNDS];

            for (int j = 0; j < NUM_BOUNDS; j++) {
                if (gridPoints[j][i] == null) {
                    row[j] = "NULL";
                    continue;
                } 

                row[j] = gridPoints[j][i].toString();
            }

            result += String.join(",", row) + "\n";
        }

        return result;
    }

    private int[] indexOf(Hashtable<String, String> tuple) throws DBAppException {
        int[] index = {-1, -1};

        // Iterate over boundaries
        for (int x = 1; x <= NUM_BOUNDS; x++) {
            String xVal = tuple.get(strColNamex);
            int comparator = Functions.cmpObj(xVal, xBounds[x], parentTable.getColType(strColNamex));

            if (comparator < 0) {
                index[0] = x - 1;
                break;
            }
        }

        for (int y = 1; y <= NUM_BOUNDS; y++) {
            String yVal = tuple.get(strColNamey);
            int comparator = Functions.cmpObj(yVal, yBounds[y], parentTable.getColType(strColNamey));

            if (comparator < 0) {
                index[1] = y - 1;
                break;
            }
        }

        // Ensure there was space
        if (index[0] == -1 || index[1] == -1) {
            throw new DBAppException("Cannot find index of a tuple");
        }

        return index;
    }
}
