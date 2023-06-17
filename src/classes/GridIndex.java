package src.classes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
    public GridIndex (Table parentTable, String indexName) throws DBAppException{
        System.out.println("Reading index " + indexName + " for table " + parentTable.getName());
        
        this.parentTable = parentTable;
        this.indexName = indexName;
        this.INDEX_PATH = new File("./src/indices/" + parentTable.getName() + "/" + indexName + ".txt");

        if (!INDEX_PATH.exists()) {
            throw new DBAppException("Cannot load index " + indexName + "\nFile does not exist.");
        }

        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(INDEX_PATH));
            String line = br.readLine();

            String [] colNames = line.split(",");
            this.strColNamex = colNames[0];
            this.strColNamey = colNames[1];

            line = br.readLine();
            this.xBounds = parseBounds(line.split(","),  parentTable.getColType(strColNamex));
            line = br.readLine();
            this.yBounds = parseBounds(line.split(","),  parentTable.getColType(strColNamey));

            // Loop over grid
            for (int i = 0; i < NUM_BOUNDS; i++) {
                // Loop each row
                String[] row = br.readLine().split(",");
                for (int j = 0; j < NUM_BOUNDS; j++) {
                    // Loop each point of row
                    String[] points = row[j].split(";");
                    for (int k = 0; k < points.length; k++) {
                        if (points[k].equals("NULL")) continue;

                        String[] pointSplit = points[k].split("|");
                        int page = Integer.parseInt(pointSplit[0]);
                        int index = Integer.parseInt(pointSplit[2]);

                        // Add tuple to grid
                        if (gridPoints[j][i] == null) {
                            gridPoints[j][i] = new GridPoint(page, index);
                            continue;
                        }

                        GridPoint curr = gridPoints[j][i];
                        while (curr.next != null) {
                            curr = curr.next;
                        }
                        curr.next = new GridPoint(page, index);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Object[] parseBounds(String[] arrstrBounds, String colType) {
        Object[] result = null;
        switch (colType) {
            case "java.lang.String": {
                return arrstrBounds;
            }
            case "java.lang.Integer": {
                result = new Integer[arrstrBounds.length];

                for (int i = 0; i < arrstrBounds.length; i++) {
                    result [i] = Integer.parseInt(arrstrBounds[i]);
                }

                break;
            }
            case "java.lang.Double": {
                result = new Double[arrstrBounds.length];

                for (int i = 0; i < arrstrBounds.length; i++) {
                    result [i] = Double.parseDouble(arrstrBounds[i]);
                }

                break;
            }
            case "java.util.Date": {
                result = new Date[arrstrBounds.length];
                SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");

                try {
                    for (int i = 0; i < arrstrBounds.length; i++) {
                        result [i] = dateFormat.parse(arrstrBounds[i]);
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                break;
            }
        }
        return result;
    }

    public void initialize() throws DBAppException {
        this.isUpdated = true;
        
        if (this.indexName == null) {
            this.indexName = Integer.toString(new Date().hashCode() * -1);
        }
        this.INDEX_PATH = new File("./src/indices/" + parentTable.getName() + "/" + indexName + ".txt");

        // Create boundaries
        String xColType = parentTable.getColType(strColNamex);
        String yColType = parentTable.getColType(strColNamey);

        xBounds = Functions.getBounds(parentTable.getColMin(strColNamex), parentTable.getColMax(strColNamex), NUM_BOUNDS, xColType);
        yBounds = Functions.getBounds(parentTable.getColMin(strColNamey), parentTable.getColMax(strColNamey), NUM_BOUNDS, yColType);

        // Sift through table linearly and get all the tuples inserted properly
        TableScanner scanner = new TableScanner(parentTable);
        this.gridPoints = new GridPoint[NUM_BOUNDS][NUM_BOUNDS];

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

        if (INDEX_PATH.exists()) {
            this.save();
            return;
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
            parentTable.loadMetadata();
            this.save();
        } catch (IOException e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public void save() {
        if (!isUpdated) return;

        // Create directory if doesn't exist
        File pathWithoutFile = new File("./src/indices/" + parentTable.getName());
        if (!pathWithoutFile.exists()) {
            pathWithoutFile.mkdirs();
        }

        try {
            FileWriter fw = new FileWriter(INDEX_PATH, false);
            fw.write(this.toString());
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // TODO:
    public void update(int pageNum) {
        isUpdated = true;

    }

    private String objToString(Object x) {
        if (x instanceof Date) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");
            return dateFormat.format(x);
        }

        return x.toString();
    }

    /**
     * Converts this grid index into a string in the following format:
     * 
     * columnx,columny
     * xBounds (separated by commas)
     * yBounds (separated by commas)
     * Grid (grid separated into rows on each line and columns separated by columns)
     * 
     * Each column in the grid contains "NULL" or grid point(s) separated by bars |
     */
    public String toString() {
        // Column names columnx,columny
        String result = strColNamex + "," + strColNamey + "\n";

        // Column x bounds
        result += objToString(xBounds[0]);   
        for (int i = 1; i < xBounds.length; i++) {
            result += "," + objToString(xBounds[i]);
        }
        result += "\n";
        
        // Column y bounds
        result += objToString(yBounds[0]);   
        for (int i = 1; i < yBounds.length; i++) {
            result += "," + objToString(yBounds[i]);
        }
        result += "\n";
        
        // Grid 
        for (int y = 0; y < NUM_BOUNDS; y++) {
            String[] row = new String[NUM_BOUNDS];

            for (int x = 0; x < NUM_BOUNDS; x++) {
                if (gridPoints[x][y] == null) {
                    row[x] = "NULL";
                } else {
                    row[x] = gridPoints[x][y].toString();
                }
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


    public Hashtable<String, String> insert(Hashtable<String, Object> tuple) throws DBAppException {
        Boolean isInserted = false;
        Hashtable<String, String> strTuple = Page.stringify(tuple, parentTable);
        Hashtable<String, String> kickedOut = null;
        int[] indexOnGrid = indexOf(strTuple);

        GridPoint point = new GridPoint(0, 0);

        // Check grid point that it belongs to
        // If null, insert based on next smaller index
        int x = indexOnGrid[0];
        int y = indexOnGrid[1];
        if (gridPoints[x][y] == null) {
            if (parentTable.getClusteringKey().equals(strColNamex)) {
                x--;
            } else {
                y--;
            }
            while (x >= 0 && y >= 0) {
                
                if (gridPoints[x][y] == null) {
                    if (parentTable.getClusteringKey().equals(strColNamex)) {
                        x--;
                    } else {
                        y--;
                    }
                    continue;
                } 
                
                GridPoint curr = gridPoints[x][y];
                while (curr.next != null) {
                    if (curr.getPage() > point.getPage()) {
                        point.setPage(curr.getPage());
                        point.setIndex(curr.getIndex() + 1);
                    }
                    curr = curr.next;
                }
                if (parentTable.getClusteringKey().equals(strColNamex)) {
                    x--;
                } else {
                    y--;
                }
                break;
            }

            Page curr = new Page(parentTable, point.getPage());

            if (curr.isFull()) {
                curr.close();
                point.setPage(point.getPage() + 1);
                point.setIndex(0);
                curr = new Page(parentTable, point.getPage());
            }

            kickedOut = curr.insertIntoPage(tuple, point.getIndex());
            isInserted = true;
            curr.close();

            gridPoints[indexOnGrid[0]][indexOnGrid[1]] = point;
        } else {
            // If not null comb through indices, finding the smallest index greater than insert
            GridPoint curr = gridPoints[x][y];

            Page currPage = null;
            String clusterType = parentTable.getColType(parentTable.getClusteringKey());
            Object inputVal = tuple.get(parentTable.getClusteringKey());
            int greatestPage = curr.getPage();
            int greatestIndex = curr.getIndex() == 0 ? 0 : curr.getIndex() - 1;
            GridPoint toInsert = new GridPoint(greatestPage, greatestIndex);
            while (curr != null) {
                if (currPage == null || currPage.getNum() != curr.getPage()) {
                    currPage = new Page(parentTable, curr.getPage());
                }

                Hashtable<String, String> pointTuple = currPage.getTuple(curr.getIndex());
                String clusterVal = pointTuple.get(parentTable.getClusteringKey());
                int comparator = Functions.cmpObj(clusterVal, inputVal, clusterType);

                if (comparator == 0) {
                    throw new DBAppException("Cannot insert duplicate cluster value " + inputVal);
                }

                // Place after
                if (comparator == -1) {
                    if (curr.getPage() > greatestPage) {
                        greatestPage = curr.getIndex();
                        greatestIndex = curr.getIndex();
                    }
                }

                curr = curr.next;
            }

            toInsert.setPage(greatestPage);
            toInsert.setIndex(greatestIndex);

            if (currPage == null || currPage.getNum() != toInsert.getPage()) {
                currPage = new Page(parentTable, toInsert.getPage());
            }

            int maxComp = Functions.cmpObj(currPage.getMaxCluster(), inputVal, clusterType);
            if (currPage.isFull() && maxComp < 1) {
                toInsert.setIndex(0);
                toInsert.setPage(greatestIndex + 1);
                currPage = new Page(parentTable, toInsert.getPage());
            }

            kickedOut = currPage.insertIntoPage(tuple, toInsert.getIndex());
            currPage.close();
            isInserted = true;
        }

        // Ensure there was space
        if (!isInserted) {
            throw new DBAppException("Cannot find space to insert tuple using index " + indexName);
        }

        initialize();
        return kickedOut;
    }


    public void updateTuple(String clusterVal, Hashtable<String, Object> newValues) throws DBAppException {
        // Find where this clusterVal is
        String colName = parentTable.getClusteringKey();
        String colType = parentTable.getColType(colName);
        Object objClusterVal = strToObj(clusterVal, colType);

        // Set search space accordingly
        int index = indexOf(clusterVal, colName);
        int yStart = 0;
        int xStart = 0;
        int xEnd = NUM_BOUNDS;
        int yEnd = NUM_BOUNDS;
        if (strColNamex.equals(colName)) {
            xStart = index;
            xEnd = index + 1;
        } else if (strColNamey.equals(colName)) {
            yStart = index;
            yEnd = index + 1;
        }

        Page currPage = null;
        for (int y = yStart; y < yEnd; y++) {

            for (int x = xStart; x < xEnd; x++) {
                GridPoint currNode = gridPoints[x][y];
                if (currNode == null) continue;

                // Loop through nodes in this slot
                while (currNode != null) {
                    // Load tuple in this node
                    if (currPage == null || currPage.getNum() != currNode.getPage()) {
                        currPage = new Page(parentTable, currNode.getPage());
                    }
                    Hashtable<String, String> tuple = currPage.getTuple(currNode.getIndex());

                    int comparator = Functions.cmpObj(tuple.get(colName), objClusterVal, colType);

                    if (comparator != 0) {
                        currNode = currNode.next;
                        continue;
                    }

                    currPage.updateTuple(currNode.getIndex(), newValues);
                    currPage.close();
                    initialize();
                    return;
                }
            }
        }

        throw new DBAppException("Cannot update tuple.\nCluster value: " + clusterVal + " doesn't exist.");
    }

    public static Object strToObj(String value, String type) throws DBAppException {
        switch (type) {
            case "java.lang.String": {
                return value;
            }
            case "java.lang.Double": {
                return Double.parseDouble(value);
            }
            case "java.lang.Integer": {
                return Integer.parseInt(value);
            }
            case "java.util.Date": {
                SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");
                try {
                    return dateFormat.parseObject(value);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            default: {
                throw new DBAppException("Invalid data type " + type + " while converting in index");
            }
        }
    }

    public String getName() {
        return this.indexName;
    }

    public ArrayList<String> query(SQLTerm[] arrSQLTerms, String[] strarrOperators) {
        ArrayList<String> result = new ArrayList<String>();
        // TODO
        return result;
    }

    private int indexOf(String value, String colName) throws DBAppException {
        Object[] bounds;
        if (colName.equals(strColNamex)) {
            bounds = xBounds;
        } else if (colName.equals(strColNamey)) {
            bounds = yBounds;
        } else {
            throw new DBAppException("Trying to get index " + colName + " in grid index " + this.indexName);
        }

        // Iterate over boundaries
        for (int x = 1; x <= NUM_BOUNDS; x++) {
            int comparator = Functions.cmpObj(value, bounds[x], parentTable.getColType(colName));

            if (comparator < 0) {
                return x - 1;
            }
        }

        throw new DBAppException("Index in boundaries for " + colName + ": " + value + " not found.");
    }

    public Hashtable<String, String> forceInsert(Hashtable<String, String> kickedOut) throws DBAppException {
        // Parse it
        Enumeration<String> colNames = kickedOut.keys();
        Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
        while (colNames.hasMoreElements()) {
            String colName = colNames.nextElement();
            Object parsed = GridIndex.strToObj(kickedOut.get(colName), parentTable.getColType(colName));
            htblColNameValue.put(colName, parsed);
        }

        return insert(htblColNameValue);
    }

    public void deleteFrom(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        int xStart = 0;
        int yStart = 0;
        int xEnd = NUM_BOUNDS;
        int yEnd = NUM_BOUNDS;

        Enumeration<String> keys = htblColNameValue.keys();
        while (keys.hasMoreElements()) {
            String colName = keys.nextElement();
            Object currObj = htblColNameValue.get(colName);

            if (colName.equals(strColNamex)) {
                int index = indexOf(objToString(currObj), colName);
                xStart = index;
                xEnd = index + 1;
            } else if (colName.equals(strColNamey)) {
                int index = indexOf(objToString(currObj), colName);
                yStart = index;
                yEnd = index + 1;
            }
        }

        Page currPage = null;
        Hashtable<Integer, ArrayList<Integer>> foundList = new Hashtable<Integer, ArrayList<Integer>>();  
        for (int y = yStart; y < yEnd; y++) {
            for (int x = xStart; x < xEnd; x++) {
                GridPoint curr = gridPoints[x][y];
                
                while (curr != null) {
                    if (currPage == null || currPage.getNum() != curr.getPage()) {
                        currPage = new Page(parentTable, curr.getPage());
                    }

                    boolean flag = true;
                    Hashtable<String, String> tuple = currPage.getTuple(curr.getIndex());
                    Enumeration<String> keys2 = htblColNameValue.keys();
                    while (keys2.hasMoreElements()) {
                        String colName2 = keys2.nextElement();

                        int comparator = Functions.cmpObj(tuple.get(colName2), htblColNameValue.get(colName2), parentTable.getColType(colName2));

                        if (comparator != 0) {
                            flag = false;
                            break;
                        }
                    }

                    if (flag) {
                        if (foundList.containsKey(curr.getPage())) {
                            (foundList.get(curr.getPage())).add(curr.getIndex());
                        } else {
                            ArrayList<Integer> i = new ArrayList<Integer>();
                            i.add(curr.getIndex());
                            foundList.put(curr.getPage(), i);
                        }

                    }

                    curr = curr.next;
                }
            }
        }

        // Delete
        Enumeration<Integer> keys2 = foundList.keys();
        while (keys2.hasMoreElements()) {
            int pageNum = keys2.nextElement();
            currPage = new Page(parentTable, pageNum);

            ArrayList<Integer> indexes = foundList.get(pageNum);
            Collections.sort(indexes);

            for (int i = (indexes.size()-1); i >= 0; i--) {
                currPage.deleteTuple(indexes.get(i));
            }
            currPage.close();
        }
    }
}
