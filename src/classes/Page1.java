package src.classes;

import java.io.*;
import java.util.*;
import src.DBApp;
import src.exceptions.DBAppException;

public class Page1 {
    final static int N = DBApp.N;

    int intPageNum;
    Boolean isUpdated = false;
    String clusterMin;
    String clusterMax;
    Table parentTable;
    ArrayList<Hashtable<String, String>> arrTuples = new ArrayList<Hashtable<String, String>>();
    File PAGE_PATH; 

    public Page1(Table tbl, int intPageNum) {
        this.parentTable = tbl;
        this.intPageNum = intPageNum;
        this.PAGE_PATH = new File("./src/tables/" + parentTable.getName() + "/" + intPageNum + ".csv");
        load();
        System.out.println("Page min: " + clusterMin + "\nPage max: " + clusterMax);
    }

    /**
     * Load existing tuples into this page
     */
    public void load() {
        if (!this.PAGE_PATH.exists()) return;

        try {
            String line = "";
            BufferedReader pageFile = new BufferedReader(new FileReader(PAGE_PATH));
            
            while ((line = pageFile.readLine()) != null) {
                Hashtable<String, String> tuple = breakTuple(line);
                arrTuples.add(tuple);
            }

            clusterMin = arrTuples.get(0).get(parentTable.getClusteringKey());
            clusterMax = arrTuples.get(arrTuples.size() - 1).get(parentTable.getClusteringKey());

            pageFile.close();
        } catch (Exception e) {
            System.err.println("Failed to load page " + intPageNum + " for table " + parentTable.getName());
        }
    }

    public boolean isFull() {
        return arrTuples.size() == N;
    }

    /**
     * Following method inserts one row only.
     * @param htblColNameValue must include a value for the primary key.
     * @throws DBAppException
     */
    public void insertIntoPage(Hashtable<String,Object> htblColNameValue) throws DBAppException {
        // Ensure page has space
        if (this.isFull()) return;

        // Insert into place based on clustering key
        int arrSize = arrTuples.size();
        if (arrSize < 1) {
            arrTuples.add(stringify(htblColNameValue));
            isUpdated = true;
            return;
        }


        String colNameClusteringKey = parentTable.getClusteringKey();
        for (int i = 0; i < arrSize; i++) {
            String curr = arrTuples.get(i).get(colNameClusteringKey);
            Object insert = htblColNameValue.get(colNameClusteringKey);

            int compare = Functions.cmpObj(curr, insert, parentTable.getColType(colNameClusteringKey));

            // Insert here
            if (compare == 1) {
                Hashtable<String, String> tempTuple = stringify(htblColNameValue);

                for (int j = i; j < arrSize; j++) {
                    Hashtable<String, String> next = arrTuples.get(j);
                    arrTuples.set(j, tempTuple);
                    tempTuple = next;
                }
                arrTuples.add(tempTuple);
                isUpdated = true;
                return;
            }
        }

        // If reached end of file and not entered, then it must be the greatest
        if (!isUpdated) {
            arrTuples.add(stringify(htblColNameValue));
            isUpdated = true;
            return;
        }
        // TODO: Update grid index
    }

    public void close() throws DBAppException {
        if (!isUpdated) return;
        FileWriter fw;
        try {
            fw = new FileWriter(PAGE_PATH, false);

            for (Hashtable<String, String> htbl: arrTuples) {
                fw.append(buildTuple(htbl));
            }

            fw.flush();
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBAppException("Error saving page " + intPageNum);
        }
    }

    private String buildTuple(Hashtable<String, String> htblColNameValue) {
        ArrayList<String> OrderedColumns = parentTable.getColNames();
        String[] temp = new String[OrderedColumns.size()];

        int i = 0;
        for (String s: OrderedColumns) {
            temp[i++] = htblColNameValue.get(s);
        }

        return String.join(",", temp) + "\n";
    }

    private Hashtable<String, String> breakTuple(String line) {
        Hashtable<String, String> tuple = new Hashtable<String, String>();
        ArrayList<String> OrderedColumns = parentTable.getColNames();
        String[] temp = line.split(",");

        int i = 0;
        for (String s: OrderedColumns) {
            tuple.put(s, temp[i++]);
        }

        return tuple;
    }

    private Hashtable<String, String> stringify(Hashtable<String, Object> tempTuple) {
        Hashtable<String, String> result = new Hashtable<String, String>();

        // Iterate over the given columns
        Enumeration<String> keys = tempTuple.keys();
        while (keys.hasMoreElements()) {
            String colName = keys.nextElement();
            result.put(colName, "" + tempTuple.get(colName));
        }

        return result;
    }
}
