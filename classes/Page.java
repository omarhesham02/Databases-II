package classes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileSystemException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;

public class Page {

    private String strTableName;
    private int intPageNum;
    private int intCountTuples;
    private boolean isModified;

    /**
     * Returns whether the page is full or not by comparing the
     * number of tuples currently present in the page with the maximum per page
     * listed in the config file
     */

    public boolean isFull() {
        // if this.intCountTuples == config.maxTuples 
        return false;
    }


    public Page(String strTableName, int intPageNum) throws FileSystemException, FileNotFoundException{
        this.intPageNum = intPageNum;
        File PAGE_PATH = new File("./tables/" + strTableName + "/" + intPageNum);
        PrintWriter pw = new PrintWriter(PAGE_PATH);

    }

    public void insertIntoPage(Hashtable<String, Object> tuple) throws FileNotFoundException {
        PrintWriter pw = new PrintWriter(this.intPageNum + ".csv");

        StringBuilder csvData = new StringBuilder();

        // Get all attributes of the table containing this page
        ArrayList<String> strAttributes = new ArrayList<>();
        
         String line = "";  
        String splitBy = ",";  
        try {
            // Iterate over rows in metadata
            BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));  
            
            // Look for this table
            while ((line = br.readLine()) != null) {  
                String[] row = line.split(splitBy);

                // If not table, continue
                if (!row[0].equals(strTableName)) continue;
                String colName = row[1];
                strAttributes.add(colName);

            br.close();
            
        }
            } catch (IOException e) {  
                e.printStackTrace();  
                 }  
        
            Enumeration<String> strColumnNames = tuple.keys();

            // Iterate over all the attributes of the table. If the attribute has a value to be inserted in the given tuple to insert, write it
            // otherwise, write an empty string 
            
            Enumeration<String> attributeEnumeration = Collections.enumeration(strAttributes);
            while (attributeEnumeration.hasMoreElements()) {
                String nextAttribute = attributeEnumeration.nextElement();

                // Check if the tuple to be inserted has this attribute and a value for it
                if (tuple.containsKey(nextAttribute)) {
                    csvData.append(tuple.get(nextAttribute));
                    csvData.append(",");
                } else {
                    // Otherwise write an empty string
                    csvData.append("");
                    csvData.append(",");
                }
            }
                // Write the tuple to the CSV file
                pw.write(csvData.toString());
                pw.close();

    }
}
