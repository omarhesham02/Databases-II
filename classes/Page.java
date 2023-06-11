package classes;

import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.file.FileSystemException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;

public class Page {

    private String strTableName;
    private String strPageNum;
    private int intCountTuples;
    private boolean isModified;

    /**
     * Returns whether the page is full or not by comparing the
     * number of tuples currently present in the page with the maximum per page
     * listed in the config file
     */

    public static boolean isFull() {
        // if this.intCountTuples == config.maxTuples 
    }


    public Page(String strPageNum) throws FileSystemException{
        this.strPageNum = strPageNum;
        PrintWriter pw = new PrintWriter(new File(strPageNum + ".csv"));

        
    }

    public void insertIntoPage(Hashtable<String, Object> tuple) {
        PrintWriter pw = new PrintWriter(this.strPageNum + ".csv");

        StringBuffer csvData = new StringBuffer();

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
                    csvData.write(tuple.get(nextAttribute));
                    csvData.write(",");
                } else {
                    // Otherwise write an empty string
                    csvData.write("");
                    csvData.write(",");
                }
            }
    }
    
    public static void main(String[] args) {
        Page p = new Page(0);
        
    }

}
