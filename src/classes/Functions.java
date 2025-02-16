package src.classes;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import src.exceptions.DBAppException;

public class Functions {
    /**
     * Compares two objects of the same type. 
     * @param o1 first object to be compared
     * @param o2 second object to be compared
     * @param strObjType string name of class
     * @return 1 if o1 > o2, 0 if o1 = o2, -1 if o1 < o2
     * @throws DBAppException
     */
    public static int cmpObj(String o1, Object o2, String strObjType) {
        
        // Cases for each type
        switch (strObjType) {
            case "java.lang.Integer": {
                Integer i1 = Integer.parseInt(o1);
                Integer i2 = (Integer) o2;

                return i1.compareTo(i2);
            }
            case "java.lang.String": {
                String s1 = (String) o1;
                String s2 = (String) o2;
                
                return s1.compareTo(s2);
            }
            case "java.lang.Double": {
                Double d1 = Double.parseDouble(o1);
                Double d2 = (Double) o2;
                
                return d1.compareTo(d2);
            }
            case "java.util.Date": {
                String [] arrstrDate = o1.split("\\.");
                int year = Integer.parseInt(arrstrDate[2]);
                int month = Integer.parseInt(arrstrDate[1]);
                int day = Integer.parseInt(arrstrDate[0]);
                
                Date d1 = new Date(year - 1900, month, day);
                Date d2 = (Date) o2;

                return d1.compareTo(d2);
            }
        }

        return 0;
    }

    /**
     * Compares two objects of the same type. 
     * @param min first object to be compared
     * @param max second object to be compared
     * @param numBounds second object to be compared
     * @param strObjType string name of class
     * @returns 
     * @throws DBAppException
     */
    public static Object[] getBounds(String min, String max, int numBounds, String strObjType) throws DBAppException {
        Object[] returnBounds = null;

        // Cases for each type
        switch (strObjType) {
            case "java.lang.Integer": {
                returnBounds = new Integer[numBounds + 1];
                int step = (Integer.parseInt(max) - Integer.parseInt(min)) / numBounds;
                int currBound = Integer.parseInt(min);

                for (int i = 0; i < numBounds; i++) {
                    returnBounds[i] = currBound;
                    currBound += step;
                }
                returnBounds[numBounds] = Integer.parseInt(max);

                break;
            }
            case "java.lang.String": {
                returnBounds = new String[numBounds + 1];
                int step = 26 / numBounds;

                char currBound = 'A';
                for (int i = 0; i < numBounds; i++) {
                    returnBounds[i] = String.valueOf(currBound);
                    currBound += step;
                }
                returnBounds[numBounds] = "Z";
                
                break;
            }
            case "java.lang.Double": {
                returnBounds = new Double[numBounds + 1];
                Double step = (Double.parseDouble(max) - Double.parseDouble(min)) / numBounds;
                Double currBound = Double.parseDouble(min);

                for (int i = 0; i < numBounds; i++) {
                    returnBounds[i] = currBound;
                    currBound += step;
                }
                returnBounds[numBounds] = Double.parseDouble(max);
                
                break;
            }
            case "java.util.Date": {
                // Get the difference in days between the min and max and divide it by the num of bounds
                SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");
                returnBounds = new Date[numBounds + 1];
                
                try {
                    long minTime = dateFormat.parse(min).getTime();
                    long maxTime = dateFormat.parse(max).getTime();
                    long step = (maxTime - minTime) / numBounds;
                    
                    // Iterate over bounds
                    long currBound = minTime;
                    for (int i = 0; i < numBounds; i++) {
                        returnBounds[i] = new Date(currBound);
                        currBound += step;
                    }
                    returnBounds[numBounds] = new Date(maxTime);

                } catch (ParseException e) {
                    e.printStackTrace();
                }
                break;
            }
            default: {
                throw new DBAppException("Invalid column type while preparing grid index bounds!");
            }
        }
        
        return returnBounds;
    }

    /**
     * Takes a string representation of a date.
     * Returns true if date is in format "DD.MM.YYYY".
     * @param strDateVal
     * @return
     */
    public static Boolean checkDate(String strDateVal) {
        String[] strarrDate = strDateVal.split("\\.");

        // Ensure only 3 parts to date
        if (strarrDate.length != 3) {
            return false;
        }

        final int year = Integer.parseInt(strarrDate[2]);
        final int month = Integer.parseInt(strarrDate[1]);
        final int day = Integer.parseInt(strarrDate[0]);

        if (year < 0) {
            return false;
        }

        if (month < 0 || month > 12) {
            return false;
        }

        if (day < 0 || day > 31) {
            return false;
        }

        return true;
    }

    /**
     * Checks type of object. Throws error if not the same or invalid class.
     * @param objValue
     * @param strObjType
     * @throws DBAppException
     */
    public static void checkType(Object objValue, String strObjType) throws DBAppException {
        Class<?> c;
        try {
            c = Class.forName(strObjType);
        } catch (ClassNotFoundException e) {
            throw new DBAppException("Class " + strObjType + " does not exist.");
        }
        
        if (!c.isInstance(objValue)) {
            throw new DBAppException("Class type mismatch while inserting " + strObjType);
        }
    }
}