package src.classes;
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
            case "java.lang.Date": {
                // TODO: Create formatter
                Date d1 = Date.from(null);
                Date d2 = (Date) o2;

                return d1.compareTo(d2);
            }
        }

        return 0;
    }

    /**
     * Takes a string representation of a date.
     * Returns true if date is in format "DD.MM.YYYY".
     * @param strDateVal
     * @return
     */
    public static Boolean checkDate(String strDateVal) {
        String[] strarrDate = strDateVal.split(".");

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