package src.main.java;
import java.util.*;

public class Table {
    private String TableName;
    private String ColumnName;
    private String ColumnType;
    private String ClusteringKey;
    private String IndexName;
    private String IndexType;
    private Hashtable<String, String> min;
    private Hashtable<String, String> max;
    private Hashtable<String, String> ForeignKey;
    private String ForeignTableName;;
    private String ForeignColumnName;
    private String[] Computed;

}
