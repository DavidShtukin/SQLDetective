package parser;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class LineageAnalysis {

    public Map<String, ColumnsLineage>  columnsLineage;
    public Map<String, JoinsLineage>    joinsLineage;

    public LineageAnalysis() {
        columnsLineage = new LinkedHashMap<>();
        joinsLineage = new LinkedHashMap<>();
    }
}
