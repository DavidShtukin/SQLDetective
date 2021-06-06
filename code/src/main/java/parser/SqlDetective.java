package parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class SqlDetective {

    public LineageAnalysis globalLineage = new LineageAnalysis();

    private  NodeVisitor visitor = new NodeVisitor(this);

    public String readInput() throws IOException {

        Path path = Paths.get("src/test/resources/sqlStatements.txt");
        Stream<String> lines = Files.lines(path);
        String data = lines.collect(Collectors.joining("\n"));
        lines.close();
        return data;
    }

    public LineageAnalysis parseInput(final List<String> sqls) {

        try {
            for (int i=0; i < sqls.size(); i++) {

                SqlNode astRoot = SqlParser.create(sqls.get(i)).parseQuery();
                System.out.println("\n SqlNode: " + astRoot);

                // Traverse AST from the root
                astRoot.accept(visitor);
            }
        }
        catch (SqlParseException e) {
            System.out.println(e.getCause());
        }
        sortLineage();
        return globalLineage;
    }

    private void sortLineage() {
        sortColumnsLineage();
        sortJoinsLineage();
    }

    private void sortColumnsLineage() {
        // 1. Tables by usage
        List<Map.Entry<String, ColumnsLineage>> tableEntries =
                new ArrayList<>(globalLineage.columnsLineage.entrySet());
        Collections.sort(tableEntries, new Comparator<Map.Entry<String, ColumnsLineage>>() {
            @Override
            public int compare(Map.Entry<String, ColumnsLineage> t1, Map.Entry<String, ColumnsLineage> t2) {
                return t2.getValue().tableCount.compareTo(t1.getValue().tableCount);
            }
        });

        Map<String, ColumnsLineage> sortedTablesMap = new LinkedHashMap<>();
        for (Map.Entry<String, ColumnsLineage> entry : tableEntries) {

            // 2. Columns within tables by sum of usage counts
            Map<String, ColumnMetrics> sortedColumnsMap = sortColumns(entry.getValue().tableColumns);
            sortedTablesMap.put(entry.getKey(), new ColumnsLineage(entry.getValue().tableCount,
                                                                   sortedColumnsMap));
        }

        // 3. Replace in globalLineage
        globalLineage.columnsLineage.clear();
        globalLineage.columnsLineage.putAll(sortedTablesMap);
    }

    private void sortJoinsLineage() {
        // 1. Joins by usage
        List<Map.Entry<String, JoinsLineage>> joinEntries =
                new ArrayList<>(globalLineage.joinsLineage.entrySet());
        Collections.sort(joinEntries, new Comparator<Map.Entry<String, JoinsLineage>>() {
            @Override
            public int compare(Map.Entry<String, JoinsLineage> j1, Map.Entry<String, JoinsLineage> j2) {
                return j2.getValue().joinCount.compareTo(j1.getValue().joinCount);
            }
        });

        Map<String, JoinsLineage> sortedJoinsMap = new LinkedHashMap<>();
        for (Map.Entry<String, JoinsLineage> entry : joinEntries) {

            // 2. Key combinations within joins by sum of usage counts
            Map<String, JoinMetrics> sortedKeysMap = sortKeyCombinations(entry.getValue().joinKeys);
            sortedJoinsMap.put(entry.getKey(), new JoinsLineage(entry.getValue().joinCount,
                                                                sortedKeysMap));
        }

        // 3. Replace in globalLineage
        globalLineage.joinsLineage.clear();
        globalLineage.joinsLineage.putAll(sortedJoinsMap);
    }

    private Map<String, ColumnMetrics> sortColumns(final Map<String, ColumnMetrics> columns) {

        List<Map.Entry<String, ColumnMetrics>> colEntries = new ArrayList<>(columns.entrySet());
        Collections.sort(colEntries, new Comparator<Map.Entry<String, ColumnMetrics>>() {
            @Override
            public int compare(Map.Entry<String, ColumnMetrics> c1, Map.Entry<String, ColumnMetrics> c2) {
                return c2.getValue().counts.values().stream().reduce(0, Integer::sum) -
                       c1.getValue().counts.values().stream().reduce(0, Integer::sum);
            }
        });

        Map<String, ColumnMetrics> sortedColumnsMap = new LinkedHashMap<>();
        for (Map.Entry<String, ColumnMetrics> colEntry : colEntries) {
            sortedColumnsMap.put(colEntry.getKey(), colEntry.getValue());
        }
        return sortedColumnsMap;
    }

    private Map<String, JoinMetrics> sortKeyCombinations(final Map<String, JoinMetrics> keyCombination) {

        List<Map.Entry<String, JoinMetrics>> keyEntries = new ArrayList<>(keyCombination.entrySet());
        Collections.sort(keyEntries, new Comparator<Map.Entry<String, JoinMetrics>>() {
            @Override
            public int compare(Map.Entry<String, JoinMetrics> k1, Map.Entry<String, JoinMetrics> k2) {
                return k2.getValue().counts.values().stream().reduce(0, Integer::sum) -
                        k1.getValue().counts.values().stream().reduce(0, Integer::sum);
            }
        });

        Map<String, JoinMetrics> sortedKeysMap = new LinkedHashMap<>();
        for (Map.Entry<String, JoinMetrics> colEntry : keyEntries) {
            sortedKeysMap.put(colEntry.getKey(), colEntry.getValue());
        }
        return sortedKeysMap;
    }

    public void printLineage() {

        System.out.println("\n\n         TOTAL Table/Column STATS:\n        ===========================");

        //        String border = "";
        //        border += formatDiv("a-----b-------------b----------c\n");
        //        System.out.println(border);

        for (Map.Entry<String, ColumnsLineage> map : globalLineage.columnsLineage.entrySet()) {
            tableSummary(map);
            columnsTableTitle();
            map.getValue().tableColumns.entrySet().stream().forEach(e -> constructColumnsRow(e));
            System.out.println(formatDiv("(=================$============$============$============$============$============$============)\n"));
        }

        if (globalLineage.joinsLineage.keySet().size() > 0) {
            System.out.println(
                    "\n\n         TOTAL Table Relationships STATS:\n        ==================================");

            for (Map.Entry<String, JoinsLineage> map : globalLineage.joinsLineage.entrySet()) {
                joinsSummary(map);
                joinsTableTitle();
                map.getValue().joinKeys.entrySet().stream().forEach(e -> constructJoinsRow(e));
                System.out.println(formatDiv("(===============================================================$=============$============)\n"));
            }
        }
    }

    private void tableSummary(final Map.Entry<String, ColumnsLineage> table) {
        System.out.println(" \n\n Table: " + table.getKey() + " used " + table.getValue().tableCount + " times");
    }

    private void columnsTableTitle() {
        String formatS = formatDiv("[-----------------x------------x------------x------------x------------x------------x------------]\n");
        formatS += formatDiv("|     Column      |   Select   |  Aggregate |   Filter   |  Group By  |  Join key  |  Order By  |\n");
        formatS += formatDiv("{-----------------@------------@------------@------------@------------@------------@------------}");
        System.out.println(formatS);
    }

    private static String formatDiv(String str) {
        return str.replace('[', '\u250f')
                  .replace('-', '\u2501')
                  .replace('x', '\u2533')
                  .replace('@', '\u2547')
                  .replace(']', '\u2513')
                  .replace('{', '\u2521')
                  .replace('}', '\u2529')
                  .replace('|', '\u2503')
                  .replace('=', '\u2500')
                  .replace('(', '\u2514')
                  .replace(')', '\u2518')
                  .replace('$', '\u2534');
    }

    private static String formatRow(String str) {
        return str.replace('|', '\u2502');
    }

    private void constructColumnsRow(final Map.Entry<String, ColumnMetrics> metrics) {

        String row = String.format("| %-15s | ", metrics.getKey());

        String[] cells = new String[6];
        Arrays.fill(cells, "           | ");

        for (int ind = 0; ind < 6; ind++) {
            if (metrics.getValue().counts.containsKey(NodeContext.valueOf(ind))) {
                    cells[ind] = metrics.getValue().counts.get(NodeContext.valueOf(ind)).toString();
                row += String.format(" %5s     | ", cells[ind]);
            }
            else {
                row += cells[ind];
            }
        }
        row += "\n";
        System.out.print(formatRow(row));
    }

    private void joinsSummary(final Map.Entry<String, JoinsLineage> join) {
        System.out.println(" \n\n Join:  " + join.getKey() + " used " + join.getValue().joinCount + " times");
    }

    private void joinsTableTitle() {
        String formatS = formatDiv("[---------------------------------------------------------------x-------------x------------]\n");
        formatS += formatDiv("|                           Key combination                     |  Equi Join  | Theta Join |\n");
        formatS += formatDiv("{---------------------------------------------------------------@-------------@------------}");
        System.out.println(formatS);
    }

    private void constructJoinsRow(final Map.Entry<String, JoinMetrics> metrics) {
        String row = String.format("| %-62s| ", metrics.getKey());

        String[] cells = new String[6];
        Arrays.fill(cells, "            |");

        for (int ind = 0; ind < 2; ind++) {
            if (metrics.getValue().counts.containsKey(JoinType.valueOf(ind))) {
                cells[ind] = metrics.getValue().counts.get(JoinType.valueOf(ind)).toString();
                row += String.format(" %5s      |", cells[ind]);
            }
            else {
                row += cells[ind];
            }
        }
        row += "\n";
        System.out.print(formatRow(row));
    }

    public static void main(String[] args) {

        SqlDetective detective = new SqlDetective();

        try {
            String sql = detective.readInput();

            SqlNode astRoot = SqlParser.create(sql).parseStmtList();
            System.out.println("\n SqlNode: " + astRoot);

            // Traverse AST from the root
            astRoot.accept(detective.visitor);

            detective.sortLineage();
            detective.printLineage();
        }
        catch (SqlParseException | IOException e) {
            System.out.println(e.getCause());
        }
    }
}
