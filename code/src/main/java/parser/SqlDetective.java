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
                System.out.println(formatDiv("(============================================================================$============)\n"));
            }
        }
    }

    private void tableSummary(final Map.Entry<String, ColumnsLineage> table) {
        System.out.println(" \n\n Table: " + table.getKey() + " used " + table.getValue().tableCount + " times.");
    }

    private void columnsTableTitle() {
        String formatS = formatDiv("[-----------------x------------x------------x------------x------------x------------x------------]\n");
        formatS += formatDiv("|       Column    |   Select   |  Aggregate |  Filter    |  Group By  |  Join key  |  Order By  |\n");
        formatS += formatDiv("{-----------------q------------q------------q------------q------------q------------q------------}");
        System.out.println(formatS);
    }

    private static String formatDiv(String str) {
        return str.replace('[', '\u250f')
                .replace('-', '\u2501')
                .replace('x', '\u2533')
                .replace('q', '\u2547')
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

        String row = String.format("|%-17s| ", metrics.getKey());

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
//            cells[ind] = metrics.getValue().counts.getOrDefault(NodeContext.valueOf(ind), 0).toString();
//            row += String.format(" %5s     | ", cells[ind]);
        }
        row += "\n";
        System.out.print(formatRow(row));
    }

    private void joinsSummary(final Map.Entry<String, JoinsLineage> join) {

        System.out.println(" \n\n Join:  " + join.getKey() + " used " + join.getValue().joinCount + " times.");
    }

    private void joinsTableTitle() {
        String formatS = formatDiv("[----------------------------------------------------------------------------x------------]\n");
        formatS += formatDiv("|                                 Key combination                            |    Count   |\n");
        formatS += formatDiv("{----------------------------------------------------------------------------q------------}");
        System.out.println(formatS);
    }

    private void constructJoinsRow(final Map.Entry<String, Integer> join) {
        String row = String.format("|     %-69s  |  %6d    |\n", join.getKey(), join.getValue());
        System.out.print(formatRow(row));
    }


    public static void main(String[] args) {

        //        List<String> sqls = List.of(
        //                "SELECT tt.x1 FROM tab1 tt"
        //                , "SELECT tt.x2 AS y1 FROM tab1 tt"
        //                ,  "SELECT x1 AS y1, x2 AS y2, x3 FROM tab1"                     //   ok
        //                , "SELECT x + 1 AS xyz FROM table1"                             //   ok
        //                , "SELECT x + 1 + y + 2 + z AS xyz FROM table1"                 //   ok
        //                , "SELECT x + 1 + y + 2 FROM table1"                            //   ok
        //                , "SELECT x + 1 * y + 2 / z - 3 FROM table1"                    //   ok
        //                , "SELECT  order_date, status FROM orders WHERE id = 5"      //   ok
        //                , "UPDATE orders SET id = 1234 WHERE id = 5   // AND xx = 7"         //   ok
        //                , "UPDATE orders SET id = 1234 WHERE x = orders.t"              //   ok
        //                , "SELECT a1, b2, c3 FROM users WHERE a1 = 10"                  //   ok
        //                , "SELECT b2, c3, d4, e5 FROM users WHERE b2 = 5"               //   ok
        //                , "Select a,b,c from tab2 where tab2.id in (x+5, y-7)"          //   ok
        //                , "Select a,b,c from tab2 where tab2.id in (select id from tab5)"
        //                , "Select a,b,c from tab2 where tab2.id in (select id from (select id from tab3))"
        //                , "UPDATE users SET b2 = 7, c3 = 8, f6 = 11 WHERE b2 = 5"       //   ok
        //                , "SELECT CONCAT('blah-', x ) AS y FROM tab4"                   //   ok
        //                , "SELECT CONCAT(x,'-blah') AS y FROM tab4"                     //   ok
        //                , "SELECT CONCAT(x5, y6, z8 ) AS xyz FROM tab4"                 //   ok
        //                , "SELECT MAX(x1, y1) AS min_val FROM tab1"                     //   ok
        //,                 "Select *, ea as ye from tab3"                                //   ok
        //                , "SELECT tab1.x1, tab2.z2 FROM tab1, tab2"
        //                , "SELECT tab1.x1, tab1.y1, tab2.z2, tab2.w2 FROM tab1 join tab2 on tab1.x1 = tab2.x2"
        ////                , "SELECT COALESCE(tab1.x1, tab2.y2, tab3.z3) FROM tab1, tab2, tab3"                     //   ok
        ////                , "SELECT ISNULL(tab1.x1, ISNULL(tab2.y2, 'aaa')) FROM tab1, tab2"                     //   ok
        ////                , "Select * from tb_A A join tb_B B on A.ID = B.ID"
        ////                 "Select x, y FROM EMP JOIN DEPT USING (depId)"
        ////                 "Select * from tb_A A " +
        ////                    "join tb_B B on A.ID = B.ID " +
        ////                    "join tb_C C on B.ID2 = C.ID2 " +
        ////                    "join tb_D D on A.ID = D.ID and C.ID3 = D.ID3 and B.ID4 = D.ID4 " +
        ////                  "where A.Foo = 6"
        ////                , "SELECT employee.Id, employee.FullName, employee.ManagerId, manager.FullName as ManagerName " +
        ////                        "FROM Employees employee " +
        ////                        "JOIN Employees manager ON employee.ManagerId = manager.Id"
        ////                ,  "Select c1, c2 from t1 where not exists ( select c3 from t2 )"
        //////
        //////                " SELECT * FROM incidents " +
        //////                "   JOIN ( SELECT i_date " +
        //////                "          FROM tutorial " +
        //////                "        ) sub " +
        //////                "   ON incidents.inc_date = sub.i_date"
        //////
        //////              ,  " SELECT * FROM incidents " +
        //////                        "   JOIN ( SELECT i_date " +
        //////                        "          FROM tutorial " +
        //////                        "          ORDER BY i_date LIMIT 5 " +
        //////                        "        ) sub " +
        //////                        "   ON incidents.inc_date = sub.i_date"
        //////
        ////              ,  "Select c1, c2 from t1 where exists ( select c3 from t2 where c4 > 5 ) and c11 > 5"
        ////               ,     "Select c1, c2 from t1 where exists ( select c3 from t2 where t1.c4 > 5 ) and t2.c22 > 5"
        ////               ,     "Select t1.c1, t2.c2 from t1, t2 where exists ( select t3.c3, t4.c4 from t3, t4 where t4.c4 = 10 ) and t1.c1 > 5"
        ////                ,    "Select c1, t2.c2 from t1, t2 where exists ( select t3.c3, t4.c4 from t3, t4 where t4.c4 = 10 ) and c1 > 5"
        //
        ////                "Select distinct(x) from users"
        //
        ////               "SELECT COUNT(DISTINCT Country) FROM Customers"
        ////               "SELECT COUNT(*) FROM Customers"
        ////                "SELECT COUNT(Country) FROM Customers"
        //             ,    "SELECT AVG(age) AS av, MIN(age) AS mi, MAX(age) AS ma FROM Employees"
        //
        //
        ////                 "SELECT department_id, COUNT(*) FROM employees GROUP BY department_id"
        ////                 "SELECT COUNT(*) FROM employees GROUP BY department_id"
        ////                , "SELECT a, b, department_id FROM employees GROUP BY department_id"
        //
        //          Union   (SqlBasicCall)
        //          With    (SqlWith)
        //        );

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

