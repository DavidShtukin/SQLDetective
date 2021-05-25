package parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.EnumMap;
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

//        return List.of(data);
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
        return globalLineage;
    }



    public void printLineage() {

        System.out.println("\n\n         TOTAL Table/Column STATS:\n        ===========================");

        for (Map.Entry<String, ColumnsLineage> map : globalLineage.columnsLineage.entrySet()) {
            tableSummary(map);
            columnsTableTitle();
            map.getValue().tableColumns.entrySet().stream().forEach(e -> constructColumnsRow(e));
        }
        System.out.println("\n");

        if (globalLineage.joinsLineage.keySet().size() > 0) {

            System.out.println(
                    "\n\n         TOTAL Table Relationships STATS:\n        ==================================");

            for (Map.Entry<String, JoinsLineage> map : globalLineage.joinsLineage.entrySet()) {
                joinsSummary(map);
                joinsTableTitle();
                map.getValue().joinKeys.entrySet().stream().forEach(e -> constructJoinsRow(e));
            }
            System.out.println("\n");
        }
    }

    private void tableSummary(final Map.Entry<String, ColumnsLineage> table) {
        System.out.println(" \n\n Table: " + table.getKey() + " used " + table.getValue().tableCount + " times.");
    }

    private void columnsTableTitle() {
        System.out.printf("\n        Column      |  Select  |  Aggregate  |  Filter  |  Group By  |  Join key  |  Order By  |");
    }

    private void constructColumnsRow(final Map.Entry<String, ColumnMetrics> metrics) {

        String row = String.format("\n %-23s ", metrics.getKey());

        String[] cells = new String[6];
        Arrays.fill(cells, String.format(" %-20s ", ""));

        for (int ind = 0; ind < 6; ind++) {
//            if (metrics.getValue().counts.containsKey(NodeContext.valueOf(ind))) {
//                cells[ind] = metrics.getValue().counts.get(NodeContext.valueOf(ind)).toString();
//                row += String.format(" %-10s ", cells[ind]);
//            }
            cells[ind] = metrics.getValue().counts.getOrDefault(NodeContext.valueOf(ind), 0).toString();
            row += String.format(" %-10s ", cells[ind]);
        }
        System.out.printf(row);
    }

    private void joinsSummary(final Map.Entry<String, JoinsLineage> join) {
        System.out.println(" \n\n Join:  " + join.getKey() + " used " + join.getValue().joinCount + " times.");
    }

    private void joinsTableTitle() {
        System.out.printf("\n                                  Key combination                            |    Count   |");
    }

    private void constructJoinsRow(final Map.Entry<String, Integer> join) {
        System.out.printf("\n     %-75s    %-10d", join.getKey(), join.getValue());
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

//            List<String> sqls = detective.readInput();

            String sql = detective.readInput();

            SqlNode astRoot = SqlParser.create(sql).parseStmtList();
            System.out.println("\n SqlNode: " + astRoot);

            // Traverse AST from the root
            astRoot.accept(detective.visitor);

//
//            for (int i=0; i < sqls.size(); i++) {
//
//                String sql = sqls.get(i);
//                if ( sql.charAt(sql.length()-1) == ';') {
//                    sql = sql.substring(0, sql.length()-1);
//                }
//
//                SqlNode astRoot = SqlParser.create(sql).parseQuery();
//                System.out.println("\n SqlNode: " + astRoot);
//
//                // Traverse AST from the root
//                astRoot.accept(detective.visitor);
//
//            }
            detective.printLineage();
        }
        catch (SqlParseException | IOException e) {
            System.out.println(e.getCause());
        }
    }

}
