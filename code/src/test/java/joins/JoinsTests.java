package joins;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import parser.ColumnsLineage;
import parser.JoinsLineage;
import parser.LineageAnalysis;
import parser.NodeContext;
import parser.SqlDetective;

public class JoinsTests {

    private SqlDetective detective;

    @BeforeEach
    public void setup() {
        detective = new SqlDetective();
    }


    @Test
    public void qualifiedColumnInUsingJoin() throws SqlParseException {

        List<String> input = ImmutableList.of("Select emp.x, dept.y FROM emp JOIN dept USING (depId)");
        LineageAnalysis resLineage = detective.parseInput(input);

        Assertions.assertEquals(2, resLineage.columnsLineage.entrySet().size());
        Assertions.assertEquals(1, resLineage.columnsLineage.get("EMP").tableColumns.get("X").counts.get(NodeContext.SELECT));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("EMP").tableColumns.get("DEPID").counts.get(NodeContext.JOIN_KEY));
        Assertions.assertNull(resLineage.columnsLineage.get("EMP").tableColumns.get("Y"));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("DEPT").tableColumns.get("Y").counts.get(NodeContext.SELECT));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("DEPT").tableColumns.get("DEPID").counts.get(NodeContext.JOIN_KEY));
        Assertions.assertNull(resLineage.columnsLineage.get("DEPT").tableColumns.get("X"));
        detective.printLineage();
    }

    @Test
    public void unqualifiedColumnInUsingJoin() throws SqlParseException {

        List<String> input = ImmutableList.of("Select x, y FROM emp JOIN dept USING (depId)");
        LineageAnalysis resLineage = detective.parseInput(input);

        Assertions.assertEquals(3, resLineage.columnsLineage.entrySet().size());
        Assertions.assertEquals(1, resLineage.columnsLineage.get("EMP").tableColumns.get("DEPID").counts.get(NodeContext.JOIN_KEY));
        Assertions.assertNull(resLineage.columnsLineage.get("EMP").tableColumns.get("X"));
        Assertions.assertNull(resLineage.columnsLineage.get("EMP").tableColumns.get("Y"));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("DEPT+EMP").tableColumns.get("X").counts.get(NodeContext.SELECT));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("DEPT+EMP").tableColumns.get("Y").counts.get(NodeContext.SELECT));
        Assertions.assertNull(resLineage.columnsLineage.get("DEPT+EMP").tableColumns.get("DEPID"));
        detective.printLineage();
    }

    @Test
    public void unqualifiedColumnsInUsingJoin() throws SqlParseException {

        List<String> input = ImmutableList.of("Select x, y FROM emp JOIN dept USING (depId, branchId)");
        LineageAnalysis resLineage = detective.parseInput(input);

        Assertions.assertEquals(3, resLineage.columnsLineage.entrySet().size());
        Assertions.assertEquals(2, resLineage.columnsLineage.get("EMP").tableColumns.size());
        Assertions.assertEquals(2, resLineage.columnsLineage.get("DEPT").tableColumns.size());
        Assertions.assertEquals(2, resLineage.columnsLineage.get("DEPT+EMP").tableColumns.size());

        Assertions.assertEquals(1, resLineage.columnsLineage.get("EMP").tableColumns.get("DEPID").counts.get(NodeContext.JOIN_KEY));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("EMP").tableColumns.get("BRANCHID").counts.get(NodeContext.JOIN_KEY));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("DEPT").tableColumns.get("DEPID").counts.get(NodeContext.JOIN_KEY));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("DEPT").tableColumns.get("BRANCHID").counts.get(NodeContext.JOIN_KEY));

        Assertions.assertEquals(1, resLineage.columnsLineage.get("DEPT+EMP").tableColumns.get("X").counts.get(NodeContext.SELECT));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("DEPT+EMP").tableColumns.get("Y").counts.get(NodeContext.SELECT));
        detective.printLineage();
    }

    @Test
    public void qualifiedColumnsInOnJoin() throws SqlParseException {

        List<String> input = ImmutableList.of("SELECT tab1.x1, tab1.y1, tab2.z2, tab2.w2 FROM tab1 join tab2 ON tab1.x1 = tab2.x2 " +
                "AND tab1.y1 = tab2.z2");
        LineageAnalysis resLineage = detective.parseInput(input);

        Assertions.assertEquals(2, resLineage.columnsLineage.entrySet().size());
        Assertions.assertEquals(3, resLineage.columnsLineage.get("TAB2").tableColumns.size());
        Assertions.assertEquals(2, resLineage.columnsLineage.get("TAB1").tableColumns.size());
        detective.printLineage();
    }

    @Test
    public void qualifiedColumnsInMultipleOnJoins() throws SqlParseException {

        List<String> input = ImmutableList.of(
                "Select * from tb_A A " +
                        "join tb_B B on A.ID = B.ID " +
                        "join tb_C C on B.ID2 = C.ID2 " +
                        "join tb_D D on A.ID = D.ID and C.ID3 = D.ID3 and B.ID4 = D.ID4 " +
                        "where A.Foo = 6");
        LineageAnalysis resLineage = detective.parseInput(input);

        Assertions.assertEquals(4, resLineage.columnsLineage.entrySet().size());
        Assertions.assertEquals(5, resLineage.joinsLineage.entrySet().size());
        detective.printLineage();
    }

    @Test
    public void sameTablesJoinsWithDifferentKeys() throws SqlParseException {

        List<String> input = ImmutableList.of("SELECT tab1.y1, tab2.y2 FROM tab1 join tab2 on tab1.x1 = tab2.x2",
                "SELECT tab1.x1, tab1.y1, tab2.z2, tab2.w2 FROM tab1 join tab2 on tab1.y1 = tab2.y2",
                "SELECT t1.x1, t2.z2 FROM tab1 t1 join tab2 t2 on t1.x1 = t2.x2",
                "SELECT t1.x1, t2.z2 FROM tab1 t1 join tab2 t2 using(x1)",
                "SELECT t1.x1, t2.z2 FROM tab1 t1, tab2 t2");
        LineageAnalysis resLineage = detective.parseInput(input);

        Assertions.assertEquals(1, resLineage.joinsLineage .entrySet().size());
        Assertions.assertEquals(3, resLineage.joinsLineage.get("(TAB1; TAB2)").joinCount);
        Assertions.assertEquals(2, resLineage.joinsLineage.get("(TAB1; TAB2)").joinKeys.get("[TAB1.X1, TAB2.X2]"));
        detective.printLineage();
    }

    @Test
    public void selfTableJoin() throws SqlParseException {

        List<String> input = ImmutableList.of("SELECT employee.Id, employee.FullName, employee.ManagerId, manager.FullName " +
                "FROM Employees employee " +
                "JOIN Employees manager ON employee.ManagerId = manager.Id");
        LineageAnalysis resLineage = detective.parseInput(input);


        Assertions.assertEquals(1, resLineage.columnsLineage.entrySet().size());
        Assertions.assertEquals(3, resLineage.columnsLineage.get("EMPLOYEES").tableColumns.size());

        Assertions.assertEquals(1, resLineage.columnsLineage.get("EMPLOYEES").tableColumns.get("ID").counts.get(NodeContext.SELECT));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("EMPLOYEES").tableColumns.get("ID").counts.get(NodeContext.JOIN_KEY));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("EMPLOYEES").tableColumns.get("MANAGERID").counts.get(NodeContext.SELECT));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("EMPLOYEES").tableColumns.get("MANAGERID").counts.get(NodeContext.JOIN_KEY));
        Assertions.assertEquals(2, resLineage.columnsLineage.get("EMPLOYEES").tableColumns.get("FULLNAME").counts.get(NodeContext.SELECT));

        Assertions.assertEquals(1, resLineage.joinsLineage .entrySet().size());
        Assertions.assertEquals(1, resLineage.joinsLineage.get("(EMPLOYEES; EMPLOYEES)").joinCount);
        Assertions.assertEquals(1, resLineage.joinsLineage.get("(EMPLOYEES; EMPLOYEES)").joinKeys.get("[EMPLOYEES.ID, EMPLOYEES.MANAGERID]"));
        detective.printLineage();
    }
}
