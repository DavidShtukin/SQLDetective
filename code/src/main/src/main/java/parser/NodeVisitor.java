package parser;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.calcite.sql.SqlKind.AS;

public class NodeVisitor extends SqlShuttle {

    private class Scope {
        Namespace scopeNamespace = new Namespace();
    }

    private class Namespace {
        Set<String>                             tables       = new HashSet<>();      // tables used in current Select scope
        Map<String, String>                     tableAliases = new HashMap<>();      // key - alias, value - table
        Map<String, Map<String, ColumnMetrics>> tableColumns = new HashMap<>();      // key - table, value - columns with metrics
    }

    private final SqlDetective detective;

    private Deque<Scope>     scopes = new ArrayDeque<>();
    private Namespace        currentNamespace;
    private NodeContext      currentNodeContext;

    private Set<String>     currentJoinTables = new HashSet<>();
    private Set<String>     currentJoinKeys = new HashSet<>();

    public NodeVisitor(final SqlDetective detective) {
        this.detective = detective;
    }

    private void addTableToNamespace(final SqlNode node) {
        if (node instanceof SqlSelect) {
            node.accept(this);
        }
        if (node instanceof SqlIdentifier) {
            currentNamespace.tables.add(node.toString());
        }
        else if (node instanceof SqlBasicCall && ((SqlBasicCall) node).getOperator().getKind() == AS) {
            String table = ((SqlBasicCall) node).operand(0).toString();
            currentNamespace.tables.add(table);
            currentNamespace.tableAliases.put(((SqlBasicCall) node).operand(1).toString(), table);
        }
    }

    private void addColumnAppearanceToNamespace(final SqlIdentifier node) {
        String column;
        if (node.isSimple()) {
            column = node.toString();
            if (currentNodeContext == NodeContext.SELECT && currentNamespace.tables.size() > 1) {
                // create artificial group key combining all joined tables with '+' as a delimiter
                StringBuilder fakeTable = new StringBuilder();
                currentNamespace.tables.stream().filter(table -> !table.contains("+")).forEach(table -> fakeTable.append(table).append("+"));
                fakeTable.deleteCharAt(fakeTable.length() - 1);
                currentNamespace.tables.add(fakeTable.toString());
                incrementColumnMetric(fakeTable.toString(), column);
            }
            else {
                currentNamespace.tables.stream().forEach(table -> incrementColumnMetric(table, column));
            }
        }
        else {
            // In case of Tab.col - first make sure to find the real table name rather than alias
            column = node.getComponent(1).toString();
            String table = resolveTable(node.getComponent(0).toString());
            incrementColumnMetric(table, column);
        }
    }

    private void incrementColumnMetric(final String table, final String column) {

        Map<String, ColumnMetrics> tableColumnsMetrics = currentNamespace.tableColumns
                .computeIfAbsent(table, k -> new HashMap<String, ColumnMetrics>());

        ColumnMetrics columnMetrics = tableColumnsMetrics.computeIfAbsent(column, k -> new ColumnMetrics());

        columnMetrics.counts.put(currentNodeContext, columnMetrics.counts.getOrDefault(currentNodeContext, 0) + 1);
    }

    private void addSelectScope() {
        scopes.push(new Scope());
        currentNamespace = scopes.peek().scopeNamespace;
    }

    private void addOrderByScope() {
        scopes.push(new Scope());
        currentNamespace.tableColumns.clear();
    }

    private void rollUpScope() {
        addScopeToLineage();
        scopes.pop();
        if (scopes.size() > 0) {
            currentNamespace = scopes.peek().scopeNamespace;
        }
    }

    private void addScopeToLineage() {

        for (Map.Entry<String, Map<String, ColumnMetrics>> table : currentNamespace.tableColumns.entrySet()) {

            ColumnsLineage tableLineage = detective.globalLineage.columnsLineage.computeIfAbsent(table.getKey(), k -> new ColumnsLineage());
            tableLineage.tableCount++;

            for (Map.Entry<String, ColumnMetrics> column : table.getValue().entrySet()) {

                ColumnMetrics metrics = tableLineage.tableColumns.computeIfAbsent(column.getKey(),
                                                                                  k -> new ColumnMetrics());
                for (NodeContext context : column.getValue().counts.keySet()) {
                    metrics.counts.put(context, metrics.counts.getOrDefault(context, 0) + column.getValue().counts.get(context));
                }
            }
        }
    }

    private void addToJoinConditionList(SqlIdentifier node) {
        if (node.isSimple()) {
            currentJoinKeys.add(node.toString());
        }
        else {
            String column = node.getComponent(1).toString();
            String table = resolveTable(node.getComponent(0).toString());
            currentJoinTables.add(table);
            currentJoinKeys.add(table + "." + column);
        }
    }

    private void addJoinToLineage(final SqlJoin joinNode) {

//        SqlNode left = joinNode.getLeft();
//        if (left instanceof SqlBasicCall) {
//            left = ((SqlBasicCall) left).operand(0);
//        }
//        SqlNode right = joinNode.getRight();
//        if (right instanceof SqlBasicCall) {
//            right = ((SqlBasicCall) right).operand(0);
//        }
//        String join = "(" + left + ", " + right + ")";
//

        List<String> tables = new ArrayList<>();
        tables.addAll(currentJoinTables);
        Collections.sort(tables);
        String join = "(" + tables.get(0) + ", " + tables.get(1) + ")";

        JoinsLineage joinLineage = detective.globalLineage.joinsLineage.computeIfAbsent(join, k -> new JoinsLineage());
        joinLineage.joinCount++;

        List<String> keys = new ArrayList<>();
        keys.addAll(currentJoinKeys);
        Collections.sort(keys);
        String keyCombination = keys.toString();

        joinLineage.joinKeys.put(keyCombination, joinLineage.joinKeys.getOrDefault(keyCombination, 0) + 1);

        currentJoinTables.clear();
        currentJoinKeys.clear();
    }
    
    private String resolveTable(final String alias) {
        String table = currentNamespace.tableAliases.get(alias);
        if (table == null) {
            table = alias;
        }
        return table;
    }

    private void addOperatorToLineage(final SqlBasicCall node) {
        SqlOperator operator = (node).getOperator();
        if (operator instanceof SqlFunction) {
            detective.globalLineage.functionsSeen.add(operator.toString());
        }
        else {
            detective.globalLineage.operatorsSeen.add(operator.toString());
        }
    }

    @Override
    public SqlNode visit(final SqlIdentifier node) {
        if (!"*".equals(node.toString())) {
            addColumnAppearanceToNamespace(node);
        }
        if (currentNodeContext == NodeContext.JOIN_KEY) {
           addToJoinConditionList(node);
        }
        return node;
    }

    @Override
    public SqlNode visit(final SqlCall node) {

        // Logging lineage
        if (node instanceof SqlBasicCall) {
            addOperatorToLineage((SqlBasicCall) node);
        }

        SqlNodeList queryNodes = null;
        SqlNode tablesNode = null;
        SqlNode whereClause = null;

        switch (node.getKind()) {
            case SELECT:
                addSelectScope();
                tablesNode = ((SqlSelect) node).getFrom();
                if (tablesNode instanceof SqlJoin) {
                    ((SqlJoin) tablesNode).accept(this);
                }
                else {
                    addTableToNamespace(tablesNode);
                }
                queryNodes = ((SqlSelect) node).getSelectList();
                whereClause = ((SqlSelect) node).getWhere();

                currentNodeContext = NodeContext.SELECT;
                queryNodes.accept(this);

                if (whereClause != null) {
                    currentNodeContext = NodeContext.FILTER;
                    whereClause.accept(this);
                }

                SqlNodeList groupByNodes = ((SqlSelect) node).getGroup();
                if (groupByNodes != null) {
                    currentNodeContext = NodeContext.GROUP_BY;
                    groupByNodes.accept(this);
                }
                rollUpScope();
                break;
            case UPDATE:
                tablesNode = ((SqlUpdate) node).getTargetTable();
                addTableToNamespace(tablesNode);
                queryNodes = ((SqlUpdate) node).getTargetColumnList();
                whereClause = ((SqlUpdate) node).getCondition();
                queryNodes.accept(this);
                if (whereClause != null) {
                    whereClause.accept(this);
                }
                break;
            case ORDER_BY:
                for (SqlNode operand : node.getOperandList()) {
                    if (operand instanceof SqlNodeList) {
                        currentNodeContext = NodeContext.ORDER_BY;
                        addOrderByScope();
                        operand.accept(this);
                        rollUpScope();
                    }
                    else if (operand instanceof SqlSelect) {
                        operand.accept(this);
                    }
                }
                break;
            case JOIN:
                SqlJoin joinNode = ((SqlJoin) node);
                SqlNode left = joinNode.getLeft();
                if (left instanceof SqlJoin ||
                        left instanceof SqlBasicCall && ((SqlBasicCall) left).operand(0) instanceof SqlSelect ) {
                    left.accept(this);
                }
                else {
                    addTableToNamespace(left);
                }
                SqlNode right = joinNode.getRight();
                if (right instanceof SqlJoin ||
                        right instanceof SqlBasicCall && ((SqlBasicCall) right).operand(0) instanceof SqlSelect ) {
                    right.accept(this);
                }
                else {
                    addTableToNamespace(right);
                }

                SqlNode condition = joinNode. getCondition();
                if (condition != null) {
                    currentNodeContext = NodeContext.JOIN_KEY;
                    condition.accept(this);
                    addJoinToLineage(joinNode);
                }
                break;
            case AS:
                node.operand(0).accept(this);
                break;
            default:
                String opName = (node).getOperator().getName();
                if ("COUNT".equals(opName) || "AVG".equals(opName) || "SUM".equals(opName) ||
                        "MIN".equals(opName) || "MAX".equals(opName)) {
                    currentNodeContext = NodeContext.AGGREGATION;
                }
                for (SqlNode operand : node.getOperandList()) {
                    operand.accept(this);
                }
                break;
        }
        return node;
    }

    @Override
    public SqlNode visit(final SqlNodeList columnNodes) {
        for (SqlNode node : columnNodes.getList()) {
            node.accept(this);
        }
        return columnNodes;
    }
}
