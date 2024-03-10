import cn.com.ptpress.cdm.optimization.RelBuilder.Utils.SqlToRelNode;
import cn.com.ptpress.cdm.optimization.RelBuilder.csvRelNode.CSVProjectWithCost;
import cn.com.ptpress.cdm.optimization.RelBuilder.optimizer.CSVProjectRule;
import cn.com.ptpress.cdm.optimization.RelBuilder.optimizer.CSVProjectRuleWithCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;


class PlannerTest {


    /**
     * todo: 2024/3/10 14:49 九师兄
     * 测试点： 测试   先注册 CSVProjectRuleWithCost  后注册  CSVProjectRule 会产生什么样的结果？
     *
     * 测试结果如下
     * LogicalProject(ID=[$0])
     *   LogicalTableScan(table=[[csv, data]])
     *
     * ===========RBO优化结果============
     * CSVProjectWithCost(ID=[$0])
     *   LogicalTableScan(table=[[csv, data]])
     *
     *  【Calcite】Calcite自定义优化规则实战
     *   https://blog.csdn.net/qq_21383435/article/details/136600521
     *
     **/
    @Test
    public void testCustomRule1() throws SqlParseException {
        final String sql = "select Id from data ";
        HepProgramBuilder programBuilder = HepProgram.builder();
        HepPlanner hepPlanner =
                new HepPlanner(
                        // 先注册 CSVProjectRuleWithCost  后注册  CSVProjectRule
                        programBuilder.addRuleInstance(CSVProjectRuleWithCost.Config.DEFAULT.toRule())
                                .addRuleInstance(CSVProjectRule.Config.DEFAULT.toRule())
                                .build());
        RelNode relNode = SqlToRelNode.getSqlNode(sql, hepPlanner);
        //未优化算子树结构
        System.out.println(RelOptUtil.toString(relNode));
        RelOptPlanner planner = relNode.getCluster().getPlanner();
        planner.setRoot(relNode);
        RelNode bestExp = planner.findBestExp();
        //优化后接结果
        System.out.println("===========RBO优化结果============");
        System.out.println(RelOptUtil.toString(bestExp));


    }

    /**
     * todo: 2024/3/10 14:50 九师兄
     *
     * 测试点： 测试  先注册 CSVProjectRule  后注册  CSVProjectRuleWithCost
     * 会产生什么样的结果？
     *
     * 测试结果如下
     * LogicalProject(ID=[$0])
     *   LogicalTableScan(table=[[csv, data]])
     *
     * ===========RBO优化结果============
     * CSVProject(ID=[$0])
     *   LogicalTableScan(table=[[csv, data]])
     *
     *
     *  【Calcite】Calcite自定义优化规则实战
     *   https://blog.csdn.net/qq_21383435/article/details/136600521
     *
     **/
    @Test
    public void testCustomRule2() throws SqlParseException {
        final String sql = "select Id from data ";
        HepProgramBuilder programBuilder = HepProgram.builder();
        HepPlanner hepPlanner =
                new HepPlanner(
                        // 先注册 CSVProjectRule  后注册  CSVProjectRuleWithCost
                        programBuilder.addRuleInstance(CSVProjectRule.Config.DEFAULT.toRule())
                                .addRuleInstance(CSVProjectRuleWithCost.Config.DEFAULT.toRule())
                                .build());
        RelNode relNode = SqlToRelNode.getSqlNode(sql, hepPlanner);
        //未优化算子树结构
        System.out.println(RelOptUtil.toString(relNode));
        RelOptPlanner planner = relNode.getCluster().getPlanner();
        planner.setRoot(relNode);
        RelNode bestExp = planner.findBestExp();
        //优化后接结果
        System.out.println("===========RBO优化结果============");
        System.out.println(RelOptUtil.toString(bestExp));
    }



    /**
     * todo: 2024/3/10 14:00 九师兄
     * 这个不知道写的哈玩意
     *
     * LogicalProject(ID=[$0])
     *   LogicalTableScan(table=[[csv, data]])
     *
     * ===========RBO优化结果============
     * CSVProjectWithCost(ID=[$0])
     *   LogicalTableScan(table=[[csv, data]])
     *
     * ===========CBO优化结果============
     * CSVProjectWithCost(ID=[$0])
     *   LogicalTableScan(table=[[csv, data]])
     **/
    @Test
    public void testCustomRule() throws SqlParseException {
        final String sql = "select Id from data ";
        HepProgramBuilder programBuilder = HepProgram.builder();
        HepPlanner hepPlanner =
                new HepPlanner(
                        // 先注册 CSVProjectRuleWithCost  后注册  CSVProjectRule
                        programBuilder.addRuleInstance(CSVProjectRuleWithCost.Config.DEFAULT.toRule())
                                .addRuleInstance(CSVProjectRule.Config.DEFAULT.toRule())
                                .build());
        RelNode relNode = SqlToRelNode.getSqlNode(sql, hepPlanner);
        //未优化算子树结构
        System.out.println(RelOptUtil.toString(relNode));
        RelOptPlanner planner = relNode.getCluster().getPlanner();
        planner.setRoot(relNode);
        RelNode bestExp = planner.findBestExp();
        //优化后接结果
        System.out.println("===========RBO优化结果============");
        System.out.println(RelOptUtil.toString(bestExp));
        RelOptPlanner relOptPlanner = relNode.getCluster().getPlanner();
        relOptPlanner.addRule(CSVProjectRule.Config.DEFAULT.toRule());
        relOptPlanner.addRule(CSVProjectRuleWithCost.Config.DEFAULT.toRule());
        relOptPlanner.setRoot(relNode);
        RelNode exp = relOptPlanner.findBestExp();
        System.out.println("===========CBO优化结果============");
        System.out.println(RelOptUtil.toString(exp));


    }



    /**
     * todo: 2024/3/10 13:58 九师兄
     * 测试点：测试下推
     *
     * LogicalProject(ID=[$0])
     *   LogicalFilter(condition=[>(CAST($0):INTEGER NOT NULL, 1)])
     *     LogicalJoin(condition=[=($0, $3)], joinType=[inner])
     *       LogicalTableScan(table=[[csv, data]])
     *       LogicalTableScan(table=[[csv, data]])
     *
     * LogicalProject(ID=[$0])
     *   LogicalJoin(condition=[=($0, $3)], joinType=[inner])
     *     LogicalFilter(condition=[>(CAST($0):INTEGER NOT NULL, 1)])
     *       LogicalTableScan(table=[[csv, data]])
     *     LogicalTableScan(table=[[csv, data]])
     **/
    @Test
    public void testHepPlanner() throws SqlParseException {
        final String sql = "select a.Id from data as a  join data b on a.Id = b.Id where a.Id>1";
        HepProgramBuilder programBuilder = HepProgram.builder();
        HepPlanner hepPlanner =
                new HepPlanner(
                        programBuilder.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.Config.DEFAULT.toRule())
                                .build());
        RelNode relNode = SqlToRelNode.getSqlNode(sql, hepPlanner);
        //未优化算子树结构
        System.out.println(RelOptUtil.toString(relNode));
        RelOptPlanner planner = relNode.getCluster().getPlanner();
        planner.setRoot(relNode);
        RelNode bestExp = planner.findBestExp();
        //优化后接结果
        System.out.println(RelOptUtil.toString(bestExp));

    }

    /**
     * todo: 2024/3/10 14:00 九师兄
     *
     * LogicalProject(Id=[$0], Name=[$1], Score=[$2])
     *   LogicalFilter(condition=[=(CAST($0):INTEGER NOT NULL, 1)])
     *     LogicalTableScan(table=[[csv, data]])
     **/
    @Test
    public void testGraph() throws SqlParseException {
        final String sql = "select * from data where Id=1";
        HepProgramBuilder programBuilder = HepProgram.builder();
        HepPlanner hepPlanner =
                new HepPlanner(
                        programBuilder.build());
        RelNode relNode = SqlToRelNode.getSqlNode(sql, hepPlanner);
        //未转化Dag算子树结构
        System.out.println(RelOptUtil.toString(relNode));
        //转化为Dag图
        hepPlanner.setRoot(relNode);
        //查看需要把log4j.properties级别改为trace

    }
}
