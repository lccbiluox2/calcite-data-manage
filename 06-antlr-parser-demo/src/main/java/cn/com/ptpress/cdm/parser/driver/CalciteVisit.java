package cn.com.ptpress.cdm.parser.driver;

import cn.com.ptpress.cdm.parser.load.SqlColMapping;
import cn.com.ptpress.cdm.parser.load.SqlLoad;
import cn.com.ptpress.cdm.parser.load.SqlLoadSource;
import cn.com.ptpress.cdm.parser.parser.CalciteRulesBaseVisitor;
import cn.com.ptpress.cdm.parser.parser.CalciteRulesParser;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * @author sunxiaojun
 * @date 2021/5/16
 * @since 2.0.0
 */
public class CalciteVisit extends CalciteRulesBaseVisitor<SqlNode> {

    /**
     * SqlParserPosition
     */
    private SqlParserPos pos = SqlParserPos.ZERO;

    @Override
    public SqlNode visitProgram(CalciteRulesParser.ProgramContext ctx) {
        return visitStmt(ctx.stmt());
    }

    @Override
    public SqlNode visitStmt(CalciteRulesParser.StmtContext ctx) {
        return visitLoadStmt(ctx.loadStmt());
    }

    @Override
    public SqlNode visitLoadStmt(CalciteRulesParser.LoadStmtContext ctx) {
        // 获取两边的数据源信息并且进行封装
        CalciteRulesParser.LoadFromStmtContext loadFromStmtContext = ctx.loadFromStmt();
        CalciteRulesParser.LoadFromStmtContext loadToStmtContext = ctx.loadFromStmt();

        // 构建 源信息
        SqlLoadSource loadFromSource = new SqlLoadSource(new SqlIdentifier(loadFromStmtContext.IDENTIFIER().getText(), pos),
                loadFromStmtContext.STRING().getText());
        // 构建 目的信息
        SqlLoadSource loadToSource = new SqlLoadSource(new SqlIdentifier(loadToStmtContext.IDENTIFIER().getText(), pos),
                loadToStmtContext.STRING().getText());

        // 构建列信息
        List<TerminalNode> identifier = ctx.loadColumns().columnsItem().IDENTIFIER();
        SqlNodeList sqlNodeList = new SqlNodeList(pos);
        for (int i = 0; i <identifier.size() ; i+=2) {
            String fromColumn= identifier.get(i).getText();
            String toColumn= identifier.get(i).getText();
            sqlNodeList.add(new SqlColMapping(pos,
                    new SqlIdentifier(fromColumn,pos),new SqlIdentifier(toColumn,pos)));
        }
        String sep=",";
        if (ctx.SEPARATOR()!=null){
            sep=ctx.STRING().getText();
        }
        return new SqlLoad(pos,loadFromSource,loadToSource,sqlNodeList,sep);
    }
}
