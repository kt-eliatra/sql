package org.opensearch.sql.planner.logical;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.ReferenceExpression;

import java.util.Collections;
import java.util.Map;

/** Append Operator. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalAppend extends LogicalPlan {

    @Getter
    private final Map<ReferenceExpression, String> appendMap;

    /** Constructor of LogicalRename. */
    public LogicalAppend(LogicalPlan child, Map<ReferenceExpression, String> renameMap) {
        super(Collections.singletonList(child));
        this.appendMap = renameMap;
    }

    @Override
    public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
        return visitor.visitAppend(this, context);
    }
}
