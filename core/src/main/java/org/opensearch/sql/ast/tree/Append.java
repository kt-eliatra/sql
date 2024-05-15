package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.List;
import java.util.Map;

@ToString
@EqualsAndHashCode(callSuper = false)
@Getter
@RequiredArgsConstructor
public class Append extends UnresolvedPlan {
    private final List<Map<UnresolvedExpression, String>> appendList;
    private UnresolvedPlan child;

    public Append(List<Map<UnresolvedExpression, String>> appendList, UnresolvedPlan child) {
        this.appendList = appendList;
        this.child = child;
    }

    @Override
    public Append attach(UnresolvedPlan child) {
        if (null == this.child) {
            this.child = child;
        } else {
            this.child.attach(child);
        }
        return this;
    }

    @Override
    public List<UnresolvedPlan> getChild() {
        return ImmutableList.of(child);
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitAppend(this, context);
    }
}
