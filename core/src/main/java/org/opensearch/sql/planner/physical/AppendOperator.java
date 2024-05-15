package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.ReferenceExpression;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

@EqualsAndHashCode(callSuper = false)
@ToString
public class AppendOperator extends PhysicalPlan {
    @Getter private final PhysicalPlan input;
    @Getter private final Map<ReferenceExpression, String> append;
    @Getter private final Map<String, String> appendMapping;



    /** Constructor of RenameOperator. */
    public AppendOperator(PhysicalPlan input, Map<ReferenceExpression, String> append) {
        this.input = input;
        this.append = append;
        this.appendMapping =
                append.entrySet().stream()
                        .collect(
                                Collectors.toMap(entry -> entry.getKey().getAttr(), Map.Entry::getValue));
    }

    @Override
    public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
        return visitor.visitAppend(this, context);
    }

    @Override
    public List<PhysicalPlan> getChild() {
        return Collections.singletonList(input);
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public ExprValue next() {
        ExprValue inputValue = input.next();
        if (STRUCT == inputValue.type()) {
            Map<String, ExprValue> tupleValue = ExprValueUtils.getTupleValue(inputValue);
            ImmutableMap.Builder<String, ExprValue> mapBuilder = new ImmutableMap.Builder<>();
            for (String fieldName : tupleValue.keySet()) {
                if (appendMapping.containsKey(fieldName) && tupleValue.get(fieldName).type().getParent().stream().anyMatch(exprType -> exprType.isCompatible(ExprCoreType.STRING))) {
                    mapBuilder.put(fieldName, new ExprStringValue(tupleValue.get(fieldName).stringValue() + appendMapping.get(fieldName)));
                } else {
                    mapBuilder.put(fieldName, tupleValue.get(fieldName));
                }
            }
            return ExprTupleValue.fromExprValueMap(mapBuilder.build());
        } else {
            return inputValue;
        }
    }
}
