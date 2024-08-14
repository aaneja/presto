package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.translator.TranslatedExpression;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.plugin.jdbc.optimization.JdbcExpression;
import com.facebook.presto.plugin.jdbc.optimization.JdbcFilterToSqlTranslator;
import com.facebook.presto.plugin.jdbc.optimization.function.OperatorTranslators;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.expressions.translator.FunctionTranslator.buildFunctionTranslator;
import static com.facebook.presto.expressions.translator.RowExpressionTreeTranslator.translateWith;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static org.testng.Assert.assertEquals;

public class JdbcToSqlTranslatorTest
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final FunctionAndTypeManager functionAndTypeManager = METADATA.getFunctionAndTypeManager();
    private static final TestingRowExpressionTranslator sqlToRowExpressionTranslator = new TestingRowExpressionTranslator(METADATA);
    public static final JdbcTypeHandle FAKE_JDBC_TYPE_HANDLE = new JdbcTypeHandle(1, "t", 1, 1);
    private final JdbcFilterToSqlTranslator jdbcFilterToSqlTranslator;

    public JdbcToSqlTranslatorTest()
    {
        this.jdbcFilterToSqlTranslator = new JdbcFilterToSqlTranslator(
                functionAndTypeManager,
                buildFunctionTranslator(getFunctionTranslators()),
                "'");
    }

    @Test
    public void testTranslationOfAndExpression()
    {
        String untranslated = "col1 = col2 AND col3 = col4";
        TypeProvider typeProvider = buildTypeProvider(4);

        RowExpression specialForm = sqlToRowExpressionTranslator.translate(expression(untranslated), typeProvider);
        Map<VariableReferenceExpression, ColumnHandle> assignmentsMap = buildFakeAssignmentsForAllVariables(specialForm);

        TranslatedExpression<JdbcExpression> translatedExpression = translateWith(
                specialForm,
                jdbcFilterToSqlTranslator,
                assignmentsMap);

        assertEquals(translatedExpression.getTranslated().get().getExpression(), "((('col1' = 'col2')) AND (('col3' = 'col4')))");
    }

    private static TypeProvider buildTypeProvider(int fakeBigIntColumCount)
    {
        ImmutableMap.Builder<String, Type> builder = ImmutableMap.builder();
        for (int i = 1; i <= fakeBigIntColumCount; i++) {
            builder.put("col" + i, BIGINT);
        }

        return TypeProvider.copyOf(builder.build());
    }

    private ImmutableMap<VariableReferenceExpression, ColumnHandle> buildFakeAssignmentsForAllVariables(RowExpression rowExpression)
    {
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> builder = ImmutableMap.builder();
        for (VariableReferenceExpression variable : VariablesExtractor.extractUnique(rowExpression)) {
            String columnName = variable.getName();
            builder.put(variable, new JdbcColumnHandle("catalog", columnName, FAKE_JDBC_TYPE_HANDLE, BIGINT, false, Optional.empty()));
        }

        return builder.build();
    }

    private Set<Class<?>> getFunctionTranslators()
    {
        return ImmutableSet.of(OperatorTranslators.class);
    }
}
