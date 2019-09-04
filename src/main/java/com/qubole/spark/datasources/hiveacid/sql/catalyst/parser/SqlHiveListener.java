// Generated from SqlHive.g4 by ANTLR 4.7.2
package com.qubole.spark.datasources.hiveacid.sql.catalyst.parser;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link SqlHiveParser}.
 */
public interface SqlHiveListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void enterSingleStatement(SqlHiveParser.SingleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void exitSingleStatement(SqlHiveParser.SingleStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#singleExpression}.
	 * @param ctx the parse tree
	 */
	void enterSingleExpression(SqlHiveParser.SingleExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#singleExpression}.
	 * @param ctx the parse tree
	 */
	void exitSingleExpression(SqlHiveParser.SingleExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#singleTableIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterSingleTableIdentifier(SqlHiveParser.SingleTableIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#singleTableIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitSingleTableIdentifier(SqlHiveParser.SingleTableIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#singleFunctionIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterSingleFunctionIdentifier(SqlHiveParser.SingleFunctionIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#singleFunctionIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitSingleFunctionIdentifier(SqlHiveParser.SingleFunctionIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#singleDataType}.
	 * @param ctx the parse tree
	 */
	void enterSingleDataType(SqlHiveParser.SingleDataTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#singleDataType}.
	 * @param ctx the parse tree
	 */
	void exitSingleDataType(SqlHiveParser.SingleDataTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#singleTableSchema}.
	 * @param ctx the parse tree
	 */
	void enterSingleTableSchema(SqlHiveParser.SingleTableSchemaContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#singleTableSchema}.
	 * @param ctx the parse tree
	 */
	void exitSingleTableSchema(SqlHiveParser.SingleTableSchemaContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementDefault(SqlHiveParser.StatementDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementDefault(SqlHiveParser.StatementDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code deleteCommand}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDeleteCommand(SqlHiveParser.DeleteCommandContext ctx);
	/**
	 * Exit a parse tree produced by the {@code deleteCommand}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDeleteCommand(SqlHiveParser.DeleteCommandContext ctx);
	/**
	 * Enter a parse tree produced by the {@code updateCommand}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterUpdateCommand(SqlHiveParser.UpdateCommandContext ctx);
	/**
	 * Exit a parse tree produced by the {@code updateCommand}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitUpdateCommand(SqlHiveParser.UpdateCommandContext ctx);
	/**
	 * Enter a parse tree produced by the {@code use}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterUse(SqlHiveParser.UseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code use}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitUse(SqlHiveParser.UseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createDatabase}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateDatabase(SqlHiveParser.CreateDatabaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createDatabase}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateDatabase(SqlHiveParser.CreateDatabaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setDatabaseProperties}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterSetDatabaseProperties(SqlHiveParser.SetDatabasePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setDatabaseProperties}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitSetDatabaseProperties(SqlHiveParser.SetDatabasePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropDatabase}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDropDatabase(SqlHiveParser.DropDatabaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropDatabase}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDropDatabase(SqlHiveParser.DropDatabaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateTable(SqlHiveParser.CreateTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateTable(SqlHiveParser.CreateTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createHiveTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateHiveTable(SqlHiveParser.CreateHiveTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createHiveTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateHiveTable(SqlHiveParser.CreateHiveTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createTableLike}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateTableLike(SqlHiveParser.CreateTableLikeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createTableLike}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateTableLike(SqlHiveParser.CreateTableLikeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code analyze}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterAnalyze(SqlHiveParser.AnalyzeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code analyze}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitAnalyze(SqlHiveParser.AnalyzeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addTableColumns}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterAddTableColumns(SqlHiveParser.AddTableColumnsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addTableColumns}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitAddTableColumns(SqlHiveParser.AddTableColumnsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterRenameTable(SqlHiveParser.RenameTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitRenameTable(SqlHiveParser.RenameTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterSetTableProperties(SqlHiveParser.SetTablePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitSetTableProperties(SqlHiveParser.SetTablePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unsetTableProperties}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterUnsetTableProperties(SqlHiveParser.UnsetTablePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unsetTableProperties}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitUnsetTableProperties(SqlHiveParser.UnsetTablePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code changeColumn}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterChangeColumn(SqlHiveParser.ChangeColumnContext ctx);
	/**
	 * Exit a parse tree produced by the {@code changeColumn}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitChangeColumn(SqlHiveParser.ChangeColumnContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setTableSerDe}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterSetTableSerDe(SqlHiveParser.SetTableSerDeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setTableSerDe}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitSetTableSerDe(SqlHiveParser.SetTableSerDeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addTablePartition}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterAddTablePartition(SqlHiveParser.AddTablePartitionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addTablePartition}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitAddTablePartition(SqlHiveParser.AddTablePartitionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code renameTablePartition}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterRenameTablePartition(SqlHiveParser.RenameTablePartitionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code renameTablePartition}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitRenameTablePartition(SqlHiveParser.RenameTablePartitionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropTablePartitions}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDropTablePartitions(SqlHiveParser.DropTablePartitionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropTablePartitions}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDropTablePartitions(SqlHiveParser.DropTablePartitionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setTableLocation}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterSetTableLocation(SqlHiveParser.SetTableLocationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setTableLocation}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitSetTableLocation(SqlHiveParser.SetTableLocationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code recoverPartitions}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterRecoverPartitions(SqlHiveParser.RecoverPartitionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code recoverPartitions}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitRecoverPartitions(SqlHiveParser.RecoverPartitionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDropTable(SqlHiveParser.DropTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDropTable(SqlHiveParser.DropTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createView}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateView(SqlHiveParser.CreateViewContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createView}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateView(SqlHiveParser.CreateViewContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createTempViewUsing}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateTempViewUsing(SqlHiveParser.CreateTempViewUsingContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createTempViewUsing}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateTempViewUsing(SqlHiveParser.CreateTempViewUsingContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterViewQuery}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterAlterViewQuery(SqlHiveParser.AlterViewQueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterViewQuery}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitAlterViewQuery(SqlHiveParser.AlterViewQueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createFunction}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateFunction(SqlHiveParser.CreateFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createFunction}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateFunction(SqlHiveParser.CreateFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropFunction}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDropFunction(SqlHiveParser.DropFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropFunction}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDropFunction(SqlHiveParser.DropFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code explain}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterExplain(SqlHiveParser.ExplainContext ctx);
	/**
	 * Exit a parse tree produced by the {@code explain}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitExplain(SqlHiveParser.ExplainContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTables}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowTables(SqlHiveParser.ShowTablesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTables}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowTables(SqlHiveParser.ShowTablesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowTable(SqlHiveParser.ShowTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowTable(SqlHiveParser.ShowTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showDatabases}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowDatabases(SqlHiveParser.ShowDatabasesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showDatabases}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowDatabases(SqlHiveParser.ShowDatabasesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTblProperties}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowTblProperties(SqlHiveParser.ShowTblPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTblProperties}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowTblProperties(SqlHiveParser.ShowTblPropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowColumns(SqlHiveParser.ShowColumnsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowColumns(SqlHiveParser.ShowColumnsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showPartitions}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowPartitions(SqlHiveParser.ShowPartitionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showPartitions}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowPartitions(SqlHiveParser.ShowPartitionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showFunctions}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowFunctions(SqlHiveParser.ShowFunctionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showFunctions}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowFunctions(SqlHiveParser.ShowFunctionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCreateTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateTable(SqlHiveParser.ShowCreateTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCreateTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateTable(SqlHiveParser.ShowCreateTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code describeFunction}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDescribeFunction(SqlHiveParser.DescribeFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code describeFunction}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDescribeFunction(SqlHiveParser.DescribeFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code describeDatabase}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDescribeDatabase(SqlHiveParser.DescribeDatabaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code describeDatabase}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDescribeDatabase(SqlHiveParser.DescribeDatabaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code describeTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDescribeTable(SqlHiveParser.DescribeTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code describeTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDescribeTable(SqlHiveParser.DescribeTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code refreshTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterRefreshTable(SqlHiveParser.RefreshTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code refreshTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitRefreshTable(SqlHiveParser.RefreshTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code refreshResource}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterRefreshResource(SqlHiveParser.RefreshResourceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code refreshResource}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitRefreshResource(SqlHiveParser.RefreshResourceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cacheTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCacheTable(SqlHiveParser.CacheTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cacheTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCacheTable(SqlHiveParser.CacheTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code uncacheTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterUncacheTable(SqlHiveParser.UncacheTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code uncacheTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitUncacheTable(SqlHiveParser.UncacheTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code clearCache}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterClearCache(SqlHiveParser.ClearCacheContext ctx);
	/**
	 * Exit a parse tree produced by the {@code clearCache}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitClearCache(SqlHiveParser.ClearCacheContext ctx);
	/**
	 * Enter a parse tree produced by the {@code loadData}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterLoadData(SqlHiveParser.LoadDataContext ctx);
	/**
	 * Exit a parse tree produced by the {@code loadData}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitLoadData(SqlHiveParser.LoadDataContext ctx);
	/**
	 * Enter a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterTruncateTable(SqlHiveParser.TruncateTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitTruncateTable(SqlHiveParser.TruncateTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code repairTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterRepairTable(SqlHiveParser.RepairTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code repairTable}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitRepairTable(SqlHiveParser.RepairTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code manageResource}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterManageResource(SqlHiveParser.ManageResourceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code manageResource}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitManageResource(SqlHiveParser.ManageResourceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code failNativeCommand}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterFailNativeCommand(SqlHiveParser.FailNativeCommandContext ctx);
	/**
	 * Exit a parse tree produced by the {@code failNativeCommand}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitFailNativeCommand(SqlHiveParser.FailNativeCommandContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setConfiguration}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterSetConfiguration(SqlHiveParser.SetConfigurationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setConfiguration}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitSetConfiguration(SqlHiveParser.SetConfigurationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code resetConfiguration}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterResetConfiguration(SqlHiveParser.ResetConfigurationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code resetConfiguration}
	 * labeled alternative in {@link SqlHiveParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitResetConfiguration(SqlHiveParser.ResetConfigurationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#unsupportedHiveNativeCommands}.
	 * @param ctx the parse tree
	 */
	void enterUnsupportedHiveNativeCommands(SqlHiveParser.UnsupportedHiveNativeCommandsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#unsupportedHiveNativeCommands}.
	 * @param ctx the parse tree
	 */
	void exitUnsupportedHiveNativeCommands(SqlHiveParser.UnsupportedHiveNativeCommandsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#delete}.
	 * @param ctx the parse tree
	 */
	void enterDelete(SqlHiveParser.DeleteContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#delete}.
	 * @param ctx the parse tree
	 */
	void exitDelete(SqlHiveParser.DeleteContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#update}.
	 * @param ctx the parse tree
	 */
	void enterUpdate(SqlHiveParser.UpdateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#update}.
	 * @param ctx the parse tree
	 */
	void exitUpdate(SqlHiveParser.UpdateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#updateFieldList}.
	 * @param ctx the parse tree
	 */
	void enterUpdateFieldList(SqlHiveParser.UpdateFieldListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#updateFieldList}.
	 * @param ctx the parse tree
	 */
	void exitUpdateFieldList(SqlHiveParser.UpdateFieldListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#updateField}.
	 * @param ctx the parse tree
	 */
	void enterUpdateField(SqlHiveParser.UpdateFieldContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#updateField}.
	 * @param ctx the parse tree
	 */
	void exitUpdateField(SqlHiveParser.UpdateFieldContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#createTableHeader}.
	 * @param ctx the parse tree
	 */
	void enterCreateTableHeader(SqlHiveParser.CreateTableHeaderContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#createTableHeader}.
	 * @param ctx the parse tree
	 */
	void exitCreateTableHeader(SqlHiveParser.CreateTableHeaderContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#bucketSpec}.
	 * @param ctx the parse tree
	 */
	void enterBucketSpec(SqlHiveParser.BucketSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#bucketSpec}.
	 * @param ctx the parse tree
	 */
	void exitBucketSpec(SqlHiveParser.BucketSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#skewSpec}.
	 * @param ctx the parse tree
	 */
	void enterSkewSpec(SqlHiveParser.SkewSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#skewSpec}.
	 * @param ctx the parse tree
	 */
	void exitSkewSpec(SqlHiveParser.SkewSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#locationSpec}.
	 * @param ctx the parse tree
	 */
	void enterLocationSpec(SqlHiveParser.LocationSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#locationSpec}.
	 * @param ctx the parse tree
	 */
	void exitLocationSpec(SqlHiveParser.LocationSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#query}.
	 * @param ctx the parse tree
	 */
	void enterQuery(SqlHiveParser.QueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#query}.
	 * @param ctx the parse tree
	 */
	void exitQuery(SqlHiveParser.QueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code insertOverwriteTable}
	 * labeled alternative in {@link SqlHiveParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void enterInsertOverwriteTable(SqlHiveParser.InsertOverwriteTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code insertOverwriteTable}
	 * labeled alternative in {@link SqlHiveParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void exitInsertOverwriteTable(SqlHiveParser.InsertOverwriteTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code insertIntoTable}
	 * labeled alternative in {@link SqlHiveParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void enterInsertIntoTable(SqlHiveParser.InsertIntoTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code insertIntoTable}
	 * labeled alternative in {@link SqlHiveParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void exitInsertIntoTable(SqlHiveParser.InsertIntoTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code insertOverwriteHiveDir}
	 * labeled alternative in {@link SqlHiveParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void enterInsertOverwriteHiveDir(SqlHiveParser.InsertOverwriteHiveDirContext ctx);
	/**
	 * Exit a parse tree produced by the {@code insertOverwriteHiveDir}
	 * labeled alternative in {@link SqlHiveParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void exitInsertOverwriteHiveDir(SqlHiveParser.InsertOverwriteHiveDirContext ctx);
	/**
	 * Enter a parse tree produced by the {@code insertOverwriteDir}
	 * labeled alternative in {@link SqlHiveParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void enterInsertOverwriteDir(SqlHiveParser.InsertOverwriteDirContext ctx);
	/**
	 * Exit a parse tree produced by the {@code insertOverwriteDir}
	 * labeled alternative in {@link SqlHiveParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void exitInsertOverwriteDir(SqlHiveParser.InsertOverwriteDirContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#partitionSpecLocation}.
	 * @param ctx the parse tree
	 */
	void enterPartitionSpecLocation(SqlHiveParser.PartitionSpecLocationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#partitionSpecLocation}.
	 * @param ctx the parse tree
	 */
	void exitPartitionSpecLocation(SqlHiveParser.PartitionSpecLocationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#partitionSpec}.
	 * @param ctx the parse tree
	 */
	void enterPartitionSpec(SqlHiveParser.PartitionSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#partitionSpec}.
	 * @param ctx the parse tree
	 */
	void exitPartitionSpec(SqlHiveParser.PartitionSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#partitionVal}.
	 * @param ctx the parse tree
	 */
	void enterPartitionVal(SqlHiveParser.PartitionValContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#partitionVal}.
	 * @param ctx the parse tree
	 */
	void exitPartitionVal(SqlHiveParser.PartitionValContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#describeFuncName}.
	 * @param ctx the parse tree
	 */
	void enterDescribeFuncName(SqlHiveParser.DescribeFuncNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#describeFuncName}.
	 * @param ctx the parse tree
	 */
	void exitDescribeFuncName(SqlHiveParser.DescribeFuncNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#describeColName}.
	 * @param ctx the parse tree
	 */
	void enterDescribeColName(SqlHiveParser.DescribeColNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#describeColName}.
	 * @param ctx the parse tree
	 */
	void exitDescribeColName(SqlHiveParser.DescribeColNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#ctes}.
	 * @param ctx the parse tree
	 */
	void enterCtes(SqlHiveParser.CtesContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#ctes}.
	 * @param ctx the parse tree
	 */
	void exitCtes(SqlHiveParser.CtesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#namedQuery}.
	 * @param ctx the parse tree
	 */
	void enterNamedQuery(SqlHiveParser.NamedQueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#namedQuery}.
	 * @param ctx the parse tree
	 */
	void exitNamedQuery(SqlHiveParser.NamedQueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#tableProvider}.
	 * @param ctx the parse tree
	 */
	void enterTableProvider(SqlHiveParser.TableProviderContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#tableProvider}.
	 * @param ctx the parse tree
	 */
	void exitTableProvider(SqlHiveParser.TableProviderContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#tablePropertyList}.
	 * @param ctx the parse tree
	 */
	void enterTablePropertyList(SqlHiveParser.TablePropertyListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#tablePropertyList}.
	 * @param ctx the parse tree
	 */
	void exitTablePropertyList(SqlHiveParser.TablePropertyListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#tableProperty}.
	 * @param ctx the parse tree
	 */
	void enterTableProperty(SqlHiveParser.TablePropertyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#tableProperty}.
	 * @param ctx the parse tree
	 */
	void exitTableProperty(SqlHiveParser.TablePropertyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#tablePropertyKey}.
	 * @param ctx the parse tree
	 */
	void enterTablePropertyKey(SqlHiveParser.TablePropertyKeyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#tablePropertyKey}.
	 * @param ctx the parse tree
	 */
	void exitTablePropertyKey(SqlHiveParser.TablePropertyKeyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#tablePropertyValue}.
	 * @param ctx the parse tree
	 */
	void enterTablePropertyValue(SqlHiveParser.TablePropertyValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#tablePropertyValue}.
	 * @param ctx the parse tree
	 */
	void exitTablePropertyValue(SqlHiveParser.TablePropertyValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#constantList}.
	 * @param ctx the parse tree
	 */
	void enterConstantList(SqlHiveParser.ConstantListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#constantList}.
	 * @param ctx the parse tree
	 */
	void exitConstantList(SqlHiveParser.ConstantListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#nestedConstantList}.
	 * @param ctx the parse tree
	 */
	void enterNestedConstantList(SqlHiveParser.NestedConstantListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#nestedConstantList}.
	 * @param ctx the parse tree
	 */
	void exitNestedConstantList(SqlHiveParser.NestedConstantListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#createFileFormat}.
	 * @param ctx the parse tree
	 */
	void enterCreateFileFormat(SqlHiveParser.CreateFileFormatContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#createFileFormat}.
	 * @param ctx the parse tree
	 */
	void exitCreateFileFormat(SqlHiveParser.CreateFileFormatContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tableFileFormat}
	 * labeled alternative in {@link SqlHiveParser#fileFormat}.
	 * @param ctx the parse tree
	 */
	void enterTableFileFormat(SqlHiveParser.TableFileFormatContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tableFileFormat}
	 * labeled alternative in {@link SqlHiveParser#fileFormat}.
	 * @param ctx the parse tree
	 */
	void exitTableFileFormat(SqlHiveParser.TableFileFormatContext ctx);
	/**
	 * Enter a parse tree produced by the {@code genericFileFormat}
	 * labeled alternative in {@link SqlHiveParser#fileFormat}.
	 * @param ctx the parse tree
	 */
	void enterGenericFileFormat(SqlHiveParser.GenericFileFormatContext ctx);
	/**
	 * Exit a parse tree produced by the {@code genericFileFormat}
	 * labeled alternative in {@link SqlHiveParser#fileFormat}.
	 * @param ctx the parse tree
	 */
	void exitGenericFileFormat(SqlHiveParser.GenericFileFormatContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#storageHandler}.
	 * @param ctx the parse tree
	 */
	void enterStorageHandler(SqlHiveParser.StorageHandlerContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#storageHandler}.
	 * @param ctx the parse tree
	 */
	void exitStorageHandler(SqlHiveParser.StorageHandlerContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#resource}.
	 * @param ctx the parse tree
	 */
	void enterResource(SqlHiveParser.ResourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#resource}.
	 * @param ctx the parse tree
	 */
	void exitResource(SqlHiveParser.ResourceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code singleInsertQuery}
	 * labeled alternative in {@link SqlHiveParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
	void enterSingleInsertQuery(SqlHiveParser.SingleInsertQueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code singleInsertQuery}
	 * labeled alternative in {@link SqlHiveParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
	void exitSingleInsertQuery(SqlHiveParser.SingleInsertQueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code multiInsertQuery}
	 * labeled alternative in {@link SqlHiveParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
	void enterMultiInsertQuery(SqlHiveParser.MultiInsertQueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code multiInsertQuery}
	 * labeled alternative in {@link SqlHiveParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
	void exitMultiInsertQuery(SqlHiveParser.MultiInsertQueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#queryOrganization}.
	 * @param ctx the parse tree
	 */
	void enterQueryOrganization(SqlHiveParser.QueryOrganizationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#queryOrganization}.
	 * @param ctx the parse tree
	 */
	void exitQueryOrganization(SqlHiveParser.QueryOrganizationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#multiInsertQueryBody}.
	 * @param ctx the parse tree
	 */
	void enterMultiInsertQueryBody(SqlHiveParser.MultiInsertQueryBodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#multiInsertQueryBody}.
	 * @param ctx the parse tree
	 */
	void exitMultiInsertQueryBody(SqlHiveParser.MultiInsertQueryBodyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code queryTermDefault}
	 * labeled alternative in {@link SqlHiveParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void enterQueryTermDefault(SqlHiveParser.QueryTermDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code queryTermDefault}
	 * labeled alternative in {@link SqlHiveParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void exitQueryTermDefault(SqlHiveParser.QueryTermDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link SqlHiveParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void enterSetOperation(SqlHiveParser.SetOperationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link SqlHiveParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void exitSetOperation(SqlHiveParser.SetOperationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link SqlHiveParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterQueryPrimaryDefault(SqlHiveParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link SqlHiveParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitQueryPrimaryDefault(SqlHiveParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code table}
	 * labeled alternative in {@link SqlHiveParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTable(SqlHiveParser.TableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code table}
	 * labeled alternative in {@link SqlHiveParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTable(SqlHiveParser.TableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link SqlHiveParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterInlineTableDefault1(SqlHiveParser.InlineTableDefault1Context ctx);
	/**
	 * Exit a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link SqlHiveParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitInlineTableDefault1(SqlHiveParser.InlineTableDefault1Context ctx);
	/**
	 * Enter a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link SqlHiveParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterSubquery(SqlHiveParser.SubqueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link SqlHiveParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitSubquery(SqlHiveParser.SubqueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#sortItem}.
	 * @param ctx the parse tree
	 */
	void enterSortItem(SqlHiveParser.SortItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#sortItem}.
	 * @param ctx the parse tree
	 */
	void exitSortItem(SqlHiveParser.SortItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	void enterQuerySpecification(SqlHiveParser.QuerySpecificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	void exitQuerySpecification(SqlHiveParser.QuerySpecificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#hint}.
	 * @param ctx the parse tree
	 */
	void enterHint(SqlHiveParser.HintContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#hint}.
	 * @param ctx the parse tree
	 */
	void exitHint(SqlHiveParser.HintContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#hintStatement}.
	 * @param ctx the parse tree
	 */
	void enterHintStatement(SqlHiveParser.HintStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#hintStatement}.
	 * @param ctx the parse tree
	 */
	void exitHintStatement(SqlHiveParser.HintStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void enterFromClause(SqlHiveParser.FromClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void exitFromClause(SqlHiveParser.FromClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#aggregation}.
	 * @param ctx the parse tree
	 */
	void enterAggregation(SqlHiveParser.AggregationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#aggregation}.
	 * @param ctx the parse tree
	 */
	void exitAggregation(SqlHiveParser.AggregationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSet(SqlHiveParser.GroupingSetContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSet(SqlHiveParser.GroupingSetContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#pivotClause}.
	 * @param ctx the parse tree
	 */
	void enterPivotClause(SqlHiveParser.PivotClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#pivotClause}.
	 * @param ctx the parse tree
	 */
	void exitPivotClause(SqlHiveParser.PivotClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#pivotColumn}.
	 * @param ctx the parse tree
	 */
	void enterPivotColumn(SqlHiveParser.PivotColumnContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#pivotColumn}.
	 * @param ctx the parse tree
	 */
	void exitPivotColumn(SqlHiveParser.PivotColumnContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#pivotValue}.
	 * @param ctx the parse tree
	 */
	void enterPivotValue(SqlHiveParser.PivotValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#pivotValue}.
	 * @param ctx the parse tree
	 */
	void exitPivotValue(SqlHiveParser.PivotValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#lateralView}.
	 * @param ctx the parse tree
	 */
	void enterLateralView(SqlHiveParser.LateralViewContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#lateralView}.
	 * @param ctx the parse tree
	 */
	void exitLateralView(SqlHiveParser.LateralViewContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
	void enterSetQuantifier(SqlHiveParser.SetQuantifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
	void exitSetQuantifier(SqlHiveParser.SetQuantifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#relation}.
	 * @param ctx the parse tree
	 */
	void enterRelation(SqlHiveParser.RelationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#relation}.
	 * @param ctx the parse tree
	 */
	void exitRelation(SqlHiveParser.RelationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#joinRelation}.
	 * @param ctx the parse tree
	 */
	void enterJoinRelation(SqlHiveParser.JoinRelationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#joinRelation}.
	 * @param ctx the parse tree
	 */
	void exitJoinRelation(SqlHiveParser.JoinRelationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#joinType}.
	 * @param ctx the parse tree
	 */
	void enterJoinType(SqlHiveParser.JoinTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#joinType}.
	 * @param ctx the parse tree
	 */
	void exitJoinType(SqlHiveParser.JoinTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	void enterJoinCriteria(SqlHiveParser.JoinCriteriaContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	void exitJoinCriteria(SqlHiveParser.JoinCriteriaContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#sample}.
	 * @param ctx the parse tree
	 */
	void enterSample(SqlHiveParser.SampleContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#sample}.
	 * @param ctx the parse tree
	 */
	void exitSample(SqlHiveParser.SampleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sampleByPercentile}
	 * labeled alternative in {@link SqlHiveParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void enterSampleByPercentile(SqlHiveParser.SampleByPercentileContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sampleByPercentile}
	 * labeled alternative in {@link SqlHiveParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void exitSampleByPercentile(SqlHiveParser.SampleByPercentileContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sampleByRows}
	 * labeled alternative in {@link SqlHiveParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void enterSampleByRows(SqlHiveParser.SampleByRowsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sampleByRows}
	 * labeled alternative in {@link SqlHiveParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void exitSampleByRows(SqlHiveParser.SampleByRowsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sampleByBucket}
	 * labeled alternative in {@link SqlHiveParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void enterSampleByBucket(SqlHiveParser.SampleByBucketContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sampleByBucket}
	 * labeled alternative in {@link SqlHiveParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void exitSampleByBucket(SqlHiveParser.SampleByBucketContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sampleByBytes}
	 * labeled alternative in {@link SqlHiveParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void enterSampleByBytes(SqlHiveParser.SampleByBytesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sampleByBytes}
	 * labeled alternative in {@link SqlHiveParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void exitSampleByBytes(SqlHiveParser.SampleByBytesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#identifierList}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierList(SqlHiveParser.IdentifierListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#identifierList}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierList(SqlHiveParser.IdentifierListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierSeq(SqlHiveParser.IdentifierSeqContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierSeq(SqlHiveParser.IdentifierSeqContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#orderedIdentifierList}.
	 * @param ctx the parse tree
	 */
	void enterOrderedIdentifierList(SqlHiveParser.OrderedIdentifierListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#orderedIdentifierList}.
	 * @param ctx the parse tree
	 */
	void exitOrderedIdentifierList(SqlHiveParser.OrderedIdentifierListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#orderedIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterOrderedIdentifier(SqlHiveParser.OrderedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#orderedIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitOrderedIdentifier(SqlHiveParser.OrderedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#identifierCommentList}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierCommentList(SqlHiveParser.IdentifierCommentListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#identifierCommentList}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierCommentList(SqlHiveParser.IdentifierCommentListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#identifierComment}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierComment(SqlHiveParser.IdentifierCommentContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#identifierComment}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierComment(SqlHiveParser.IdentifierCommentContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link SqlHiveParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTableName(SqlHiveParser.TableNameContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link SqlHiveParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTableName(SqlHiveParser.TableNameContext ctx);
	/**
	 * Enter a parse tree produced by the {@code aliasedQuery}
	 * labeled alternative in {@link SqlHiveParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterAliasedQuery(SqlHiveParser.AliasedQueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code aliasedQuery}
	 * labeled alternative in {@link SqlHiveParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitAliasedQuery(SqlHiveParser.AliasedQueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code aliasedRelation}
	 * labeled alternative in {@link SqlHiveParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterAliasedRelation(SqlHiveParser.AliasedRelationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code aliasedRelation}
	 * labeled alternative in {@link SqlHiveParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitAliasedRelation(SqlHiveParser.AliasedRelationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code inlineTableDefault2}
	 * labeled alternative in {@link SqlHiveParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterInlineTableDefault2(SqlHiveParser.InlineTableDefault2Context ctx);
	/**
	 * Exit a parse tree produced by the {@code inlineTableDefault2}
	 * labeled alternative in {@link SqlHiveParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitInlineTableDefault2(SqlHiveParser.InlineTableDefault2Context ctx);
	/**
	 * Enter a parse tree produced by the {@code tableValuedFunction}
	 * labeled alternative in {@link SqlHiveParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTableValuedFunction(SqlHiveParser.TableValuedFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tableValuedFunction}
	 * labeled alternative in {@link SqlHiveParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTableValuedFunction(SqlHiveParser.TableValuedFunctionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#inlineTable}.
	 * @param ctx the parse tree
	 */
	void enterInlineTable(SqlHiveParser.InlineTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#inlineTable}.
	 * @param ctx the parse tree
	 */
	void exitInlineTable(SqlHiveParser.InlineTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#functionTable}.
	 * @param ctx the parse tree
	 */
	void enterFunctionTable(SqlHiveParser.FunctionTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#functionTable}.
	 * @param ctx the parse tree
	 */
	void exitFunctionTable(SqlHiveParser.FunctionTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#tableAlias}.
	 * @param ctx the parse tree
	 */
	void enterTableAlias(SqlHiveParser.TableAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#tableAlias}.
	 * @param ctx the parse tree
	 */
	void exitTableAlias(SqlHiveParser.TableAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code rowFormatSerde}
	 * labeled alternative in {@link SqlHiveParser#rowFormat}.
	 * @param ctx the parse tree
	 */
	void enterRowFormatSerde(SqlHiveParser.RowFormatSerdeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code rowFormatSerde}
	 * labeled alternative in {@link SqlHiveParser#rowFormat}.
	 * @param ctx the parse tree
	 */
	void exitRowFormatSerde(SqlHiveParser.RowFormatSerdeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code rowFormatDelimited}
	 * labeled alternative in {@link SqlHiveParser#rowFormat}.
	 * @param ctx the parse tree
	 */
	void enterRowFormatDelimited(SqlHiveParser.RowFormatDelimitedContext ctx);
	/**
	 * Exit a parse tree produced by the {@code rowFormatDelimited}
	 * labeled alternative in {@link SqlHiveParser#rowFormat}.
	 * @param ctx the parse tree
	 */
	void exitRowFormatDelimited(SqlHiveParser.RowFormatDelimitedContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#tableIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterTableIdentifier(SqlHiveParser.TableIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#tableIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitTableIdentifier(SqlHiveParser.TableIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#functionIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterFunctionIdentifier(SqlHiveParser.FunctionIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#functionIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitFunctionIdentifier(SqlHiveParser.FunctionIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#namedExpression}.
	 * @param ctx the parse tree
	 */
	void enterNamedExpression(SqlHiveParser.NamedExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#namedExpression}.
	 * @param ctx the parse tree
	 */
	void exitNamedExpression(SqlHiveParser.NamedExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 */
	void enterNamedExpressionSeq(SqlHiveParser.NamedExpressionSeqContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 */
	void exitNamedExpressionSeq(SqlHiveParser.NamedExpressionSeqContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(SqlHiveParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(SqlHiveParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link SqlHiveParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogicalNot(SqlHiveParser.LogicalNotContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link SqlHiveParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogicalNot(SqlHiveParser.LogicalNotContext ctx);
	/**
	 * Enter a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link SqlHiveParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterPredicated(SqlHiveParser.PredicatedContext ctx);
	/**
	 * Exit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link SqlHiveParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitPredicated(SqlHiveParser.PredicatedContext ctx);
	/**
	 * Enter a parse tree produced by the {@code exists}
	 * labeled alternative in {@link SqlHiveParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterExists(SqlHiveParser.ExistsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link SqlHiveParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitExists(SqlHiveParser.ExistsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link SqlHiveParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogicalBinary(SqlHiveParser.LogicalBinaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link SqlHiveParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogicalBinary(SqlHiveParser.LogicalBinaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterPredicate(SqlHiveParser.PredicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitPredicate(SqlHiveParser.PredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link SqlHiveParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterValueExpressionDefault(SqlHiveParser.ValueExpressionDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link SqlHiveParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitValueExpressionDefault(SqlHiveParser.ValueExpressionDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link SqlHiveParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterComparison(SqlHiveParser.ComparisonContext ctx);
	/**
	 * Exit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link SqlHiveParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitComparison(SqlHiveParser.ComparisonContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link SqlHiveParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticBinary(SqlHiveParser.ArithmeticBinaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link SqlHiveParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticBinary(SqlHiveParser.ArithmeticBinaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link SqlHiveParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticUnary(SqlHiveParser.ArithmeticUnaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link SqlHiveParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticUnary(SqlHiveParser.ArithmeticUnaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code struct}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterStruct(SqlHiveParser.StructContext ctx);
	/**
	 * Exit a parse tree produced by the {@code struct}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitStruct(SqlHiveParser.StructContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterDereference(SqlHiveParser.DereferenceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitDereference(SqlHiveParser.DereferenceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSimpleCase(SqlHiveParser.SimpleCaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSimpleCase(SqlHiveParser.SimpleCaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterColumnReference(SqlHiveParser.ColumnReferenceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitColumnReference(SqlHiveParser.ColumnReferenceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterRowConstructor(SqlHiveParser.RowConstructorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitRowConstructor(SqlHiveParser.RowConstructorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code last}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterLast(SqlHiveParser.LastContext ctx);
	/**
	 * Exit a parse tree produced by the {@code last}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitLast(SqlHiveParser.LastContext ctx);
	/**
	 * Enter a parse tree produced by the {@code star}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterStar(SqlHiveParser.StarContext ctx);
	/**
	 * Exit a parse tree produced by the {@code star}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitStar(SqlHiveParser.StarContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubscript(SqlHiveParser.SubscriptContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubscript(SqlHiveParser.SubscriptContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubqueryExpression(SqlHiveParser.SubqueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubqueryExpression(SqlHiveParser.SubqueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cast}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCast(SqlHiveParser.CastContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCast(SqlHiveParser.CastContext ctx);
	/**
	 * Enter a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterConstantDefault(SqlHiveParser.ConstantDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitConstantDefault(SqlHiveParser.ConstantDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code lambda}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterLambda(SqlHiveParser.LambdaContext ctx);
	/**
	 * Exit a parse tree produced by the {@code lambda}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitLambda(SqlHiveParser.LambdaContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterParenthesizedExpression(SqlHiveParser.ParenthesizedExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitParenthesizedExpression(SqlHiveParser.ParenthesizedExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code extract}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterExtract(SqlHiveParser.ExtractContext ctx);
	/**
	 * Exit a parse tree produced by the {@code extract}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitExtract(SqlHiveParser.ExtractContext ctx);
	/**
	 * Enter a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterFunctionCall(SqlHiveParser.FunctionCallContext ctx);
	/**
	 * Exit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitFunctionCall(SqlHiveParser.FunctionCallContext ctx);
	/**
	 * Enter a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSearchedCase(SqlHiveParser.SearchedCaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSearchedCase(SqlHiveParser.SearchedCaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code position}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterPosition(SqlHiveParser.PositionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code position}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitPosition(SqlHiveParser.PositionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code first}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterFirst(SqlHiveParser.FirstContext ctx);
	/**
	 * Exit a parse tree produced by the {@code first}
	 * labeled alternative in {@link SqlHiveParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitFirst(SqlHiveParser.FirstContext ctx);
	/**
	 * Enter a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link SqlHiveParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterNullLiteral(SqlHiveParser.NullLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link SqlHiveParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitNullLiteral(SqlHiveParser.NullLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link SqlHiveParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterIntervalLiteral(SqlHiveParser.IntervalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link SqlHiveParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitIntervalLiteral(SqlHiveParser.IntervalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link SqlHiveParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterTypeConstructor(SqlHiveParser.TypeConstructorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link SqlHiveParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitTypeConstructor(SqlHiveParser.TypeConstructorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link SqlHiveParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterNumericLiteral(SqlHiveParser.NumericLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link SqlHiveParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitNumericLiteral(SqlHiveParser.NumericLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link SqlHiveParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterBooleanLiteral(SqlHiveParser.BooleanLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link SqlHiveParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitBooleanLiteral(SqlHiveParser.BooleanLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link SqlHiveParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterStringLiteral(SqlHiveParser.StringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link SqlHiveParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitStringLiteral(SqlHiveParser.StringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void enterComparisonOperator(SqlHiveParser.ComparisonOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void exitComparisonOperator(SqlHiveParser.ComparisonOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#arithmeticOperator}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticOperator(SqlHiveParser.ArithmeticOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#arithmeticOperator}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticOperator(SqlHiveParser.ArithmeticOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#predicateOperator}.
	 * @param ctx the parse tree
	 */
	void enterPredicateOperator(SqlHiveParser.PredicateOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#predicateOperator}.
	 * @param ctx the parse tree
	 */
	void exitPredicateOperator(SqlHiveParser.PredicateOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void enterBooleanValue(SqlHiveParser.BooleanValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void exitBooleanValue(SqlHiveParser.BooleanValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#interval}.
	 * @param ctx the parse tree
	 */
	void enterInterval(SqlHiveParser.IntervalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#interval}.
	 * @param ctx the parse tree
	 */
	void exitInterval(SqlHiveParser.IntervalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#intervalField}.
	 * @param ctx the parse tree
	 */
	void enterIntervalField(SqlHiveParser.IntervalFieldContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#intervalField}.
	 * @param ctx the parse tree
	 */
	void exitIntervalField(SqlHiveParser.IntervalFieldContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#intervalValue}.
	 * @param ctx the parse tree
	 */
	void enterIntervalValue(SqlHiveParser.IntervalValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#intervalValue}.
	 * @param ctx the parse tree
	 */
	void exitIntervalValue(SqlHiveParser.IntervalValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#colPosition}.
	 * @param ctx the parse tree
	 */
	void enterColPosition(SqlHiveParser.ColPositionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#colPosition}.
	 * @param ctx the parse tree
	 */
	void exitColPosition(SqlHiveParser.ColPositionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code complexDataType}
	 * labeled alternative in {@link SqlHiveParser#dataType}.
	 * @param ctx the parse tree
	 */
	void enterComplexDataType(SqlHiveParser.ComplexDataTypeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code complexDataType}
	 * labeled alternative in {@link SqlHiveParser#dataType}.
	 * @param ctx the parse tree
	 */
	void exitComplexDataType(SqlHiveParser.ComplexDataTypeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code primitiveDataType}
	 * labeled alternative in {@link SqlHiveParser#dataType}.
	 * @param ctx the parse tree
	 */
	void enterPrimitiveDataType(SqlHiveParser.PrimitiveDataTypeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code primitiveDataType}
	 * labeled alternative in {@link SqlHiveParser#dataType}.
	 * @param ctx the parse tree
	 */
	void exitPrimitiveDataType(SqlHiveParser.PrimitiveDataTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#colTypeList}.
	 * @param ctx the parse tree
	 */
	void enterColTypeList(SqlHiveParser.ColTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#colTypeList}.
	 * @param ctx the parse tree
	 */
	void exitColTypeList(SqlHiveParser.ColTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#colType}.
	 * @param ctx the parse tree
	 */
	void enterColType(SqlHiveParser.ColTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#colType}.
	 * @param ctx the parse tree
	 */
	void exitColType(SqlHiveParser.ColTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#complexColTypeList}.
	 * @param ctx the parse tree
	 */
	void enterComplexColTypeList(SqlHiveParser.ComplexColTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#complexColTypeList}.
	 * @param ctx the parse tree
	 */
	void exitComplexColTypeList(SqlHiveParser.ComplexColTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#complexColType}.
	 * @param ctx the parse tree
	 */
	void enterComplexColType(SqlHiveParser.ComplexColTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#complexColType}.
	 * @param ctx the parse tree
	 */
	void exitComplexColType(SqlHiveParser.ComplexColTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#whenClause}.
	 * @param ctx the parse tree
	 */
	void enterWhenClause(SqlHiveParser.WhenClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#whenClause}.
	 * @param ctx the parse tree
	 */
	void exitWhenClause(SqlHiveParser.WhenClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#windows}.
	 * @param ctx the parse tree
	 */
	void enterWindows(SqlHiveParser.WindowsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#windows}.
	 * @param ctx the parse tree
	 */
	void exitWindows(SqlHiveParser.WindowsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#namedWindow}.
	 * @param ctx the parse tree
	 */
	void enterNamedWindow(SqlHiveParser.NamedWindowContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#namedWindow}.
	 * @param ctx the parse tree
	 */
	void exitNamedWindow(SqlHiveParser.NamedWindowContext ctx);
	/**
	 * Enter a parse tree produced by the {@code windowRef}
	 * labeled alternative in {@link SqlHiveParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void enterWindowRef(SqlHiveParser.WindowRefContext ctx);
	/**
	 * Exit a parse tree produced by the {@code windowRef}
	 * labeled alternative in {@link SqlHiveParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void exitWindowRef(SqlHiveParser.WindowRefContext ctx);
	/**
	 * Enter a parse tree produced by the {@code windowDef}
	 * labeled alternative in {@link SqlHiveParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void enterWindowDef(SqlHiveParser.WindowDefContext ctx);
	/**
	 * Exit a parse tree produced by the {@code windowDef}
	 * labeled alternative in {@link SqlHiveParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void exitWindowDef(SqlHiveParser.WindowDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#windowFrame}.
	 * @param ctx the parse tree
	 */
	void enterWindowFrame(SqlHiveParser.WindowFrameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#windowFrame}.
	 * @param ctx the parse tree
	 */
	void exitWindowFrame(SqlHiveParser.WindowFrameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#frameBound}.
	 * @param ctx the parse tree
	 */
	void enterFrameBound(SqlHiveParser.FrameBoundContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#frameBound}.
	 * @param ctx the parse tree
	 */
	void exitFrameBound(SqlHiveParser.FrameBoundContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void enterQualifiedName(SqlHiveParser.QualifiedNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void exitQualifiedName(SqlHiveParser.QualifiedNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(SqlHiveParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(SqlHiveParser.IdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link SqlHiveParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterUnquotedIdentifier(SqlHiveParser.UnquotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link SqlHiveParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitUnquotedIdentifier(SqlHiveParser.UnquotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link SqlHiveParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifierAlternative(SqlHiveParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link SqlHiveParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifierAlternative(SqlHiveParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifier(SqlHiveParser.QuotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifier(SqlHiveParser.QuotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link SqlHiveParser#number}.
	 * @param ctx the parse tree
	 */
	void enterDecimalLiteral(SqlHiveParser.DecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link SqlHiveParser#number}.
	 * @param ctx the parse tree
	 */
	void exitDecimalLiteral(SqlHiveParser.DecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link SqlHiveParser#number}.
	 * @param ctx the parse tree
	 */
	void enterIntegerLiteral(SqlHiveParser.IntegerLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link SqlHiveParser#number}.
	 * @param ctx the parse tree
	 */
	void exitIntegerLiteral(SqlHiveParser.IntegerLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link SqlHiveParser#number}.
	 * @param ctx the parse tree
	 */
	void enterBigIntLiteral(SqlHiveParser.BigIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link SqlHiveParser#number}.
	 * @param ctx the parse tree
	 */
	void exitBigIntLiteral(SqlHiveParser.BigIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link SqlHiveParser#number}.
	 * @param ctx the parse tree
	 */
	void enterSmallIntLiteral(SqlHiveParser.SmallIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link SqlHiveParser#number}.
	 * @param ctx the parse tree
	 */
	void exitSmallIntLiteral(SqlHiveParser.SmallIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link SqlHiveParser#number}.
	 * @param ctx the parse tree
	 */
	void enterTinyIntLiteral(SqlHiveParser.TinyIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link SqlHiveParser#number}.
	 * @param ctx the parse tree
	 */
	void exitTinyIntLiteral(SqlHiveParser.TinyIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link SqlHiveParser#number}.
	 * @param ctx the parse tree
	 */
	void enterDoubleLiteral(SqlHiveParser.DoubleLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link SqlHiveParser#number}.
	 * @param ctx the parse tree
	 */
	void exitDoubleLiteral(SqlHiveParser.DoubleLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link SqlHiveParser#number}.
	 * @param ctx the parse tree
	 */
	void enterBigDecimalLiteral(SqlHiveParser.BigDecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link SqlHiveParser#number}.
	 * @param ctx the parse tree
	 */
	void exitBigDecimalLiteral(SqlHiveParser.BigDecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlHiveParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void enterNonReserved(SqlHiveParser.NonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlHiveParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void exitNonReserved(SqlHiveParser.NonReservedContext ctx);
}