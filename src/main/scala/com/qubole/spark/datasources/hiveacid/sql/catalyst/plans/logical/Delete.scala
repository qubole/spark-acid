package com.qubole.spark.datasources.hiveacid.sql.catalyst.plans.logical

import com.qubole.spark.hiveacid.HiveAcidErrors
import com.qubole.spark.hiveacid.datasource.HiveAcidRelation
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation

case class Delete(
    table: LogicalPlan,
    condition: Expression)
  extends RunnableCommand {

  // We don't want `table` in children as sometimes we don't want to transform it.
  override def children: Seq[LogicalPlan] = Seq(table)
  override def output: Seq[Attribute] = Seq.empty
  override lazy val resolved: Boolean = childrenResolved
  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (children.size != 1) {
      throw new IllegalArgumentException("DELETE command should specify exactly one table, whereas this has: "
        + children.size)
    }
    children(0) match {
      case LogicalRelation(relation: HiveAcidRelation, _, _ , _) => {
        relation.delete(new Column(condition))
      }
      case _ => HiveAcidErrors.unsupportedOperationTypeNonAcidTable("DELETE", table.toString())
    }
    Seq.empty[Row]
  }
}