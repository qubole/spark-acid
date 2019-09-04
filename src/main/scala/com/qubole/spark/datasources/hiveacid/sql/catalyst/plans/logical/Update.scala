package com.qubole.spark.datasources.hiveacid.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

case class Update(
    table: LogicalPlan,
    fieldValues: Seq[Expression],
    filter: Option[LogicalPlan],
    tableConverted: Boolean = false)
  extends LogicalPlan {

  // We don't want `table` in children as sometimes we don't want to transform it.
  override def children: Seq[LogicalPlan] = filter.toList
  override def output: Seq[Attribute] = Seq.empty
  override lazy val resolved: Boolean = tableConverted && childrenResolved && fieldValues.forall(_.resolved)
}