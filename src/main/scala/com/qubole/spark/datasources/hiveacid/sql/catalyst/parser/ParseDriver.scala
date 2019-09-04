package com.qubole.spark.datasources.hiveacid.sql.catalyst.parser

import com.qubole.spark.datasources.hiveacid.sql.catalyst.parser.{SqlHiveParser => SqlBaseParser}
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.TerminalNodeImpl
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.StructType

/**
 * A copy of [[org.apache.spark.sql.catalyst.parser.UpperCaseCharStream]]
 */
class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume
  override def getSourceName(): String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = {
    // ANTLR 4.7's CodePointCharStream implementations have bugs when
    // getText() is called with an empty stream, or intervals where
    // the start > end. See
    // https://github.com/antlr/antlr4/commit/ac9f7530 for one fix
    // that is not yet in a released ANTLR artifact.
    if (size() > 0 && (interval.b - interval.a >= 0)) wrapped.getText(interval) else ""
  }

  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}

/**
 * An adaptation of [[org.apache.spark.sql.catalyst.parser.PostProcessor]]
 */
case object PostProcessor extends SqlHiveBaseListener {

  /** Remove the back ticks from an Identifier. */
  override def exitQuotedIdentifier(ctx: SqlBaseParser.QuotedIdentifierContext): Unit = {
    replaceTokenByIdentifier(ctx, 1) { token =>
      // Remove the double back ticks in the string.
      token.setText(token.getText.replace("``", "`"))
      token
    }
  }

  /** Treat non-reserved keywords as Identifiers. */
  override def exitNonReserved(ctx: SqlBaseParser.NonReservedContext): Unit = {
    replaceTokenByIdentifier(ctx, 0)(identity)
  }

  private def replaceTokenByIdentifier(
                                        ctx: ParserRuleContext,
                                        stripMargins: Int)(
                                        f: CommonToken => CommonToken = identity): Unit = {
    val parent = ctx.getParent
    parent.removeLastChild()
    val token = ctx.getChild(0).getPayload.asInstanceOf[Token]
    val newToken = new CommonToken(
      new org.antlr.v4.runtime.misc.Pair(token.getTokenSource, token.getInputStream),
      SqlBaseParser.IDENTIFIER,
      token.getChannel,
      token.getStartIndex + stripMargins,
      token.getStopIndex - stripMargins)
    parent.addChild(new TerminalNodeImpl(f(newToken)))
  }
}

/**
 * An adaptation of [[org.apache.spark.util.random.RandomSampler]]
 */
object RandomSampler {
  /**
   * Sampling fraction arguments may be results of computation, and subject to floating
   * point jitter.  I check the arguments with this epsilon slop factor to prevent spurious
   * warnings for cases such as summing some numbers to get a sampling fraction of 1.000000001
   */
  val roundingEpsilon = 1e-6
}

object SparkAdaptation {
  /**
   * An adaptation of [[org.apache.spark.sql.types.StructType#toAttributes]]
   */
  def toAttributes(structType: StructType): Seq[AttributeReference] =
    structType.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
}