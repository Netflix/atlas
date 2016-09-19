package com.netflix.atlas.lwcapi

import com.netflix.atlas.core.model.Query

abstract class ExpressionSplitter() {
  import ExpressionSplitter._

  def split(e: ExpressionWithFrequency): SplitResult
}

object ExpressionSplitter {
  case class SplitResult(expression: String, frequency: Long, id: String, split: List[QueryContainer])

  case class QueryContainer(matchExpr: Query, dataExpr: String) extends Ordered[QueryContainer] {
    override def toString: String = {
      s"QueryContainer(<$matchExpr> <$dataExpr>)"
    }

    def compare(that: QueryContainer) = {
      dataExpr compare that.dataExpr
    }
  }
}
