package com.netflix.atlas.core.algorithm

import com.typesafe.config.Config

/**
  * Delays the values by the window size. This is similar to the `:offset` operator
  * except that it can be applied to any input line instead of just changing the time
  * window fetched with a DataExpr. Short delays can be useful for alerting to detect
  * changes in slightly shifted trend lines.
  */
case class OnlineDelay(buf: RollingBuffer) extends OnlineAlgorithm {

  override def next(v: Double): Double = buf.add(v)

  override def reset(): Unit = buf.clear()

  override def state: Config = {
    OnlineAlgorithm.toConfig(Map("type" -> "delay", "buffer" -> buf.state))
  }
}

object OnlineDelay {

  def apply(n: Int): OnlineDelay = new OnlineDelay(RollingBuffer(n))

  def apply(config: Config): OnlineDelay = {
    apply(RollingBuffer(config.getConfig("buffer")))
  }
}
