package com.netflix.atlas.lwcapi

/**
  * Created by mgraff on 9/12/16.
  */
object GlobalUUID {
  private lazy val uuid = java.util.UUID.randomUUID.toString

  def get = uuid
}
