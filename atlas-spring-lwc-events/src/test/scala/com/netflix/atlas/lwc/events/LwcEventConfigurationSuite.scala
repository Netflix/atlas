package com.netflix.atlas.lwc.events

import munit.FunSuite
import org.springframework.context.annotation.AnnotationConfigApplicationContext

import scala.util.Using

class LwcEventConfigurationSuite extends FunSuite {

  test("load module") {
    Using.resource(new AnnotationConfigApplicationContext()) { context =>
      context.scan("com.netflix")
      context.refresh()
      context.start()
      assert(context.getBean(classOf[LwcEventClient]) != null)
    }
  }
}
