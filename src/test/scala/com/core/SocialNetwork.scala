package com.core

import org.scalatest._

class SocialNetworkSpec extends FunSuite with Matchers {
  test("number of data loaded") {
    val data = SocialNetwork.loadData
    data.count should be (809)
    SocialNetwork.testClass
  }
}
