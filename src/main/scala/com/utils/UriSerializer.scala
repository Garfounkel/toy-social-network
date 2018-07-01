package com.utils

import java.net.URI
import org.json4s.{CustomSerializer, DefaultFormats}
import org.json4s._
import org.json4s.native.Serialization


class UriSerializer extends CustomSerializer[URI](format => (
  {
    case JString(uri) => URI.create(uri)
    case JNull => null
  },
  {
    case uri: URI => JString(uri.toString)
  }
))

object UriSerializer extends UriSerializer()
