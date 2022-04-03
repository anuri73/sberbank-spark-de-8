package org.sberbank.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.net.URLDecoder
import scala.util.Try

object URL {
  def decode: UserDefinedFunction = udf { (url: String) =>
    Try(URLDecoder.decode(url, "UTF-8")).toOption match {
      case Some(value) => value
      case None        => ""
    }
  }

  def getDomain: UserDefinedFunction = udf { (url: String) =>
    Try(new java.net.URL(url).getHost).toOption match {
      case Some(value) => value.replaceAll("^www\\.", "")
      case None        => ""
    }
  }
}
