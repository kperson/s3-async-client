package com.github.kperson.aws


import org.asynchttpclient._

class S3Client(val credentials: Option[KeyAndSecret], val region: String) extends ObjectOps {

  val httpClient = Dsl.asyncHttpClient()
  val s3Host = "s3.amazonaws.com"

}
