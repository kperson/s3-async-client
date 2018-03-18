package com.github.kperson.aws

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

sealed trait S3Payload

case class KeyAndSecret(key: String, secret: String)

case class Signing(
  service: String,
  credentials: KeyAndSecret,
  region: String,
  httpMethod: String,
  canonicalURI: String,
  headersToSign: Map[String, String],
  queryParams: Map[String, Option[String]] = Map.empty,
  payload: Array[Byte] = Array.empty,
  date: Date = new Date()
) {

  val payloadHash = Signing.toHex(Signing.SHA256(payload))
  val encodeURI = Signing.uriEncode(canonicalURI, false)
  val headersWithoutSignature = headersToSign ++ Map(
    "x-amz-content-sha256" -> payloadHash,
    "x-amz-date" -> Signing.longDateFormatter.format(date)
  )

  def sortedHeaderKeys = headersWithoutSignature.keys.toList.sortWith { (kOne, kTwo) => kOne.toLowerCase < kTwo.toLowerCase }

  def encodedParams = {
    queryParams.map { case (k, v) =>
      (Signing.uriEncode(k, true), Signing.uriEncode(v.getOrElse(""), true))
    }
  }

  def sortedParams = {
    val ep = encodedParams
    val sortedKeys = ep.keys.toList.sortWith { (kOne, kTwo) => kOne < kTwo }
    sortedKeys.map { k =>
      (k, ep(k))
    }
  }
  def canonicalRequest: String = {
    val strBuilder = new StringBuilder()
    strBuilder.append(httpMethod + "\n")
    strBuilder.append(encodeURI + "\n")
    strBuilder.append(sortedParams.map { case (k, v) => s"$k=$v" }.mkString("&") + "\n")

    val sortedKeys = sortedHeaderKeys
    sortedKeys.foreach { h =>
      strBuilder.append(s"${h.toLowerCase}:${headersWithoutSignature(h.trim)}\n")
    }
    strBuilder.append("\n")

    strBuilder.append(sortedKeys.map{ _.toLowerCase }.mkString(";") + "\n")
    strBuilder.append(payloadHash)

    strBuilder.toString
  }

  def stringToSign: String = {
    val cr = canonicalRequest
    val crHash = Signing.SHA256(cr.getBytes(StandardCharsets.UTF_8))
    s"${Signing.signingVersion}-${Signing.signingAlgorithm}" +
      "\n" + Signing.longDateFormatter.format(date) +
      "\n" + Signing.shortDateFormatter.format(date) + "/" + region + s"/$service/${Signing.signingVersion.toLowerCase}_request" +
      "\n" + Signing.toHex(crHash)

  }

  def signature = {
    val kSecret = (Signing.signingVersion +  credentials.secret).getBytes(StandardCharsets.UTF_8)
    val kDate = Signing.hmacSHA256(Signing.shortDateFormatter.format(date), kSecret)
    val kRegion = Signing.hmacSHA256(region, kDate)
    val kService = Signing.hmacSHA256(service, kRegion)
    val kSigning = Signing.hmacSHA256(s"${Signing.signingVersion.toLowerCase}_request", kService)
    Signing.toHex(Signing.hmacSHA256(stringToSign, kSigning))
  }

  def authorizationHeader = {
    val sortedHeadersDelimited = sortedHeaderKeys.map { _.toLowerCase }.mkString(";")
    s"${Signing.signingVersion}-${Signing.signingAlgorithm} Credential=${credentials.key}/${Signing.shortDateFormatter.format(date) }/${region}/${service}/${Signing.signingVersion.toLowerCase}_request,SignedHeaders=${sortedHeadersDelimited},Signature=${signature}"
  }

  def headers = {
    headersWithoutSignature ++ Map("Authorization" -> authorizationHeader)
  }

}

object Signing {

  val signingVersion = "AWS4"
  val signingAlgorithm = "HMAC-SHA256"

  private lazy val shortDateFormatter = sFormatter
  private lazy val longDateFormatter = lFormatter

  private def sFormatter() = {
    val f = new SimpleDateFormat("yyyyMMdd")
    f.setTimeZone(TimeZone.getTimeZone("GMT"))
    f
  }

  private def lFormatter() = {
    val f = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'")
    f.setTimeZone(TimeZone.getTimeZone("GMT"))
    f
  }

  def hmacSHA256(data: String, key: Array[Byte]): Array[Byte] = {
    val algorithm = "HmacSHA256"
    val mac = Mac.getInstance(algorithm)
    mac.init(new SecretKeySpec(key, algorithm))
    mac.doFinal(data.getBytes(StandardCharsets.UTF_8))
  }

  def SHA256(bytes: Array[Byte]): Array[Byte] = {
    val sha256Digest = MessageDigest.getInstance("SHA-256")
    return sha256Digest.digest(bytes)
  }

  def toHex(bytes: Array[Byte]): String = bytes.map("%02x".format(_)).mkString

  def uriEncode(input: CharSequence, encodeSlash: Boolean): String = {
    val result = new StringBuilder();
    for { i <- 0 until input.length() } {
      val ch = input.charAt(i)
      if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' || ch == '~' || ch == '.') {
        result.append(ch)
      }
      else if (ch == '/') {
        val x = if(encodeSlash) "%2F" else ch
        result.append(x)
      }
      else {
        result.append("%" + toHex(Array(ch.toByte)).toUpperCase)
      }
    }
    return result.toString
  }

}