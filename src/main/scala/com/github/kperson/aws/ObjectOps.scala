package com.github.kperson.aws

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.asynchttpclient.{AsyncHttpClient, RequestBuilder}

import scala.concurrent.{ExecutionContext, Future}
import scala.xml.XML

sealed trait StorageClass
case object Standard extends StorageClass
case object StandardInfrequentAccess extends StorageClass
case object ReducedRedundancy extends StorageClass
case object Glacier extends StorageClass
case class UnknownStorageClass(name: String) extends StorageClass

case class DirectoryEntry(key: String, size: Long, storageClass: StorageClass, lastModifiedAt: Date)
case class DirectoryListing(nextContinuationToken: Option[String], entries: List[DirectoryEntry])

trait ObjectOps {

  import Http._

  def httpClient: AsyncHttpClient
  def credentials: Option[KeyAndSecret]
  def region: String
  def s3Host: String


  val listDateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'")
  listDateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"))


  /**
    * Puts a object into a bucket
    *
    * @param bucket the name of the bucket
    * @param key the name of the key
    * @param value
    * @param contentType the content type of the data (defaults to application/octet-stream)
    * @param storageClass the S3 storage class
    * @return a future of a async response
    */
  def put(
   bucket: String,
   key: String,
   value: Array[Byte],
   contentType: String = "application/octet-stream",
   storageClass: StorageClass = Standard
  ): Future[S3HttpResponse] = {
    val host = s"$bucket.$s3Host"
    val path = if(key.startsWith("/")) key else "/" + key

    val storageClassHeaderValue = storageClass match {
      case Standard => "STANDARD"
      case StandardInfrequentAccess => "STANDARD_IA"
      case ReducedRedundancy => "REDUCED_REDUNDANCY"
      case Glacier => throw new RuntimeException("you may not set a storage class of GLACIER for a PUT")
      case UnknownStorageClass(_) => throw new RuntimeException("unknown storage class allows future storage class compatibility, only use STANDARD, STANDARD_IA, and REDUCED_REDUNDANCY")
    }

    val signing = Signing(
      "s3",
      credentials,
      region,
      "PUT",
      path,
      Map(
        "Content-Type" -> contentType,
        "Host" -> host,
        "x-amz-storage-class" -> storageClassHeaderValue
      ),
      Map.empty,
      value
    )

    val builder = new RequestBuilder("PUT", true)
    builder.setBody(value)
    builder.setUrl(s"https://$host${Signing.uriEncode(path, false)}")

    httpClient.future(builder, signing)
  }

  /**
    * Deletes a object from a bucket
    *
    * @param bucket the name of the bucket
    * @param key the name of the key
    * @return a future of a async response
    */
  def delete(bucket: String, key: String): Future[S3HttpResponse] = {
    val host = s"$bucket.$s3Host"
    val path = if(key.startsWith("/")) key else "/" + key

    val signing = Signing(
      "s3",
      credentials,
      region,
      "DELETE",
      path,
      Map(
        "Host" -> host
      ),
      Map.empty,
      Array.empty
    )
    val builder = new RequestBuilder("DELETE", true)
    builder.setUrl(s"https://$host${Signing.uriEncode(path, false)}")

    httpClient.future(builder, signing)
  }

  /**
    * Gets a object
    *
    * @param bucket the name of the bucket
    * @param key the name of the key
    * @return a future of a async response
    */
  def get(bucket: String, key: String): Future[S3HttpResponse] = {
    val host = s"$bucket.$s3Host"
    val path = if(key.startsWith("/")) key else "/" + key

    val signing = Signing(
      "s3",
      credentials,
      region,
      "GET",
      path,
      Map(
        "Host" -> host
      ),
      Map.empty,
      Array.empty
    )
    val builder = new RequestBuilder("GET", true)
    builder.setUrl(s"https://$host${Signing.uriEncode(path, false)}")

    httpClient.future(builder, signing)
  }

  /**
    * List the keys within a bucket
    *
    * @param bucket the name of the bucket
    * @param prefix a prefix to limit the listing
    * @param maxKeys the max number of results to return
    * @param continuationToken a pagination token to continue fetching from end of past list request
    * @param ec the execution context for future transformation
    * @return a future of a directory listing
    */
  def list(
    bucket: String,
    prefix: Option[String] = None,
    maxKeys: Option[Int] = None,
    continuationToken: Option[String] = None
  )(implicit ec: ExecutionContext): Future[DirectoryListing] = {
    val host = s"$bucket.$s3Host"
    val path = "/"

    var m = scala.collection.mutable.Map[String, Option[String]]()
    prefix.foreach { p => m = m ++ Map("prefix" -> Some(p)) }
    maxKeys.foreach { p => m = m ++ Map("max-keys" -> Some(p.toString)) }
    continuationToken.foreach { p => m = m ++ Map("continuation-token" -> Some(p)) }

    val signing = Signing(
      "s3",
      credentials,
      region,
      "GET",
      path,
      Map(
        "Host" -> host
      ),
      Map(
        "list-type" -> Some("2")
      ) ++ m,
      Array.empty
    )
    val builder = new RequestBuilder("GET", true)
    builder.setUrl(s"https://$host${Signing.uriEncode(path, false)}")

    httpClient.future(builder, signing).map { resp =>
      val xml = XML.loadString(new String(resp.body))
      val isTruncated = (xml \ "IsTruncated").text != "false"
      val nextContinuationToken = if(isTruncated) Some((xml \ "NextContinuationToken").text) else None
      val entries = (xml  \\ "ListBucketResult" \\ "Contents").map { contents =>
        val key = (contents \ "Key").text
        val size = (contents \ "Size").text.toLong
        val storageClassStr = (contents \ "StorageClass").text
        val storageClass = storageClassStr match {
          case "STANDARD" => Standard
          case "STANDARD_IA" => StandardInfrequentAccess
          case "REDUCED_REDUNDANCY" => ReducedRedundancy
          case "GLACIER" => Glacier
          case x => UnknownStorageClass(x)
        }
        DirectoryEntry(key, size, storageClass, listDateFormatter.parse((contents \ "LastModified").text))
      }.toList

      DirectoryListing(nextContinuationToken, entries)
    }
  }

}
