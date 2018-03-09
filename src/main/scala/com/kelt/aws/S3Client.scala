package com.kelt.aws

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import io.netty.handler.codec.http.HttpHeaders

import org.asynchttpclient._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}
import scala.xml.XML

case class AsyncResponse(status: Int, headers: Map[String, String] = Map.empty, body: Array[Byte] = Array.empty)

case class AWSError(response: AsyncResponse) extends RuntimeException(new String(response.body))

sealed trait StorageClass
case object Standard extends StorageClass
case object StandardInfrequentAccess extends StorageClass
case object ReducedRedundancy extends StorageClass
case object Glacier extends StorageClass
case class UnknownStorageClass(name: String) extends StorageClass


sealed trait CannedACL             //TODO
sealed trait ServiceSideEncryption //TODO



case class DirectoryEntry(key: String, size: Long, storageClass: StorageClass, lastModifiedAt: Date)

case class DirectoryListing(nextContinuationToken: Option[String], entries: List[DirectoryEntry])

class S3Client(credentials: KeyAndSecret, region: String) {

  import S3Client._

  val httpClient = Dsl.asyncHttpClient()

  val listDateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'")
  listDateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"))

  def put(
    bucket: String,
    key: String,
    value: Array[Byte],
    mimeType:String = "application/octet-stream",
    storageCLass: StorageClass = Standard
   ): Future[Any] = {
    val host = s"$bucket.s3.amazonaws.com"
    val path = if(key.startsWith("/")) key else "/" + key

    val storageClassHeaderValue = storageCLass match {
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
        "Content-Type" -> mimeType,
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
    * @param key the nname of the key
    * @return a future of a async response
    */
  def delete(bucket: String, key: String): Future[Any] = {
    val host = s"$bucket.s3.amazonaws.com"
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
  def get(bucket: String, key: String): Future[AsyncResponse] = {
    val host = s"$bucket.s3.amazonaws.com"
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
    val host = s"$bucket.s3.amazonaws.com"
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

object S3Client {

  implicit class HttpClientExtension(self: AsyncHttpClient) {

    def future(builder: RequestBuilder, signing: Signing): Future[AsyncResponse] = {
      val promise = Promise[AsyncResponse]()
      signing.headers.foreach { case (k, v) =>
        if(k.toLowerCase != "host") {
          builder.setHeader(k, v)
        }
      }
      signing.encodedParams.map { case (k, v) =>
        builder.addQueryParam(k, v)
      }
      self.executeRequest(builder.build(), new AsyncHandler[Int] {

        private var response = AsyncResponse(-1)

        def onStatusReceived(responseStatus: HttpResponseStatus): AsyncHandler.State =  {
          response = response.copy(status = responseStatus.getStatusCode)
          AsyncHandler.State.CONTINUE
        }

        def onHeadersReceived(onHeadersReceived: HttpHeaders): AsyncHandler.State =  {
          val headers: Map[String, String] = onHeadersReceived.entries()
            .asScala
            .map { x => (x.getKey, x.getValue)
            }.toMap
          response = response.copy(headers = headers)
          AsyncHandler.State.CONTINUE
        }

        def onBodyPartReceived(onHeadersReceived: HttpResponseBodyPart): AsyncHandler.State = {
          response = response.copy(body = response.body ++ onHeadersReceived.getBodyPartBytes)
          AsyncHandler.State.CONTINUE
        }

        def onCompleted(): Int = {
          if(!promise.isCompleted) {
            if(response.status < 400) {
              promise.complete(Success(response))
            }
            else {
              promise.complete(Failure(AWSError(response)))
            }
          }
          response.status
        }

        def onThrowable(t: Throwable) {
          if(!promise.isCompleted) {
            promise.complete(Failure(t))
          }
        }

      })
      promise.future
    }
  }

}
