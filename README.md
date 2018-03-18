# Async S3 Client for Scala

An async S3 client for scala built on [AsyncHttpClient](https://github.com/AsyncHttpClient/async-http-client).

## Creating the Client
```scala
import com.github.kperson.aws._

val credentials = Some(KeyAndSecret("MY_AWS_KEY", "MY_AWS_SECRET"))
//use None for credentials if IAM permission handle authorization or if you desire anonymous mode

val region = "us-east-1"
val client = new S3Client(credentials, region)
```

## Put Object

```scala
val bucket = "my_buckey"
val key = "my_object_key"
val contents = "hello world"
val contenType = "text/plain"

val f: Future[S3HttpResponse] = client.put(bucket, key, contents.getBytes(StandardCharsets.UTF_8))
```

## Delete Object

```scala
val bucket = "my_buckey"
val key = "my_object_key"

val f: Future[S3HttpResponse] = client.delete(bucket, key)
```

## GET Object

```scala
val bucket = "my_buckey"
val key = "my_object_key"

val f: Future[String] = client.get(bucket, key).map { res => new String(res.body) } 
```

## List Objects

```scala
val bucket = "my_buckey"

val f: Future[DirectoryListing] = client.list(bucket)
```