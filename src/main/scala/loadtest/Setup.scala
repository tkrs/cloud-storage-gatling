package loadtest

import scala.util.Random
import com.google.cloud.storage.StorageOptions
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

trait Base {
  val bucket = sys.env.getOrElse("CLOUD_STORAGE_BUCKET", "load-test-3u90f754rw8wu")
  val prefix = "ids"
}

object Setup extends App with Base {
  import S._
  val random = new Random()
  val storage = StorageOptions.getDefaultInstance().getService()
  val strings =
    (1 to 1000).map(_ => new String(random.alphanumeric.take(14).toArray))

  strings.foreach { s =>
    val blobId = BlobId.of(bucket, prefix + "/" + s + "/hello")
    val content = Iterator.continually("hello").take(100 * 1024 / 2).mkString
    val bout = new ByteArrayOutputStream()
    val gout = new GZIPOutputStream(bout)
    gout.write(content.getBytes())
    gout.flush()
    gout.close()
    val bytes = bout.toByteArray()
    val blobInfo =
      BlobInfo
        .newBuilder(blobId)
        .setContentEncoding("gzip")
        .setCacheControl("no-cache")
        .setContentType("text/plain")
        .build()
    storage.create(blobInfo, bytes)
  }
}
