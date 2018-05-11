package com.loyalty.testing.s3

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.routes.S3Routes

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class S3Mock(implicit val system: ActorSystem, override val repository: Repository) extends S3Routes {

  override protected implicit val log: LoggingAdapter = system.log
  override protected implicit val mat: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system))

  private val http = Http()
  private var bind: Http.ServerBinding = _

  def start(host: String = "0.0.0.0", port: Int = 9090): Unit = {
    bind = Await.result(http.bindAndHandle(s3Routes, host, port), Duration.Inf)
    log.info("Server online at http://{}:{}", host, port)
  }

  def shutdown(): Unit = {
    import system.dispatcher
    repository.clean()
    val stopped =
      for {
        _ <- bind.unbind()
        _ <- http.shutdownAllConnectionPools()
        _ <- system.terminate()
      } yield ()
    Await.result(stopped, Duration.Inf)
  }

}

object S3Mock {
  def apply()(implicit system: ActorSystem, repository: Repository): S3Mock = new S3Mock()
}
