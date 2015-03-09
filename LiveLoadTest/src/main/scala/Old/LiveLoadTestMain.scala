//import scala.concurrent.Future
//import scala.concurrent.duration._
//
//import akka.actor.ActorSystem
//import akka.util.Timeout
//import akka.pattern.ask
//import akka.io.IO
//
//import spray.can.Http
//import spray.http._
//import HttpMethods._
//
//
//
//object LiveLoadTestMain
//{
//     def main(args: Array[String])
//     {
//          implicit val system: ActorSystem = ActorSystem()
//
//          implicit val timeout: Timeout = Timeout(15.seconds)
//
//          import system.dispatcher // implicit execution context
//
//          import spray.httpx.RequestBuilding._
//
////          val obj = "hellllooo"
////          val req = Post("http://il-bigdata-3.dev.kaltura.com/", obj) //~> addHeader("X-Foo", "bar")
//
//          val response: Future[HttpResponse] =
//               (IO(Http) ? HttpRequest(GET, Uri("http://il-bigdata-3.dev.kaltura.com/"))).mapTo[HttpResponse]
//
//          // or, with making use of spray-httpx
////
////          val response2: Future[HttpResponse] =
////               (IO(Http) ? Get("http://spray.io")).mapTo[HttpResponse]
//
//
//          println("hi")
//     }
//}
