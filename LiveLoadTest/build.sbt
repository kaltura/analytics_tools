name := "LiveLoadTest"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "com.netflix.astyanax" % "astyanax-core" % "1.56.49"

libraryDependencies += "com.netflix.astyanax" % "astyanax-thrift" % "1.56.49"

libraryDependencies += "com.netflix.astyanax" % "astyanax-cassandra" % "1.56.49"

libraryDependencies += "com.netflix.astyanax" % "astyanax-examples" % "1.56.49"


//resolvers += "spray repo" at "http://repo.spray.io"

//resolvers += "spray nightlies" at "http://nightlies.spray.io"

//resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"

libraryDependencies += "com.ning" % "async-http-client" % "1.8.13"

////libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4-SNAPSHOT"

////libraryDependencies += "com.typesafe.akka" %% "akka-actor_2.10" % "2.3.5"
//libraryDependencies += "com.typesafe.akka" % "akka-actor_2.10" % "2.3.5"


////libraryDependencies += "com.typesafe.akka" %% "akka-slf4j_2.10" % "2.3.5"
//libraryDependencies += "com.typesafe.akka" % "akka-slf4j_2.10" % "2.3.5"

//libraryDependencies += "io.spray" % "spray-can" % "1.3.1"

//libraryDependencies += "io.spray" % "spray-httpx" % "1.3.1"

//libraryDependencies += "io.spray" % "spray-routing" % "1.3.1"

//libraryDependencies += "io.spray" % "spray-json_2.10" % "1.2.6"

//libraryDependencies += "io.spray" % "spray-client" % "1.2-20130705"

//libraryDependencies += "cc.spray" % "spray-client" % "0.8.0"


//libraryDependencies ++= Seq(

//     "ch.qos.logback"      % "logback-classic"  % "1.0.13",
//     "io.spray"            % "spray-can"        % "1.2-20130712",
//     "io.spray"            % "spray-routing"    % "1.2-20130712",
//     "io.spray"           %% "spray-json"       % "1.2.3",
//     "org.specs2"         %% "specs2"           % "1.14"         % "test",
//     "io.spray"            % "spray-testkit"    % "1.2-20130712" % "test",
//     "com.typesafe.akka"  %% "akka-testkit"     % "2.2.0"        % "test",
//     "com.novocode"        % "junit-interface"  % "0.7"          % "test->default",
//     "org.scalautils" % "scalautils_2.10" % "2.0",
//     "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
//)
//
//scalacOptions ++= Seq(
//     "-unchecked",
//     "-deprecation",
//     "-Xlint",
//     "-Ywarn-dead-code",
//     "-language:_",
//     "-target:jvm-1.7",
//     "-encoding", "UTF-8"
//)
    