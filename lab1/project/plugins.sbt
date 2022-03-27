val SBT_PLUGIN_VERSION = "2.4.6"
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % SBT_PLUGIN_VERSION)
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")

resolvers += Resolver.sonatypeRepo("public")
