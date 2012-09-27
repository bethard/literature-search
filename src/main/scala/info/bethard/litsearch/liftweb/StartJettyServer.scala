package info.bethard.litsearch.liftweb

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.webapp.WebAppContext

object StartJettyServer {

  def main(args: Array[String]) = {
    val server = new Server(8180)
    val webApp = new WebAppContext()
    webApp.setServer(server)
    webApp.setContextPath("/")
    webApp.setWar("src/main/webapp")
    server.setHandler(webApp)

    println(">>> STARTING EMBEDDED JETTY SERVER, PRESS ANY KEY TO STOP")
    server.start()
    while (System.in.available() == 0) {
      Thread.sleep(5000)
    }
    server.stop()
    server.join()
  }
}
