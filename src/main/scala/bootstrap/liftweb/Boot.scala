package bootstrap.liftweb

import info.bethard.litsearch.RestService
import net.liftweb.http.LiftRules
import net.liftweb.http.Html5Properties
import net.liftweb.http.Req

class Boot {
  def boot = {
    LiftRules.htmlProperties.default.set((r: Req) => new Html5Properties(r.userAgent))
    LiftRules.dispatch.append(RestService)
  }
}