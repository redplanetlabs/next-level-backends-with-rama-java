package nlb;

import com.rpl.rama.*;
import com.rpl.rama.helpers.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;

public class TimedNotificationsModule implements RamaModule {
  public boolean isTestMode = false;

  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*scheduled-post-depot", Depot.hashBy("id"));
    if(isTestMode) setup.declareDepot("*tick", Depot.random()).global();
    else setup.declareTickDepot("*tick", 1000);
    StreamTopology topology = topologies.stream("core");
    TopologyScheduler scheduler = new TopologyScheduler("$$scheduled");

    topology.pstate("$$feeds",
                    PState.mapSchema(String.class,
                                     PState.listSchema(String.class).subindexed()));
    scheduler.declarePStates(topology);

    topology.source("*scheduled-post-depot").out("*scheduled-post")
            .each(Ops.GET, "*scheduled-post", "time-millis").out("*time-millis")
            .macro(scheduler.scheduleItem("*time-millis", "*scheduled-post"));

    topology.source("*tick")
            .macro(scheduler.handleExpirations(
              "*scheduled-post",
              "*current-time-millis",
              Block.each(Ops.GET, "*scheduled-post", "id").out("*id")
                   .each(Ops.GET, "*scheduled-post", "post").out("*post")
                   .localTransform("$$feeds",
                                   Path.key("*id").afterElem().termVal("*post"))));
  }
}
