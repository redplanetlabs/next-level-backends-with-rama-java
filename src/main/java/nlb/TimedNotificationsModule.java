package nlb;

import com.rpl.rama.Depot;
import com.rpl.rama.RamaModule;
import com.rpl.rama.RamaModule.Setup;
import com.rpl.rama.RamaModule.Topologies;
import com.rpl.rama.module.StreamTopology;

public class TimedNotificationsModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*scheduled-post-depot", Depot.hashBy("id"));
    StreamTopology topology = topologies.stream("core");
  }


//  (declare-depot setup *scheduled-post-depot (hash-by :id))
//  (if (test-mode?)
//    (declare-depot setup *tick :random {:global? true})
//    (declare-tick-depot setup *tick 1000))
//  (let [topology (stream-topology topologies "core")
//        scheduler (TopologyScheduler. "$$scheduled")]
//    (declare-pstate
//      topology
//      $$feeds
//      {String (vector-schema String {:subindex? true})})
//    (.declarePStates scheduler topology)
//   (<<sources topology
//     (source> *scheduled-post-depot :> {:keys [*time-millis] :as *scheduled-post})
//     (java-macro! (.scheduleItem scheduler "*time-millis" "*scheduled-post"))
//
//     (source> *tick)
//     (java-macro!
//      (.handleExpirations
//        scheduler
//        "*scheduled-post"
//        "*current-time-millis"
//        (java-block<-
//          (identity *scheduled-post :> {:keys [*id *post]})
//          (local-transform> [(keypath *id) AFTER-ELEM (termval *post)] $$feeds)
//          )))
//     )))


}
