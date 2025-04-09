package nlb;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;

import java.util.*;

public class WhoToFollowModule implements RamaModule {
  public int numRecommendations = 300;
  public boolean isTestMode = false;

  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*follows-depot", Depot.hashBy("from"));
    if(isTestMode) setup.declareDepot("*who-to-follow-tick", Depot.random()).global();
    else setup.declareTickDepot("*who-to-follow-tick", 30000);

    StreamTopology topology = topologies.stream("core");
    topology.pstate(
      "$$follows",
      PState.mapSchema(Long.class,
                       PState.setSchema(Long.class).subindexed()));
    topology.source("*follows-depot").out("*data")
            .each(Ops.GET, "*data", "from").out("*from")
            .each(Ops.GET, "*data", "to").out("*to")
            .localTransform("$$follows", Path.key("*from").voidSetElem().termVal("*to"));


    MicrobatchTopology mb = topologies.microbatch("who-to-follow");
    mb.pstate(
      "$$who-to-follow",
      PState.mapSchema(Long.class,
                       PState.listSchema(Long.class)));
    mb.pstate("$$next-id", Long.class);

    mb.source("*who-to-follow-tick").out("*microbatch")
      .explodeMicrobatch("*microbatch")
      .batchBlock(
         Block.allPartition()
              .localSelect("$$next-id", Path.stay()).out("*start-id")
              .localSelect("$$follows",
                           Path.sortedMapRangeFrom("*start-id",
                                                   SortedRangeFromOptions.maxAmt(15)
                                                                         .excludeStart())).out("*m")
              .ifTrue(new Expr(Ops.LESS_THAN, new Expr(Ops.SIZE, "*m"), 15),
                 Block.localTransform("$$next-id", Path.termVal(-1L)),
                 Block.each((SortedMap m) -> m.lastKey(), "*m").out("*max-id")
                      .localTransform("$$next-id", Path.termVal("*max-id")))
              .each(Ops.EXPLODE_MAP, "*m").out("*account-id", "*follows")
              .each(Ops.EXPLODE, "*follows").out("*following-id")
              .hashPartition("*following-id")
              .localSelect("$$follows", Path.key("*following-id")
                                            .all()).out("*candidate-id")
              .keepTrue(new Expr(Ops.NOT_EQUAL, "*account-id", "*candidate-id"))
              .hashPartition("*account-id")
              .compoundAgg(CompoundAgg.map("*account-id",
                                           CompoundAgg.map("*candidate-id",
                                                           Agg.count()))).out("*m")
              .each(Ops.EXPLODE_MAP, "*m").out("*account-id", "*candidate-counts")
              .each((Map cc) -> {
                 ArrayList<Map.Entry<Long, Integer>> l = new ArrayList(cc.entrySet());
                 l.sort(Map.Entry.comparingByValue());
                 Collections.reverse(l);
                 List ret = new LinkedList();
                 for(int i=0; i < Math.min(1000, l.size()); i++) ret.add(l.get(i).getKey());
                 return ret;
              }, "*candidate-counts").out("*candidate-order")
              .each((RamaFunction0) ArrayList::new).out("*who-to-follow")
              .loop(
                 Block.ifTrue(new Expr((List w, List c, Integer numRecommendations) -> w.size() >= numRecommendations || c.size() == 0,
                                       "*who-to-follow", "*candidate-order", numRecommendations),
                   Block.emitLoop(),
                   Block.yieldIfOvertime()
                        .each((LinkedList l) -> l.pop(), "*candidate-order").out("*candidate-id")
                        .localSelect("$$follows",
                                     Path.key("*account-id")
                                         .view(Ops.CONTAINS, "*candidate-id")).out("*already-follows?")
                        .ifTrue(new Expr(Ops.NOT, "*already-follows?"),
                          Block.each((List l, Long cid) -> l.add(cid), "*who-to-follow", "*candidate-id"))
                        .continueLoop()))
              .localTransform("$$who-to-follow", Path.key("*account-id").termVal("*who-to-follow")));
  }
}
