package nlb;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;

import java.util.*;

public class ContentModerationModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*post-depot", Depot.hashBy("to-user-id"));
    setup.declareDepot("*mute-depot", Depot.hashBy("user-id"));

    StreamTopology topology = topologies.stream("core");
    topology.pstate("$$posts",
                    PState.mapSchema(Long.class,
                                     PState.listSchema(Map.class).subindexed()));
    topology.pstate("$$mutes",
                    PState.mapSchema(Long.class,
                                     PState.setSchema(Long.class).subindexed()));

    topology.source("*post-depot").out("*post")
            .each(Ops.GET, "*post", "to-user-id").out("*to-user-id")
            .localTransform("$$posts",
                            Path.key("*to-user-id")
                                .afterElem()
                                .termVal("*post"));

    topology.source("*mute-depot").out("*data")
            .each(Ops.GET, "*data", "type").out("*type")
            .each(Ops.GET, "*data", "user-id").out("*user-id")
            .ifTrue(new Expr(Ops.EQUAL, "*type", "mute"),
              Block.each(Ops.GET, "*data", "muted-user-id").out("*muted-user-id")
                   .localTransform("$$mutes",
                                   Path.key("*user-id")
                                       .voidSetElem()
                                       .termVal("*muted-user-id")),
              Block.each(Ops.GET, "*data", "unmuted-user-id").out("*unmuted-user-id")
                   .localTransform("$$mutes",
                                   Path.key("*user-id")
                                       .setElem("*unmuted-user-id")
                                       .termVoid()));

    topologies.query("get-posts-helper", "*user-id", "*start-offset", "*end-offset").out("*posts")
              .hashPartition("*user-id")
              .localSelect("$$posts",
                           Path.key("*user-id")
                               .sublist("*start-offset", "*end-offset")
                               .all()).out("*post")
              .each(Ops.GET, "*post", "from-user-id").out("*from-user-id")
              .localSelect("$$mutes",
                           Path.key("*user-id")
                               .view(Ops.CONTAINS, "*from-user-id")).out("*muted?")
              .keepTrue(new Expr(Ops.NOT, "*muted?"))
              .originPartition()
              .agg(Agg.list("*post")).out("*posts");

    topologies.query("get-posts", "*user-id", "*from-offset", "*limit").out("*ret")
              .hashPartition("*user-id")
              .each(() -> new ArrayList()).out("*posts")
              .loopWithVars(LoopVars.var("*query-offset", "*from-offset"),
                Block.localSelect("$$posts", Path.key("*user-id").view(Ops.SIZE)).out("*num-posts")
                     .each(Ops.MINUS, "*limit", new Expr(Ops.SIZE, "*posts")).out("*fetch-amount")
                     .each(Ops.MIN,
                           "*num-posts",
                           new Expr(Ops.PLUS, "*query-offset", "*fetch-amount")).out("*end-offset")
                     .invokeQuery("get-posts-helper",
                                  "*user-id",
                                  "*query-offset",
                                  "*end-offset").out("*fetched-posts")
                     .each((List posts, List fetchedPosts) -> posts.addAll(fetchedPosts),
                           "*posts", "*fetched-posts")
                     .cond(Case.create(new Expr(Ops.EQUAL, "*end-offset", "*num-posts"))
                               .emitLoop(null),
                           Case.create(new Expr(Ops.EQUAL, new Expr(Ops.SIZE, "*posts"), "*limit"))
                               .emitLoop("*end-offset"),
                           Case.create(true)
                               .continueLoop("*end-offset"))
                ).out("*next-offset")
              .originPartition()
              .each((List posts, Integer nextOffset) -> {
                Map ret = new HashMap();
                ret.put("posts", posts);
                ret.put("next-offset", nextOffset);
                return ret;
              }, "*posts", "*next-offset").out("*ret");
  }
}
