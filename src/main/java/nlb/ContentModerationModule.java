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

    topologies.query("get-posts-helper", "*user-id", "*from-offset", "*limit").out("*ret")
              .hashPartition("*user-id")
              .localSelect("$$posts", Path.key("*user-id").view(Ops.SIZE)).out("*num-posts")
              .each(Ops.MIN, "*num-posts", new Expr(Ops.PLUS, "*from-offset", "*limit")).out("*end-offset")
              .ifTrue(new Expr(Ops.EQUAL, "*end-offset", "*num-posts"),
                Block.each(Ops.IDENTITY, null).out("*next-offset"),
                Block.each(Ops.IDENTITY, "*end-offset").out("*next-offset"))
              .localSelect("$$posts",
                           Path.key("*user-id")
                               .sublist("*from-offset", "*end-offset")
                               .all()).out("*post")
              .each(Ops.GET, "*post", "from-user-id").out("*from-user-id")
              .localSelect("$$mutes",
                           Path.key("*user-id")
                               .view(Ops.CONTAINS, "*from-user-id")).out("*muted?")
              .keepTrue(new Expr(Ops.NOT, "*muted?"))
              .originPartition()
              .agg(Agg.list("*post")).out("*posts")
              .agg(Agg.last("*next-offset")).out("*next-offset")
              .each((List posts, Integer nextOffset) -> {
                Map ret = new HashMap();
                ret.put("fetched-posts", posts);
                ret.put("next-offset", nextOffset);
                return ret;
              }, "*posts", "*next-offset").out("*ret");

    topologies.query("get-posts", "*user-id", "*from-offset", "*limit").out("*ret")
              .hashPartition("*user-id")
              .each(() -> new ArrayList()).out("*posts")
              .loopWithVars(LoopVars.var("*query-offset", "*from-offset"),
                Block.invokeQuery("get-posts-helper",
                                  "*user-id",
                                  "*query-offset",
                                  new Expr(Ops.MINUS, "*limit",
                                                      new Expr(Ops.SIZE, "*posts"))).out("*m")
                     .each(Ops.GET, "*m", "fetched-posts").out("*fetched-posts")
                     .each(Ops.GET, "*m", "next-offset").out("*next-offset")
                     .each((List posts, List fetchedPosts) -> posts.addAll(fetchedPosts),
                           "*posts", "*fetched-posts")
                     .ifTrue(new Expr(Ops.OR, new Expr(Ops.IS_NULL, "*next-offset"),
                                              new Expr(Ops.EQUAL, new Expr(Ops.SIZE, "*posts"), "*limit")),
                        Block.emitLoop("*next-offset"),
                        Block.continueLoop("*next-offset"))
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
