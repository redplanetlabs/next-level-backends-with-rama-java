package nlb;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;

import java.util.*;

public class FamilyTreeModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*people-depot", Depot.hashBy("id"));
    StreamTopology topology = topologies.stream("core");
    topology.pstate(
      "$$family-tree",
      PState.mapSchema(UUID.class,
                       PState.fixedKeysSchema(
                         "parent1", UUID.class,
                         "parent2", UUID.class,
                         "name", String.class,
                         "children", PState.setSchema(UUID.class)
                         )));
    topology.source("*people-depot").out("*person")
            .each(Ops.GET, "*person", "id").out("*id")
            .each(Ops.GET, "*person", "parent1").out("*parent1")
            .each(Ops.GET, "*person", "parent2").out("*parent2")
            .each(Ops.GET, "*person", "name").out("*name")
            .localTransform(
              "$$family-tree",
              Path.key("*id")
                  .multiPath(Path.key("parent1").termVal("*parent1"),
                             Path.key("parent2").termVal("*parent2"),
                             Path.key("name").termVal("*name")))
            .each(Ops.TUPLE, "*parent1", "*parent2").out("*parents")
            .each(Ops.EXPLODE, "*parents").out("*parent")
            .hashPartition("*parent")
            .localTransform(
              "$$family-tree",
              Path.key("*parent", "children").voidSetElem().termVal("*id"));

    topologies.query("ancestors", "*start-id", "*num-generations").out("*ancestors")
              .loopWithVars(LoopVars.var("*id", "*start-id")
                                    .var("*generation", 0),
                Block.keepTrue(new Expr(Ops.LESS_THAN_OR_EQUAL, "*generation", "*num-generations"))
                     .hashPartition("*id")
                     .localSelect("$$ancestors$$", Path.view(Ops.CONTAINS, "*id")).out("*traversed?")
                     .keepTrue(new Expr(Ops.NOT, "*traversed?"))
                     .localTransform("$$ancestors$$", Path.voidSetElem().termVal("*id"))
                     .localSelect("$$family-tree",
                                  Path.key("*id")
                                      .multiPath(Path.key("parent1"),
                                                 Path.key("parent2"))
                                      .filterPred(Ops.IS_NOT_NULL)).out("*parent")
                     .emitLoop("*parent")
                     .continueLoop("*parent", new Expr(Ops.INC, "*generation"))).out("*ancestor")
              .originPartition()
              .agg(Agg.set("*ancestor")).out("*ancestors");

    topologies.query("descendants-count", "*start-id", "*num-generations").out("*result")
              .loopWithVars(LoopVars.var("*id", "*start-id")
                                    .var("*generation", 0),
                Block.keepTrue(new Expr(Ops.LESS_THAN, "*generation", "*num-generations"))
                     .hashPartition("*id")
                     .localSelect("$$family-tree", Path.key("*id", "children")).out("*children")
                     .emitLoop("*generation", new Expr(Ops.SIZE, "*children"))
                     .each(Ops.EXPLODE, "*children").out("*c")
                     .continueLoop("*c", new Expr(Ops.INC, "*generation"))).out("*gen", "*count")
              .originPartition()
              .compoundAgg(CompoundAgg.map("*gen", Agg.sum("*count"))).out("*result");
  }
}
