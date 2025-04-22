package nlb;

import org.junit.Test;

import com.rpl.rama.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;

import java.util.*;

import static org.junit.Assert.*;

public class FamilyTreeModuleTest {
  public static Map createPerson(UUID id, UUID parent1, UUID parent2, String name) {
    Map ret = new HashMap();
    ret.put("id", id);
    ret.put("parent1", parent1);
    ret.put("parent2", parent2);
    ret.put("name", name);
    return ret;
  }

  private static Set asSet(Object... elems) {
    Set ret = new HashSet();
    for(Object o: elems) ret.add(o);
    return ret;
  }

  private static Map asMap(Object... kvs) {
    Map ret = new HashMap();
    for(int i=0; i<kvs.length; i+=2) {
      ret.put(kvs[i], kvs[i+1]);
    }
    return ret;
  }

  @Test
  public void test() throws Exception {
    try(InProcessCluster ipc = InProcessCluster.create()) {
      FamilyTreeModule module = new FamilyTreeModule();
      String moduleName = module.getClass().getName();
      ipc.launchModule(module, new LaunchConfig(4, 2));
      Depot peopleDepot = ipc.clusterDepot(moduleName, "*people-depot");
      PState familyTree = ipc.clusterPState(moduleName, "$$family-tree");
      QueryTopologyClient<Set> ancestorsQuery = ipc.clusterQuery(moduleName, "ancestors");
      QueryTopologyClient<Map> descendantsCountQuery = ipc.clusterQuery(moduleName, "descendants-count");
      UUID p1 = UUID.randomUUID();
      UUID p2 = UUID.randomUUID();
      UUID p3 = UUID.randomUUID();
      UUID p4 = UUID.randomUUID();
      UUID p5 = UUID.randomUUID();
      UUID p6 = UUID.randomUUID();
      UUID p7 = UUID.randomUUID();
      UUID p8 = UUID.randomUUID();
      UUID p9 = UUID.randomUUID();
      UUID p10 = UUID.randomUUID();
      UUID p11 = UUID.randomUUID();
      UUID p12 = UUID.randomUUID();
      UUID p13 = UUID.randomUUID();
      UUID p14 = UUID.randomUUID();

      peopleDepot.append(createPerson(p1, null, null, "Person 1"));
      peopleDepot.append(createPerson(p2, null, null, "Person 2"));
      peopleDepot.append(createPerson(p3, null, null, "Person 3"));
      peopleDepot.append(createPerson(p4, null, null, "Person 4"));
      peopleDepot.append(createPerson(p5, null, null, "Person 5"));
      peopleDepot.append(createPerson(p6, null, null, "Person 6"));
      peopleDepot.append(createPerson(p7, p1, p2, "Person 7"));
      peopleDepot.append(createPerson(p8, p3, p4, "Person 8"));
      peopleDepot.append(createPerson(p9, p1, p5, "Person 9"));
      peopleDepot.append(createPerson(p10, p7, p8, "Person 10"));
      peopleDepot.append(createPerson(p11, p9, p10, "Person 11"));
      peopleDepot.append(createPerson(p12, p10, p11, "Person 12"));
      peopleDepot.append(createPerson(p13, p10, p11, "Person 13"));
      peopleDepot.append(createPerson(p14, p10, p11, "Person 14"));

      assertEquals("Person 6", familyTree.selectOne(Path.key(p6, "name")));
      assertEquals(0, (int) familyTree.selectOne(Path.key(p6, "children").view(Ops.SIZE)));
      assertEquals(4, (int) familyTree.selectOne(Path.key(p10, "children").view(Ops.SIZE)));
      assertEquals(3, (int) familyTree.selectOne(Path.key(p11, "children").view(Ops.SIZE)));

      assertEquals(asSet(p7, p8), ancestorsQuery.invoke(p10, 0));
      assertEquals(asSet(p7, p8, p1, p2, p3, p4), ancestorsQuery.invoke(p10, 1));
      assertEquals(asSet(p10, p11, p7, p8, p9, p1, p2, p3, p4, p5), ancestorsQuery.invoke(p14, 3));

      assertEquals(asMap(0, 0L), descendantsCountQuery.invoke(p14, 10));
      assertEquals(asMap(0, 3L, 1, 0L), descendantsCountQuery.invoke(p11, 10));
      assertEquals(asMap(0, 2L), descendantsCountQuery.invoke(p1, 1));
      assertEquals(asMap(0, 2L, 1, 2L), descendantsCountQuery.invoke(p1, 2));
      assertEquals(asMap(0, 2L, 1, 2L, 2, 7L), descendantsCountQuery.invoke(p1, 3));
    }
  }
}
