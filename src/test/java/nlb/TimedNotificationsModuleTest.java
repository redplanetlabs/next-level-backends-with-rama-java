package nlb;

import org.junit.Test;

import com.rpl.rama.*;
import com.rpl.rama.test.*;
import com.rpl.rama.helpers.*;
import java.io.Closeable;
import java.util.*;

import static org.junit.Assert.*;

public class TimedNotificationsModuleTest {
  public static Map makeScheduledPost(String id, long timeMillis, String post) {
    Map ret = new HashMap();
    ret.put("id", id);
    ret.put("time-millis", timeMillis);
    ret.put("post", post);
    return ret;
  }

  @Test
  public void test() throws Exception {
    try(InProcessCluster ipc = InProcessCluster.create();
        Closeable simTime = TopologyUtils.startSimTime()) {
      TimedNotificationsModule module = new TimedNotificationsModule();
      module.isTestMode = true;
      String moduleName = module.getClass().getName();
      ipc.launchModule(module, new LaunchConfig(4, 2));
      Depot scheduledPostDepot = ipc.clusterDepot(moduleName, "*scheduled-post-depot");
      Depot tick = ipc.clusterDepot(moduleName, "*tick");
      PState feeds = ipc.clusterPState(moduleName, "$$feeds");

      scheduledPostDepot.append(makeScheduledPost("alice", 1500, "Post 1"));
      scheduledPostDepot.append(makeScheduledPost("alice", 800, "Post 2"));
      scheduledPostDepot.append(makeScheduledPost("alice", 2000, "Post 3"));

      tick.append(null);
      assertEquals(Arrays.asList(), feeds.select(Path.key("alice").all()));

      TopologyUtils.advanceSimTime(799);
      tick.append(null);
      assertEquals(Arrays.asList(), feeds.select(Path.key("alice").all()));

      TopologyUtils.advanceSimTime(1);
      tick.append(null);
      assertEquals(Arrays.asList("Post 2"), feeds.select(Path.key("alice").all()));

      TopologyUtils.advanceSimTime(600);
      tick.append(null);
      assertEquals(Arrays.asList("Post 2"), feeds.select(Path.key("alice").all()));

      TopologyUtils.advanceSimTime(100);
      tick.append(null);
      assertEquals(Arrays.asList("Post 2", "Post 1"), feeds.select(Path.key("alice").all()));

      TopologyUtils.advanceSimTime(450);
      tick.append(null);
      assertEquals(Arrays.asList("Post 2", "Post 1"), feeds.select(Path.key("alice").all()));

      TopologyUtils.advanceSimTime(50);
      tick.append(null);
      assertEquals(Arrays.asList("Post 2", "Post 1", "Post 3"), feeds.select(Path.key("alice").all()));
    }
  }
}
