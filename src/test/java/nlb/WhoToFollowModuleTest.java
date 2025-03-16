package nlb;

import org.junit.Test;

import com.rpl.rama.*;
import com.rpl.rama.test.*;

import java.util.*;

import static org.junit.Assert.*;

public class WhoToFollowModuleTest {
  public static Map makeFollow(long from, long to) {
    Map ret = new HashMap();
    ret.put("from", from);
    ret.put("to", to);
    return ret;
  }

  @Test
  public void test() throws Exception {
    try(InProcessCluster ipc = InProcessCluster.create()) {
      WhoToFollowModule module = new WhoToFollowModule();
      module.isTestMode = true;
      module.numRecommendations = 3;
      String moduleName = module.getClass().getName();
      ipc.launchModule(module, new LaunchConfig(4, 2));
      Depot followsDepot = ipc.clusterDepot(moduleName, "*follows-depot");
      Depot whoToFollowTick = ipc.clusterDepot(moduleName, "*who-to-follow-tick");
      PState whoToFollow = ipc.clusterPState(moduleName, "$$who-to-follow");

      followsDepot.append(makeFollow(1, 2));
      followsDepot.append(makeFollow(1, 3));
      followsDepot.append(makeFollow(1, 4));
      followsDepot.append(makeFollow(1, 5));

      followsDepot.append(makeFollow(2, 1));
      followsDepot.append(makeFollow(2, 3));
      followsDepot.append(makeFollow(2, 6));
      followsDepot.append(makeFollow(2, 7));
      followsDepot.append(makeFollow(2, 8));
      followsDepot.append(makeFollow(2, 9));
      followsDepot.append(makeFollow(2, 10));

      followsDepot.append(makeFollow(3, 1));
      followsDepot.append(makeFollow(3, 8));
      followsDepot.append(makeFollow(3, 9));
      followsDepot.append(makeFollow(3, 11));
      followsDepot.append(makeFollow(3, 12));

      followsDepot.append(makeFollow(4, 8));
      followsDepot.append(makeFollow(4, 3));
      followsDepot.append(makeFollow(4, 10));
      followsDepot.append(makeFollow(4, 13));
      followsDepot.append(makeFollow(4, 14));

      followsDepot.append(makeFollow(5, 1));
      followsDepot.append(makeFollow(5, 3));
      followsDepot.append(makeFollow(5, 10));

      followsDepot.append(makeFollow(6, 12));
      followsDepot.append(makeFollow(7, 12));
      followsDepot.append(makeFollow(8, 12));
      followsDepot.append(makeFollow(9, 12));

      whoToFollowTick.append(null);
      ipc.waitForMicrobatchProcessedCount(moduleName, "who-to-follow", 1);

      Set expected = new HashSet();
      expected.add(10L);
      expected.add(8L);
      expected.add(9L);

      assertEquals(expected, new HashSet((List) whoToFollow.selectOne(Path.key(1L))));
    }
  }
}