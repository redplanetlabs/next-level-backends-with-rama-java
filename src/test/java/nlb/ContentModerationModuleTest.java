package nlb;

import org.junit.Test;

import com.rpl.rama.*;
import com.rpl.rama.test.*;

import java.util.*;

import static org.junit.Assert.*;

public class ContentModerationModuleTest {
  public static Map createPost(long fromUserId, long toUserId, String content) {
    Map ret = new HashMap();
    ret.put("from-user-id", fromUserId);
    ret.put("to-user-id", toUserId);
    ret.put("content", content);
    return ret;
  }

  public static Map createMute(long userId, long mutedUserId) {
    Map ret = new HashMap();
    ret.put("type", "mute");
    ret.put("user-id", userId);
    ret.put("muted-user-id", mutedUserId);
    return ret;
  }

  public static Map createUnmute(long userId, long unmutedUserId) {
    Map ret = new HashMap();
    ret.put("type", "unmute");
    ret.put("user-id", userId);
    ret.put("unmuted-user-id", unmutedUserId);
    return ret;
  }

  public static Map queryRes(Integer nextOffset, Map... posts) {
    Map ret = new HashMap();
    ret.put("next-offset", nextOffset);
    List l = new ArrayList();
    for(Map m: posts) l.add(m);
    ret.put("posts", l);
    return ret;
  }

  @Test
  public void test() throws Exception {
    try(InProcessCluster ipc = InProcessCluster.create()) {
      ContentModerationModule module = new ContentModerationModule();
      String moduleName = module.getClass().getName();
      ipc.launchModule(module, new LaunchConfig(4, 2));
      Depot postDepot = ipc.clusterDepot(moduleName, "*post-depot");
      Depot muteDepot = ipc.clusterDepot(moduleName, "*mute-depot");
      QueryTopologyClient<Map> getPosts = ipc.clusterQuery(moduleName, "get-posts");
      long user1 = 100;
      long user2 = 101;
      long user3 = 102;
      long user4 = 103;
      long user5 = 104;

      Map post1 = createPost(user2, user1, "Post 1");
      Map post2 = createPost(user1, user1, "Post 2");
      Map post3 = createPost(user2, user1, "Post 3");
      Map post4 = createPost(user5, user1, "Post 4");
      Map post5 = createPost(user4, user1, "Post 5");
      Map post6 = createPost(user1, user1, "Post 6");
      Map post7 = createPost(user2, user1, "Post 7");
      Map post8 = createPost(user3, user1, "Post 8");

      postDepot.append(post1);
      postDepot.append(post2);
      postDepot.append(post3);
      postDepot.append(post4);
      postDepot.append(post5);
      postDepot.append(post6);
      postDepot.append(post7);
      postDepot.append(post8);

      assertEquals(queryRes(3, post1, post2, post3), getPosts.invoke(user1, 0, 3));
      assertEquals(queryRes(6, post4, post5, post6), getPosts.invoke(user1, 3, 3));
      assertEquals(queryRes(null, post7, post8), getPosts.invoke(user1, 6, 3));
      assertEquals(queryRes(null), getPosts.invoke(user1, 8, 3));

      muteDepot.append(createMute(user1, user2));
      muteDepot.append(createMute(user1, user4));

      assertEquals(queryRes(6, post2, post4, post6), getPosts.invoke(user1, 0, 3));
      assertEquals(queryRes(null, post8), getPosts.invoke(user1, 6, 3));

      muteDepot.append(createUnmute(user1, user2));

      assertEquals(queryRes(3, post1, post2, post3), getPosts.invoke(user1, 0, 3));
      assertEquals(queryRes(7, post4, post6, post7), getPosts.invoke(user1, 3, 3));
      assertEquals(queryRes(null, post8), getPosts.invoke(user1, 7, 3));
    }
  }
}
