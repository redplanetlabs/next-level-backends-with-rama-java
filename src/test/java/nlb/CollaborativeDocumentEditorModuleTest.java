package nlb;

import org.junit.Test;

import com.rpl.rama.*;
import com.rpl.rama.test.*;

import java.util.*;

import static org.junit.Assert.*;
import static nlb.CollaborativeDocumentEditorModule.*;

public class CollaborativeDocumentEditorModuleTest {
  public static Map makeAddEdit(long id, int version, int offset, String content) {
    Map ret = new HashMap();
    ret.put("type", "add");
    ret.put("id", id);
    ret.put("version", version);
    ret.put("offset", offset);
    ret.put("content", content);
    return ret;
  }

  public static Map makeAddEdit(int offset, String content) {
    return makeAddEdit(123L, 0, offset, content);
  }

  public static Map makeRemoveEdit(long id, int version, int offset, int amount) {
    Map ret = new HashMap();
    ret.put("type", "remove");
    ret.put("id", id);
    ret.put("version", version);
    ret.put("offset", offset);
    ret.put("amount", amount);
    return ret;
  }

  public static Map makeRemoveEdit(int offset, int amount) {
    return makeRemoveEdit(123L, 0, offset, amount);
  }

  @Test
  public void transformEditTest() {
    Map edit = makeAddEdit(10, "abcde");

    // Add against missed add
    assertEquals(Arrays.asList(makeAddEdit(14, "abcde")),
                 transformEdit(edit, Arrays.asList(makeAddEdit(8, "...."))));
    assertEquals(Arrays.asList(makeAddEdit(12, "abcde")),
                 transformEdit(edit, Arrays.asList(makeAddEdit(10, ".."))));
    assertEquals(Arrays.asList(makeAddEdit(10, "abcde")),
                 transformEdit(edit, Arrays.asList(makeAddEdit(17, "..."))));
    assertEquals(Arrays.asList(makeAddEdit(10, "abcde")),
                 transformEdit(edit, Arrays.asList(makeAddEdit(20, "."))));
    assertEquals(Arrays.asList(makeAddEdit(10, "abcde")),
                 transformEdit(edit, Arrays.asList(makeAddEdit(12, "."))));

    // Add against missed remove
    assertEquals(Arrays.asList(makeAddEdit(7, "abcde")),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(8, 3))));
    assertEquals(Arrays.asList(makeAddEdit(6, "abcde")),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(10, 4))));
    assertEquals(Arrays.asList(makeAddEdit(10, "abcde")),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(15, 2))));
    assertEquals(Arrays.asList(makeAddEdit(10, "abcde")),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(20, 2))));


   edit = makeRemoveEdit(10, 6);

   // Remove against missed add
   assertEquals(Arrays.asList(makeRemoveEdit(13, 6)),
                 transformEdit(edit, Arrays.asList(makeAddEdit(8, "..."))));
   assertEquals(Arrays.asList(makeRemoveEdit(14, 6)),
                 transformEdit(edit, Arrays.asList(makeAddEdit(10, "...."))));
   assertEquals(Arrays.asList(makeRemoveEdit(10, 6)),
                 transformEdit(edit, Arrays.asList(makeAddEdit(16, "..."))));
   assertEquals(Arrays.asList(makeRemoveEdit(10, 6)),
                 transformEdit(edit, Arrays.asList(makeAddEdit(20, "..."))));
   assertEquals(Arrays.asList(makeRemoveEdit(15, 4), makeRemoveEdit(10, 2)),
                 transformEdit(edit, Arrays.asList(makeAddEdit(12, "..."))));

   // Remove against missed remove
   assertEquals(Arrays.asList(makeRemoveEdit(8, 6)),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(0, 2))));
   assertEquals(Arrays.asList(makeRemoveEdit(8, 3)),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(8, 5))));
   assertEquals(Arrays.asList(),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(7, 100))));
   assertEquals(Arrays.asList(makeRemoveEdit(10, 5)),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(10, 1))));
   assertEquals(Arrays.asList(),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(10, 6))));
   assertEquals(Arrays.asList(),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(10, 10))));
   assertEquals(Arrays.asList(makeRemoveEdit(10, 4)),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(12, 2))));
   assertEquals(Arrays.asList(makeRemoveEdit(10, 2)),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(12, 10))));
   assertEquals(Arrays.asList(makeRemoveEdit(10, 6)),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(16, 1))));
   assertEquals(Arrays.asList(makeRemoveEdit(10, 6)),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(16, 10))));
   assertEquals(Arrays.asList(makeRemoveEdit(10, 6)),
                 transformEdit(edit, Arrays.asList(makeRemoveEdit(18, 10))));

   // Transform against multiple edits
   assertEquals(Arrays.asList(makeRemoveEdit(22, 3), makeRemoveEdit(19, 1)),
                 transformEdit(makeRemoveEdit(20, 5),
                               Arrays.asList(makeAddEdit(10, "..."),
                                             makeRemoveEdit(100, 10),
                                             makeRemoveEdit(19, 5),
                                             makeAddEdit(20, ".."))));
  }

  private static Map docVersion(String doc, int version) {
    Map ret = new HashMap();
    ret.put("doc", doc);
    ret.put("version", version);
    return ret;
  }

  @Test
  public void moduleTest() throws Exception {
    try(InProcessCluster ipc = InProcessCluster.create()) {
      CollaborativeDocumentEditorModule module = new CollaborativeDocumentEditorModule();
      String moduleName = module.getClass().getName();
      ipc.launchModule(module, new LaunchConfig(4, 2));
      Depot editDepot = ipc.clusterDepot(moduleName, "*edit-depot");
      QueryTopologyClient<Map> docAndVersion = ipc.clusterQuery(moduleName, "doc+version");

      editDepot.append(makeAddEdit(123L, 0, 0, "Hellox"));
      assertEquals(docVersion("Hellox", 1), docAndVersion.invoke(123L));

      editDepot.append(makeRemoveEdit(123L, 1, 5, 1));
      assertEquals(docVersion("Hello", 2), docAndVersion.invoke(123L));

      editDepot.append(makeAddEdit(123L, 2, 5, " wor"));
      assertEquals(docVersion("Hello wor", 3), docAndVersion.invoke(123L));

      editDepot.append(makeAddEdit(123L, 3, 9, "ld!"));
      assertEquals(docVersion("Hello world!", 4), docAndVersion.invoke(123L));

      editDepot.append(makeAddEdit(123L, 2, 5, "abcd"));
      editDepot.append(makeRemoveEdit(123L, 2, 0, 4));
      editDepot.append(makeRemoveEdit(123L, 1, 0, 3));
      assertEquals(docVersion("o world!abcd", 6), docAndVersion.invoke(123L));
    }
  }
}
