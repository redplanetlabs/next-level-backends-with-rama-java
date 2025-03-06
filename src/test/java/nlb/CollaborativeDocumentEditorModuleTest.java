package nlb;

import org.junit.Test;
import java.util.*;

import static org.junit.Assert.*;
import static nlb.CollaborativeDocumentEditorModule.*;

public class CollaborativeDocumentEditorModuleTest {
  private static Map testAddEdit(int offset, String content) {
    Map ret = new HashMap();
    ret.put("type", "add");
    ret.put("id", 123L);
    ret.put("version", 0);
    ret.put("offset", offset);
    ret.put("content", content);

    return ret;
  }

  private static Map testRemoveEdit(int offset, int amount) {
    Map ret = new HashMap();
    ret.put("type", "remove");
    ret.put("id", 123L);
    ret.put("version", 0);
    ret.put("offset", offset);
    ret.put("amount", amount);
    return ret;
  }

  @Test
  public void transformEditTest() {
    Map edit = testAddEdit(10, "abcde");

    // Add against missed add
    assertEquals(Arrays.asList(testAddEdit(14, "abcde")),
                 transformEdit(edit, Arrays.asList(testAddEdit(8, "...."))));
    assertEquals(Arrays.asList(testAddEdit(12, "abcde")),
                 transformEdit(edit, Arrays.asList(testAddEdit(10, ".."))));
    assertEquals(Arrays.asList(testAddEdit(10, "abcde")),
                 transformEdit(edit, Arrays.asList(testAddEdit(17, "..."))));
    assertEquals(Arrays.asList(testAddEdit(10, "abcde")),
                 transformEdit(edit, Arrays.asList(testAddEdit(20, "."))));
    assertEquals(Arrays.asList(testAddEdit(10, "abcde")),
                 transformEdit(edit, Arrays.asList(testAddEdit(12, "."))));

    // Add against missed remove
    assertEquals(Arrays.asList(testAddEdit(7, "abcde")),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(8, 3))));
    assertEquals(Arrays.asList(testAddEdit(6, "abcde")),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(10, 4))));
    assertEquals(Arrays.asList(testAddEdit(10, "abcde")),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(15, 2))));
    assertEquals(Arrays.asList(testAddEdit(10, "abcde")),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(20, 2))));


   edit = testRemoveEdit(10, 6);

   // Remove against missed add
   assertEquals(Arrays.asList(testRemoveEdit(13, 6)),
                 transformEdit(edit, Arrays.asList(testAddEdit(8, "..."))));
   assertEquals(Arrays.asList(testRemoveEdit(14, 6)),
                 transformEdit(edit, Arrays.asList(testAddEdit(10, "...."))));
   assertEquals(Arrays.asList(testRemoveEdit(10, 6)),
                 transformEdit(edit, Arrays.asList(testAddEdit(16, "..."))));
   assertEquals(Arrays.asList(testRemoveEdit(10, 6)),
                 transformEdit(edit, Arrays.asList(testAddEdit(20, "..."))));
   assertEquals(Arrays.asList(testRemoveEdit(10, 2), testRemoveEdit(15, 4)),
                 transformEdit(edit, Arrays.asList(testAddEdit(12, "..."))));

   // Remove against missed remove
   assertEquals(Arrays.asList(testRemoveEdit(8, 6)),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(0, 2))));
   assertEquals(Arrays.asList(testRemoveEdit(8, 3)),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(8, 5))));
   assertEquals(Arrays.asList(),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(7, 100))));
   assertEquals(Arrays.asList(testRemoveEdit(10, 5)),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(10, 1))));
   assertEquals(Arrays.asList(),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(10, 6))));
   assertEquals(Arrays.asList(),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(10, 10))));
   assertEquals(Arrays.asList(testRemoveEdit(10, 4)),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(12, 2))));
   assertEquals(Arrays.asList(testRemoveEdit(10, 2)),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(12, 10))));
   assertEquals(Arrays.asList(testRemoveEdit(10, 6)),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(16, 1))));
   assertEquals(Arrays.asList(testRemoveEdit(10, 6)),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(16, 10))));
   assertEquals(Arrays.asList(testRemoveEdit(10, 6)),
                 transformEdit(edit, Arrays.asList(testRemoveEdit(18, 10))));

   // Transform against multiple edits
   assertEquals(Arrays.asList(testRemoveEdit(19, 1), testRemoveEdit(22, 3)),
                 transformEdit(testRemoveEdit(20, 5),
                               Arrays.asList(testAddEdit(10, "..."),
                                             testRemoveEdit(100, 10),
                                             testRemoveEdit(19, 5),
                                             testAddEdit(20, ".."))));

  }
}
