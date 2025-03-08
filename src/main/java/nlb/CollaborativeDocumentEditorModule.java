package nlb;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;

import java.util.*;

public class CollaborativeDocumentEditorModule implements RamaModule {
  private static boolean isAddEdit(Map edit) {
    return edit.get("type").equals("add");
  }

  private static String content(Map addEdit) {
    return (String) addEdit.get("content");
  }

  private static int amount(Map removeEdit) {
    return (int) removeEdit.get("amount");
  }

  private static int offset(Map edit) {
    return (int) edit.get("offset");
  }

  private static int addActionAdjustment(Map edit) {
    if(isAddEdit(edit)) {
      String content = content(edit);
      return content.length();
    } else {
      return -amount(edit);
    }
  }

  private static List transformRemoveAgainstAdd(List<Map> removes, Map missedEdit) {
    int offset = offset(missedEdit);
    int addAmount = content(missedEdit).length();
    List newRemoves = new ArrayList();
    for(Map edit: removes) {
      int start = offset(edit);
      int removeAmount = amount(edit);
      int end = start + removeAmount;
      if(offset >= end) newRemoves.add(edit);
      else if(offset <= start) {
        Map newEdit = new HashMap(edit);
        newEdit.put("offset", offset(newEdit) + addAmount);
        newRemoves.add(newEdit);
      } else {
        int remove1 = offset - start;
        int remove2 = removeAmount - remove1;
        int start2 = offset + addAmount;
        long id = (long) edit.get("id");
        int version = (int) edit.get("version");
        Map edit1 = new HashMap();
        edit1.put("type", "remove");
        edit1.put("id", id);
        edit1.put("version", version);
        edit1.put("offset", start);
        edit1.put("amount", remove1);
        Map edit2 = new HashMap();
        edit2.put("type", "remove");
        edit2.put("id", id);
        edit2.put("version", version);
        edit2.put("offset", start2);
        edit2.put("amount", remove2);
        newRemoves.add(edit2);
        newRemoves.add(edit1);
      }
    }
    return newRemoves;
  }

  private static List transformRemoveAgainstRemove(List<Map> removes, Map missedEdit) {
    int offset = offset(missedEdit);
    int missedRemoveAmount = amount(missedEdit);
    List newRemoves = new ArrayList();
    for(Map edit: removes) {
      int start = offset(edit);
      int removeAmount = amount(edit);
      int end = start + removeAmount;
      if(offset >= end) newRemoves.add(edit);
      else if(offset <= start) {
        int overlap = Math.min(removeAmount,
                               Math.max(0,
                                        offset + missedRemoveAmount - start));
        if(overlap < removeAmount) {
          Map newEdit = new HashMap(edit);
          newEdit.put("offset", offset(edit) - missedRemoveAmount + overlap);
          newEdit.put("amount", amount(edit) - overlap);
          newRemoves.add(newEdit);
        }
      } else {
        int overlap = Math.min(end, offset + missedRemoveAmount) - offset;
        Map newEdit = new HashMap(edit);
        newEdit.put("amount", amount(newEdit) - overlap);
        newRemoves.add(newEdit);
      }
    }
    return newRemoves;
  }

  public static List transformEdit(Map edit, List<Map> missedEdits) {
    int offset = offset(edit);
    if(isAddEdit(edit)) {
      int adjustment = 0;
      for(Map e: missedEdits) {
        if(offset(e) <= offset) offset += addActionAdjustment(e);
      }
      Map newEdit = new HashMap(edit);
      newEdit.put("offset", offset + adjustment);
      return Arrays.asList(newEdit);
    } else {
      List removes = Arrays.asList(edit);
      for(Map e: missedEdits) {
        if(isAddEdit(e)) removes = transformRemoveAgainstAdd(removes, e);
        else removes = transformRemoveAgainstRemove(removes, e);
      }
      return removes;
    }
  }

  private static String applyEdits(String doc, List<Map> edits) {
    for(Map edit: edits) {
      int offset = offset(edit);
      if(isAddEdit(edit)) {
        doc = doc.substring(0, offset) + content(edit) + doc.substring(offset);
      } else {
        doc = doc.substring(0, offset) + doc.substring(offset + amount(edit));
      }
    }
    return doc;
  }

  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*edit-depot", Depot.hashBy("id"));

    StreamTopology topology = topologies.stream("core");
    topology.pstate("$$docs", PState.mapSchema(Long.class, String.class));
    topology.pstate("$$edits",
                    PState.mapSchema(Long.class,
                                     PState.listSchema(Map.class).subindexed()));

    topology.source("*edit-depot").out("*edit")
            .each(Ops.GET, "*edit", "id").out("*id")
            .each(Ops.GET, "*edit", "version").out("*version")
            .localSelect("$$edits", Path.key("*id").view(Ops.SIZE)).out("*latest-version")
            .ifTrue(new Expr(Ops.EQUAL, "*latest-version", "*version"),
              Block.each(Ops.TUPLE, "*edit").out("*final-edits"),
              Block.localSelect("$$edits",
                                Path.key("*id")
                                    .sublist("*version", "*latest-version")).out("*missed-edits")
                   .each(CollaborativeDocumentEditorModule::transformEdit,
                         "*edit", "*missed-edits").out("*final-edits"))
            .localSelect("$$docs", Path.key("*id").nullToVal("")).out("*latest-doc")
            .each(CollaborativeDocumentEditorModule::applyEdits,
                  "*latest-doc", "*final-edits").out("*new-doc")
            .localTransform("$$docs", Path.key("*id").termVal("*new-doc"))
            .localTransform("$$edits", Path.key("*id").end().termVal("*final-edits"));

    topologies.query("doc+version", "*id").out("*ret")
              .hashPartition("*id")
              .localSelect("$$docs", Path.key("*id")).out("*doc")
              .localSelect("$$edits", Path.key("*id").view(Ops.SIZE)).out("*version")
              .each((String doc, Integer version) -> {
                Map ret = new HashMap();
                ret.put("doc", doc);
                ret.put("version", version);
                return ret;
              }, "*doc", "*version").out("*ret")
              .originPartition();
  }
}