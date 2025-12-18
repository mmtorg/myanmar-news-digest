// script properties一覧取得
function exportScriptPropertiesAsJson() {
  const props = PropertiesService.getScriptProperties().getProperties();
  console.log("=== SCRIPT PROPERTIES JSON ===");
  console.log(JSON.stringify(props, null, 2));
}

// script propertiesを追加したい場合
function importScriptPropertiesSafe() {
  const json = {
    NEW_SCRIPT_PROPERTY: "xxxx",
  };

  const props = PropertiesService.getScriptProperties();
  props.setProperties(json, false); // 既存に影響を与えないためにfalse
}

// script propertiesを削除したい場合
function deleteOneScriptProperty() {
  const props = PropertiesService.getScriptProperties();
  props.deleteProperty("NEW_SCRIPT_PROPERTY ");
}
