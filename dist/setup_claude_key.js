/************************************************************
 * setup_claude_key.js
 *
 * スクリプトプロパティ ANTHROPIC_API_KEY を設定するための一回限りの関数。
 * （プロパティが多く、プロジェクト設定画面から追加できない場合用）
 *
 * 使い方:
 *   1. 対象のスプレッドシートを開いておく
 *   2. GASエディタで setupAnthropicApiKey を選んで実行
 *   3. スプレッドシート側に出るダイアログにAPIキーを貼り付けてOK
 *
 * キーはコードに書かず、入力ダイアログから受け取る。
 ************************************************************/

function setupAnthropicApiKey() {
  const ui = SpreadsheetApp.getUi();
  const res = ui.prompt(
    "ANTHROPIC_API_KEY の設定",
    "Claude の API キーを貼り付けてください（sk-ant- で始まる文字列）",
    ui.ButtonSet.OK_CANCEL,
  );
  if (res.getSelectedButton() !== ui.Button.OK) {
    ui.alert("キャンセルしました。");
    return;
  }

  const key = String(res.getResponseText() || "").trim();
  if (key.indexOf("sk-ant-") !== 0) {
    ui.alert("sk-ant- で始まっていないため、設定しませんでした。");
    return;
  }

  PropertiesService.getScriptProperties().setProperty("ANTHROPIC_API_KEY", key);
  ui.alert(
    "ANTHROPIC_API_KEY を設定しました（末尾: ..." + key.slice(-4) + "）",
  );
}

// 設定されているか確認する（キー本体は表示しない）
function checkAnthropicApiKey() {
  const key =
    PropertiesService.getScriptProperties().getProperty("ANTHROPIC_API_KEY") ||
    "";
  Logger.log(
    key
      ? "ANTHROPIC_API_KEY: 設定済み（末尾 ..." + key.slice(-4) + "）"
      : "ANTHROPIC_API_KEY: 未設定",
  );
}
