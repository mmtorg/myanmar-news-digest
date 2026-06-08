# myanmar-news-digest

コミット前に `ruff check --fix` と `ruff format` を自動実行

```
pip install -U pre-commit ruff
pre-commit install
```

## Selection ML

Driveフォルダ内でファイル名が `prod_` から始まる月別アーカイブを対象にし、
各ファイルの `prod` シートを共通教師データとして使用します。現在の予測対象が
`prod` / `dev` のどちらでも、この共通教師データから学習します。K列が `a` の
アーカイブ行を採用例として扱い、予測結果を現在の `prod` / `dev` の `AA:AC` に
書き込みます。

Apps ScriptのScript Propertiesに以下を設定します。

```text
GITHUB_OWNER
GITHUB_REPO
GITHUB_TOKEN
ARCHIVE_DRIVE_FOLDER_ID
SELECTION_ML_GITHUB_WORKFLOW_FILE=selection-ml.yml
SELECTION_ML_TARGET_HOUR=1
SELECTION_ML_TARGET_MINUTE=30
```

GitHubの `production` Environmentには `GOOGLE_SERVICE_ACCOUNT_JSON` secretが必要です。
サービスアカウントには、月別アーカイブフォルダの閲覧権限と現在のスプレッドシートの
編集権限を付与します。

Apps Scriptで `installSelectionMlWatcherTrigger()` を一度実行すると5分おきのwatcherが
作成され、毎日ミャンマー時間01:30から10分以内にprodだけを1日1回処理します。

prodを任意の時刻に手動実行する場合:

```javascript
triggerSelectionMlProdNow()
```

devを手動実行する場合:

```javascript
triggerSelectionMlDevNow()
```

devには定時実行がありません。どちらの手動実行も通常のprod定時実行済み判定には
影響しません。
