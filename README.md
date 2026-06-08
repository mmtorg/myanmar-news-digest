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
SELECTION_ML_TARGET_SHEETS=prod,dev
```

GitHubの `production` Environmentには `GOOGLE_SERVICE_ACCOUNT_JSON` secretが必要です。
サービスアカウントには、月別アーカイブフォルダの閲覧権限と現在のスプレッドシートの
編集権限を付与します。

Apps Scriptで `installSelectionMlWatcherTrigger()` を一度実行すると5分おきのwatcherが
作成され、標準では毎日ミャンマー時間01:30から10分以内に1日1回
`.github/workflows/selection-ml.yml` を起動します。即時確認には
`triggerSelectionMlGitHubActionsNow()` を使用します。このテスト実行は時刻制限を受けず、
通常の当日実行済み判定にも影響しません。
