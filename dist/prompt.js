/************************************************************
 * 0. 共通ルール（Python側の COMMON_TRANSLATION_RULES / TITLE_OUTPUT_RULES / HEADLINE_PROMPT_x 相当）
 ************************************************************/

// 用語統一＋通貨変換ルール（COMMON_TRANSLATION_RULES 相当）
const COMMON_TRANSLATION_RULES = `
【翻訳時の用語統一ルール（必ず従うこと）】
このルールは記事タイトルと本文の翻訳に必ず適用してください。
軍事評議会 → 軍事政権
軍事委員会 → 軍事政権
徴用 → 徴兵
軍事評議会軍 → 国軍
アジア道路 → アジアハイウェイ
来客登録 → 宿泊登録
来客登録者 → 宿泊登録者
タウンシップ → 郡区
北オークカラパ → 北オカラッパ
北オカラパ → 北オカラッパ
サリンギ郡区 → タンリン郡区
ネーピードー → ネピドー
ミャンマー国民 → ミャンマー人
タディンユット → ダディンジュ

【ミャンマー情勢に関する用語置き換えルール】
ミャンマー情勢の記事で、反政権側の運動・組織を指す文脈では、次のように訳語を統一すること。
- 革命 → 抵抗
- 革命勢力 → 抵抗勢力
- 革命軍 → 抵抗勢力

【その他の用語統一】
- サイドカー → サイカー
- SpaceX → スペースX
- KK Park → KKパーク
- Starlink → スターリンク

【翻訳時の特別ルール】
このルールも記事タイトルと本文の翻訳に必ず適用してください。
「ဖမ်းဆီး」の訳語は文脈によって使い分けること。
- 犯罪容疑や法律違反に対する文脈の場合は「逮捕」とする。
- 犯罪容疑や法律違反に基づかない文脈の場合は「拘束」とする。

【政党名の訳語ルール（USDP関連）】
以下の政党名が原文に出てきた場合や、見出し・要約で触れる場合は、
文脈に応じて、下記の訳語のいずれかを用いること。それ以外の訳語は使わない。

対象となる原文：
- 「ကြံ့ဖွတ်ပါတီ」
- 「ပြည်ထောင်စုကြံ့ခိုင်ရေးနှင့် ဖွံ့ဖြိုးရေးပါတီ」
- 「Union Solidarity and Development Party」または「USDP」

使用してよい訳語：
- 「国軍系政党」
- 「国軍系USDP党」
- 「国軍系連邦団結発展党（USDP）」

使い分けの目安：
- 見出しにおいては、「国軍系USDP党」を優先する。
- 記事内で政党名を初めて説明する箇所では、可能であれば 「国軍系連邦団結発展党（USDP）」を優先する。
- 2回目以降は可能な限り「USDP」を用いて簡略化する。
- 文脈上、特定の政党名ではなく「国軍側の与党勢力」として一般的にまとめても問題ない場合は、「国軍系政党」を用いる。

禁止：
- 上記3種類のいずれにも該当しない省略形・短縮形（例：「国軍系USDP」「USDP政党」など）は使用禁止。

【武装組織名の訳語ルール（BGF関連）】
翻訳・要約の結果として、次の表現を日本語で出力する場合は、
文脈に応じて、下記の訳語のいずれかに統一すること。それ以外の訳語は使わない。

対象となる原文：
- 「BGF」
- 「Karen Border Guard Force」
- 「カレン国境警備隊」

使用してよい訳語：
- 「国軍系勢力」
- 「国軍系勢力BGF」
- 「国軍傘下のカレン国境警備隊」
- 「国軍傘下のカレン国境警備隊（BGF）」

使い分けの目安：
- 見出しにおいては、「国軍傘下BGF」を優先する。
- 記事内で初めて出てくる箇所では 「国軍傘下のカレン国境警備隊（BGF）」を優先する。
- 2回目以降は、可能な限り「BGF」に短縮する。
- 細かい組織名より「国軍側の武装勢力」である点を強調したい文脈では「国軍系勢力」とだけ記述してもよい。

禁止：
- 上記4種類のいずれにも該当しない省略形・短縮形（例：「国軍傘下BGF」「BGF部隊」「国軍系BGF」など）は使用禁止。

【武装組織名の訳語ルール（DKBA関連）】
翻訳・要約の結果として、次の表現を日本語で出力する場合は、
文脈に応じて、下記の訳語のいずれかに統一すること。それ以外の訳語は使わない。

対象となる原文：
- 「ဒီမိုကရေစီ အကျိုးပြု ကရင်တပ်မတော်」
- 「ဒီမိုကရက်တစ်ကရင်အကျိုးပြုတပ်မတော်」
- 「ဒီမိုကရေစီ ကရင် တပ်မတော်」
- 「DKBA」
- 「D.K.B.A」

使用してよい訳語：
- 「国軍傘下DKBA」
- 「国軍傘下の民主カレン仏教徒軍（DKBA）」
- 「DKBA」
- 「国軍系勢力」

使い分けの目安：
- 見出しにおいては、「国軍傘下DKBA」を優先する。
- 記事内で初めて出てくる箇所では「国軍傘下の民主カレン仏教徒軍（DKBA）」を優先する。
- 2回目以降は、可能な限り「DKBA」に短縮する。
- 細かい組織名より「国軍側の武装勢力」である点を強調したい文脈では「国軍系勢力」とだけ記述してもよい。

禁止：
- 上記4種類のいずれにも該当しない省略形・短縮形（例：「国軍系DKBA」「DKBA部隊」「国軍傘下DKBA軍」など）は使用禁止。

【武装組織名の訳語ルール（ピューソーティー関連）】
翻訳・要約の出力に「ピューソーティー」または「ピュー・ソー・ティー」が含まれる場合は、
文脈に応じて以下のように必ず使い分けること。

■ 見出し（HEADLINE）
- すべての見出しでは訳語を「国軍民兵」とする。

■ 本文（BODY）
- 記事本文内での最初の出現：  
  → 「国軍民兵ピューソーティー」
- 記事本文内で2回目以降の出現：  
  → 「ピューソーティー」

■ 補足
- 「ピューソーティー」「ピュー・ソー・ティー」は同一語として扱う。

【中立的記述ルール（必ず守ること）】
- 翻訳・要約のいずれの場合も、記者の地の文（＝誰の発言にも帰属していない叙述部分）については、
  政治的に偏った語、価値判断語、レッテル語を使用せず、中立語に置き換える。
  （例：民主派、国家派、正統政府、傀儡政権、違法政権、クーデター指導者、
        テロリスト、ファシスト、違法組織、不正な選挙、偽装選挙 等）

- 記者の地の文に含まれる主観的評価・立場表明・感情語は削除し、
  客観的な事実記述に改める。
  ・残虐な攻撃 → 「攻撃」
  ・不当な拘束 → 「拘束が行われた」
  ・違法な政権 → 「違法と批判されている」など、誰かの評価であることが分かる表現に言い換える。
  ・不正な／偽りの／偽装選挙 → 「選挙」

- 組織や個人名に付随するレッテル語は、翻訳・要約どちらの場合も除去し、中立的名称に統一する。

  【ミン・アウン・フライン個人に付くレッテル】
  ・クーデター指導者ミン・アウン・フライン
  ・テロリスト指導者ミン・アウン・フライン
  ・テロリストのミン・アウン・フライン
  ・ファシスト指導者ミン・アウン・フライン
    → 翻訳・要約では「ミン・アウン・フライン総司令官」または「ミン・アウン・フライン」とする。
  ・使用してよい肩書きは「総司令官（Commander-in-Chief）」のみとする。
  ・Chairman（議長）、Acting President など国家元首性を示す表現は、記者の地の文では使用しない。
    （ただし、引用・スローガン・発言部分の中に含まれる場合は原文どおり保持する。）

  【国軍・軍事政権に付くレッテル】
  ・ファシスト国軍
  ・テロリスト軍
  ・クーデター軍
  ・テロリスト軍事政権
  ・テロリスト軍事評議会
  ・クーデター軍事評議会
    → 翻訳・要約では「国軍」「ミャンマー国軍」「軍事政権」のいずれかに統一する。

- 反政権側の組織に対する国営系メディアのレッテル語も削除する。
  ・テロ組織NUG → 「NUG」
  ・違法武装組織PDF → 「PDF」
  ・分離主義テロ組織○○ → 「○○武装組織」

【引用・スローガン・発言部分の扱い】
- 以下は原文を改変せず保持する（引用符内は絶対に書き換えない）：
  1) デモ参加者・団体・市民の掲げるスローガン
  2) 当事者の発言・インタビュー・声明
  3) いずれかの側による主張・批判・要求

- 要約では、これらの発言は必ず
  「〜と述べた／〜と主張した／〜と訴えた」等の形式で紹介し、
  要約側が断定しない。

- 引用か地の文か判断が難しい場合は次で判定する：
  ・引用符（" " / 「 」 / 『 』）内 → 残す
  ・明確に誰かの発言と分かる文 → 残す
  ・それ以外 → 中立化して扱う

【選挙に関する特例（引用部分の扱い）】
※ 上記の【引用・スローガン・発言部分の扱い】を、選挙に関する表現について具体化したものである。
  中立化ルールは「記者の地の文」にのみ適用し、以下のような引用・発言には適用しない。

  次の場合は、不正な／偽りの／偽装選挙などの修飾語を含め原文のニュアンスを保持する：
  ・スローガン（例：「偽りの選挙は不要」）
  ・団体声明（例：「軍評議会による選挙は民主的でない」）
  ・個人・組織の主張（例：「不当な選挙だ」と主張）

【時制表現の禁止ルール（要約用）】
要約文では、次の相対的な時制表現は使用禁止とする。
- 「本日」
- 「昨日」
- 「明日」
これらの表現はニュース配信日と原文の日付がずれ、誤解を招くため使用しないこと。
必要な場合は、必ず原文に記載されている具体的な日付を明記すること。

【日付の扱いルール】
- 原文に年が書かれていない場合、翻訳・要約のどちらでも年（◯年）を補わないこと。
- 例：原文が「ဒီဇင်ဘာ ၄ ရက်」など月日だけの場合、日本語でも必ず「12月4日」のように月日のみで表記する。
- 原文に存在しない年を推測して付与することは禁止（例：2024年12月4日、202X年12月4日 など）。
- どのような場合でも西暦を自動補完しない。

【日付の年の自動補完を禁止（重要・汎用）】
- 原文が英語またはビルマ語の場合も同様に、日付の「年」は原文に明示がある場合のみ出力する。
- 原文内に「年あり日付」と「年なし日付」が混在しても、年なし日付に年を推測で付けてはならない。
  例）原文に「March 2026」と「28 December」がある場合、要約で「2026年12月28日」としてはならない。
- 「原文の別箇所に同じ年がある」ことは、年付与の根拠にならない。
- 年を出力してよい条件：原文の同じ日付表現に年が結び付いている（例：2026年3月、March 2026、၂၀၂၆ ခုနှစ် မတ်လ、など）。

【通貨換算ルール】
このルールも記事タイトルと本文の翻訳に必ず適用してください。
※ただし、別途「見出し専用ルール」で指定がある場合は、そちらを優先すること。

ミャンマー通貨「チャット（Kyat、ကျပ်）」が出てきた場合は、日本円に換算して併記してください。
- 換算レートは 1チャット = 0.039円 を必ず使用すること。
- 記事中にチャットが出た場合は必ず「◯チャット（約◯円）」の形式に翻訳すること。
- 日本円は小数点以下を四捨五入すること（例：16,500円）。
- 日本円の金額は、計算で得られた数値をもとに、機械的に「兆・億・万」に分解して表記すること。
  - 元の数値の桁（オーダー）を増減させたり、便宜的に単位（万→億、億→兆など）を繰り上げたりしないこと。
  - 合計金額が必ず一致するように機械的に分解すること。

【金額分解の正しい例】
- 21,060,000,000円 → 210億6000万円
- 5,432,100,000円 → 54億3210万円
- 1,234,567,890,000円 → 1兆2345億6789万円
- 987,654,321円 → 9億8765万4321円
- 50,000,000,000円 → 500億円
- 3,045,678,900,000円 → 3兆456億7890万円

【誤りの例（してはいけない表記）】
- 21,060,000,000円 を「2兆1060億円」と書く（桁が10倍に変わっており誤り）
- 5,432,100,000円 を「543億2100万円」とする（億の位置がずれて誤り）
- 987,654,321円 を「9.8億円」など概算にする（丸め禁止）
- 計算値にない単位へ勝手に繰り上げる・繰り下げることは禁止

他のレートは使用禁止。
チャット以外の通貨（例：タイの「バーツ」や米ドルなど）には適用しない。換算は行わないこと。

【ミャンマー語の数詞・単位ルール】
このルールも記事タイトルと本文の翻訳に必ず適用してください。

■ 共通ルール
ミャンマー語の金額単位は、以下のように「数字の前後どちらに置かれてもよい」ものとして扱うこと。
また、数字と単位の間にスペースがある／ないの両方を許容すること。
- 例： 「၅၀ သိန်း」「သိန်း ၅၀」「၅၀သိန်း」「သိန်း၅၀」は、すべて同じ意味として扱う。

--------------------------------
■ 1) 「သိန်း」：10万チャットの単位
--------------------------------
- ミャンマー語の「သိန်း」は 100,000（10万）チャットを表す単位である。
- 「数字 + သိန်း」「သိန်း + 数字」のどちらの並びも有効とみなす。
- 数字と「သိန်း」の間にスペースがない場合も同様に解釈する。

＜解釈ルール＞
- 「◯◯ သိန်း」「သိန်း ◯◯」「◯◯သိန်း」「သိန်း◯◯」
  → いずれも「◯◯ × 10万チャット」と解釈する。

＜例（ビルマ数字）＞
- 「သိန်း ၅၀」「၅၀ သိန်း」「၅၀သိန်း」「သိန်း၅０」
  → 50 × 10万 ＝ 5,000,000 チャット（500万チャット）
- 「သိန်း ၃၀၀၀」「၃၀၀၀ သိန်း」「၃၀၀၀သိန်း」「သိန်း၃၀၀၀」
  → 3,000 × 10万 ＝ 300,000,000 チャット（3億チャット）

＜例（アラビア数字）＞
- 「သိန်း 50」「50 သိန်း」「50သိန်း」「သိန်း50」
  → 50 × 10万 ＝ 5,000,000 チャット（500万チャット）
- 「သိန်း 3000」「3000 သိန်း」「3000သိန်း」「သိန်း3000」
  → 3,000 × 10万 ＝ 300,000,000 チャット（3億チャット）

＜日本語への表記ルール（重要）＞
- 「◯◯ သိန်း」などを日本語に訳すときは、金額部分を必ず
  「◯◯0万チャット」の形式で表記すること。
  - 具体的には、ビルマ語の数字 N に対して、
    「N သိန်း」→「(N×10)万チャット」と表記する。
- 例：
  - 「သိန်း ၃၀」「30 သိန်း」→「300万チャット」
  - 「သိန်း ၅၀」「50 သိန်း」→「500万チャット」
- 絶対に「N သိန်း」を「N万チャット」と短く訳してはいけない。
  - 誤りの例：「30 သိန်း」を「30万チャット」と訳す（※本来は300万チャット）
- 円での併記が必要な場合は、
  まず正しいチャット建てを計算し、その後に「約◯◯円」を併記すること。

--------------------------------
■ 2) 「သန်း」：100万チャット（100万）の単位
--------------------------------
- ミャンマー語の「သန်း」は 1,000,000（100万）チャットを表す単位である。
- 「数字 + သန်း」「သန်း + 数字」のどちらの並びも有効とみなす。
- 数字と「သန်း」の間にスペースがない場合も同様に解釈する。

＜解釈ルール＞
- 「◯◯ သန်း」「သန်း ◯◯」「◯◯သန်း」「သန်း◯◯」
  → いずれも「◯◯ × 100万チャット」と解釈する。

＜例（ビルマ数字）＞
- 「၁ သန်း」「သန်း ၁」
  → 1 × 100万 ＝ 1,000,000 チャット（100万チャット）
- 「၁၀ သန်း」「သန်း ၁၀」
  → 10 × 100万 ＝ 10,000,000 チャット（1000万チャット）
- 「၅၀ သန်း」「သန်း ၅၀」
  → 50 × 100万 ＝ 50,000,000 チャット（5000万チャット）

＜例（アラビア数字）＞
- 「1 သန်း」「သန်း 1」
  → 100万チャット
- 「10 သန်း」「သန်း 10」
  → 1000万チャット
- 「50 သန်း」「သန်း 50」
  → 5000万チャット

＜日本語への表記ルール（重要）＞
- 「◯◯ သန်း」などを日本語に訳すときは、金額部分を必ず
  「◯◯00万チャット」または「◯◯百万チャット」の形式で表記すること。
  - 具体的には、
    「N သန်း」→「(N×100)万チャット」＝「N×100万チャット」
    として表記する。
- 例：
  - 「1 သန်း」→「100万チャット」
  - 「10 သန်း」→「1000万チャット」
  - 「50 သန်း」→「5000万チャット」
- 絶対に「N သန်း」を「N万チャット」など、日本語の「万」への短縮で表記してはいけない。
  - 誤りの例：「50 သန်း」→「50万チャット」（※本来は5000万チャット）
- 円併記が必要な場合も、まず正しいチャット建てを計算し、
  その後「約◯◯円」を併記すること。

--------------------------------
■ 3) 「ဘီလီယံ」：10億チャット（10億）の単位
--------------------------------
- ミャンマー語の「ဘီလီယံ」は 1,000,000,000（10億）チャットを表す単位である。
- 「数字 + ဘီလီယံ」「ဘီလီယံ + 数字」のどちらの並びも有効とみなす。
- 数字と「ဘီလီယံ」の間にスペースがない場合も同様に解釈する。

＜解釈ルール＞
- 「◯◯ ဘီလီယံ」「ဘီလီယံ ◯◯」「◯◯ဘီလီယံ」「ဘီလီယံ◯◯」
  → いずれも「◯◯ × 10億チャット」と解釈する。
- 重要：N が 1000 以上の場合は、日本語表記では必ず「兆」が立つ。

＜例（ビルマ数字）＞
- 「ဘီလီယံ ၅」「၅ ဘီလီယံ」「၅ဘီလီယံ」「ဘီလီယံ၅」
  → 5 × 10億 ＝ 50億チャット
- 「ဘီလီယံ ၅၄၀」「၅၄၀ ဘီလီယံ」「၅၄၀ဘီလီယံ」
  → 540 × 10億 ＝ 5,400億チャット
- 「ဘီလီယံ ၁၀၀၀」 → 1兆0億チャット
- 「ဘီလီယံ ၅၃၂၉」 → 5兆3,290億チャット
- 「ဘီလီယံ ၅၉၂၃」 → 5兆9,230億チャット
- 「၁၀၆၈ ဒသမ ၆၆ ဘီလီယံ」 → 1兆686億6000万チャット

＜例（アラビア数字）＞
- 「ဘီလီယံ 5」「5 ဘီလီယံ」「5ဘီလီယံ」
  → 5 × 10億 ＝ 50億チャット
- 「ဘီလီယံ 540」「540 ဘီလီယံ」「540ဘီလီယံ」
  → 540 × 10億 ＝ 5,400億チャット
- 「ဘီလီယံ 1000」「1000 ဘီလီယံ」「1000ဘီလီယံ」
  → 1000 × 10億 ＝ 1兆0億チャット
- 「ဘီလီယံ 5329」「5329 ဘီလီယံ」「5329ဘီလီယံ」
  → 5329 × 10億 ＝ 5兆3,290億チャット
- 「ဘီလီယံ 5923」「5923 ဘီလီယံ」「5923ဘီလီယံ」
  → 5923 × 10億 ＝ 5兆9,230億チャット
- 「1068.66 ဘီလီယံ」「ဘီလီယံ 1068.66」
  → 1068.66 × 10億 ＝ 1兆686億6000万チャット

＜日本語への表記ルール（重要）＞
- 「◯◯ ဘီလီယံ」などを日本語に訳すときは、金額部分を必ず
  「N × 10億チャット」の形式で表記すること。
- 例：
  - 「ဘီလီယံ ၅」 → 50億チャット
  - 「ဘီလီယံ ၅၄၀」 → 5,400億チャット
  - 「ဘီလီယံ ၁၀၀၀」 → 1兆0億チャット（= 1000 × 10億）
  - 「ဘီလီယံ ၅၃၂၉」 → 5兆3,290億チャット（= 5329 × 10億）
  - 「ဘီလီယံ ၅၉၂၃」 → 5兆9,230億チャット（= 5923 × 10億）
  - 「၁၀၆၈ ဒသမ ၆၆ ဘီလီယံ」 → 1兆686億6000万チャット（= 1068.66 × 10億）
- 絶対に「N ဘီလီယံ」を「N億チャット」などへ短縮してはいけない
  （本来は「N × 10億チャット」である点を必ず保持すること）。
- 円併記が必要な場合も、まず正しいチャット建て（10億 × N）を計算し、
  その後「約◯◯円」を併記すること。

--------------------------------
■ 4) 語尾が付く場合の扱い
--------------------------------
以下のような語尾がついていても、前述のルールで数値部分と単位を認識し、金額を解釈すること。
- လောက်（〜くらい）
- ကျော်（〜超）
- ခန့်（およそ）

＜例（ビルマ数字）＞
- 「သိန်း ၅၀ ကျော်」「၅၀သိန်းလောက်」など
  → まず「၅０ × 10万チャット」として解釈し、その上で「〜超」「〜くらい」などのニュアンスを日本語に反映する。
- 「သန်း ၅０ လောက်」「၅０သန်းခန့်」など
  → まず「၅０ × 100万チャット」として解釈し、その上で「〜くらい」「およそ」などのニュアンスを日本語に反映する。

＜例（アラビア数字）＞
- 「သိန်း 50 ကျော်」「50သိန်းလောက်」など
  → まず「50 × 10万チャット」として解釈し、その上で「〜超」「〜くらい」などのニュアンスを日本語に反映する。
- 「သန်း 50 လောက်」「50သန်းခန့်」など
  → まず「50 × 100万チャット」として解釈し、その上で「〜くらい」「およそ」などのニュアンスを日本語に反映する。
`;

// セルフチェック用ルール
const PROMPT_SELF_CHECK_RULE = `
【出力前のセルフチェック（最重要）】
あなたは翻訳者であると同時に、翻訳ルールの監査者でもあります。
以下の作業を必ず行ってから JSON を出力してください。

1. このプロンプト内に記載されている「すべてのルール」（翻訳ルール + 要約/出力形式ルール）を一つずつ再確認すること。
   ※カテゴリ名の暗記ではなく、実際のルールブロックを上から順に見直すこと（抜け漏れ防止）。

   【必ず確認するルールブロック（この順番）】
   - COMMON_TRANSLATION_RULES（用語統一/中立化/引用/選挙/時制/日付/通貨/金額分解/数詞・単位 など）
   - TITLE_OUTPUT_RULES（タイトル専用ルール：1行・ラベル禁止・表記制約・見出し専用の例外 など）
   - SUMMARY_TASK（要約の構造/見出し/段落/空行/記号禁止/文字数上限 など）
   - regions / region-glossary（用語集の強制訳：地域名などの固定訳があれば必ず優先）
   - 出力形式(JSON)のルール（JSON以外を出さない、キー名固定、余計な注釈やコードブロック禁止 など）

   【参考：翻訳ルールのカテゴリ（COMMON_TRANSLATION_RULES 内）】
     用語統一ルール、
     ミャンマー情勢に関する用語置き換えルール、
     特殊訳語・文脈依存訳語ルール、
     政党名・武装組織名の訳語ルール（USDP / BGF / DKBA / ピューソーティー等）、
     人物名・肩書きルール、
     団体名ルール、
     中立的記述ルール（レッテル除去）、
     引用・発言・スローガン保持ルール、
     選挙に関する特例ルール、
     時制表現の禁止ルール、
     日付の扱いルール、
     日付の年の自動補完を禁止、
     通貨換算ルール、
     金額分解ルール、
     ミャンマー語の数詞・単位ルール（သိန်း／သန်း／ဘီလီယံ 等）
   ）
   を一つずつ再確認すること

2. あなたが生成した翻訳結果が、上記「すべてのルール」に完全に従っているか自己点検すること。

3. ルールに違反している箇所が1つでも存在した場合は、その部分を必ず修正してから出力すること。

4. 迷う箇所がある場合は、
   「ルールにより厳密に従う側」へ必ず寄せること。

5. JSON 出力前に必ず
   「すべてのルールを遵守しているか」を最終確認してから応答を確定すること。

上記のセルフチェックを経るまでは、翻訳結果を出力してはならない。
`;

// タイトルの出力ルール（TITLE_OUTPUT_RULES 相当）
const TITLE_OUTPUT_RULES = `
出力は見出し文だけを1行で返してください。
【翻訳】や【日本語見出し案】、## 翻訳 などのラベル・注釈タグ・見出しは出力しないでください。
文体は だ・である調。必要に応じて体言止めを用いる（乱用は避ける）。

【通貨表記ルール（見出し専用）】
- 見出し内でミャンマー通貨「チャット（Kyat、ကျပ်）」建ての金額が出てきた場合、
  COMMON_TRANSLATION_RULES に書かれている日本円への換算ルールは適用せず、
  日本円への換算・併記は行わないこと。
- 見出しでは「◯◯チャット」のように、チャット建てのみで表記すること。
- 本文翻訳や要約では、COMMON_TRANSLATION_RULES の通貨換算ルールをそのまま適用してよい。
`;

// HEADLINE_PROMPT_1：原題ベースの日本語見出し（A）
const HEADLINE_PROMPT_1 = `
${COMMON_TRANSLATION_RULES}
${TITLE_OUTPUT_RULES}
あなたは報道見出しの専門翻訳者です。
以下の英語/ビルマ語のニュース見出しタイトルを、
自然で簡潔な日本語見出しに翻訳してください。
固有名詞は一般的な日本語表記を優先し、
意訳しすぎず要点を保ち、記号の乱用は避けてください。
【追加要件】
- 直訳ではなく、ニュース見出しとして自然な日本語にする
- 30文字以内で要点を端的に
- 主語・動作を明確に
- 重複語を避ける
- 報道機関の見出し調を模倣する（主語と動作を明確に／冗長や過剰な修飾を削る）
- 「〜と述べた」「〜が行われた」などの曖昧・婉曲表現は避ける
`;

// HEADLINE_PROMPT_3：本文を読んで作る見出し（B/B’）
const HEADLINE_PROMPT_3 = `
${COMMON_TRANSLATION_RULES}
${TITLE_OUTPUT_RULES}
あなたは新聞社の見出しデスクです。
以下の本文（原文／機械翻訳含む可能性あり）を読み、
記事の要点（誰／どこ／何が起きた／規模・数値／結果／時点）を抽出し、
自然で簡潔な日本語の報道見出しを1行で作成してください。

ルール：
- 主語と動作を明確に（曖昧表現や冗長な修飾は削る）
- 重要な固有名詞・数値は優先して残す
- 「〜と述べた」「〜が行われた」等の婉曲表現は避ける
- 事実関係が曖昧な断定は避ける（必要な場合のみ推定語を最小限に使う）
`;

// make_headline_prompt_2_from 相当：案1 から案2を作るプロンプトを生成
function buildHeadlinePrompt2From_(variant1Ja) {
  return `
${COMMON_TRANSLATION_RULES}
${TITLE_OUTPUT_RULES}
以下は先に作成した日本語見出し（案1）です。
【案1】${variant1Ja}

この案1を素材に、次の要件で新しい別案（案2）を1行で出力してください。
・直訳ではなく、ニュース見出しとして自然な日本語にする
・30文字以内で要点を端的に
・主語・動作を明確に
・重複語を避ける
・報道機関の見出し調を模倣する（主語と動作を明確に／冗長や過剰な修飾を削る）
・「〜と述べた」「〜が行われた」などの曖昧・婉曲表現は避ける
`;
}

// 本文要約タスク（Python の STEP3_TASK 相当）
const SUMMARY_TASK = `
Step 3: 翻訳と要約処理
以下のルールに従って、本文を要約してください。

${COMMON_TRANSLATION_RULES}
本文要約：
- 以下の記事本文について重要なポイントをまとめ、最大500字で具体的に要約する（500字を超えない）。
- 自然な日本語で表現する。文体は だ・である調。必要に応じて体言止めを用いる（乱用は避ける）。
- 個別記事の本文のみを対象とし、メディア説明やページ全体の解説は不要です。
- レスポンスでは要約のみを返してください、それ以外の文言は不要です。

【内容の優先順位（重要）】
- まず最初の段落で「いつ・どこで・誰が・何をした・結果・規模」を簡潔にまとめる。
- 決議・声明・非難決議の記事では次の情報を優先する：
  ・何を決議／表明したか（承認・非難・拒否など）
  ・誰を正統な代表として認め、誰の正統性を否定したか
  ・制裁・武器禁輸・支援など、具体的な措置・要求
- 経済協力や投資の記事では：
  ・誰と誰の間で、いくら、どの分野について合意／投資したか
  ・同じ種類の合意や金額が並ぶ場合は、可能な範囲で1文に圧縮する。
- 人権侵害・軍事攻撃・オンライン詐欺などの事件では：
  ・加害主体（国軍・軍事政権・武装組織など）、被害者、人数・被害の規模
  ・場所（州・郡区レベル）と現在の状況（拘束中／送還が滞っている 等）
  を優先し、細かな経緯や枝葉の情報は必要に応じて削る。

【細かい情報の扱い】
- 個別のビル名・支店名・通り名・部屋番号などの詳細な住所情報は、
  事件の特定に不可欠な場合を除き、原則として要約では省略する。
- 同じ趣旨の外交的な一般表現（
  「対話への参加を呼びかけた」「協力強化を強調した」
  「連帯を表明した」など）は、字数制限内で重要度が低いと判断できる場合は
  積極的に削ってよい。
- 国軍や軍事政権に関連する組織について、
  「国軍傘下」「軍事政権傘下」であると本文で説明されている場合は、
  その上下関係を要約に1回だけ明示する。

【主体の書き方】
- 組織声明や会見の場合、報道官や担当者の個人名ではなく、
  可能な限り「KNUは〜」「欧州議会は〜」など組織名を主語にしてまとめる。
- 個人の発言が記事の中心テーマでない場合、
  個人名は1回だけまたは省略してもよい。

本文要約の出力条件：
- 1行目は\`【要約】\`とだけ書いてください。
- 2行目以降が全て空行になってはいけません。

【見出しと構造化のルール】
- 見出しを使う場合は \`[見出し名]\` の形式で出力してください。
- 記事の内容が複数の論点や時系列の段階（例：発生・経過・現在の状況・今後の見通し）に分かれているなど、文量が一定以上ある場合は、なるべく \`[背景]\`\`[現在の状況]\`\`[影響]\` などの見出しを1〜3個程度付けて、構造がひと目で分かるようにしてください。
- 一方で、本文全体が2〜3文程度で一続きの話題に収まっており、見出しを付ける必要がないと判断できる場合は、見出しは作らなくてもよい。
  ただしこの場合でも、本文は段落に分けること。目安として、2〜3文ごとに1段落とし、段落と段落の間には空行を1行（改行2回）入れること
- 見出しや箇条書きを用いて構造化する場合も、要約全体は最大500字以内に収めてください。
- 見出しや箇条書きにはマークダウン記号（#, *, -）を使わず、単純なテキストだけで書いてください。

【空行ルール（厳守）】
- \`【要約】\` の直後に空行は入れない（すぐに見出しか本文を書く）。
- 見出し \`[見出し名]\` の直後には空行を入れず、次の行に本文を書く。
- その本文ブロック（1段落）が終わったら、必ず空行を1行入れること。
  ※これにより「見出し → 本文 → 空行 → 次の見出し」という形にする。
- 見出し同士を空行なしで連続させてはならない（必ず本文＋空行を挟む）。
- 本文が複数段落になる場合は、段落（複数行）と段落（複数行）の間に空行を1行だけ入れる。
  （例：段落1 → 空行 → 段落2）
- 箇条書き（・）同士の間には空行を入れない。
- 空行を2行以上連続させない（常に1行まで）。
- 上記以外の用途で空行を作らない。

【見出しの書式例（厳守）】
[現状]
本文本文本文…
（空行）
[身代金と送致]
本文本文本文…
（空行）
[住民の抵抗]
本文本文本文…

【その他のルール】
- 箇条書きは \`・\` を使ってください。
- 特殊記号は使わないでください。
- 「【要約】」は冒頭の1回のみ使用してください。
- 思考手順（Step1/2、Q1/Q2、→ など）は出力に含めないでください。
- 要約全体は最大500字以内とし、不要な背景説明や重複表現は削ること。
  特に重要情報（日時／主体／行為／規模／結果）を優先すること。
`;

// 3タスク（見出しA / 見出しB' / 本文要約）を1回で投げるまとめプロンプト
function buildMultiTaskPromptForRow_(params) {
  const {
    titleRaw,
    bodyRaw,
    sourceVal,
    urlVal,
    titleGlossaryRules,
    bodyGlossaryRules,
  } = params;

  return `
以下は1つのニュース記事です。
あなたはこの1記事から、次の3つの結果を同時に生成してください。

[記事タイトル]
${titleRaw || ""}

[記事本文]
${bodyRaw || ""}

====================
[Task1: 見出しA（タイトル翻訳ベース）]
- 記事タイトルをインプットとしてください。
- 次のプロンプトとルールに従って、日本語見出しAを1行で作成してください。
--- HEADLINE_PROMPT_1 ---
${HEADLINE_PROMPT_1}
-------------------------
【タイトル用 用語固定ルール】
${titleGlossaryRules || "(なし)"}

====================
[Task2: 見出しB'（本文を読んで作る見出し）]
- 記事本文をインプットとしてください。
- 次のプロンプトとルールに従って、日本語見出しB'を1行で作成してください。
--- HEADLINE_PROMPT_3 ---
${HEADLINE_PROMPT_3}
---------------------------
【本文用 用語固定ルール】
${bodyGlossaryRules || "(なし)"}
本文を主な根拠としつつ、必要であればタイトルも補助情報として用いて構いません。

====================
[Task3: 本文要約]
- 記事本文をインプットとしてください。
- 次のプロンプトとルールに従って、本文要約を作成してください。
--- SUMMARY_TASK ---
${SUMMARY_TASK}
--------------------
【本文用 用語固定ルール】
${bodyGlossaryRules || "(なし)"}

====================

${PROMPT_SELF_CHECK_RULE}

【最終出力フォーマット（必須）】

3つのタスク結果だけを含む JSON オブジェクトを 1 つだけ出力してください。

{
  "headlineA": "ここにTask1の見出しAを入れる",
  "headlineBPrime": "ここにTask2の見出しB'を入れる",
  "summary": "ここにTask3の本文要約を入れる"
}

制約:
- 上記の JSON オブジェクト以外の文字（解説・ラベル・マークダウンなど）は一切出力しないこと。
- 特に、\`\`\`json 〜 \`\`\` のようなコードブロックで囲まず
  純粋な JSON テキストのみを出力すること。
- JSON 全体としては 1 つのオブジェクトだけを出力すればよい。summary の値の中では、
  「【要約】」「[見出し]」「・箇条書き」などのために改行が必要な場合は、
   JSON文字列を壊さないように **実改行は使わず**、必ず "\\n"（バックスラッシュ+n の2文字）として出力すること。
`;
}

/************************************************************
 * スプレッドシート用語集
 ************************************************************/

// 正規表現用に特殊文字をエスケープ
function escapeRegExp_(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// ==== 用語集（州・管区訳）: A:mm / B:en / C:本文訳 / D:見出し訳 ====
let REGION_GLOSSARY_CACHE = null;

function loadRegionGlossary_() {
  if (REGION_GLOSSARY_CACHE) return REGION_GLOSSARY_CACHE;

  // ★必要に応じて書き換え：同じスプレッドシート内の "regions" シートを想定
  const ss = SpreadsheetApp.getActive();
  const sheetName = "regions"; // Python 側の MNA_REGION_SHEET_NAME と合わせる
  const sh = ss.getSheetByName(sheetName);
  if (!sh) {
    Logger.log("[region-glossary] sheet not found: " + sheetName);
    REGION_GLOSSARY_CACHE = [];
    return REGION_GLOSSARY_CACHE;
  }

  const lastRow = sh.getLastRow();
  if (lastRow < 2) {
    REGION_GLOSSARY_CACHE = [];
    return REGION_GLOSSARY_CACHE;
  }

  const values = sh.getRange(2, 1, lastRow - 1, 4).getValues(); // A:D
  const out = [];
  values.forEach(function (r) {
    const mm = (r[0] || "").toString().trim(); // A
    const en = (r[1] || "").toString().trim(); // B
    const jaBody = (r[2] || "").toString().trim(); // C
    const jaHead = (r[3] || "").toString().trim(); // D
    if (!mm && !en && !jaBody && !jaHead) return;
    const ja = jaHead || jaBody; // 後方互換
    out.push({
      mm: mm,
      en: en,
      ja: ja,
      ja_body: jaBody,
      ja_headline: jaHead,
    });
  });

  Logger.log("[region-glossary] loaded " + out.length + " entries");
  REGION_GLOSSARY_CACHE = out;
  return REGION_GLOSSARY_CACHE;
}

function selectRegionEntriesForText_(text, entries) {
  if (!text || !entries || !entries.length) return [];
  const t = text.toString(); // GAS の文字列

  const picked = [];
  const seen = {};

  entries.forEach(function (e) {
    const mm = e.mm || "";
    const en = e.en || "";
    let hit = false;

    // Myanmar 語は単純な部分文字列マッチ（Python 側とほぼ同等）
    if (mm) {
      if (t.indexOf(mm) !== -1) {
        hit = true;
      }
    }

    // 英語は \bword\b の単語境界で大文字小文字無視
    if (!hit && en) {
      const re = new RegExp("\\b" + escapeRegExp_(en) + "\\b", "i");
      if (re.test(t)) {
        hit = true;
      }
    }

    if (hit) {
      const key = mm + "|" + en;
      if (!seen[key]) {
        seen[key] = true;
        picked.push(e);
      }
    }
  });

  return picked;
}

function buildRegionGlossaryPromptFor_(entries, useHeadlineJa) {
  if (!entries || !entries.length) return "";

  const lines = [];
  entries.forEach(function (e) {
    const mm = e.mm || "";
    const en = e.en || "";
    const ja = useHeadlineJa ? e.ja_headline || e.ja : e.ja_body || e.ja;
    if (!ja) return;

    if (mm && en) {
      lines.push(
        "- 「" +
          mm +
          "」または「" +
          en +
          "」が出たら、必ず「" +
          ja +
          "」と訳す。"
      );
    } else if (mm) {
      lines.push("- 「" + mm + "」が出たら、必ず「" + ja + "」と訳す。");
    } else if (en) {
      lines.push("- 「" + en + "」が出たら、必ず「" + ja + "」と訳す。");
    }
  });

  if (!lines.length) return "";
  return "【用語固定（必須）】\n" + lines.join("\n") + "\n";
}

// タイトル用（D列=見出し訳）
function buildRegionRulesForTitle_(title) {
  const entries = selectRegionEntriesForText_(
    title || "",
    loadRegionGlossary_()
  );
  return buildRegionGlossaryPromptFor_(entries, true);
}

// 本文用（C列=本文訳）
function buildRegionRulesForBody_(body) {
  const entries = selectRegionEntriesForText_(
    body || "",
    loadRegionGlossary_()
  );
  return buildRegionGlossaryPromptFor_(entries, false);
}

/************************************************************
 * 1. メディアごとの API キー切り替え（prod / dev）
 ************************************************************/

// メディア名 → ベースキー名（末尾）のマップ
// 例: "mizzima" → "MIZZIMA"
// prod: GEMINI_API_KEY_MIZZIMA
// dev : GEMINI_API_TEST_KEY_MIZZIMA
const SOURCE_KEY_BASE_MAP = {
  bbc: "BBC",
  "bbc burmese": "BBC",
  mizzima: "MIZZIMA",
  "mizzima burmese": "MIZZIMA",
  "mizzima (burmese)": "MIZZIMA",
  "khit thit": "KHITTHIT",
  "khit thit media": "KHITTHIT",
  "myanmar now": "MYANMARNOW",
  dvb: "DVB",
  irrawaddy: "IRRAWADDY",
  "global new light of myanmar": "GNLM",
  "global new light": "GNLM",
  gnlm: "GNLM",
  "global new light of myanmar (国営紙)": "GNLM",
  "news eleven": "NEWSELEVEN",
  "news eleven burmese": "NEWSELEVEN",
  newseleven: "NEWSELEVEN",
  "popular myanmar": "POPULARMYANMAR",
  "popular myanmar (国軍系メディア)": "POPULARMYANMAR",
  "frontier myanmar": "FRONTIERMYANMAR",
  frontier: "FRONTIERMYANMAR",
};

const DEFAULT_BASE_KEY = "MIZZIMA"; // マップにない場合のフォールバック

// シート名ごとのプレフィックス
const SHEET_KEY_PREFIX_MAP = {
  prod: "GEMINI_API_KEY_", // 例: GEMINI_API_KEY_MIZZIMA
  dev: "GEMINI_API_TEST_KEY_", // 例: GEMINI_API_TEST_KEY_MIZZIMA
};

const DEFAULT_PREFIX = "GEMINI_API_KEY_"; // prod/dev以外のシート用

function normalizeSourceName_(s) {
  if (!s) return "";
  let out = s.toString().trim();
  try {
    out = out.normalize("NFKC");
  } catch (e) {}
  out = out.replace(/\s+/g, " ");
  return out.toLowerCase();
}

/************************************************************
 * APIキー ローテーション（メディア別） + 日次カウンタ（Pacific）
 ************************************************************/

// Gemini の日次リセットに合わせる（Pacific日付）
function _todayKeyStrPacific_() {
  return Utilities.formatDate(new Date(), "America/Los_Angeles", "yyyyMMdd");
}

// 1日単位の使用回数カウンタ（Script Properties）
// propName例: "GEMINI_API_KEY_MIZZIMA"
// counterKey例: "GEMINI_REQCOUNT_GEMINI_API_KEY_MIZZIMA_20251212"（Pacific日付）
function _counterKeyForApiProp_(apiPropName) {
  return "GEMINI_REQCOUNT_" + apiPropName + "_" + _todayKeyStrPacific_();
}

function _getReqCountToday_(apiPropName) {
  const props = PropertiesService.getScriptProperties();
  const k = _counterKeyForApiProp_(apiPropName);
  return Number(props.getProperty(k) || "0");
}

function _incReqCountToday_(apiPropName) {
  const props = PropertiesService.getScriptProperties();
  const k = _counterKeyForApiProp_(apiPropName);
  const cur = Number(props.getProperty(k) || "0");
  props.setProperty(k, String(cur + 1));
  return cur + 1;
}

/**
 * メディアごとのローテ設定
 * - キーの切替をしたいメディアだけ書く（それ以外は従来どおり単一キー）
 * - baseKeys は「末尾（baseKey）」の配列（prefixはprod/devで自動）
 *   例: prodなら "GEMINI_API_KEY_" + "MIZZIMA2" → Script Propertiesに GEMINI_API_KEY_MIZZIMA2 を用意
 */
const API_KEY_ROTATION_RULES = {
  "khit thit": { baseKeys: ["KHITTHIT", "KHITTHIT2"], maxReqPerDayPerKey: 240 },
  "khit thit media": {
    baseKeys: ["KHITTHIT", "KHITTHIT2"],
    maxReqPerDayPerKey: 240,
  },
  khitthit: { baseKeys: ["KHITTHIT", "KHITTHIT2"], maxReqPerDayPerKey: 240 }, // 念のため残してOK

  dvb: { baseKeys: ["DVB", "DVB2"], maxReqPerDayPerKey: 240 },

  gnlm: { baseKeys: ["GNLM", "GNLM2"], maxReqPerDayPerKey: 240 },
  "global new light of myanmar": {
    baseKeys: ["GNLM", "GNLM2"],
    maxReqPerDayPerKey: 240,
  }, // メディア表記ゆれ対策（任意）

  "global new light of myanmar (国営紙)": {
    baseKeys: ["GNLM", "GNLM2"],
    maxReqPerDayPerKey: 240,
  },

  "popular myanmar": {
    baseKeys: ["POPULARMYANMAR", "POPULARMYANMAR2"],
    maxReqPerDayPerKey: 240,
  },
  "popular myanmar (国軍系メディア)": {
    baseKeys: ["POPULARMYANMAR", "POPULARMYANMAR2"],
    maxReqPerDayPerKey: 240,
  },
  popularmyanmar: {
    baseKeys: ["POPULARMYANMAR", "POPULARMYANMAR2"],
    maxReqPerDayPerKey: 240,
  }, // 念のため残してOK
};

function _pickApiKeyPropNameWithRotation_(sheetName, sourceRaw) {
  const prefix = SHEET_KEY_PREFIX_MAP[sheetName] || DEFAULT_PREFIX;
  const norm = normalizeSourceName_(sourceRaw || "");

  // ローテ設定が無いメディアは従来通り
  const baseKeyDefault = SOURCE_KEY_BASE_MAP[norm] || DEFAULT_BASE_KEY;
  const rule = API_KEY_ROTATION_RULES[norm];

  if (!rule || !rule.baseKeys || rule.baseKeys.length === 0) {
    return prefix + baseKeyDefault;
  }

  const maxPerKey = Number(rule.maxReqPerDayPerKey || 0);

  // maxPerKey が 0 の場合は「常に先頭キー」を使う（設定ミスでも安全側）
  if (maxPerKey <= 0) {
    return prefix + rule.baseKeys[0];
  }

  // まだ上限未満のキーを優先して選ぶ
  for (let i = 0; i < rule.baseKeys.length; i++) {
    const apiPropName = prefix + rule.baseKeys[i];
    const used = _getReqCountToday_(apiPropName);
    if (used < maxPerKey) {
      return apiPropName;
    }
  }

  // 全部上限に達したら最後のキーを返す（エラーは呼び出し側で起きる想定）
  return prefix + rule.baseKeys[rule.baseKeys.length - 1];
}

// シート名 & メディア名から API キーを取得
function getApiKeyFromSheetAndSource_(sheetName, sourceRaw, usageTagOpt) {
  const scriptProps = PropertiesService.getScriptProperties();

  // ★ ローテ考慮して「どの Script Property 名を使うか」を決める（Gemini専用）
  const propName = _pickApiKeyPropNameWithRotation_(sheetName, sourceRaw);
  const apiKey = scriptProps.getProperty(propName);

  // ★ 使用回数を1加算（Pacific日付で日次リセット）
  const newCount = _incReqCountToday_(propName);

  // ログ（値そのものは出さない）
  const tag = usageTagOpt || sheetName || "unknown";
  const msg =
    "use apiKeyProp=" +
    propName +
    " reqCountToday=" +
    newCount +
    " (sheet=" +
    sheetName +
    ", sourceRaw=" +
    sourceRaw +
    ", norm=" +
    normalizeSourceName_(sourceRaw || "") +
    ")";

  Logger.log("[gemini-key] " + msg);
  _appendGeminiLog_("INFO", tag, msg);

  return apiKey || null;
}

function getOpenAiApiKey_(usageTagOpt) {
  const apiKey =
    PropertiesService.getScriptProperties().getProperty(OPENAI_API_KEY_PROP) ||
    "";
  const tag = usageTagOpt || "openai";
  if (!apiKey) {
    const msg = "OPENAI_API_KEY is missing in Script Properties";
    Logger.log("[openai-key] " + msg);
    _appendGeminiLog_("ERROR", tag, msg);
    return null;
  }
  Logger.log(
    "[openai-key] use apiKeyProp=" + OPENAI_API_KEY_PROP + " (tag=" + tag + ")"
  );
  _appendGeminiLog_("INFO", tag, "use apiKeyProp=" + OPENAI_API_KEY_PROP);
  return apiKey;
}

/************************************************************
 * Gemini 共通設定（リトライ＆ログ）
 ************************************************************/

// リトライ設定（少し控えめに）
const GEMINI_JS_MAX_RETRIES = 1; // 2 → 1
const GEMINI_JS_BASE_DELAY_SEC = 8; // 5 → 8
const GEMINI_JS_MAX_DELAY_SEC = 90; // 60 → 90

// OpenAI(gpt-5-mini) 用リトライ設定
// - 「リトライの条件」は Gemini と同じ判定ロジックに近い判定を使う
// - 「回数」は最大2回（= 初回 + 2リトライの合計3回まで）
const GPT5_MINI_MODEL = "gpt-5-mini";
const OPENAI_API_KEY_PROP = "OPENAI_API_KEY";
const GPT_JS_MAX_RETRIES = 2; // 追加リトライ回数（最大2回）

// 乱数ジッター付き指数バックオフ: attempt=0,1,2,... → 待機ミリ秒
function _expBackoffMs_(attempt) {
  const baseMs = GEMINI_JS_BASE_DELAY_SEC * 1000;
  const maxMs = GEMINI_JS_MAX_DELAY_SEC * 1000;
  let delay = Math.min(maxMs, Math.pow(2, attempt) * baseMs); // 2^attempt * base
  delay += Math.floor(Math.random() * 1000); // 0〜999ms のジッター
  return delay;
}

// Gemini RESTレスポンスから usage を取り出す（snake/camel両対応）
function _usageFromData_(data) {
  if (!data) return null;
  const usage = data.usageMetadata || data.usage_metadata;
  if (!usage) return null;

  function _get(obj, key, fallback) {
    return obj && key in obj ? obj[key] : fallback;
  }

  // prompt
  const promptTokens =
    _get(usage, "prompt_token_count", null) ??
    _get(usage, "promptTokenCount", null) ??
    _get(usage, "input_token_count", null) ??
    _get(usage, "inputTokenCount", null) ??
    _get(usage, "input_tokens", null) ??
    _get(usage, "inputTokens", null) ??
    0;

  // candidates/output
  const candTokens =
    _get(usage, "candidates_token_count", null) ??
    _get(usage, "candidatesTokenCount", null) ??
    _get(usage, "output_token_count", null) ??
    _get(usage, "outputTokenCount", null) ??
    _get(usage, "output_tokens", null) ??
    _get(usage, "outputTokens", null) ??
    0;

  // total
  const totalTokens =
    _get(usage, "total_token_count", null) ??
    _get(usage, "totalTokenCount", null) ??
    _get(usage, "total_tokens", null) ??
    _get(usage, "totalTokens", null) ??
    Number(promptTokens) + Number(candTokens);

  // cache (存在しないことも多いので0でOK)
  const cacheCreate =
    _get(usage, "cache_creation_input_token_count", null) ??
    _get(usage, "cacheCreationInputTokenCount", null) ??
    0;

  const cacheRead =
    _get(usage, "cache_read_input_token_count", null) ??
    _get(usage, "cacheReadInputTokenCount", null) ??
    0;

  return {
    prompt_token_count: Number(promptTokens) || 0,
    candidates_token_count: Number(candTokens) || 0,
    total_token_count: Number(totalTokens) || 0,
    cache_creation_input_token_count: Number(cacheCreate) || 0,
    cache_read_input_token_count: Number(cacheRead) || 0,
  };
}

// usage ログ（標準出力＝Apps Script 実行ログ）
function _logGeminiUsage_(data, usageTag, model) {
  const u = _usageFromData_(data);
  if (!u) return;
  const tag = usageTag || "gen";
  const m = model || "gemini-2.5-flash";
  Logger.log(
    "📊 TOKENS[%s] in=%s out=%s total=%s (cache create/read=%s/%s) model=%s",
    tag,
    u.prompt_token_count,
    u.candidates_token_count,
    u.total_token_count,
    u.cache_creation_input_token_count,
    u.cache_read_input_token_count,
    m
  );
}

// Free tier の「generate_content_free_tier_requests」系 429 を判定
function _isFreeTierQuotaErrorData_(data) {
  try {
    const err = data && data.error;
    if (!err) return false;
    const msg = (err.message || "").toString();
    return msg.indexOf("generate_content_free_tier_requests") !== -1;
  } catch (e) {
    return false;
  }
}

// 503/429 などリトライ対象かどうか判定（HTTPコード + エラー内容から）
function _isRetriableError_(httpCode, data) {
  const err = data && data.error;
  const status = err && err.status ? String(err.status) : "";
  const msg = err && err.message ? String(err.message) : "";

  if (httpCode === 503 || httpCode === 500) return true;
  if (httpCode === 429) return true;

  const lower = (status + " " + msg).toLowerCase();
  const hints = [
    "unavailable",
    "resource_exhausted",
    "timeout",
    "temporar",
    "overload",
    "server error",
    "internal",
  ];
  return hints.some(function (h) {
    return lower.indexOf(h) !== -1;
  });
}

// ===== グローバルスロットリング（Script Properties共有）=====
// どのトリガー/実行経路でも、Gemini呼び出しを最低この間隔だけ空ける
const GEMINI_GLOBAL_MIN_INTERVAL_MS = 20000; // 20秒（安全策）

const _GEMINI_LAST_CALL_PROP = "GEMINI_LAST_CALL_MS";

// 全実行（別トリガー含む）で共通の最小間隔を保証する
function _throttleGeminiCallGlobal_() {
  const props = PropertiesService.getScriptProperties();

  // 直列化のために短時間ロック（Properties更新競合を避ける）
  const lock = LockService.getScriptLock();
  lock.waitLock(30 * 1000);

  try {
    const now = Date.now();
    const last = Number(props.getProperty(_GEMINI_LAST_CALL_PROP) || "0");
    const waitMs = last
      ? Math.max(0, GEMINI_GLOBAL_MIN_INTERVAL_MS - (now - last))
      : 0;

    if (waitMs > 0) Utilities.sleep(waitMs);

    // 「次の人」がここから計算できるよう、呼び出し直前の時刻を記録
    props.setProperty(_GEMINI_LAST_CALL_PROP, String(Date.now()));
  } finally {
    lock.releaseLock();
  }
}

/************************************************************
 * Gemini 呼び出しログ用シート出力
 ************************************************************/

const GEMINI_LOG_SHEET_NAME_PROD = "gemini_logs_prod";
const GEMINI_LOG_SHEET_NAME_DEV = "gemini_logs_dev";

// usageTag からどのログシートに書くか判定する
// 例: "prod#row5:E(...)" → "gemini_logs_prod"
//     "dev#row10:I(...)" → "gemini_logs_dev"
function _getLogSheetNameForTag_(tag) {
  if (!tag) return null;
  const s = String(tag);
  const sharpIndex = s.indexOf("#");
  const head = sharpIndex >= 0 ? s.substring(0, sharpIndex) : s;

  if (head === "prod") return GEMINI_LOG_SHEET_NAME_PROD;
  if (head === "dev") return GEMINI_LOG_SHEET_NAME_DEV;

  // prod/dev 以外（manualInit など）はログを残さない
  return null;
}

// 実際にログシートに1行追加する
function _appendGeminiLog_(level, tag, message) {
  try {
    const logSheetName = _getLogSheetNameForTag_(tag);
    if (!logSheetName) {
      // prod/dev 以外のタグは無視
      return;
    }

    const ss = SpreadsheetApp.getActive();
    let sh = ss.getSheetByName(logSheetName);

    // 初回のみシート作成
    if (!sh) {
      sh = ss.insertSheet(logSheetName);
    }

    sh.appendRow([new Date(), level || "", tag || "", message || ""]);
  } catch (e) {
    // ログ書き込み失敗は本体処理に影響させない
    Logger.log("[gemini-log] failed to append log: " + e);
  }
}

/************************************************************
 * 2. Gemini 呼び出し共通
 ************************************************************/

function callGeminiWithKey_(apiKey, prompt, usageTagOpt) {
  if (!apiKey) {
    Logger.log("[gemini] ERROR: API key not set");
    return "ERROR: API key not set";
  }

  const usageTag = usageTagOpt || "generic";
  const model = "gemini-2.5-flash";
  const url =
    "https://generativelanguage.googleapis.com/v1beta/models/" +
    model +
    ":generateContent?key=" +
    encodeURIComponent(apiKey);

  const payload = {
    contents: [
      {
        parts: [{ text: prompt }],
      },
    ],
    generationConfig: {
      response_mime_type: "application/json",
      temperature: 0.1,
      topP: 0.8,
      topK: 20,
    },
  };

  const options = {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payload),
    muteHttpExceptions: true, // 非200もレスポンスを返させる
  };

  let lastErrorText = "";

  for (let attempt = 0; attempt < GEMINI_JS_MAX_RETRIES; attempt++) {
    const promptChars = (prompt || "").length;
    // processRowsBatch() の推定と同じロジックに統一
    const estPromptTokens = estimateTokensFromChars_(promptChars);
    Logger.log(
      "[gemini-call] try %s/%s tag=%s model=%s prompt_chars=%s est_prompt_tokens=%s",
      attempt + 1,
      GEMINI_JS_MAX_RETRIES,
      usageTag,
      model,
      promptChars,
      estPromptTokens
    );

    _appendGeminiLog_(
      "INFO",
      usageTag,
      "try " +
        (attempt + 1) +
        "/" +
        GEMINI_JS_MAX_RETRIES +
        " model=" +
        model +
        " prompt_chars=" +
        (prompt || "").length
    );

    let res;
    try {
      _throttleGeminiCallGlobal_();
      res = UrlFetchApp.fetch(url, options);
    } catch (e) {
      // ネットワーク例外など
      lastErrorText = (e && e.toString()) || "fetch error";
      Logger.log("[gemini] fetch exception: %s", lastErrorText);

      _appendGeminiLog_("ERROR", usageTag, "fetch exception: " + lastErrorText);

      if (attempt === GEMINI_JS_MAX_RETRIES - 1) {
        return "ERROR: " + lastErrorText;
      }

      const sleepMs = _expBackoffMs_(attempt);
      Logger.log(
        "[gemini] retry after %sms (fetch exception, attempt=%s)",
        sleepMs,
        attempt + 1
      );

      _appendGeminiLog_(
        "WARN",
        usageTag,
        "retry after " +
          sleepMs +
          "ms (fetch exception, attempt=" +
          (attempt + 1) +
          ")"
      );

      Utilities.sleep(sleepMs);
      continue;
    }

    const code = res.getResponseCode();
    const text = res.getContentText();

    let data;
    try {
      data = JSON.parse(text);
    } catch (e) {
      lastErrorText = "invalid JSON: " + text.substring(0, 500);
      Logger.log("[gemini] invalid JSON (code=%s): %s", code, lastErrorText);

      if (attempt === GEMINI_JS_MAX_RETRIES - 1) {
        return "ERROR: " + lastErrorText;
      }

      const sleepMs = _expBackoffMs_(attempt);
      Utilities.sleep(sleepMs);
      continue;
    }

    // Free tier の日次上限エラーは即諦める
    if (_isFreeTierQuotaErrorData_(data)) {
      const errMsg =
        (data.error && data.error.message) ||
        "free tier quota exceeded (generate_content_free_tier_requests)";
      Logger.log("🚫 [gemini] free tier quota exceeded: %s", errMsg);

      _appendGeminiLog_(
        "ERROR",
        usageTag,
        "free tier quota exceeded: " + errMsg
      );

      return "ERROR: " + errMsg;
    }

    // 2xx かつ error 無し → 成功とみなす
    if (code >= 200 && code < 300 && !(data && data.error)) {
      try {
        // usage ログ
        _logGeminiUsage_(data, usageTag, model);
      } catch (e) {
        // usage ログ失敗は致命的ではないので無視
      }

      let out = "";
      try {
        out =
          (((data.candidates || [])[0] || {}).content.parts || [])[0].text ||
          "";
      } catch (e) {
        out = "";
      }
      out = (out || "").trim();

      Logger.log(
        "[gemini] success tag=%s model=%s len(resp)=%s",
        usageTag,
        model,
        out.length
      );

      // SUCCESSログに実トークンも付与（usageMetadata が無い場合は空）
      let tok = "";
      try {
        const u = _usageFromData_(data);
        if (u) {
          tok =
            " tokens(in=" +
            u.prompt_token_count +
            " out=" +
            u.candidates_token_count +
            " total=" +
            u.total_token_count +
            " cache=" +
            u.cache_creation_input_token_count +
            "/" +
            u.cache_read_input_token_count +
            ")";
        }
      } catch (e) {
        tok = "";
      }

      _appendGeminiLog_(
        "SUCCESS",
        usageTag,
        "success model=" + model + " len(resp)=" + out.length + tok
      );

      return out;
    }

    // error オブジェクトがあれば詳細ログ
    if (data && data.error) {
      const err = data.error;
      const status = String(err.status || "");
      const message = String(err.message || "");

      Logger.log(
        "[gemini] HTTP %s error status=%s message=%s",
        code,
        status,
        message
      );
      lastErrorText = message || "HTTP " + code;

      _appendGeminiLog_(
        "WARN",
        usageTag,
        "HTTP " + code + " error status=" + status + " message=" + message
      );
    } else {
      Logger.log("[gemini] HTTP %s unexpected response body: %s", code, text);
      lastErrorText = "HTTP " + code;

      _appendGeminiLog_(
        "WARN",
        usageTag,
        "HTTP " + code + " unexpected response: " + text.substring(0, 200)
      );
    }

    // リトライ対象か判定
    const retriable = _isRetriableError_(code, data);
    if (!retriable || attempt === GEMINI_JS_MAX_RETRIES - 1) {
      Logger.log(
        "[gemini] give up (retriable=%s): %s",
        retriable,
        lastErrorText
      );

      _appendGeminiLog_(
        "ERROR",
        usageTag,
        "give up (retriable=" + retriable + "): " + lastErrorText
      );

      return "ERROR: " + lastErrorText;
    }

    const sleepMs = _expBackoffMs_(attempt);
    Logger.log(
      "⚠️ [gemini] retry %s/%s after %sms (HTTP %s)",
      attempt + 1,
      GEMINI_JS_MAX_RETRIES,
      sleepMs,
      code
    );

    _appendGeminiLog_(
      "WARN",
      usageTag,
      "retry " +
        (attempt + 1) +
        "/" +
        GEMINI_JS_MAX_RETRIES +
        " after " +
        sleepMs +
        "ms (HTTP " +
        code +
        ")"
    );

    Utilities.sleep(sleepMs);
  }

  // ここまで来ることはほぼ無い想定
  return "ERROR: " + (lastErrorText || "Gemini call failed");
}

// ============================================================
// OpenAI Responses API (gpt-5-mini)
// ============================================================
function _extractOutputTextFromResponses_(data) {
  if (!data) return "";

  // SDK 互換の output_text が返る場合
  if (typeof data.output_text === "string" && data.output_text.trim()) {
    return data.output_text;
  }

  const out = data.output;
  if (!Array.isArray(out)) return "";

  const parts = [];
  out.forEach(function (item) {
    if (!item) return;
    if (item.type !== "message") return;
    const content = item.content;
    if (!Array.isArray(content)) return;
    content.forEach(function (c) {
      if (!c) return;
      if (c.type === "output_text" && typeof c.text === "string") {
        parts.push(c.text);
      }
    });
  });
  return parts.join("");
}

function _isRetriableOpenAIError_(httpCode, data, rawText) {
  // OpenAI 側の代表的な一時エラー
  if (httpCode === 502 || httpCode === 504) return true;

  // 既存の Gemini 判定ロジックに寄せるため、形を合わせて _isRetriableError_ を再利用
  let status = "";
  let msg = "";
  try {
    const err = data && (data.error || (data.error && data.error.error));
    if (err) {
      status = String(err.status || err.type || err.code || "");
      msg = String(err.message || "");
    }
  } catch (e) {
    // ignore
  }
  if (!msg) msg = String(rawText || "");
  const mapped = { error: { status: status, message: msg } };
  return _isRetriableError_(httpCode, mapped);
}

// multi2（2件まとめ）の Structured Outputs 用スキーマ
// ★ json_schema は最上位が object 必須なので、items 配列を object で包む
const GPT_MULTI2_WRAPPED_SCHEMA_ = {
  type: "object",
  additionalProperties: false,
  properties: {
    items: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          id: { type: "string" },
          headlineA: { type: "string" },
          headlineBPrime: { type: "string" },
          summary: { type: "string" },
        },
        required: ["id", "headlineA", "headlineBPrime", "summary"],
      },
    },
  },
  required: ["items"],
};

// formatTypeOpt:
//   - "json_object" (default): 単体行のオブジェクト出力向け
//   - "json_schema_batch": multi2（2件まとめ）で配列を厳密に返させたいとき
//   - "none": text.format を付けずに呼ぶ（最終手段）
function callGpt5MiniWithKey_(apiKey, promptText, usageTagOpt, formatTypeOpt) {
  if (!apiKey) {
    return "ERROR: missing " + OPENAI_API_KEY_PROP;
  }

  const url = "https://api.openai.com/v1/responses";
  const fmt = String(formatTypeOpt || "json_object");
  const payload = {
    model: GPT5_MINI_MODEL,
    input: String(promptText || ""),
  };
  if (fmt !== "none") {
    if (fmt === "json_schema_batch") {
      // Structured Outputs: JSON Schema を強制（配列を返せる）
      // docs: text.format に type:"json_schema", strict:true, schema:... を指定 :contentReference[oaicite:2]{index=2}
      payload.text = {
        format: {
          type: "json_schema",
          name: "multi2_array",
          strict: true,
          schema: GPT_MULTI2_WRAPPED_SCHEMA_,
        },
      };
    } else {
      payload.text = { format: { type: fmt } };
    }
  }

  const options = {
    method: "post",
    contentType: "application/json",
    muteHttpExceptions: true,
    headers: {
      Authorization: "Bearer " + apiKey,
    },
    payload: JSON.stringify(payload),
  };

  for (let attempt = 0; attempt <= GPT_JS_MAX_RETRIES; attempt++) {
    try {
      const res = UrlFetchApp.fetch(url, options);
      const code = res.getResponseCode();
      const bodyText = res.getContentText() || "";

      let data = null;
      try {
        data = bodyText ? JSON.parse(bodyText) : null;
      } catch (e) {
        data = null;
      }

      if (code >= 200 && code < 300) {
        const outText = _extractOutputTextFromResponses_(data);
        if (outText) return outText;
        // 200 なのに本文が取れない場合も一旦エラー扱い
        const msg = "ERROR: OpenAI response has no output_text";
        if (attempt < GPT_JS_MAX_RETRIES) {
          Utilities.sleep(_expBackoffMs_(attempt));
          continue;
        }
        return msg;
      }

      // 非2xx
      const retriable = _isRetriableOpenAIError_(code, data, bodyText);
      const shortBody = bodyText ? bodyText.slice(0, 300) : "";
      const errMsg = "ERROR: OpenAI HTTP " + code + " " + shortBody;

      if (retriable && attempt < GPT_JS_MAX_RETRIES) {
        Utilities.sleep(_expBackoffMs_(attempt));
        continue;
      }
      return errMsg;
    } catch (e) {
      const err = "ERROR: OpenAI fetch exception: " + e;
      if (attempt < GPT_JS_MAX_RETRIES) {
        Utilities.sleep(_expBackoffMs_(attempt));
        continue;
      }
      return err;
    }
  }

  return "ERROR: OpenAI retries exhausted";
}

/************************************************************
 * 3. 1行分の処理
 *
 * 入力:
 *   - C列: メディア
 *   - M列: タイトル原文
 *   - N列: 本文原文
 *   - J列: URL（あれば）
 *
 * 出力:
 *   - E列: HEADLINE_PROMPT_1(タイトル)         → 見出しA
 *   - F列: make_headline_prompt_2_from(E)       → 見出しA'
 *   - G列: HEADLINE_PROMPT_3(本文のみ)         → 見出しB'
 *   - I列: 本文要約（STEP3_TASK）
 ************************************************************/

// JSON内に "\\n" として入ってきた改行表現を、表示用に実改行へ戻す
function decodeJsonNewlines_(s) {
  const t = String(s == null ? "" : s);
  // まず \\r\\n を優先して \n に寄せる
  return t.replace(/\\r\\n/g, "\n").replace(/\\n/g, "\n");
}

// 「【要約】」直後に改行が無い場合のみ、改行を1つ補う
function normalizeSummaryHeader_(summary) {
  let s = String(summary || "");

  // 先頭が「【要約】」で始まり、直後が改行でない場合のみ処理
  // ^【要約】(?!\n)
  s = s.replace(/^【要約】(?!\n)[ \t\u3000]*/, "【要約】\n");

  return s;
}

function processRow_(sheet, row, prevStatus) {
  const colC = 3; // メディア
  const colM = 13; // タイトル原文
  const colN = 14; // 本文原文

  // gpt-5-mini 側のリトライ上限（GPTNG(2) 以上は打ち切り）
  const useGpt = shouldUseGpt5Mini_(prevStatus || "");
  const gptRetryCount = parseGptRetryCount_(prevStatus || "");
  if (useGpt && gptRetryCount >= GPT_JS_MAX_RETRIES) {
    Logger.log(
      "[processRow_] skip row %s (gptRetryCount=%s >= %s)",
      row,
      gptRetryCount,
      GPT_JS_MAX_RETRIES
    );
    return;
  }

  const colE = 5; // 見出しA
  const colF = 6; // 見出しA'
  const colG = 7; // 見出しB'（本文のみ）
  const colI = 9; // 本文要約（STEP3_TASK）
  const colJ = 10; // URL（任意）

  const sourceVal = sheet.getRange(row, colC).getValue();
  const titleRaw = sheet.getRange(row, colM).getValue();
  const bodyRaw = sheet.getRange(row, colN).getValue();
  const urlVal = sheet.getRange(row, colJ).getValue();

  if (!titleRaw && !bodyRaw) {
    // 何もなければ何もしない
    return;
  }

  // === ここで行ごとの用語固定ルールを作る ===
  // タイトルに出た語 → D列（見出し訳）を採用
  const regionRulesTitle = buildRegionRulesForTitle_(titleRaw || "");
  // 本文に出た語 → C列（本文訳）を採用
  const regionRulesBody = buildRegionRulesForBody_(bodyRaw || "");

  // タイトル翻訳用（見出しA/A'）
  const titleGlossaryRules = regionRulesTitle;

  // 本文を読む見出し・要約用（タイトル＋本文両方を対象）
  const bodyGlossaryRules = regionRulesTitle + regionRulesBody;

  const sheetName = sheet.getName();

  /********************************************
   * E / G / I を 1回の Gemini 呼び出しでまとめて生成
   ********************************************/
  let headlineA = "";
  let headlineB2 = "";
  let summaryJa = "";

  if (titleRaw || bodyRaw) {
    const multiParams = {
      titleRaw: titleRaw || "",
      bodyRaw: bodyRaw || "",
      sourceVal: sourceVal || "",
      urlVal: urlVal || "",
      titleGlossaryRules: titleGlossaryRules || "",
      bodyGlossaryRules: bodyGlossaryRules || "",
    };

    const multiPrompt = buildMultiTaskPromptForRow_(multiParams);
    const tagMulti = sheetName + "#row" + row + ":EGI(multi)";

    // ★ tagMulti を渡して APIキー名ログも紐付ける
    const propName = useGpt ? "__OPENAI__" : null;
    const apiKey = useGpt
      ? getOpenAiApiKey_(tagMulti)
      : getApiKeyFromSheetAndSource_(sheetName, sourceVal, tagMulti);

    const resp = useGpt
      ? callGpt5MiniWithKey_(apiKey, multiPrompt, tagMulti)
      : callGeminiWithKey_(apiKey, multiPrompt, tagMulti);

    if (typeof resp === "string" && resp.indexOf("ERROR:") === 0) {
      // callGeminiWithKey_ 自体がエラーを返した場合 → そのまま3列とも同じエラー扱い
      headlineA = resp;
      headlineB2 = resp;
      summaryJa = resp;
    } else {
      try {
        let cleaned = (resp || "").trim();

        // もし ``` で始まっていたら、コードブロックを剥がす
        if (cleaned.startsWith("```")) {
          // 先頭の ```json / ``` を削除
          cleaned = cleaned.replace(/^```[a-zA-Z]*\s*/, "");
          // 最後の ``` 以降を削る
          const lastFence = cleaned.lastIndexOf("```");
          if (lastFence !== -1) {
            cleaned = cleaned.substring(0, lastFence);
          }
          cleaned = cleaned.trim();
        }

        const obj = JSON.parse(cleaned);

        headlineA = (obj.headlineA || "").toString().trim();
        headlineB2 = (obj.headlineBPrime || obj.headlineB || "")
          .toString()
          .trim();
        summaryJa = decodeJsonNewlines_((obj.summary || "").toString().trim());
        summaryJa = normalizeSummaryHeader_(summaryJa);

        // ★ まず「チャット以外」の（約◯◯円）を削除（ドル等の誤換算対策）
        summaryJa = removeYenForNonKyat_(summaryJa);
        // ★ 次に「チャット」の（約◯◯円）だけを再計算で矯正
        summaryJa = fixKyatYenInText_(summaryJa);

        headlineA = headlineA || "";
        headlineB2 = headlineB2 || "";
        summaryJa = summaryJa || "";
      } catch (e) {
        const errMsg =
          "ERROR: invalid JSON from Gemini: " + String(resp).substring(0, 200);
        headlineA = errMsg;
        headlineB2 = errMsg;
        summaryJa = errMsg;
      }
    }
  }

  // ★ここで地域名ログを出す
  logRegionUsageForRow_(sheet, row, {
    sourceVal,
    urlVal,
    titleRaw,
    bodyRaw,
    headlineA,
    headlineB2,
    summaryJa,
  });

  // シートに書き込み
  sheet.getRange(row, colE).setValue(headlineA); // 見出しA
  // F列（見出しA'）は従来どおり一時停止のまま
  sheet.getRange(row, colG).setValue(headlineB2); // 見出しB'
  sheet.getRange(row, colI).setValue(summaryJa); // 本文要約

  /********************************************
   * L列：ステータス判定（詳細エラー + 複数記録）
   ********************************************/
  const colL = 12;

  function isError_(val) {
    return typeof val === "string" && val.indexOf("ERROR:") === 0;
  }

  // タイトルも本文も無い場合は EMPTY
  if (!titleRaw && !bodyRaw) {
    sheet.getRange(row, colL).setValue("EMPTY");
    return;
  }

  const vE = sheet.getRange(row, colE).getValue();
  // F列（案2）は一時停止中のためステータス判定対象から外す
  // const vF = sheet.getRange(row, colF).getValue();
  const vG = sheet.getRange(row, colG).getValue();
  const vI = sheet.getRange(row, colI).getValue();

  const errors = [];

  if (isError_(vE)) {
    errors.push("E=" + String(vE));
  }
  // if (isError_(vF)) {
  //   errors.push("F=" + String(vF));
  // }
  if (isError_(vG)) {
    errors.push("G=" + String(vG));
  }
  if (isError_(vI)) {
    errors.push("I=" + String(vI));
  }

  let statusText = "";

  if (errors.length === 0) {
    statusText = useGpt ? "OK(GPT)" : "OK";
  } else {
    // 呼び出し元から渡された「前回までのステータス」から回数を計算
    const retryKind = useGpt ? "GPTNG" : "NG";
    const prevCount = useGpt
      ? parseGptRetryCount_(prevStatus || "")
      : parseRetryCount_(prevStatus || "");
    const newCount = prevCount + 1;
    statusText = `${retryKind}(${newCount}): ` + errors.join(" / ");
  }

  sheet.getRange(row, colL).setValue(statusText);
}

/************************************************************
 * 4. トリガー
 *   - M列 or N列 が編集されたとき、その行の E〜G を再計算
 ************************************************************/

// prod / dev シート用のログシート内容をクリア
function _clearLogSheetFor_(sheetName) {
  const ss = SpreadsheetApp.getActive();
  let logSheetName = null;

  if (sheetName === "prod") {
    logSheetName = GEMINI_LOG_SHEET_NAME_PROD;
  } else if (sheetName === "dev") {
    logSheetName = GEMINI_LOG_SHEET_NAME_DEV;
  } else {
    return; // 対象外
  }

  const sh = ss.getSheetByName(logSheetName);
  if (!sh) return;

  // ★シート全体の中身をクリア（ヘッダーも残さない）
  sh.clearContents();
  // ※書式も消したければ sh.clear(); に変更
}

// ミャンマー時間 16:00〜翌 1:00 の間だけ true を返す
function isWithinProcessingWindow_() {
  // appsscript.json の timeZone が "Asia/Yangon" になっている前提
  const now = new Date();
  const h = now.getHours(); // 0〜23
  const m = now.getMinutes(); // 0〜59
  const t = h * 60 + m; // その日の 0:00 からの経過分数

  const START = 16 * 60; // 16:00 → 960 分
  const END = 1 * 60; // 01:00 → 60 分

  // 日付をまたぐウィンドウの判定:
  // 16:00〜24:00 か 0:00〜2:00 のどちらかなら OK
  return t >= START || t <= END;
}

// "NG(3): xxxx" のような形式から試行回数を取り出す
function parseRetryCount_(status) {
  if (!status) return 0;
  const m = status.match(/^NG\((\d+)\)/);
  if (!m) return 0;
  return Number(m[1]);
}

function parseGptRetryCount_(status) {
  if (!status) return 0;
  const m = status.match(/^GPTNG\((\d+)\)/);
  if (!m) return 0;
  return Number(m[1]);
}

function shouldUseGpt5Mini_(status) {
  const s = String(status || "");
  if (s.startsWith("RUNNING(GPT)")) return true;
  if (s.startsWith("GPTNG(")) return true;
  const m = s.match(/^NG\((\d+)\)/);
  if (!m) return false;
  return Number(m[1]) >= MAX_RETRY_COUNT; // NG(3) 以上なら gpt-5-mini に切替
}

// ★ 古い RUNNING ステータスを NG(1): timeout に置き換える
function cleanupStaleRunningStatuses_() {
  const ss = SpreadsheetApp.getActive();
  const sheetNames = ["prod", "dev"]; // 対象シート

  sheetNames.forEach(function (sheetName) {
    const sh = ss.getSheetByName(sheetName);
    if (!sh) return;

    const lastRow = sh.getLastRow();
    if (lastRow < 2) return; // データ行なし

    const numRows = lastRow - 1;
    const statusRange = sh.getRange(2, STATUS_COL, numRows, 1); // L列
    const values = statusRange.getValues();

    let changed = false;

    for (let i = 0; i < numRows; i++) {
      const status = (values[i][0] || "").toString();

      // 前回実行で RUNNING のまま残った行とみなす
      if (status.startsWith("RUNNING(GPT)")) {
        values[i][0] = "GPTNG(1): timeout";
        changed = true;
      } else if (status.startsWith("RUNNING")) {
        // 1回目の失敗として扱う
        values[i][0] = "NG(1): timeout";
        changed = true;
      }
    }

    if (changed) {
      statusRange.setValues(values);
      Logger.log(
        "[cleanupStaleRunningStatuses_] sheet=%s cleaned RUNNING rows",
        sheetName
      );
    }
  });
}

// pythonで操作した時にも動く
const MAX_ROWS_PER_RUN = 5; // 1回の実行で処理する最大行数
const STATUS_COL = 12; // L列 (ステータス列の列番号)

// NG の最大試行回数（これ以上失敗したら「打ち切り完了」とみなす）
const MAX_RETRY_COUNT = 2;

// ============================================================
// ★ バッチ化（キー別まとめ投げ）＋推定トークンで 1件/2件自動調整
// ============================================================
const MAX_ROWS_PER_GEMINI_BATCH = 2; // 上限2件

// ★ 2行まとめ可否の判断は「トークン推定のみ」に統一
// CSV分析の実測（1行 ≒ 19k〜23k in_tokens）から、精度優先なら 40k 推奨
const BATCH_MAX_EST_INPUT_TOKENS = 40000;

// ★ 文字数→トークン推定（あなたのログ実測に寄せるなら 1 token ≒ 1.7 chars が近い）
// ※ここは将来ログの実測から調整してOK（例: 1.6〜2.0）
const CHARS_PER_TOKEN_EST = 1.7;

function estimateTokensFromChars_(nChars) {
  return Math.ceil(Number(nChars || 0) / CHARS_PER_TOKEN_EST);
}

// ============================================================
// 通貨換算・金額分解を機械側で固定（円表記の再発防止）
// ============================================================

// 1チャット=0.039円、四捨五入
function kyatToYenInt_(kyatInt) {
  return Math.round(Number(kyatInt) * 0.039);
}

// 例: 21060000000 -> "210億6000万円" / 987654321 -> "9億8765万4321円"
function formatYenJa_(yenInt) {
  let y = Math.round(Number(yenInt) || 0);
  const sign = y < 0 ? "-" : "";
  y = Math.abs(y);

  const T = 1000000000000; // 兆
  const O = 100000000; // 億
  const M = 10000; // 万

  const cho = Math.floor(y / T);
  y = y % T;
  const oku = Math.floor(y / O);
  y = y % O;
  const man = Math.floor(y / M);
  const en = y % M;

  let out = "";
  if (cho) out += cho + "兆";
  if (oku) out += oku + "億";
  if (man) out += man + "万";

  // 兆/億/万がある場合でも、最終的に必ず「円」で終わらせる
  if (cho || oku || man) {
    if (en) out += en + "円";
    else out += "円"; // ★ 端数0でも「円」を付ける
  } else {
    // 1万円未満は "123円" の形
    out = String(en) + "円";
  }

  return sign + (out || "0円");
}

// 全角数字→半角数字（例: "５４００" -> "5400"）
function zenkakuDigitsToAscii_(s) {
  return String(s || "").replace(/[０-９]/g, (ch) =>
    String.fromCharCode(ch.charCodeAt(0) - 0xfee0)
  );
}

// 例: "5400億チャット" / "1兆2345億6789万チャット" -> 整数チャット
function parseJaKyatToInt_(s) {
  const t = String(s || "")
    .replace(/[,，\s]/g, "")
    .replace(/チャット.*/, "");

  let rest = t;
  let total = 0;

  function takeUnit(unitChar, unitValue) {
    const idx = rest.indexOf(unitChar);
    if (idx === -1) return;
    const numStr = rest.slice(0, idx);
    const n = Number(zenkakuDigitsToAscii_(numStr) || 0);
    total += n * unitValue;
    rest = rest.slice(idx + 1);
  }

  // 兆→億→万 の順で機械的に処理
  takeUnit("兆", 1000000000000);
  takeUnit("億", 100000000);
  takeUnit("万", 10000);

  // 残りが数字なら（単位なし）として加算
  if (rest) total += Number(zenkakuDigitsToAscii_(rest) || 0);

  return total;
}

// "◯◯チャット（約◯◯円）" の円側を必ず正しい表記へ矯正
function fixKyatYenInText_(text) {
  const s = String(text || "");
  return s.replace(
    /([0-9０-９,，\s兆億万]+チャット)（約[^）]*円）/g,
    function (_m, kyatPart) {
      const kyatInt = parseJaKyatToInt_(kyatPart);
      const yenInt = kyatToYenInt_(kyatInt);
      const yenJa = formatYenJa_(yenInt);
      return kyatPart + "（約" + yenJa + "）";
    }
  );
}

// チャット以外の通貨（ドル/バーツ等）に誤って付いた「（約◯◯円）」を削除
// 例: 「10億ドル（約390億円）」→「10億ドル」
function removeYenForNonKyat_(text) {
  if (!text) return text;
  let s = String(text);
  // 代表的な「非チャット」通貨ラベル（必要なら増やせます）
  const NON_KYAT_CCY =
    "(?:米ドル|ドル|USD|US\\$|\\$|バーツ|THB|ユーロ|EUR|ポンド|GBP|元|人民元|CNY|ウォン|KRW)";

  // 「（約…円）」の中身は数字/カンマ/兆億万などを許容（“約”の有無も吸収）
  const YEN_PAREN = "（\\s*(?:約)?\\s*[0-9０-９,，兆億万\\.]+(?:円|えん)\\s*）";

  // 1) 「10億ドル（約…円）」のように “金額→通貨→円” の順
  const pat1 = new RegExp(
    "([0-9０-９,，\\s兆億万\\.]+\\s*" + NON_KYAT_CCY + ")\\s*" + YEN_PAREN,
    "g"
  );
  s = s.replace(pat1, "$1");

  // 2) 「USD 1 billion（約…円）」のように “通貨→金額→円” の順（保険）
  const pat2 = new RegExp(
    "(" + NON_KYAT_CCY + "\\s*[0-9０-９,，\\s兆億万\\.]+)\\s*" + YEN_PAREN,
    "g"
  );
  s = s.replace(pat2, "$1");

  return s;
}

function estimateTokensFromText_(s) {
  const t = (s || "").toString();
  // ざっくり 1 token ≒ 4 chars を基準に、少し安全側に +10%
  return Math.ceil((t.length / 4) * 1.1);
}
function estimateSourceTokens_(titleRaw, bodyRaw) {
  return estimateTokensFromText_(titleRaw) + estimateTokensFromText_(bodyRaw);
}
// getApiKeyFromSheetAndSource_ と同じ判定で「Script Properties のキー名」を作る
function _propNameForSheetAndSource_(sheetName, sourceRaw) {
  const prefix = SHEET_KEY_PREFIX_MAP[sheetName] || DEFAULT_PREFIX;
  const norm = normalizeSourceName_(sourceRaw || "");
  const baseKey = SOURCE_KEY_BASE_MAP[norm] || DEFAULT_BASE_KEY;
  return prefix + baseKey; // 例: GEMINI_API_KEY_MIZZIMA
}

// 2件まとめ用：配列(JSON)で返させるプロンプト
function buildMultiTaskPromptForRows_(items) {
  // items: [{id,titleRaw,bodyRaw,titleGlossaryRules,bodyGlossaryRules}]
  const blocks = items
    .map(function (it, idx) {
      return `
====================
[ARTICLE ${idx + 1}]
id: ${it.id}
[記事タイトル]
${it.titleRaw || ""}

[記事本文]
${it.bodyRaw || ""}

--- Task1 見出しAルール ---
${HEADLINE_PROMPT_1}
【タイトル用 用語固定ルール】
${it.titleGlossaryRules || "(なし)"}

--- Task2 見出しB'ルール ---
${HEADLINE_PROMPT_3}
【本文用 用語固定ルール】
${it.bodyGlossaryRules || "(なし)"}
--- Task3 本文要約ルール ---
${SUMMARY_TASK}
【本文用 用語固定ルール】
${it.bodyGlossaryRules || "(なし)"}
`.trim();
    })
    .join("\n\n");

  return `
以下は複数のニュース記事です（最大2件）。
各記事ごとに、次の3つの結果を同時に生成してください：
1) 見出しA（タイトル翻訳ベース）
2) 見出しB'（本文を読んで作る見出し）
3) 本文要約

${PROMPT_SELF_CHECK_RULE}

【最終出力フォーマット（必須）】
入力順のまま、JSON 配列を 1つだけ出力してください（それ以外の文字は一切出力しない）。

[
  {
    "id": "入力の id をそのまま入れる",
    "headlineA": "Task1 の見出しA",
    "headlineBPrime": "Task2 の見出しB'",
    "summary": "Task3 の本文要約"
  }
]

制約:
- 配列の要素数は入力記事数と一致させること
- 各要素の "id" は、各 ARTICLE ブロックにある「id: <value>」の <value> を一字一句そのままコピーすること（連番に作り直さない）
- 例: 入力が id: 6 なら出力は "id": "6"（文字列）とすること
- \`\`\`json などのコードブロック禁止。純粋な JSON テキストのみ

${blocks}
`.trim();
}

// processRow_ の「書き込み＋ステータス更新」部分を共通化（Gemini呼び出しはしない）
function _applyOutputsToRow_(
  sheet,
  row,
  prevStatus,
  ctx,
  headlineA,
  headlineB2,
  summaryJa,
  retryKindOpt
) {
  const colE = 5;
  const colG = 7;
  const colI = 9;
  const colL = 12;

  const titleRaw = ctx.titleRaw;
  const bodyRaw = ctx.bodyRaw;

  // 地域名ログ（既存と同じ）
  logRegionUsageForRow_(sheet, row, {
    sourceVal: ctx.sourceVal,
    urlVal: ctx.urlVal,
    titleRaw: titleRaw,
    bodyRaw: bodyRaw,
    headlineA: headlineA,
    headlineB2: headlineB2,
    summaryJa: summaryJa,
  });

  // ★ まず「チャット以外」の（約◯◯円）を削除（ドル等の誤換算対策）
  summaryJa = removeYenForNonKyat_(summaryJa);
  // ★ 次に「チャット」の（約◯◯円）だけを再計算で矯正
  summaryJa = fixKyatYenInText_(summaryJa);

  // 書き込み
  sheet.getRange(row, colE).setValue(headlineA);
  sheet.getRange(row, colG).setValue(headlineB2);
  sheet.getRange(row, colI).setValue(summaryJa);

  function isError_(val) {
    return typeof val === "string" && val.indexOf("ERROR:") === 0;
  }

  if (!titleRaw && !bodyRaw) {
    sheet.getRange(row, colL).setValue("EMPTY");
    return;
  }

  const vE = sheet.getRange(row, colE).getValue();
  const vG = sheet.getRange(row, colG).getValue();
  const vI = sheet.getRange(row, colI).getValue();

  const errors = [];
  if (isError_(vE)) errors.push("E=" + String(vE));
  if (isError_(vG)) errors.push("G=" + String(vG));
  if (isError_(vI)) errors.push("I=" + String(vI));

  let statusText = "";
  if (errors.length === 0) {
    statusText = retryKindOpt === "GPTNG" ? "OK(GPT)" : "OK";
  } else {
    const retryKind = retryKindOpt || "NG";
    const prevCount =
      retryKind === "GPTNG"
        ? parseGptRetryCount_(prevStatus || "")
        : parseRetryCount_(prevStatus || "");
    const newCount = prevCount + 1;
    statusText = `${retryKind}(${newCount}): ` + errors.join(" / ");
  }
  sheet.getRange(row, colL).setValue(statusText);
}

/************************************************************
 * メール通知用設定
 ************************************************************/

// 送信先アドレス
function getNotifyEmailListForSheet_(sheetName) {
  const props = PropertiesService.getScriptProperties();

  let raw = "";
  if (sheetName === "prod") {
    raw = props.getProperty("NOTIFY_EMAIL_TO_PROD") || "";
  } else if (sheetName === "dev") {
    raw = props.getProperty("NOTIFY_EMAIL_TO_DEV") || "";
  }

  // 空なら空配列を返す
  if (!raw) return [];

  // カンマ区切り → トリム → 空要素除去
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

// ★件名のベース
const NOTIFY_EMAIL_SUBJECT_BASE = "【MNA 記事収集完了】";

// ★ 統合版：この関数だけを時間トリガーで動かす
function processRowsBatch() {
  const lock = LockService.getDocumentLock();
  try {
    // 同時実行防止
    lock.waitLock(30 * 1000);

    // ★ ここで「前回の RUNNING」を NG に戻す
    cleanupStaleRunningStatuses_();

    // ★ 時間帯外なら即スキップ（16:00〜翌1:00だけ動かす）
    if (!isWithinProcessingWindow_()) {
      Logger.log("[processRowsBatch] outside allowed time window → skip");
      return;
    }

    const ss = SpreadsheetApp.getActive();
    const sheetNames = ["prod", "dev"]; // 対象シート
    let remaining = MAX_ROWS_PER_RUN; // 1回の実行で処理する最大行数（既存の定数）

    for (let s = 0; s < sheetNames.length; s++) {
      if (remaining <= 0) break;

      const sheetName = sheetNames[s];
      const sh = ss.getSheetByName(sheetName);
      if (!sh) continue;

      const lastRow = sh.getLastRow();
      if (lastRow < 2) continue; // データ行なし

      const startRow = 2;
      const numRows = lastRow - 1;
      const numCols = 14; // A〜N まで読む
      const values = sh.getRange(startRow, 1, numRows, numCols).getValues();

      // ★ まず処理対象を集めて、propName(APIキー)ごとにグループ化
      const groups = {}; // propName -> items[]
      const groupOrder = []; // 登場順

      for (let i = 0; i < numRows; i++) {
        if (remaining <= 0) break;

        const rowIndex = startRow + i;
        const row = values[i];

        const sourceVal = row[3 - 1]; // C列
        const titleRaw = row[13 - 1]; // M列 (13)
        const bodyRaw = row[14 - 1]; // N列 (14)
        const status = (row[STATUS_COL - 1] || "").toString(); // L列 (STATUS_COL=12)

        // タイトルも本文も空 → 処理不要
        if (!titleRaw && !bodyRaw) {
          continue;
        }

        // すでに成功している行はスキップ
        if (status.startsWith("OK")) {
          continue;
        }

        // 前回 RUNNING のまま残っている行は、とりあえずスキップ
        // （タイムアウトで止まっている可能性もあるが、安全寄り）
        if (status.startsWith("RUNNING")) {
          continue;
        }

        // ★ 再試行回数チェック
        const gemRetryCount = parseRetryCount_(status);
        const gptRetryCount = parseGptRetryCount_(status);
        const useGpt = shouldUseGpt5Mini_(status);

        // gpt-5-mini 側のリトライ上限（GPTNG(2) になったら打ち切り）
        if (useGpt && gptRetryCount >= GPT_JS_MAX_RETRIES) {
          Logger.log(
            "[processRowsBatch] skip row %s (gptRetryCount=%s >= %s)",
            rowIndex,
            gptRetryCount,
            GPT_JS_MAX_RETRIES
          );
          continue;
        }

        // Gemini 側は MAX_RETRY_COUNT 未満のみ再試行（NG(3) 以上は gpt-5-mini に回す）
        if (!useGpt && gemRetryCount >= MAX_RETRY_COUNT) {
          Logger.log(
            "[processRowsBatch] skip row %s (gemRetryCount=%s >= %s)",
            rowIndex,
            gemRetryCount,
            MAX_RETRY_COUNT
          );
          continue;
        }

        // ここまで来たら「未処理 or 失敗 or PENDING」なので処理対象
        Logger.log(
          "[processRowsBatch] processing %s row %s (status=%s)",
          sheetName,
          rowIndex,
          status
        );

        // この行の「処理前ステータス」を保持（NG(1) など）
        const prevStatus = status;

        // 処理開始マーク
        sh.getRange(rowIndex, STATUS_COL).setValue(
          useGpt ? "RUNNING(GPT)" : "RUNNING"
        );

        const propName = useGpt
          ? "__OPENAI__"
          : _pickApiKeyPropNameWithRotation_(sheetName, sourceVal || "");
        if (!groups[propName]) {
          groups[propName] = [];
          groupOrder.push(propName);
        }
        groups[propName].push({
          rowIndex: rowIndex,
          prevStatus: prevStatus,
          sourceVal: sourceVal || "",
          titleRaw: titleRaw || "",
          bodyRaw: bodyRaw || "",
          urlVal: row[10 - 1] || "", // J列
        });

        remaining--; // 行数上限は従来どおり行単位で消費
      }

      // ★ グループごとに「最大2件」＋「推定トークン予算」で詰めて Geminiへ
      for (let gi = 0; gi < groupOrder.length; gi++) {
        const propName = groupOrder[gi];
        const items = groups[propName] || [];
        if (!items.length) continue;

        let p = 0;
        while (p < items.length) {
          const first = items[p];
          // まずは1件
          let chunk = [first];

          // 2件案があるなら「2件にした場合の推定input tokens」で判定する（トークンのみ）
          if (p + 1 < items.length) {
            const second = items[p + 1];

            // 2件ぶんの promptItems を一旦作って batchPrompt を組み、推定トークンを算出
            const promptItems2 = [first, second].map(function (it) {
              const regionRulesTitle = buildRegionRulesForTitle_(
                it.titleRaw || ""
              );
              const regionRulesBody = buildRegionRulesForBody_(
                it.bodyRaw || ""
              );
              const titleGlossaryRules = regionRulesTitle;
              const bodyGlossaryRules = regionRulesTitle + regionRulesBody;
              return {
                id: String(it.rowIndex),
                rowIndex: it.rowIndex,
                prevStatus: it.prevStatus,
                sourceVal: it.sourceVal,
                urlVal: it.urlVal,
                titleRaw: it.titleRaw,
                bodyRaw: it.bodyRaw,
                titleGlossaryRules: titleGlossaryRules || "",
                bodyGlossaryRules: bodyGlossaryRules || "",
              };
            });

            const batchPrompt2 = buildMultiTaskPromptForRows_(promptItems2);
            const estInTokens2 = estimateTokensFromChars_(
              (batchPrompt2 || "").length
            );

            // ★ここが唯一の判断基準：推定 input tokens
            if (estInTokens2 <= BATCH_MAX_EST_INPUT_TOKENS) {
              chunk = [first, second];
            }
          }

          const promptItems = chunk.map(function (it) {
            const regionRulesTitle = buildRegionRulesForTitle_(
              it.titleRaw || ""
            );
            const regionRulesBody = buildRegionRulesForBody_(it.bodyRaw || "");
            const titleGlossaryRules = regionRulesTitle;
            const bodyGlossaryRules = regionRulesTitle + regionRulesBody;
            return {
              id: String(it.rowIndex),
              rowIndex: it.rowIndex,
              prevStatus: it.prevStatus,
              sourceVal: it.sourceVal,
              urlVal: it.urlVal,
              titleRaw: it.titleRaw,
              bodyRaw: it.bodyRaw,
              titleGlossaryRules: titleGlossaryRules || "",
              bodyGlossaryRules: bodyGlossaryRules || "",
            };
          });

          const batchPrompt = buildMultiTaskPromptForRows_(promptItems);
          const n = promptItems.length;
          const tagBatch =
            sheetName +
            "#rows" +
            promptItems.map((x) => x.rowIndex).join(",") +
            ":EGI(multi2:auto|n=" +
            n +
            ")";

          // 代表1件目のsourceでキー取得（propNameで既にグループ化しているのでOK）
          const apiKey =
            propName === "__OPENAI__"
              ? getOpenAiApiKey_(tagBatch)
              : getApiKeyFromSheetAndSource_(
                  sheetName,
                  chunk[0].sourceVal,
                  tagBatch
                );

          // ★ multi2 (バッチ) は JSON配列が返る必要があるため、OpenAI 側は Structured Outputs で配列スキーマを強制する
          const resp =
            propName === "__OPENAI__"
              ? callGpt5MiniWithKey_(
                  apiKey,
                  batchPrompt,
                  tagBatch,
                  "json_schema_batch"
                )
              : callGeminiWithKey_(apiKey, batchPrompt, tagBatch);

          // API呼び出し自体がエラーなら全員同じエラー
          if (typeof resp === "string" && resp.indexOf("ERROR:") === 0) {
            promptItems.forEach(function (pi) {
              _applyOutputsToRow_(
                sh,
                pi.rowIndex,
                pi.prevStatus,
                {
                  sourceVal: pi.sourceVal,
                  urlVal: pi.urlVal,
                  titleRaw: pi.titleRaw,
                  bodyRaw: pi.bodyRaw,
                },
                resp,
                resp,
                resp,
                propName === "__OPENAI__" ? "GPTNG" : "NG"
              );
            });
            p += chunk.length;
            continue;
          }

          // JSON配列をパースして id=rowIndex で突合
          try {
            let cleaned = (resp || "").trim();
            if (cleaned.startsWith("```")) {
              cleaned = cleaned.replace(/^```[a-zA-Z]*\s*/, "");
              const lastFence = cleaned.lastIndexOf("```");
              if (lastFence !== -1) cleaned = cleaned.substring(0, lastFence);
              cleaned = cleaned.trim();
            }

            const parsed = JSON.parse(cleaned);
            // OpenAI json_schema_batch は { items: [...] } で返る想定。
            // 旧挙動（配列直返し）も許容して両対応にする。
            const arr = Array.isArray(parsed)
              ? parsed
              : parsed && Array.isArray(parsed.items)
              ? parsed.items
              : [];
            const byId = {};
            if (Array.isArray(arr)) {
              arr.forEach(function (o) {
                if (!o) return;
                const id = String(o.id || "");
                byId[id] = o;
              });
            }
            promptItems.forEach(function (pi) {
              const o = byId[String(pi.rowIndex)] || null;
              if (!o) {
                const modelLabel = propName === "__OPENAI__" ? "GPT" : "Gemini";
                const errMsg =
                  "ERROR: invalid JSON array from " +
                  modelLabel +
                  " (missing id=" +
                  pi.rowIndex +
                  ")";
                _applyOutputsToRow_(
                  sh,
                  pi.rowIndex,
                  pi.prevStatus,
                  {
                    sourceVal: pi.sourceVal,
                    urlVal: pi.urlVal,
                    titleRaw: pi.titleRaw,
                    bodyRaw: pi.bodyRaw,
                  },
                  errMsg,
                  errMsg,
                  errMsg,
                  propName === "__OPENAI__" ? "GPTNG" : "NG"
                );
                return;
              }
              const hA = String(o.headlineA || "").trim();
              const hB = String(o.headlineBPrime || o.headlineB || "").trim();
              const sm = decodeJsonNewlines_(String(o.summary || "").trim());
              const sm2 = normalizeSummaryHeader_(sm);

              _applyOutputsToRow_(
                sh,
                pi.rowIndex,
                pi.prevStatus,
                {
                  sourceVal: pi.sourceVal,
                  urlVal: pi.urlVal,
                  titleRaw: pi.titleRaw,
                  bodyRaw: pi.bodyRaw,
                },
                hA || "",
                hB || "",
                sm2 || "",
                propName === "__OPENAI__" ? "GPTNG" : "NG"
              );
            });
          } catch (e) {
            const modelLabel = propName === "__OPENAI__" ? "GPT" : "Gemini";
            const errMsg =
              "ERROR: invalid JSON from " +
              modelLabel +
              ": " +
              String(resp).substring(0, 200);
            promptItems.forEach(function (pi) {
              _applyOutputsToRow_(
                sh,
                pi.rowIndex,
                pi.prevStatus,
                {
                  sourceVal: pi.sourceVal,
                  urlVal: pi.urlVal,
                  titleRaw: pi.titleRaw,
                  bodyRaw: pi.bodyRaw,
                },
                errMsg,
                errMsg,
                errMsg,
                propName === "__OPENAI__" ? "GPTNG" : "NG"
              );
            });
          }
          p += chunk.length; // 1 or 2
        }
      }
    }

    Logger.log(
      "[processRowsBatch] done, processed rows=%s",
      MAX_ROWS_PER_RUN - remaining
    );

    // ★ prod / dev それぞれについて、完了していればメール通知
    checkAndNotifyAllDoneIfNeeded_();

    _cleanupOldGeminiLogs_(); // ← 5分ごとに必ず上詰め＆24時間整理
  } catch (err) {
    Logger.log("[processRowsBatch] lock error: " + err);
  } finally {
    try {
      lock.releaseLock();
    } catch (e2) {}
  }
}

/************************************************************
 * 5. 全行完了時のメール通知
 *
 *   完了の定義（対象行）:
 *   - A列が埋まっている
 *   - M列・N列が両方埋まっている
 *   - L列が OK または NG(x) かつ x >= MAX_RETRY_COUNT
 *
 *   prod / dev それぞれで、
 *   「対象行のすべてが上記を満たした時点」でメール送信。
 *   そのときの「最終行のB列の値」をメールに含める。
 *   同じ最終行(B)まで完了している状態では二重送信しない。
 ************************************************************/

function checkAndNotifyAllDoneIfNeededForSheet_(sheetName) {
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(sheetName);
  if (!sh) return;

  const lastRow = sh.getLastRow();
  if (lastRow < 2) {
    // データ行が無い
    return;
  }

  const lastCol = Math.max(STATUS_COL, 14); // 少なくとも L〜N までは読む
  const numRows = lastRow - 1;
  const values = sh.getRange(2, 1, numRows, lastCol).getValues();

  let allDone = true;
  let targetRowCount = 0;
  let lastTargetRowIndex = 0;
  let lastTargetBValue = null;

  for (let i = 0; i < numRows; i++) {
    const row = values[i];

    const colAVal = row[0]; // A列
    const colBVal = row[1]; // B列（日時）
    const titleRaw = row[13 - 1]; // M列
    const bodyRaw = row[14 - 1]; // N列
    const status = (row[STATUS_COL - 1] || "").toString(); // L列

    // A列が空なら対象外
    if (!colAVal) {
      continue;
    }

    // M・N のどちらかでも空なら対象外（その行は「翻訳・要約対象」ではない）
    if (!titleRaw || !bodyRaw) {
      continue;
    }

    // ここまで来たら対象行
    targetRowCount++;
    const absRowIndex = 2 + i;
    lastTargetRowIndex = absRowIndex;
    lastTargetBValue = colBVal;

    // 完了かどうか判定
    let isDone = false;

    if (status.startsWith("OK")) {
      isDone = true;
    } else if (status.startsWith("NG(")) {
      const retryCount = parseRetryCount_(status);
      if (retryCount >= MAX_RETRY_COUNT) {
        isDone = true;
      }
    }

    if (!isDone) {
      allDone = false;
      break;
    }
  }

  // 対象行が1つもないなら通知しない
  if (targetRowCount === 0) {
    return;
  }

  // まだ完了していない
  if (!allDone) {
    return;
  }

  // ここまで来たら「そのシートの対象行がすべて完了」状態

  // ─ 同じ「最終行(B)」に対しては二重送信しないためのキーを作成 ─
  const props = PropertiesService.getScriptProperties();
  const tz = Session.getScriptTimeZone() || "Asia/Yangon";

  let bStr = "";
  if (lastTargetBValue instanceof Date) {
    bStr = Utilities.formatDate(lastTargetBValue, tz, "yyyy-MM-dd HH:mm:ss");
  } else {
    bStr = String(lastTargetBValue || "");
  }

  const notifyKey = sheetName + "#row" + lastTargetRowIndex + "#B=" + bStr;
  const propName = "LAST_NOTIFIED_KEY_" + sheetName;
  const alreadyKey = props.getProperty(propName);

  if (alreadyKey === notifyKey) {
    // この最終行(B)まではすでに通知済み
    Logger.log(
      "[notify] sheet=%s already notified for key=%s",
      sheetName,
      notifyKey
    );
    return;
  }

  // ─ メール送信先の決定（Script Propertiesから複数取得） ─
  const emailList = getNotifyEmailListForSheet_(sheetName);

  if (emailList.length === 0) {
    Logger.log("[notify] no email configured for sheet=" + sheetName);
    return;
  }

  // ─ メール件名・本文を先に作る ─
  const subject = NOTIFY_EMAIL_SUBJECT_BASE + bStr;

  const ssUrl = ss.getUrl();
  const body =
    "シート「" +
    sheetName +
    "」で" +
    bStr +
    "分の記事収集が完了しました。\n\n" +
    "翌 01:00 までにスプレッドシートを更新してください。\n\n" +
    "スプレッドシートURL:\n" +
    ssUrl +
    "\n";

  // ─ 複数アドレスに順次送る ─
  emailList.forEach(function (emailTo) {
    GmailApp.sendEmail(emailTo, subject, body);
    Logger.log(
      "[notify] sent mail to %s for sheet=%s key=%s",
      emailTo,
      sheetName,
      notifyKey
    );
  });

  // 最後に通知した状態を記録
  props.setProperty(propName, notifyKey);
}

// prod / dev まとめてチェックするヘルパー
function checkAndNotifyAllDoneIfNeeded_() {
  const sheetNames = ["prod", "dev"];
  sheetNames.forEach(function (name) {
    checkAndNotifyAllDoneIfNeededForSheet_(name);
  });
}

/************************************************************
 * 6. ログシートクリア
 ************************************************************/
// スプレッドシート主導（インストール型）の「編集時」トリガー用
function onEditClearGeminiLogs(e) {
  const range = e.range;
  const sheet = range.getSheet();
  const sheetName = sheet.getName();

  // 対象は prod / dev シートのみ
  if (sheetName !== "prod" && sheetName !== "dev") return;

  // A2 の変更だけを監視
  if (range.getRow() !== 2 || range.getColumn() !== 1) return;

  const newValue = range.getValue();

  // 「クリアされた（空になった）」ときだけログシートをクリア
  if (newValue === "" || newValue === null) {
    _clearLogSheetFor_(sheetName); // prod or dev に応じてログシート全クリア
    Logger.log("[onEditClearGeminiLogs] cleared logs for sheet=%s", sheetName);
  }
}

/************************************************************
 * 12時間より古いログを削除しつつ、値のある行だけ上に詰める（ヘッダー無し版）
 * 対象シート: gemini_logs_prod / gemini_logs_dev
 ************************************************************/
function _cleanupOldGeminiLogs_() {
  const ss = SpreadsheetApp.getActive();
  const logSheetNames = [GEMINI_LOG_SHEET_NAME_PROD, GEMINI_LOG_SHEET_NAME_DEV];

  const now = new Date();
  const cutoffMs = now.getTime() - 12 * 60 * 60 * 1000; // 12時間前

  logSheetNames.forEach(function (logSheetName) {
    const sh = ss.getSheetByName(logSheetName);
    if (!sh) return;

    const lastRow = sh.getLastRow();
    if (lastRow < 1) return; // データ無し

    const numRows = lastRow;
    const numCols = sh.getLastColumn() || 4; // 念のため自動検出（なければ4）

    const range = sh.getRange(1, 1, numRows, numCols);
    const values = range.getValues();

    const keptRows = [];

    for (let i = 0; i < numRows; i++) {
      const row = values[i];

      const ts = row[0];
      const level = row[1];
      const tag = row[2];
      const msg = row[3];

      // 行全体が空ならスキップ
      const isAllEmpty = !ts && !level && !tag && !msg;
      if (isAllEmpty) continue;

      // timestamp をパース
      let tsDate = null;
      if (ts instanceof Date) {
        tsDate = ts;
      } else if (ts) {
        const parsed = new Date(ts);
        if (!isNaN(parsed.getTime())) tsDate = parsed;
      }

      // timestamp 無し or 不明 → 安全側で残す
      if (!tsDate) {
        keptRows.push(row);
        continue;
      }

      // 24時間以内 → 残す
      if (tsDate.getTime() >= cutoffMs) {
        keptRows.push(row);
      }
    }

    // 元データ消去（書式は保持）
    range.clearContent();

    // 上から詰めて書き戻し
    if (keptRows.length > 0) {
      sh.getRange(1, 1, keptRows.length, numCols).setValues(keptRows);
    }

    Logger.log(
      "[_cleanupOldGeminiLogs_] sheet=%s kept_rows=%s deleted_rows=%s",
      logSheetName,
      keptRows.length,
      numRows - keptRows.length
    );
  });
}

/************************************************************
 * 地名ログ出力用
 ************************************************************/
// 地名ログを書き出す先のスプレッドシートを取得
function getRegionLogSpreadsheet_() {
  const props = PropertiesService.getScriptProperties();
  const logId = props.getProperty("REGION_LOG_SPREADSHEET_ID");

  // 設定されていれば、そのスプレッドシートに書き出す
  return SpreadsheetApp.openById(logId);
}

function openRegionLogSheet_() {
  // ★ ここで別のログ用スプレッドシートを開く
  const ss = getRegionLogSpreadsheet_();
  const name = "region_logs";
  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
  }

  // ★ ヘッダー行がまだ何も無い場合だけ、ヘッダーを追加
  if (sh.getLastRow() === 0) {
    sh.appendRow([
      "timestamp",
      "sheet",
      "row",
      "source",
      "url",
      "part",
      "type",
      "mm",
      "en",
      "used_in_output",
      "output_ja",
      "note",
    ]);
  }
  return sh;
}

function logRegionUsageForRow_(sheet, row, ctx) {
  const logSheet = openRegionLogSheet_();
  const entriesAll = loadRegionGlossary_();

  const sheetName = sheet.getName();
  const {
    sourceVal,
    urlVal,
    titleRaw,
    bodyRaw,
    headlineA,
    headlineB2,
    summaryJa,
  } = ctx;

  const now = new Date();

  // 判定用：元テキスト
  const titleText = (titleRaw || "").toString();
  const bodyText = (bodyRaw || "").toString();

  // タイトル／本文それぞれで regions マッチ（既知地名）
  const entriesTitle = selectRegionEntriesForText_(titleRaw || "", entriesAll);
  const entriesBody = selectRegionEntriesForText_(bodyRaw || "", entriesAll);

  // --- known（regions にある地名）をログ ---
  // タイトル用：見出しA に dict_ja が含まれているか
  entriesTitle.forEach(function (e) {
    const ja = e.ja_headline || e.ja || "";
    const used = ja && headlineA && headlineA.indexOf(ja) !== -1;

    // 出力で使われていないものはログしない
    if (!used) return;

    const mm = e.mm || "";
    const en = e.en || "";

    // この記事タイトルで mm / en のどちらが実際に出ているかを判定
    let mmHit = false;
    let enHit = false;

    if (mm) {
      if (titleText.indexOf(mm) !== -1) {
        mmHit = true;
      }
    }
    if (en) {
      // 英語は単語境界で判定
      const re = new RegExp("\\b" + escapeRegExp_(en) + "\\b", "i");
      if (re.test(titleText)) {
        enHit = true;
      }
    }

    // ログに書き込む mm / en を決定
    let mmOut = "";
    let enOut = "";
    if (mmHit && !enHit) {
      mmOut = mm;
    } else if (enHit && !mmHit) {
      enOut = en;
    } else if (mmHit && enHit) {
      // 両方出ているケースは、とりあえず mm を優先
      mmOut = mm;
    } else {
      // 念のため、どちらも検出できない場合は従来通り両方入れておく
      mmOut = mm;
      enOut = en;
    }

    logSheet.appendRow([
      now,
      sheetName,
      row,
      sourceVal,
      urlVal,
      "title",
      "known",
      mmOut,
      enOut,
      true, // used_in_output
      ja, // output_ja
      "",
    ]);
  });

  // 本文用：見出しB' + 要約 に dict_ja が含まれているか（日本語側のかたまり）
  const blobBodyJa = (headlineB2 || "") + "\n" + (summaryJa || "");

  entriesBody.forEach(function (e) {
    const ja = e.ja_body || e.ja || "";
    const used = ja && blobBodyJa.indexOf(ja) !== -1;

    // 出力で使われていないものはログしない
    if (!used) return;

    const mm = e.mm || "";
    const en = e.en || "";

    // この記事本文(bodyText)で mm / en のどちらが出ているかを判定
    let mmHit = false;
    let enHit = false;

    if (mm) {
      if (bodyText.indexOf(mm) !== -1) {
        mmHit = true;
      }
    }
    if (en) {
      const re = new RegExp("\\b" + escapeRegExp_(en) + "\\b", "i");
      if (re.test(bodyText)) {
        enHit = true;
      }
    }

    let mmOut = "";
    let enOut = "";
    if (mmHit && !enHit) {
      mmOut = mm;
    } else if (enHit && !mmHit) {
      enOut = en;
    } else if (mmHit && enHit) {
      // 両方出ている場合は mm を優先
      mmOut = mm;
    } else {
      // 念のため両方なしのときは元の値をそのまま入れておく
      mmOut = mm;
      enOut = en;
    }

    logSheet.appendRow([
      now,
      sheetName,
      row,
      sourceVal,
      urlVal,
      "body",
      "known",
      mmOut,
      enOut,
      true, // used_in_output
      ja, // output_ja
      "",
    ]);
  });

  // ★ unknown 判定でも使う日本語出力のかたまり
  const blobTitleJa = headlineA || "";

  // --- unknown（regions にない地名）を 1 回の呼び出しで検出 ---
  const unknownList = detectUnknownRegionsForArticle_(
    titleRaw || "",
    bodyRaw || "",
    headlineA || "",
    headlineB2 || "",
    summaryJa || "",
    entriesTitle,
    entriesBody
  );

  unknownList.forEach(function (item) {
    const part = (item.part || "").toString().toLowerCase();
    const normalizedPart = part === "title" ? "title" : "body"; // 不正値は body 扱い

    const jaOut = (item.ja || "").toString();
    let used = false;
    if (jaOut) {
      if (normalizedPart === "title") {
        // タイトル用: headlineA の中に含まれているか
        used = blobTitleJa.indexOf(jaOut) !== -1;
      } else {
        // 本文用: 見出しB' + 要約 の中に含まれているか
        used = blobBodyJa.indexOf(jaOut) !== -1;
      }
    }

    // ★ ここから追加：src を mm / en に振り分ける
    const src = (item.src || "").toString();

    let mmOut = "";
    let enOut = "";

    if (src) {
      // ビルマ文字を含んでいるかどうかで判定
      if (/[က-႟]/.test(src)) {
        // ミャンマー語とみなして mm 列へ
        mmOut = src;
      } else {
        // それ以外は英語（ローマ字等）とみなして en 列へ
        enOut = src;
      }
    }

    logSheet.appendRow([
      now,
      sheetName,
      row,
      sourceVal,
      urlVal,
      normalizedPart,
      "unknown",
      mmOut, // ← ミャンマー語ならここ
      enOut, // ← 英語ならここ
      used, // used_in_output
      jaOut, // output_ja
      "",
    ]);
  });
}

function getRegionLogApiKey_() {
  const props = PropertiesService.getScriptProperties();
  const v = props.getProperty("GEMINI_API_KEY_REGION_LOG");
  return v || ""; // 空なら呼び出し側でフォールバック
}

// 記事単位で未知地名を検出する関数
function detectUnknownRegionsForArticle_(
  titleRaw,
  bodyRaw,
  headlineA,
  headlineB2,
  summaryJa,
  knownEntriesTitle,
  knownEntriesBody
) {
  const apiKey = getRegionLogApiKey_();
  if (!apiKey) return []; // ログ専用キーが無ければスキップ

  // 原文 or 出力どちらも何も無ければスキップ
  if (!(titleRaw || bodyRaw)) return [];
  if (!(headlineA || headlineB2 || summaryJa)) return [];

  // 既知エントリ(mm/en)をマージ＋重複除去
  const allKnown = []
    .concat(knownEntriesTitle || [], knownEntriesBody || [])
    .filter(Boolean);

  const seen = {};
  const knownList = [];
  allKnown.forEach(function (e) {
    const mm = e.mm || "";
    const en = e.en || "";
    const key = mm + "|" + en;
    if (seen[key]) return;
    seen[key] = true;
    knownList.push({ mm: mm, en: en });
  });

  // 本文側日本語（見出しB' + 要約）
  const bodyJa = [headlineB2 || "", summaryJa || ""].join("\n").trim();

  const prompt = [
    "あなたは対訳ペアから地名の対応を抽出するツールです。",
    "",
    "与えられた原文タイトル・本文と、その日本語タイトル・本文から、",
    "regions 用語集には載っていないミャンマー国内の地名のみを抽出してください。",
    "",
    "出力は JSON 配列1つのみとし、フォーマットは次の通りです（日本語以外は英数字）：",
    '[{"part":"titleまたはbody","src":"...元の地名...","ja":"...日本語訳..."}]',
    "",
    "制約:",
    "- regions 用語集に含まれている mm/en は抽出しないこと",
    "- 「src」は原文（ミャンマー語または英語）側の地名をそのまま出すこと",
    "- 「ja」は対応する日本語訳をできるだけ短く自然な形で出すこと",
    "- 地名以外（人名・肩書き・一般名詞など）は含めないこと",
    "",
    "【既知の地名（regionsに既に存在）】",
    JSON.stringify(knownList),
    "",
    "【原文タイトル】",
    titleRaw || "(なし)",
    "",
    "【原文本文】",
    bodyRaw || "(なし)",
    "",
    "【日本語タイトル】",
    headlineA || "(なし)",
    "",
    "【日本語本文（見出しB' + 要約）】",
    bodyJa || "(なし)",
  ].join("\n");

  const resp = callGeminiWithKey_(apiKey, prompt, "regionlog#article");
  if (typeof resp !== "string" || resp.indexOf("ERROR:") === 0) return [];

  let cleaned = resp.trim();
  // ```json ... ``` のガード
  if (cleaned.startsWith("```")) {
    cleaned = cleaned.replace(/^```[a-zA-Z]*\s*/, "");
    const last = cleaned.lastIndexOf("```");
    if (last !== -1) cleaned = cleaned.substring(0, last);
    cleaned = cleaned.trim();
  }

  try {
    const parsed = JSON.parse(cleaned);
    const arr = Array.isArray(parsed)
      ? parsed
      : parsed && Array.isArray(parsed.items)
      ? parsed.items
      : null;
    if (!arr) return [];
    // part が title/body のものだけ返す
    return arr.filter(function (item) {
      if (!item || typeof item !== "object") return false;
      const p = (item.part || "").toString().toLowerCase();
      return p === "title" || p === "body";
    });
  } catch (e) {
    return [];
  }
}
