# unituner SPEC

> mirakc最小版 — チューナー共有と抽象化に特化したサーバ
>
> 本書は unituner の実装仕様。計画段階の決定事項（mind checkpoint `projects/unituner`）と、設計レビューで発見した内部不整合の修正を反映した確定版。
> 実装開始前に読み、仕様どおりに実装すること。仕様と実装が食い違う場合は本書を正とする。

## 1. 概要

- 言語: **Rust**（axum / tokio）。single binary crate。
- 配置: `~/projects/unituner`（GitHub リポジトリは作らない）。
- 役割: recpt1 系チューナー（外部コマンド）の共有・排他制御と、mirakurun 互換 API の最小サブセット配信。
- 扱わないもの: MMT/TLV の内部処理、EPG、ロゴ (CDT)、B-CAS/B25/C-CAS の内蔵デコード、録画/タイムシフト、HDHR/xmltv、websocket、永続化 DB、hot reload。
  - TLV は必ず pipeline 内の外部コマンドで TS 変換されてから内蔵コードに来る前提。内蔵処理は MPEG-TS のみ。
- TS 変換・デスクランブルは全てユーザー定義 pipeline の外部コマンドに委譲する（デスクランブルは外部 B25 デコーダ、例: `arib-b25-stream-test`）。パイプライン通過後の TS だけをサービス検出・フィルタ・配信に使う。

## 2. 自己識別（mirakc 方式）

- HTTP レスポンスヘッダ: `Server: unituner/<version>`（mirakc は `mirakc/<version>` で運用され実用上問題ない前例）。
- `GET /api/version` → `{"current":"<unituner version>","latest":"<同じ>"}`。update チェックを持たないため current == latest。
- `GET /api/status` の `version` フィールドも unituner 自身のバージョン。
- レスポンスボディのどこにも `Mirakurun` の文字列を出さない。
  - 例外: ワイヤプロトコル識別子として `X-Mirakurun-Tuner-User-ID` / `X-Mirakurun-Priority` / `X-Mirakurun-Tuner` は残す（自己を mirakurun と偽るものではない）。

## 3. config

mirakurun 風に YAML を複数ファイルに分割する。既定 config dir は `/etc/unituner`（`--config-dir` で変更可）。

### 3.1 `server.yml`

```yaml
addr: ":40772"                       # 待受アドレス。既定は全インタフェース(0.0.0.0:40772)か要定義
allowed_cidrs:                       # 許容 CIDR。既定は RFC1918 + loopback
  - 10.0.0.0/8
  - 127.0.0.0/8
  - 172.16.0.0/12
  - 192.168.0.0/16
allowed_hosts: []                    # 任意。webui が追加で受け付ける Host 名（既定は空）
idle_timeout_secs: 5                 # 既定 5。最後の購読者が抜けてからプロセス群を kill
subscriber_buffer_chunks: 1024       # 既定 1024。購読者ごとの bounded queue 容量（chunks）
chunk_size_bytes: 65536              # 既定 65536。購読者へ渡す chunk サイズ（64KB）
max_subscribers_per_session: 32      # 既定 32。1 セッションの購読者上限。超過は 429
max_subscribers_per_ip: 16           # 既定 16。1 クライアント IP の全セッション合計上限。超過は 429
max_sessions: 16                     # 既定 16。同時セッション数（Starting/Stopping 含む）。超過は 429
max_subscriber_buffer_bytes: 2147483648   # 既定 2GiB。全購読者キューの合計上限。超過は 429
tuner_startup_probe_ms: 1000         # 既定 1000、上限 10000。routes 使用チャンネルの起動 probe 窓（ms）
```

- `addr` の既定値（loopback か全インタフェースか）は実装時に確定し、本書と README に記す。
  - 確定: 既定は `":40772"`（全インタフェース 0.0.0.0:40772）。`--config-dir` 既定は `/etc/unituner`。
- `allowed_cidrs` はネットワーク制御であり認証ではない（webui も同じ）。README 制限欄に明記する。
- `allowed_hosts` は webui の `Host` 許可リスト（任意・既定は空）。
  - 空のときは loopback / `allowed_cidrs` 内の IP / `localhost` の `Host` だけを受け付ける。これは DNS リバインディング対策であり、名前や逆プロキシ経由で公開する配備では、そのホスト名を `allowed_hosts` に**必ず列挙する**。
  - 値はポート・スキーム・ブラケットを含まないホスト名か IP リテラル。大文字小文字は区別しない。
- 上限フィールド（`max_*`）はすべて 1 以上。0 は「無制限」ではなく設定エラーとして拒否する。
  - `max_subscribers_per_session` は 1 セッションあたりの同時購読者数。
  - `max_subscribers_per_ip` は 1 クライアント IP が**全セッション合計**で持てる購読者数。TCP 接続元 IP で数え、`X-Forwarded-For` は無視する（クライアントが詐称できるため）。
  - `max_sessions` は同時に存在するセッションオブジェクト数（登録中の `Starting` / `Stopping` も含む）。奪取で退避した victim はレジストリから外れた時点で数えない（その購読者は EOF に向かう途中であり、置換の予約を妨げないため）。
  - `max_subscriber_buffer_bytes` は全購読者キューの合計バイト上限。1 購読者は `chunk_size_bytes × subscriber_buffer_chunks`（既定 ≈64 MiB）を worst-case で予約する。既定 2 GiB は 32 購読者分にあたり、`max_subscribers_per_session: 32` と同じなので通常は per-session 上限の方が先に効く。この予算は「最悪ケースの予約」であり実使用量ではない（実際のキューはクライアントに配信中の分しか持たない）ため、2 GiB の予約は一般的なホストで許容できる。より多くの同時視聴が必要なら `max_subscriber_buffer_bytes` を上げ、メモリが制約されたホストでは `subscriber_buffer_chunks` を下げる。
  - 上限を超えたストリーム要求は **429**（`{"code":429,"reason":"Too Many Requests"}`）。既存の購読者・セッションには影響しない。判定はチューナー選定より前（新規セッションは予約時、既存セッションへの参加は attach 時）に行うため、通常はチューナーを起動しない。
- `tuner_startup_probe_ms` は `routes` を持つチャンネルの**起動 probe 窓**（ミリ秒、既定 1000、1 以上、上限 10000）。詳細は §6.4。
  - 窓内に最初のデータが来れば起動成功。窓内にデータ無しでプロセスが終了すれば起動失敗として次の route を試す。
  - **窓を過ぎてもプロセスが生きていれば（データ未着でも）起動成功とみなす**（no signal はフォールバックの理由にしない）。
  - 上限 10000 は、probe がリクエスト全体の待ち期限（readiness 10 秒 + 余裕）を超えないようにするための制約。超える値は起動エラー。
  - routes を持たないチャンネルには影響しない（従来の readiness gate のまま）。
- webui では **閲覧のみ**（編集不可。意図的に誤設定でロックアウトすることを防ぐ）。

### 3.2 `tuners.yml`

```yaml
- name: pt3-gr0                      # 一意必須
  types: [GR]                        # このチューナーが扱えるタイプ（ユーザー定義名、完全一致）
  command: "recpt1 --device /dev/pt3video0 {channel} -"   # {channel} / {type} / {extra_args} プレースホルダ可
  disabled: false                    # ★追加。pinning の disabled→400 を実装可能にする
```

### 3.3 `channels.yml`

```yaml
- name: ＮＨＫ総合
  type: GR                           # ユーザー定義の半角 ASCII 名。正規表現: ^[A-Za-z0-9][A-Za-z0-9_-]*$
  channel: "27"                      # チューナーコマンドの {channel} に入る値。数字以外（C17 等）も可
  extra-args: ""                     # 任意・既定 ""。チューナーコマンドの {extra_args} に入る追加引数
  disabled: false                    # ★追加
  routes:                            # 任意・既定 []（未指定は従来どおり全チューナーから選択）
    - tuner: catv-tuner              # 必須: tuners.yml の name
      channel: "13"                  # 任意: この route だけ channel を上書き
      extra-args: "--lnb 15"         # 任意: この route だけ extra-args を上書き（extra_args 綴りも可）
    - tuner: ptx-s110
```

- チャンネルタイプは任意名（半角）。GR / BS / CS 等どんな名称でもよい。
- `name`/`(type,channel)` の組み合わせは一意必須。
- `extra-args` は任意・既定 `""`（未指定と同じ）。`extra_args`（アンダースコア）綴りも別名として受理する。値はチューナーコマンドの `{extra_args}` に置換される（§3.5）。
  - 値は standalone な argv 断片であり、シェル文字列の中に置いてはならない。引用符（`'` `"`）、バックスラッシュ（`\`）、改行（`\n` `\r`）を含む値は起動エラー。それ以外（空白を含む）は許容する。
  - `{extra_args}` は `--lnb 15` のように複数 argv トークンへ展開する前提のため、`{channel}` のような charset ホワイトリスト検証は行わない。
- `routes` は任意・既定 `[]`。空（未指定）なら従来の選択（§6.2）、非空ならそのチャンネルは**列挙したチューナーだけ**を順に試す（§6.4）。`routes` はチャンネルタイプに依存しない。
  - `route.tuner` は必須で、`tuners.yml` の `name` を指す。未知のチューナーは起動エラー。
  - 同一チャンネルの `routes` に同じチューナーを 2 回書くことはできない（起動エラー）。
  - `route.channel` を使う場合、`channel` と同じ文字種検証（§3.5）を通る必要がある。
  - `route.extra-args` はチャンネルの `extra-args` をこの route だけ上書きする。未指定ならチャンネルの値を継承する。
  - 同じ**実効 channel** に解決される 2 つの route は、実効 extra-args も一致していなければならない（起動エラー）。実効 channel の再利用は route 順序やチューナーより優先されるため、異なる extra-args は黙って無視されることになる（§6.4）。
- 同一 `(type, channel)` の重複エントリは**起動エラー**とする。mirakc-BS4K は警告して最初の定義を採用する（first wins）が、unituner は既存の fail-fast 規則を維持する。したがって `(type, channel)` に対する `routes` の解決は常に一意である。

### 3.4 `pipelines.yml`

```yaml
pipelines:
  cas:                                # ユーザー定義名
    - "arib-b25-stream-test"          # stdin→stdout で直列に繋がる外部コマンド（外部 B25 デコーダ）
  cas4k:
    - "arib-b25-stream-test"          # 4K/BS も同じ B25 デコーダを 1 段目に置く
    - "ts-converter <args>"           # 2 段目は TS 変換（MMT/TLV → MPEG-TS 等）
types:                                # チャンネルタイプ → pipeline バインド（ユーザー定義名）
  GR:         { pipeline: cas }
  BS:         { pipeline: cas4k }
  CABLE_CATV: { pipeline: cas }
  # バインド無しのタイプ = 生 TS そのまま配信
```

- CAS（デスクランブル）は特定実装に依存しない。B25 系のスクランブル解除は外部 B25 デコーダ
  （例: `arib-b25-stream-test`。引数なしで stdin の TS を stdout へ流す）を pipeline の
  1 段として置く。段数・順序はユーザー定義で、TS 変換段はその後段に置く。

### 3.5 プレースホルダとコマンド実行

- チューナーコマンドと pipeline の各コマンドには `{channel}`、`{type}`、`{extra_args}` のプレースホルダを使える。
  - `{channel}`: 要求されたチャンネルの `channel` 値（例: `"27"`, `"BS13_1"`, `"C17"`）。`routes` 使用時はその route の実効 channel（route の `channel` 上書きがあればそれ、なければチャンネルの `channel`）（§6.4）。
  - `{type}`: 要求されたチャンネルの `type` 値（例: `GR`, `BS`, `CABLE_CATV`）
  - `{extra_args}`: チューナーコマンドの追加引数。`routes` 使用時はその route の実効 extra-args（route の上書きがあればそれ、なければチャンネルの `extra-args`）、それ以外はチャンネルの `extra-args`。既定は空文字列。
  - pipeline は既にタイプごとにバインドされているので `{type}` は実質不変だが、両コマンドで同じ規則にする。
- 置換は Session の spawn 時に行う。Session キーが (tuner, type, 実効 channel) なので一意に決まる。
- `{extra_args}` はチャンネル/route 定義由来の信頼済み断片であり、`--lnb 15` のように複数 argv トークンへ展開することを意図するため、`{channel}` の charset ホワイトリスト検証は行わない。ただし定義値自体は §3.3 の規則（引用符・バックスラッシュ・改行を含まない）を満たさなければならない。
  - `{extra_args}` が空文字列のときは何も置換しない。argv 分割が周囲の空白を畳むため、引用符なしの `cmd {channel} {extra_args}` は `["cmd", "<channel>"]` となりトークンが消える。引用した `"{extra_args}"` は空トークン 1 個になる（シェルと同じ）。
  - 未知の `{...}`（例: `{chanel}`, `{extra_argz}`）は空白化せず**そのまま残す**。typo がコマンド側で目に見える形で失敗するようにするため。
- コマンドは **argv 分割して直接 spawn** する（`/bin/sh -c` は使わない）。`{channel}`/`{type}` の値は生成時に charset 検証 + クォーティングを通す。
  - config 値と URL パス由来の値（`/api/channels/{type}/{channel}/stream`）は同じ検証関数に通す。URL 由来のメタ文字がシェルに流入しないこと（command injection 防止）。
  - 確定（Phase 1）: `{channel}` の許容文字は ASCII 英数字と `. _ + - : @ ,` のみ（空白・引用符・`\`・`/`・シェルメタ文字は拒否）。{type} は §3.3 の正規表現 `^[A-Za-z0-9][A-Za-z0-9_-]*$`。

### 3.6 バリデーション（起動時と webui 保存時）

起動時 fail-fast。webui 保存時も同じ検証を通過しなければ書き戻さない。検証項目：

- チューナー名 / チャンネル `(type, channel)` の重複なし
- タイプ名正規表現 `^[A-Za-z0-9][A-Za-z0-9_-]*$` を `tuners.types` / `pipelines.types` キー / `channels.type` の全てに適用
- `types` が参照する pipeline が存在すること
- `command` が空でないこと
- unknown field を拒否すること（`cmd:` と `command:` の typo 等を黙って無視しない）
- `pipelines` エントリが 1 つ以上あること

## 4. データパス

```
  tuner command ─stdout─► [204→188 正規化] ─► pipeline stage1 ─► ... ─► 188B ──┐
  (child)                                  (RSパリティ除去・先頭junk読み飛ばし)   │
                                                              下層の broadcast
                                                              (post-pipeline / pre-filter 共通)
                                                                 ├──► channel stream (そのまま配信)
                                                                 ├──► service stream
                                                                 │      (購読者ごとに TSFilter を 1 個生成)
                                                                 └──► scanner
                                                                        (filter しない専用購読として分岐)
```

- **Session** = (tuner, type, channel) 単位の共有プロセス木（tuner + pipeline 直列）。tuner の stdout が pipeline 段に流入し、最終段の stdout が 188B アラインされて broadcast される。
- **共通 broadcast の層は post-pipeline / pre-filter**。ここが設計の要。
  - channel stream はこの層をそのまま配信する。
  - service stream はこの層から購読者ごとに **TSFilter インスタンスを 1 個ずつ** 用意し、PAT/PMT 書き換えを購読者側で独立に行う（購読者同士で sid が違っても干渉しない）。
  - scan はこの層から **filter しない専用購読者**として分岐する。filter 下流で scan すると 1 サービスしか見えずサービステーブルを作れないため。
- **204 → 188 正規化は pipeline 入力側**（tuner stdout 直後）で行う。204B は下位 16B の RS パリティを落とし、188B に揃えた上で pipeline に渡す。先頭 junk は最初の 0x47 まで読み飛ばす。
- 子プロセスは setsid で専用プロセスグループに置き、SIGTERM→猶予→SIGKILL をグループ全体に送る。
- プロセスツリーの部分的な spawn 失敗（tuner は起動したのに pipeline 段が起動しない等）は、セッションを「登録済みだが出力なし」にしないこと。起動失敗時は Session を作らずに落とし、tuner 側のプロセスも確実に kill する。

### 4.1 `decode=0`

- `decode=0` は **pipeline 全段をバイパスし、生 TS をそのまま配信**する。
  - mirakurun の `decode=0` は B-25 デコーダのみスキップだが、unituner は pipeline 内にデスクランブルも TS 変換も全部入れる設計のため、この解釈で確定（ユーザー確認済み）。
- `decode=0` のとき内蔵 TSFilter（service stream の SID 絞り込み）も適用するかは仕様として要定義。既定方針：`decode=0` でも service stream の TSFilter は適用する（絞り込みは pipeline ではないため）。ただし README 制限欄に「decode=0 は pipeline 全段 bypass」と明記する。

## 5. 内蔵 TSFilter（service stream の SID 絞り込み）

- 目的: service stream として対象サービスだけの TS を返す。mirakurun の TSFilter 相当。
- **必ず pipeline の後段**に配置する（post-pipeline / pre-filter の broadcast から購読者側で実行）。
- 動作：
  - ISDB の PAT の `program_number == service_id(sid)` 照合
  - PAT を対象プログラム 1 件のみに書き換え **+ CRC32 (MPEG-2) 再計算**
  - 対象 PMT（PCR_PID 含む）+ 対象 ES PIDs + 固定 PID (`0x0000` / `0x0001` / `0x0010` / `0x0011`) のみ通過
  - NULL (`0x1FFF`) と TEI 付きパケットは破棄
- アクセスは ServiceItemId（`nid*1e5 + sid`）または sid 直指定の両方を許容する。ただし衝突回避：完全一致（ServiceItemId）を優先し、sid 直指定は一意な場合のみ解決する。複数 nid で同じ sid が存在する場合は 404。

## 6. チューナー選択・競合・奪取

### 6.1 優先度モデル（mirakurun 厳格で統一）

- HTTP ヘッダ `X-Mirakurun-Priority` で要求優先度を指定する。既定 0。
- 奪取が許されるのは `priority >= 0` の要求のみ。
  - victim セッションの **max ユーザー priority < 要求 priority**（全ユーザーより真に大きい）が真のときのみ奪取可。
  - 奪取先は victim の max priority が小さい順に最弱のものを選ぶ。
  - スキャン（priority −1000）は奪えない。
- `priority == 128` は mirakc の GRAB 互換で **無条件奪取**（例外として明文化）。
- 1〜127 の中間帯は mirakurun のみ適用する（mirakc の max>0→128 正規化は採用しない）。
- 複数 `X-Mirakurun-Priority` ヘッダが来た場合の集約は仕様外。単一値前提とし、複数来た場合は最後の値を使う（mirakc は max 集約だが、ここでは単一値前提で統一）。
  - 負の優先度を外部クライアントが送ってきた場合は 0 として扱うか否かは要定義（現行：負値は奪取不可だが参加/起動は可能、とする）。

### 6.2 選択アルゴリズム（mirakurun の `_pickTunerDevice` 相当）

参加検索は **(type, channel) 基準**で行う。Session の登録キーとして `(tuner, type, channel)` を持つが、「参加検索は type/channel で行う」と明記し、duplicate spawn を禁止する。`routes` を持つチャンネルでは、この `channel` は route の**実効 channel**（§6.4）である。

1. 同 `(type, channel)` の使用中セッションがあれば、**priority 関係なく参加**する。
2. 完全フリーのタイプ適合チューナーがあれば、それで起動する。
3. 空き無し → 6.1 の奪取条件を満たす使用中チューナーがあれば、最弱の victim を kill して奪取する。
4. それ以外は HTTP **503** `{"code":503,"reason":"Tuner Resource Unavailable"}`。
5. ただし `server.yml` の上限（§7）を超える要求は、チューナー選定より前に HTTP **429** `{"code":429,"reason":"Too Many Requests"}` で拒否する。

- **原子的 pick-and-spawn**: pick→reserve→spawn は単一ロッカー（登録レジストリ）で原子化し、同時 spawn や二重 kill を防ぐ。「Starting」状態を導入して、pick されたチューナーは他要求から不可視にする。
- **0-user セッションの再利用**: idle 期間中の 0-user セッションについて、同 `(type, channel)` なら再利用する。別 channel なら kill + 再起動する（SIGTERM の-latency が発生する点は仕様として記録）。
- **奪取の順序（kill-first）**: チューナーの frontend は排他デバイスであり、victim が保持している間は代替プロセスが `open()` できない（実機では `Device or resource busy` で spawn が失敗し、victim は生きたまま 503 になる）。したがって奪取決定後は **victim を先に kill する**：victim をレジストリから外して購読者に EOF を返し、SIGTERM → 猶予 → SIGKILL でプロセスグループを止め、**プロセスグループが消えてデバイスが解放されたことを確認してから**代替を spawn する。spawn 失敗時は victim を kill 済みのため 503 となり、そのチャンネルは次回要求まで停止する（この tradeoff は仕様として許容する）。応答の readiness gate（最初の TS パケットまで 200 を返さない）は維持する。victim 選択後に他要求が victim へ参加した場合は、レジストリロック下での再検査で奪取を中止し victim のストリームを維持する。
- **終了**: SIGTERM を送信し、プロセスグループが退出するまで待つ。猶予は 2 秒。固定 wait ではなく退出確認で行い、猶予を超えたら SIGKILL する。
- **unpinned 起動失敗時は次候補 tuner を試す** fall-through を既定 ON とする（broken tuner が pool 全体を落とさないため）。
- **奪取の kill 対象**: victim セッションの全ユーザーが切断される。
- クライアントへの通知：奪取 kill / pipeline クラッシュ / tuner 終了はいずれも headers 送信後の EOF として見える。区別できる通知はしない（ここは仕様として記録）。

### 6.3 チューナー pinning（`X-Mirakc-Tuner` / `X-Mirakurun-Tuner`）

- クライアントが tuner を明示指定する拡張。mirakc-BS4K 互換の厳密 pinning（フォールバック無し）。
- 受け付けるヘッダ：`X-Mirakc-Tuner` と **`X-Mirakurun-Tuner` の両方**。どちらも同じ pinning 意味として扱う（alias）。
  - 両ヘッダ同時指定時の優先規則は実装時に 1 つに決める（例：非空値の最後指定優先、または `X-Mirakc-Tuner > X-Mirakurun-Tuner` の優先）。README 制限欄に記載する。
  - 空ヘッダ / 空白のみは無視する。複数ヘッダは最後の値を使う（whitespace trim 後）。
- 適用先（mirakc-BS4K からの **unituner 独自拡張**；mirakc-BS4K は channel stream のみ）：
  - `GET /api/channels/{type}/{channel}/stream`
  - `GET /api/channels/{type}/{channel}/services/{id}/stream`
  - `GET /api/services/{id}/stream`
  - 他のエンドポイント（programs は未実装）には効かない。
- 動作（mirakc-BS4K 通り）：
  - 値 = `tuners.yml` の `name`
  - 未知 / `disabled: true` の名 → **400**
  - pinned tuner の `types` がチャンネルの type に対応しない → **400**
  - 同 `(type, channel)` セッション使用中なら priority 無関係に参加
  - フリーなら起動。busy なら優先度による奪取可否で決まる
  - 奪取不可 / 起動失敗 → **503**
  - pinned でもフォールバックしない（別 tuner を試さない）
  - service endpoint 側でも pinning が効くことは README 制限欄に明記する。
- **エラー優先順位**：service 解決（404）が tuner 検証（400）より先。

### 6.4 ルート（`routes`）

mirakc-BS4K の `channels[].routes` 互換。`routes` を持つチャンネルは、**列挙したチューナーだけを、書いた順に**試す。`routes` はチャンネルタイプに依存せず（GR / BS / CS / 任意の type で使える）、type 適合の判定はこれまでどおり `tuners.types` で行う。

- 選択の手順（route ごと、mirakc-BS4K と同じ順）:
  1. その route の実効 `(type, channel)` のセッションが既にあれば、`priority` に関係なく**参加/再利用**する。
  2. その route のチューナーが完全にフリーなら起動する。
  3. その route のチューナーが下位 priority の利用者から奪えるなら、kill-first で奪取して起動する（§6.2）。
  4. それ以外（busy・奪取不可・type 非対応・disabled・未知）は次の route へ。
- **フォールバックは起動失敗のときだけ**次 route に進む:
  - チューナーコマンドが render / spawn できない（`{channel}` 等の検証失敗、exec 失敗）。
  - チューナーコマンドが**最初のデータを出す前に**終了した（起動 probe 窓内の early exit）。
  - 起動 probe: spawn 後 `tuner_startup_probe_ms`（§3.1、既定 1000 ms）待つ。
    - 窓内に最初のデータが来たら起動成功。
    - 窓内にプロセスが終了（データなし）なら起動失敗 → 次 route。
    - 窓を過ぎてもプロセスが生きていれば（データ未着でも）**起動成功とみなし**、probe を終了して通常の idle/EOF 処理に委ねる。**no signal はフォールバックの理由にしない**（mirakc の文書化された挙動）。
    - probe の境界でデータ無しに終了した場合は**起動失敗 → 次 route**。readiness の終了報告はタイマーより優先されるため、境界の競合でも early exit が確実に fallback を起こす。
  - 一度ストリームが始まった後に停止してもフォールバックしない。
- **terminal な失敗は route walk を続行しない**: decode モードの衝突（**409**）と購読者/セッション上限（**429**）はリクエスト全体に対して terminal であり、次の route を試さない。次の route は実効 channel/チューナーが異なり、別の decode モードで 2 本目のストリームを起動したり、上限を迂回したりすることになるため。
- route の `channel`/`extra-args` 上書きは**実効値**として、チューナーコマンドの `{channel}`/`{extra_args}` とセッション identity・再利用の両方に使う。不変条件「実効 `(type, channel)` ごとに最大 1 セッション」は維持される。2 つの route が異なる channel 文字列を持っても、session を誤って共有/衝突させない。
- **実効 channel の再利用は route の順序やチューナーより優先される**。ある route の実効 `(type, channel)` に既存セッションがあれば、その route のチューナーが別でもまずそのセッションへ参加する（同一チャンネルの二重起動を防ぐため）。したがって同じ実効 channel に解決される route は、実効 extra-args も一致していなければならない（§3.3）。
- セッションは実効 `(type, channel)` でキー付けされる。route ごとの `channel` 上書きは、このキーとチューナーコマンドの両方に反映される。
- ある route のセッションが `Starting` / `Stopping` で進まない場合、その route の待ちがリクエスト期限内で期限切れになれば次の route へ進む。全体はリクエストの待ち期限で制限される。
- **全 route 失敗**は既存の `TunerUnavailable` 経路（HTTP **503**）。
- **pinning は routes に優先する**（§6.3）。`X-Mirakc-Tuner` / `X-Mirakurun-Tuner` を指定した要求は routes を無視し、そのチューナーだけを使う。フォールバックは一切しない。
- `disabled: true` のチャンネルは route を試す前に 404（§9）。routes は `disabled` 判定の後。
- `routes` を持たないチャンネルは従来どおり §6.2 の選択（全 type 適合チューナーからの fall-through あり）で、起動 probe は使わず従来の readiness gate（10 秒）で成否を決める。
- 起動 probe と readiness gate の関係: probe は**route フォールバックの可否**を決め、通常の readiness gate は**ストリーム開始**を決める。routes 使用時は probe のタイムアウトを「起動成功」として扱うため、10 秒の readiness gate は実質使われない。routes 非使用時は従来の readiness gate のみ。

## 7. バックプレッシャとリソース

- 購読者ごとに bounded `mpsc` を持たせ、溢れたらそのクライアントだけ切断する（他購読者には影響しない）。
  - chunk サイズは 64KB 固定として購読者毎のバッファ予算を明記する（既定 1024 chunks）。chunk サイズが未定義だと最小/最大で 350× のメモリ差が出るため。
  - broadcast チャンネルの容量（tokio broadcast の Lagged 発生可否）も定義する。mpsc 側の容量だけでなく broadcast 容量も budget に含める。
- 最後の購読者が抜けた後、`idle_timeout`（既定 5 秒）でプロセス群を kill する。
  - idle timeout と subscriber 上限は `server.yml` で変更可能にする（webui では閲覧のみなので値は定義だけ行う）。
- per-IP / per-session の購読者数・セッション数に cap を設け、DoS（subprocess 爆発・メモリ枯渇）を防ぐ。
  - **実装済み**。`server.yml` の次のフィールドで設定する（すべて任意、既定値あり、1 以上。0 は「無制限」ではなく設定エラー）。
    | フィールド | 既定 | 意味 |
    |---|---|---|
    | `max_subscribers_per_session` | `32` | 1 セッションあたりの同時購読者数 |
    | `max_subscribers_per_ip` | `16` | 1 クライアント IP が全セッション合計で持てる購読者数（TCP 接続元で数え、`X-Forwarded-For` は無視） |
    | `max_sessions` | `16` | 同時に存在するセッション数（登録中の `Starting` / `Stopping` も含む） |
    | `max_subscriber_buffer_bytes` | `2147483648`（2 GiB） | 全購読者キューの合計バイト上限 |
  - `max_sessions: 1` 相当の「1 チューナー 1 セッション」は既存の不変条件（`(tuner, type, channel)` ごとに 1 プロセス木、1 チャンネル 1 セッション）で保証されているため、`max_sessions_per_tuner` という独立フィールドは設けない。
  - 購読者 1 人は `chunk_size_bytes × subscriber_buffer_chunks`（既定 65536 × 1024 ≈ 64 MiB）を worst-case で予約する。既定 2 GiB は 32 購読者分にあたり、`max_subscribers_per_session: 32` と同じなので通常は per-session 上限の方が先に効く。この予算は「最悪ケースの予約」であり実使用量ではない（実際のキューはクライアントに配信中の分しか持たない）ため、2 GiB の予約は一般的なホストで許容できる。メモリが制約されたホストでは `max_subscriber_buffer_bytes` をさらに上げるか `subscriber_buffer_chunks` を下げる。
  - 上限は acquire 時にセッションレジストリのロック下で判定する（新規セッションは `reserve`、既存セッションへの参加は `attach`）。超過した要求は **429** `{"code":429,"reason":"Too Many Requests"}` を返し、既存の購読者はそのまま配信を続ける。参加の拒否は spawn 前に決まるため、拒否された要求がチューナーを起動することはない。
  - カウンタは別カウンタではなく購読者リストから導出する。購読解除（クライアントの切断 / EOF / idle / 奪取による停止）は購読者エントリを消す同じ経路で行われるため、二重減算も取りこぼしもない。
  - 予約中（`Starting`）のセッションは、まだ購読者がいなくても 1 人分として数える。これにより並行 acquire が同じ最後の 1 枠を同時に通過できない。
  - 起動時 / 探索のスキャンも通常購読者と同じ枠を 1 つ使う（専用バッファ要件は未実装）。上限が厳しい構成ではスキャンが「no signal」としてスキップされうるため、同時視聴数を増やすときは `max_subscriber_buffer_bytes` も合わせて上げる。

## 8. スキャン（起動時サービス検出 / チャンネル探索 API）

- 起動時のバックグラウンドスキャンで PAT/PMT/SDT をパースし、in-memory の ServiceTable を構築する。EPG は見ない。
- scan は **priority −1000** の専用購読者として、post-pipeline / pre-filter の broadcast から **filter しない専用購読**で分岐する。
  - scan 専用に大きな購読バッファを持つ（通常購読者の 1024 chunks とは別）。
  - 中間的に通常購読者と同一の購読扱いで scan が溢れて自己切断しないようにする。
- スキャンは全 channel に対して通常セッションとして扱わず、scan 用セッションで取得する。相手が busy なら skip + 次回 retry。
- テーブルが ready に至るまでは `/api/services/{id}/stream` が 404 を返してよい（README に記載）。
- ARIB サービス名/SDT デコード：`siketyan/arib-rs`（MIT、STD-B24 text / STD-B10 TS SI tables）を第一候補とし、実 TS で早期検証する。動かなければ手書き SI パーサにフォールバックする。
- スキャンのスケジュール（定期 re-scan の要否・間隔）は要定義。

### 8.1 チャンネル探索 API（mirakurun 本家の `PUT /api/config/channels/scan` 互換）

- 目的: `channels.yml` に未登録のチャンネルを列挙して追加する。既存の登録済みエントリは維持し、探索は未登録チャンネルを実際にチューニングして SDT/NIT から名前とサービスを取得する。
- 既存 channels.yml の **登録済みエントリは維持**する（`disabled: true` のエントリは再スキャン対象）。`refresh` オプションが真のときは既存エントリを列挙し直して上書きする。refresh で既存エントリを置換するときも、手書きの `routes` と `extra-args` は保持する（scan が発見するのはサービスであり、経路設定ではないため）。
- エラー（`no_signal` 等）はそのチャンネルを**スキップして継続**する。dry run でもログ出力のみ。
- channels.yml の書き換えは**追加のみ**とする（type 単位で完全置換しない）。他の type は一切触らない。
- 完了後は**手動再起動**が必要とする（`**RESTART REQUIRED**` メッセージを出力。restart はしない）。
- 返し方は `text/plain` ストリーミング（mirakurun 通り：完了まで curl の応答が返り、進捗テキストを逐次書き出す）。
- クエリパラメータ（mirakurun 互換）:
  - `type`: ユーザー定義のタイプ名（文字列）。型列挙ではなく任意の type 文字列を受け付ける。
  - `minCh` / `maxCh`: 列挙するチャンネル番号の範囲（整数）。
  - `skipCh`: 個別にスキップする番号。カンマ区切りの複数指定を許容。
  - `refresh`: true で既存エントリを上書き再列挙。
  - `dryRun`: true で保存せず進捗テキストのみ出力。
  - `scanMode`: `Service` / `Channel`。既定は mirakurun 通り GR = `Channel`、BS/CS = `Service`。
  - `setDisabledOnAdd`: 新規追加を `disabled: true` で登録するか。既定は GR = false、BS/CS = true。
  - `channelNameFormat`: `{ch}` / `{ch00}` / `{subch}` 等のプレースホルダで `channels.yml` の `channel` を機械生成。
  - BS 専用: `useSubCh` / `minSubCh` / `maxSubCh`（`minSubCh`/`maxSubCh` はカンマ区切り可）。
  - **`async` は実装しない**（同期 text/plain ストリーミングのみ）。`async=true` が来た場合は無視して同期実行する。`GET /api/config/channels/scan` は 404（非対応）。
- サービス種別のフィルタは mirakurun と同じ：`0x01` デジタルTV / `0x02` デジタル音声 / `0xA1` 臨時映像 / `0xA4` エンジニアリング / `0xA5` プロモーション / `0xAD` 4K / `0xC0` データのみ。
- 実行中の重複リクエストは **409 `{"code":409,"reason":"Already Scanning"}`**。
- 探索は「未登録のチャンネルを実際にチューニング → SDT/NIT から名前とサービスを取得 → channels.yml に追加」の流れ。既存の登録済みエントリには触れない。`refresh=true` のときだけ既存エントリを列挙し直して置き換える。
- 完了後はユーザーに restart を案内する（`**RESTART REQUIRED**` を出力。自動再起動はしない）。

#### 使用例（curl）

```sh
# GR 13-62 を探索して channels.yml に追加（進捗が text/plain で流れる）
curl -N -X PUT 'http://127.0.0.1:40772/api/config/channels/scan?type=GR&minCh=13&maxCh=62'

# 保存せず結果だけ確認
curl -N -X PUT 'http://127.0.0.1:40772/api/config/channels/scan?type=BS&dryRun=true'

# 既存エントリも含めて列挙し直す
curl -N -X PUT 'http://127.0.0.1:40772/api/config/channels/scan?type=GR&minCh=13&maxCh=62&refresh=true'
```

```sh
# 出力例（text/plain）
channel scanning... (type: "GR")

channel: "13" (1/50) [2%] ...
-> 3 services found.
Found 1 channels for 13
...
**RESTART REQUIRED** to apply changes.
```

## 9. HTTP API（mirakurun 互換サブセット）

| エンドポイント | 仕様 |
|---|---|
| `GET /api/version` | `{"current":"<unituner ver>","latest":"<同じ>"}`、`Server: unituner/<ver>` |
| `GET /api/status` | ミニマム JSON（konomiTV 依存しないが mirakurun 互換向けに持ち、version は unituner） |
| `GET /api/channels` | チャンネル一覧 |
| `GET /api/channels/{type}` | type 絞り込み |
| `GET /api/channels/{type}/{channel}` | チャンネル詳細（services は簡易 embed） |
| `GET /api/channels/{type}/{channel}/stream` | **pipeline 通過後の TS をそのまま返す**（channel stream） |
| `GET /api/channels/{type}/{channel}/services/{id}/stream` | 内蔵 TSFilter 適用 |
| `GET /api/services` | サービス一覧（id, serviceId, networkId, tsid, name, type, logoId:0, hasLogoData:false, remoteControlKeyId, channel{type,channel}） |
| `GET /api/services/{id}` | サービス詳細 |
| `GET /api/services/{id}/stream` | 内蔵 TSFilter 適用（`{id}` は ServiceItemId または sid） |
| `GET /api/tuners` | index, name, types, isAvailable, isFree, isUsing, isFault:false, users:[{date,id,priority,provider}] |
| `GET /api/tuners/{index}` | 同上（単体） |
| `GET /api/programs` | **200 `[]` 固定**（EPG 非実装の明示） |
| `PUT /api/config/channels/scan` | チャンネル探索（8.1）。`text/plain` ストリーミング、完了まで応答が返る |
| `GET /api/config/channels/scan` | **404**（`async` 非対応のため） |

- Stream 系は HEAD も可。`X-Mirakurun-Tuner-User-ID` ヘッダを付与する。
- `decode=0` は 4.1 通り。
- `X-Mirakc-Tuner` / `X-Mirakurun-Tuner` は 6.3 通り。
- 未実装（404 `{code,reason}`）: `/api/programs/{id}(/stream)`, jobs/events/log, config CRUD（mirakurun 互換外。`PUT /api/config/channels/scan` のみ例外）, `/api/config/channels/scan` の GET ポーリング, `/api/restart`, iptv/HDHR/websocket, logos, `/rpc`, `/docs`, mirakurun 静的 web UI。
  - `hasLogoData:false` なので KonomiTV は `/api/services/{id}/logo` を呼ばない。

## 10. webui（フル管理 UI）

- 埋め込み静的アセット + vanilla JS。フロントエンドビルド工程なし。axum で配信。
- 認証なし（`allowed_cidrs` + `allowed_hosts`）。`allowed_hosts` は `Host` 許可リストで、空なら loopback / `allowed_cidrs` 内の IP / `localhost` のみ受理する（DNS リバインディング対策）。単一バイナリに埋め込むため、遠隔コード実行面であることは README に明記する。
- 編集対象：**tuners / channels / pipelines**（追加・編集・削除）。**server.yml は閲覧のみ**。
  - Channels タブは `routes`（順序・`tuner`・route ごとの `channel` / `extra-args` 上書き）と `extra-args` も編集できる。route の追加・削除・並べ替えに対応する。
- 内部 API は mirakurun 互換外の独自名前空間 `/webui/api/*`：
  - `GET/PUT /webui/api/{tuners,channels,pipelines}`
  - `GET /webui/api/server-config`（閲覧）
  - `POST /webui/api/restart`
- 保存フロー：入力バリデーション（3.6）→ `.bak` 世代管理付きコピー → tmp 書き込み → fsync → 原子的 rename。失敗時は書き戻さない。
  - 同時編集を防ぐ楽観排他（ETag / version）を持つ。1 世代のみの `.bak` は上書きで失われるため、時刻付き世代管理とするか否かは要定義。
- 反映：「再起動」ボタン → サーバ側で新 config を再検証 → **自己再exec**（`std::env::current_exe()`、systemd 非依存）。再検証失敗なら再起動せずエラー返却（壊れた設定で自己再exec してループしない）。
- `/api/config/{tuners,channels}`（Mirakurun 互換の mutation path）は実装しない。非対応は README 制限欄に明記する。

## 11. プロジェクト構成

```
unituner/
  Cargo.toml            # single binary crate
  src/main.rs           # clap: --config-dir (default /etc/unituner)
  src/config.rs         # server/tuners/channels/pipelines YAML 読込 + validate (+ webui 保存/原子書き込み/.bak)
  src/tsframe.rs        # 188/204 packetizer (204→188 正規化は pipeline 入力側)
  src/si.rs             # PAT/PMT/SDT 解析 (arib クレート呼出 or 手書き)
  src/tsfilter.rs       # 内蔵 TSFilter (PAT 書き換え + PID 通過制御)
  src/services.rs       # in-memory ServiceTable + スキャン更新
  src/session.rs        # Session 管理: 予約/奪取/fanout/アイドル解放/バックプレッシャ
  src/pipeline.rs       # pipeline spawn (stdin→stdout 連結, setsid, SIGTERM→猶予→SIGKILL)
  src/api.rs            # axum routes (mirakurun 互換サブセット)
  src/webui/            # webui 内部 API + 埋め込み静的アセット
  src/scan.rs           # scan 専用購読 + ServiceTable 構築（起動時と API 駆動の両経路）
  src/lib.rs
  tests/                # fixture TS + fake tuner (cat/head -c) による統合テスト
  docs/SPEC.md          # 本書
  README.md             # 概要/config 例/API 一覧/制限事項のみ（接続手順は書かない）
```

## 12. 実装順序

1. スキャフォールド + config 検証 + ロギング（arib クレートの実 TS 検証はここ。ダメなら方針変更を報告）
2. TS フレーミング + SI パース + サービステーブル + ARIB サービス名デコード
3. Session 管理（予約/共有/奪取/fanout/バックプレッシャ/アイドル解放）+ pipeline spawn
4. HTTP API サブセット + streaming + ヘッダ + HEAD
5. 内蔵 TSFilter（service stream 適用、pipeline 後段）
6. webui（内部 CRUD API + 埋め込み SPA + 再起動ボタン自己再exec）
7. 起動時サービススキャン + チャンネル探索 API（priority −1000、実行中チャンネルは共有 TS から更新、アイドルなら一時チューン、既定 timeout 10s/ch。探索結果は channels.yml へ追加のみ・手動再起動）
8. 統合テスト + README

## 13. 検証（end-to-end）

- `cargo test`（unit + 統合。配信バイト == 入力バイト、503、奪取、優先度同値は FCFS、scan→`/api/services` 反映、decode=0 バイパス、内蔵 TSFilter の PID 絞り込みと PAT 単一 program 化）
- `/api/services/{id}/stream` が対象サービスのみの TS（他サービスの ES PID が落ちる）を返すことを fixture で検証
- 同時要求（同一 `(type,channel)` の共有、別 channel への 503 / 奪取）、queued retry の挙動、pinning（未知 tuner → 400）
- config 保存 → YAML 書き換え + .bak 作成、不正値 → 書き戻し拒否、再起動ボタン → 自己再exec で新 config が反映
- `PUT /api/config/channels/scan`：type 範囲を指定して探索 → channels.yml に追加のみ書き込まれる（既存エントリ維持）、no_signal をスキップして継続、重複実行は 409、`dryRun` で保存されない、`refresh=true` で既存を上書き
- KonomiTV の mirakurun 接続でチャンネル一覧・視聴が通ること（ユーザー環境。手順は README に書かない）

## 14. 要定義（実装前に確定させるべき事項）

- `addr` の既定値（loopback か全インタフェースか）
  - 確定（Phase 1）: `":40772"` = 全インタフェース。`server.yml` の `idle_timeout_secs` / `subscriber_buffer_chunks` / `chunk_size_bytes` の既定はそれぞれ 5 / 1024 / 65536。
- `X-Mirakurun-Priority` 負値の扱い
- 奪取リトライキュー（mirakurun の 50 回 × 250ms 相当の bounded wait を入れるか。推奨：入れる）
- スキャンの定期 re-scan の要否・間隔
- `X-Mirakc-Tuner` / `X-Mirakurun-Tuner` 同時指定時の優先規則
- per-IP/per-session cap の既定値
  - 確定: `max_subscribers_per_session: 32` / `max_subscribers_per_ip: 16` / `max_sessions: 16` / `max_subscriber_buffer_bytes: 2147483648`（2 GiB）。超過は 429。2 GiB は 32 購読者分（64 MiB × 32）にあたるため、通常は per-session 上限の方が先に効く。詳細は §7。
- `.bak` の世代管理方針
- `PUT /api/config/channels/scan` の `skipCh` / `minSubCh` / `maxSubCh` のカンマ区切り入力の正規化

## 15. README 制限欄に必ず書くこと

- `decode=0` は pipeline 全段 bypass（mirakurun の B-25 スキップとは異なる）
- priority の multi-header 集約は未対応（単一値前提）
- `X-Mirakc-Tuner` / `X-Mirakurun-Tuner` が service stream にも効く（mirakc-BS4K からの拡張）
- `/api/config/{tuners,channels}` の GET/PUT（設定 CRUD）は非対応（`PUT /api/config/channels/scan` のみ例外）
- webui は認証なし（allowed_cidrs + allowed_hosts のみ。LAN 内のみに配備すること）
- スキャン完了までは `/api/services/{id}/stream` が 404 しうる
- `routes` を持つチャンネルは**列挙したチューナーだけ**を使う（type 非依存）。フォールバックは起動失敗のときだけで、起動 probe（既定 1000 ms）のタイムアウトは「起動成功」として扱う（no signal はフォールバックしない）。pinning（`X-Mirakc-Tuner` / `X-Mirakurun-Tuner`）は routes を無視する
- webui の Channels は `routes` / `extra-args` も編集できる（route の追加・削除・並べ替えを含む）。
- `PUT /api/config/channels/scan` は curl 等で直接叩く（webui には出さない）。`async` / `GET` ポーリングは非対応。channels.yml 書き換え後は **RESTART REQUIRED**（自動再起動しない）
- 探索 API は既存エントリを維持し、**追加のみ**行う（type 単位の完全置換はしない。`refresh=true` のときのみ既存を置換）
