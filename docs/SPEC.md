# unituner SPEC

> mirakc最小版 — チューナー共有と抽象化に特化したサーバ
>
> 本書は unituner の実装仕様。計画段階の決定事項（mind checkpoint `projects/unituner`）と、設計レビューで発見した内部不整合の修正を反映した確定版。
> 実装開始前に読み、仕様どおりに実装すること。仕様と実装が食い違う場合は本書を正とする。

## 1. 概要

- 言語: **Rust**（axum / tokio）。single binary crate。
- 配置: `~/projects/unituner`（GitHub リポジトリは作らない）。
- 役割: recpt1 系チューナー（外部コマンド）の共有・排他制御と、mirakurun 互換 API の最小サブセット配信。
- **設計原則: 標準仕様にのみ対応する。** 本体が実装するのは MPEG-2 TS（188 バイト。204 はパリティを落として受ける）、ARIB STD-B10 の SI、ARIB STD-B24 の文字、mirakurun 互換 API のサブセットだけ。特殊ケース（192 バイトフレーミング、TLV/MMT、ベンダー固有のスクランブル/リマックス等）は pipeline の外部コマンド・設定・別ツールで吸収する。ベンダー名・機種名で分岐するコードを本体に足さない（[POLICY.md](POLICY.md)）。
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
service_stream_keep_eit: false       # 既定 false。true で service stream の EIT (PID 0x0012) を加工せず通す
max_subscribers_per_session: 32      # 既定 32。1 セッションの購読者上限。超過は 429
max_subscribers_per_ip: 16           # 既定 16。1 クライアント IP の全セッション合計上限。超過は 429
max_sessions: 16                     # 既定 16。同時セッション数（Starting/Stopping 含む）。超過は 429
max_subscriber_buffer_bytes: 2147483648   # 既定 2GiB。全購読者キューの合計上限。超過は 429
tuner_startup_probe_ms: 1000         # 既定 1000、上限 10000。routes 使用チャンネルの起動 probe 窓（ms）
tuner_readiness_timeout_secs: 30     # 既定 30、1..=600。最初のデータを待つ readiness gate（秒）
scan_channel_timeout_secs: 60        # 既定 60、1..=600。1 チャンネルの scan 予算（秒）。readiness より大
child_path: "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
                                     # 子プロセスの PATH（空不可）
child_home: "/var/lib/unituner"      # 子プロセスの HOME（空不可）
child_tmpdir: "/tmp"                 # 子プロセスの TMPDIR（空不可）
child_cwd: "/var/lib/unituner"       # 子プロセスの cwd。"" はサーバの cwd を継承
child_env: {}                        # 子プロセスの追加環境変数（PATH/HOME/TMPDIR より優先）
```

- `addr` の既定値（loopback か全インタフェースか）は実装時に確定し、本書と README に記す。
  - 確定: 既定は `":40772"`（全インタフェース 0.0.0.0:40772）。`--config-dir` 既定は `/etc/unituner`。
- `allowed_cidrs` はネットワーク制御であり認証ではない（webui も同じ）。README 制限欄に明記する。
- `allowed_hosts` は webui の `Host` 許可リスト（任意・既定は空）。
  - 空のときは loopback / `allowed_cidrs` 内の IP / `localhost` の `Host` だけを受け付ける。これは DNS リバインディング対策であり、名前や逆プロキシ経由で公開する配備では、そのホスト名を `allowed_hosts` に**必ず列挙する**。
  - **`Host` が無い（または空の）リクエストは拒否しない**。BonDriver_Mirakurun のようなネイティブクライアントは `Host` を送らないため、これを拒否するとチャンネル一覧は通るのに選局・ストリームが 403 になる。ブラウザは authority 無しでリクエストを送れない（HTTP/1.1 は `Host` 必須、HTTP/2 は `:authority`）ので、この緩和で DNS リバインディング対策が弱まることはない。**存在するが許可外**の `Host` は従来どおり 403。
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
  - 上限 10000 は、probe がリクエスト全体の待ち期限（設定された readiness timeout + 余裕）を超えないようにするための制約。超える値は起動エラー。
  - routes を持たないチャンネルには影響しない（従来の readiness gate のまま）。
  - **probe と readiness gate は別物**。probe は route をフォールバックするか（起動 probe 窓）だけを決め、readiness gate はストリーム開始（最初のデータ）を待つ上限を決める。
- `tuner_readiness_timeout_secs` は、最初のデータが来るまで待つ **readiness gate**（秒、既定 30、1..=600）。tune が 6–11 秒かかり、デスクランブラが最初の出力まで 9–15 秒かかる実機に合わせた既定値。0 や 600 超は起動エラー。
- `scan_channel_timeout_secs` は 1 チャンネルの **scan 予算**（秒、既定 60、1..=600）。起動時スキャン・手動 Re-scan・探索 API のすべてで使う。acquire と SI 読みの合計であり、`tuner_readiness_timeout_secs` より**厳密に大きく**なければ起動エラー（そうでないと scan 自身の期限が acquire を先に切ってしまう）。acquire は `readiness + 5 秒` に制限し、残りを SI に使う。
- `service_stream_keep_eit` は service stream の内蔵 TSFilter（§5）が EIT (PID `0x0012`) を通すかどうか（既定 `false`）。
  - `false` では PID `0x0012` を許可集合に入れないため、EIT は他の未知 PID と同様に落ちる（従来の挙動）。
  - `true` では全 EIT パケットを PAT/PMT より前でも**加工せずそのまま**通す。他のサービスの EIT（EIT-other）も含み、unituner は EIT を解析もスコープもしない。
  - 対象は service stream（`/api/services/{id}/stream` と `/api/channels/{t}/{c}/services/{id}/stream`）だけ。channel stream（`/api/channels/{t}/{c}/stream`）は常に pipeline 後の TS をそのまま返すため影響しない。
- `child_path` / `child_home` / `child_tmpdir` は子プロセスの `PATH` / `HOME` / `TMPDIR`（いずれも空不可）。NUL・改行を含む値は起動エラー。
  - `child_cwd` だけは空を許し、その場合はサーバの作業ディレクトリを継承する。存在しない非空の `child_cwd` は**起動エラーにしない**: 起動時に 1 回 warn し、各 spawn では `current_dir` を付けずに warn する（既定の `/var/lib/unituner` が未作成の既存配備を壊さないため）。
- `child_env` は子プロセスへ追加する環境変数（既定 `{}`）。`PATH` / `HOME` / `TMPDIR` と同じ名前を書いた場合は `child_env` が**優先**する。名前は空でなく NUL・改行を含まないこと、値は NUL を含まないこと。
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
  mmt:                                # BS4K の TLV/MMT → TS 変換
    - "b61dec-remote <args>"          # b61dec-remote も TLV 入力を受ける
    - "dantto4k"                      # または "mmt2ts"
types:                                # チャンネルタイプ → pipeline バインド（ユーザー定義名）
  GR:         { pipeline: cas }
  BS:         { pipeline: cas4k }
  CABLE_CATV: { pipeline: cas }
  BS4K:       { pipeline: mmt }      # TLV/MMT → TS は pipeline の仕事
  # バインド無しのタイプ = 生 TS そのまま配信
```

- `types.<type>.pipeline` はそのタイプのチューナー出力を流す pipeline 名。フィールドはこれだけである。
- **pipeline の契約**: pipeline 1 段目の stdin にはチューナー stdout が **byte-for-byte** で入る。チューナーが 188 / 204 / TLV/MMT のどれを出すかは unituner が関知しない。pipeline は必ず **MPEG-TS** を出力しなければならない。TLV/MMT → TS 変換は pipeline の責務（例: `dantto4k` / `mmt2ts`、必要なら前段に `b61dec-remote`）。unituner は pipeline 通過後の TS だけを正規化（204→188・先頭 junk 読み飛ばし）して SI 解析・フィルタ・配信に使う（§4）。
  - したがって pipeline に TLV を渡す前に TS 正規化をかける構成は無い。TLV 内の偶然の `0x47` を拾うとストリームが壊れるため、unituner は生バイトに同期バイト探索を行わない。
- typo フィールド（`inputs:` や旧 `input:` 等）は `deny_unknown_fields` により起動エラー。`input` フィールドはスキーマから削除済みで、指定すると parse エラーになる。

- CAS（デスクランブル）は特定実装に依存しない。B25 系のスクランブル解除は外部 B25 デコーダ
  （例: `arib-b25-stream-test`。引数なしで stdin の TS を stdout へ流す）を pipeline の
  1 段として置く。段数・順序はユーザー定義で、TS 変換段はその後段に置く。
- `passthrough` は予約済みの疑似コマンド。前後の空白を除き ASCII 大文字小文字を無視して
  **完全一致**した段だけを特別扱いし、外部プロセスを spawn しない。
  - 全段が `passthrough` の pipeline は、型にバインドしない場合と同じく生 TS をそのまま配信する。
  - 混合チェーンでは該当段だけを落とす（`["passthrough", "cat"]` と `["cat", "passthrough"]` は
    どちらも `["cat"]` と同じ）。
  - `passthrough-x`、`/usr/bin/passthrough`、`passthrough command` などは通常のコマンドとして実行する
    （typo を黙って疑似コマンド扱いしない）。

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
  - `passthrough` の判定はプレースホルダ置換より**前**に行う。疑似コマンド自身は `{channel}` / `{type}` / `{extra_args}` の置換対象にならない
    （`{channel}` が偶然 `passthrough` に展開されても疑似コマンドにはならない）。
- **子プロセスの環境は明示する**（§3.1 の `child_*`）。チューナーコマンドと pipeline の全段の spawn で:
  1. 継承した環境を `env_clear()` で捨てる。サービスマネージャ経由の `PATH`（例 `/root/.cargo/bin`）や未設定の `HOME` を子へ漏らさない。
  2. `PATH` / `HOME` / `TMPDIR` を `child_path` / `child_home` / `child_tmpdir` から設定する。
  3. `child_env` を最後にマージする（同じ名前なら `child_env` が勝つ）。
  4. `child_cwd` が非空なら `current_dir` に設定する。存在しなければ省略し、プロセスごとに warn する。
  - したがって `dantto4k` のような子が `dtv.names` を探す場合も、`HOME` と `PATH` はサーバプロセスの環境に依存せず、配備ごとに `server.yml` で決められる。
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
  pipeline あり
  tuner command ─stdout─► pipeline stage1 ─► ... ─► [204→188 正規化] ─┐
  (child)                 (生バイトをそのまま受ける /                │
                           TLV/MMT→TS 変換も pipeline の仕事)          │
                                                                       ▼
                                                             下層の broadcast
                                                             (post-pipeline / pre-filter 共通)
                                                                ├──► channel stream (そのまま配信)
                                                                ├──► service stream
                                                                │      (購読者ごとに TSFilter を 1 個生成)
                                                                └──► scanner
                                                                       (filter しない専用購読として分岐)

  pipeline なし
  tuner command ─stdout─► [204→188 正規化] ─► 下層の broadcast（同上）
  (child)

  decode=0 / false（pipeline 全段 bypass）
  tuner command ─stdout─► 下層の broadcast（正規化なし・生バイトそのまま）
  (child)
```

- **Session** = (tuner, type, channel) 単位の共有プロセス木（tuner + pipeline 直列）。tuner の stdout が pipeline 段に流入し、最終段（または pipeline 無しの tuner）の stdout が 188B アラインされて broadcast される。
- **共通 broadcast の層は post-pipeline / pre-filter**。ここが設計の要。
  - channel stream はこの層をそのまま配信する。
  - service stream はこの層から購読者ごとに **TSFilter インスタンスを 1 個ずつ** 用意し、PAT/PMT 書き換えを購読者側で独立に行う（購読者同士で sid が違っても干渉しない）。
  - scan はこの層から **filter しない専用購読者**として分岐する。filter 下流で scan すると 1 サービスしか見えずサービステーブルを作れないため。
- **正規化は常に pipeline の後段**で行う。pipeline はチューナーが emit した生バイト（188 / 204 / TLV のいずれでも）をそのまま受け、必ず TS を出力する。pump が最終段の出力に 204→188 正規化（RS パリティ除去・先頭 junk 読み飛ばし・再同期）を適用してから broadcast する。unituner が pipeline 入力側で正規化を行うことはない。
- **pipeline 無し**（バインド無し、または `passthrough` に縮んだバインド）の場合は、pump がチューナー stdout を直接正規化する。
- 子プロセスは setsid で専用プロセスグループに置き、SIGTERM→猶予→SIGKILL をグループ全体に送る。
- プロセスツリーの部分的な spawn 失敗（tuner は起動したのに pipeline 段が起動しない等）は、セッションを「登録済みだが出力なし」にしないこと。起動失敗時は Session を作らずに落とし、tuner 側のプロセスも確実に kill する。
- TS 正規化（`tsframe.rs`）は読み出し済みの先頭バイトをオフセットで進め、パケットを 1 個捨てるたびの `memmove` を避ける（バッファは全消費時、または未使用領域が 64 KiB 以上になったときに圧縮する）。188 バイトで連続する区間は 1 回のコピーでまとめて出力する。

### 4.1 `decode` クエリ

`decode` は mirakc/mirakurun 互換のデコード指定として解釈する。3 つの stream ルートすべて（`/api/channels/{type}/{channel}/stream`、`/api/channels/{type}/{channel}/services/{id}/stream`、`/api/services/{id}/stream`）の GET と HEAD で同じ規則を適用する。

| 値 | 動作 |
|---|---|
| 無し / `1` / `true` | pipeline を適用（通常配信） |
| `0` / `false` | pipeline 全段をバイパス（チューナーの生バイト） |
| それ以外（`2`、`abc`、空の `decode=` など） | **400** `{"code":400,"reason":"Invalid decode value","errors":[]}` |

- 値は前後の空白を除去し、ASCII 大文字小文字を無視して比較する（`FALSE` や ` false ` も `false` と同義）。
- `0` / `false` は **pipeline 全段をバイパスし、正規化も行わずにチューナーの生バイトをそのまま配信**する。
  - mirakurun の `decode=0` は B-25 デコーダのみスキップだが、unituner は pipeline 内にデスクランブルも TS 変換も全部入れる設計のため、この解釈で確定（ユーザー確認済み）。
  - pipeline が TLV→TS 変換そのものを担う構成（BS4K 等）では、`0` / `false` は **生の TLV/MMT をそのまま配信する**（188B アラインもしない）。これは「unituner は TS のみを扱う」原則の**許容済み例外**である。mirakurun クライアントがこの型に付けると TS ではなく TLV を受け取る点に注意（README 制限欄にも明記）。
- 不正値の 400 は HEAD でも同じで、セッションを取得せずに返す（チューナーを起動しない）。
- `0` / `false` のときも service stream の内蔵 TSFilter は適用する（絞り込みは pipeline ではないため）。README 制限欄に「pipeline 全段 bypass」と明記する。

## 5. 内蔵 TSFilter（service stream の SID 絞り込み）

- 目的: service stream として対象サービスだけの TS を返す。mirakurun の TSFilter 相当。
- **必ず pipeline の後段**に配置する（post-pipeline / pre-filter の broadcast から購読者側で実行）。
- 動作：
  - ISDB の PAT の `program_number == service_id(sid)` 照合
  - PAT を対象プログラム 1 件のみに書き換え **+ CRC32 (MPEG-2) 再計算**
  - 対象 PMT（PCR_PID 含む）+ 対象 ES PIDs + 固定 PID (`0x0000` / `0x0001` / `0x0010` / `0x0011`) のみ通過
  - EIT (`0x0012`) は**任意**。`server.yml` の `service_stream_keep_eit: true` のときだけ許可集合に入れ、PAT/PMT より前から全サービスの EIT を加工せず通す（既定 `false` では破棄）
  - NULL (`0x1FFF`) と TEI 付きパケットは破棄
- 実装メモ:
  - PID の通過判定は 8192 bit のビットマップ（`allowed_bitmap: [u64; 128]`）で行い、PAT/PMT が PID 集合を変えたときだけ再構築する（パケットごとの線形探索をしない）。
  - PAT 書き換えの CRC32 (MPEG-2) はテーブル駆動で計算する。書き換えは、188 バイト固定配列で保持したテンプレートを出力バッファへコピーし、その場でパッチする（テンプレート → `Vec` → 出力の二重コピーをしない）。
  - 入力が 188 の倍数でバッファに端数が無い場合、パケットはコピーせず入力スライス上でそのまま処理する。
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
- `routes` を持たないチャンネルは従来どおり §6.2 の選択（全 type 適合チューナーからの fall-through あり）で、起動 probe は使わず readiness gate（`tuner_readiness_timeout_secs`、既定 30 秒）で成否を決める。
- 起動 probe と readiness gate の関係: probe は**route フォールバックの可否**を決め、通常の readiness gate は**ストリーム開始**を決める。routes 使用時は probe のタイムアウトを「起動成功」として扱うため readiness gate は実質使われない。routes 非使用時は readiness gate のみを使う。

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
  - pipeline の有無にかかわらず、scan が見るのは pipeline 通過後の 188B TS である（TLV 型では pipeline が TLV→TS 変換を済ませている）。
  - scan 専用に大きな購読バッファを持つ（通常購読者の 1024 chunks とは別）。
  - 中間的に通常購読者と同一の購読扱いで scan が溢れて自己切断しないようにする。
- スキャンは全 channel に対して通常セッションとして扱わず、scan 用セッションで取得する。相手が busy なら skip + 次回 retry。
- 1 チャンネルの予算は `server.scan_channel_timeout_secs`（既定 60 秒、1..=600）を起動時スキャン・手動 Re-scan・探索 API のすべてで使う。予算は acquire と SI 読みの合計で、`server.tuner_readiness_timeout_secs`（既定 30 秒、1..=600）より厳密に大きいこと。
  - acquire 部分は `readiness + 5 秒` に制限し、残りを SI に使う。したがって遅いデスクランブラが最初の出力に 9–15 秒かかっても、既定値なら acquire がそれを待ち、SI のための時間も残る。
  - readiness gate は「最初のデータまでの待ち」、`tuner_startup_probe_ms` は「route をフォールバックするか」の判定であり、両者を混同しない（§3.1、§6.4）。
  - 遅いデスクランブラで `tuner failed to start: timed out waiting for the tuner to start` が出る場合は、まず `tuner_readiness_timeout_secs` と `scan_channel_timeout_secs` を実測に合わせて広げる（常に scan > readiness）。
- **結果の分類**（進捗テキストとログで区別する。チューナーが起動できなかったことを `no signal` と混同しない）:

  | 内部結果 | 進捗テキスト | 意味 | 起動時スキャンの再試行 |
  |---|---|---|---|
  | `Services` | `-> N services found.` | PAT/PMT/SDT/NIT からサービスを取得 | なし（完了） |
  | `Busy` | `skipped (tuner busy)` | 全チューナーが使用中（または decode mode 衝突） | **する**（30→60→120→240→300 秒の backoff） |
  | `Preempted` | `skipped (preempted by a user request)` | 利用者の要求（priority ≥ 0）がチューナーを奪った | **する**（busy と同じ backoff） |
  | `NoTuner` | `skipped (no tuner for type <type>)` | その type をサポートする有効チューナーが無い（設定不備） | **しない**（待っても状態は変わらず spin するだけ） |
  | `TunerFailed` | `tuner failed to start: <短い理由>` | チューナーコマンドの spawn 失敗、最初のデータ前に終了、または予算内に ready にならなかった（acquire タイムアウト） | **しない**（壊れたコマンドを繰り返すだけ） |
  | `NoSignal` | `no signal` | セッションを取得でき、期限までに使用可能な SI が無かった | しない |
  | `Failed` | `error (scan task failed)` | スキャンタスクが結果を返さず終了（panic / abort） | しない |

  短い理由は 1 行・最大約 200 文字に丸める。spawn エラーは**展開後の argv**（例: `/nonexistent/tuner 27`）と OS エラー（例: `No such file or directory`）を含む。`{channel}` のテンプレートは出さない。`stream ended before any data`（最初のデータ前に EOF）や `timed out waiting for the tuner to start`（ready 待ちのタイムアウト）も入る。stderr 全体は進捗に出さない（ログ側で従来どおり stage ごとに記録される）。
- 1 パスで 1 チャンネル以上を試し、その**すべて**が `NoTuner` / `TunerFailed` / `Failed` だった場合、起動時スキャンは追加で 1 回 `every channel failed to start its tuner; check tuners.yml (command/device path)` を warn する。手動 Re-scan は進捗の最後（完了サマリの前）に `all N channels failed to start their tuner — check tuners.yml (command/device)` を 1 行追加する。一部だけ失敗した場合やチャンネルが 0 件の場合は出さない。
- `tuner failed to start` は「チューナーコマンド／デバイスが使えなかった」、`no signal` は「チューンはできたが使用可能な SI が無かった」を表す。前者を後者として扱うと、デバイス未接続でも全チャンネルが一瞬で「無信号」に見えてしまう。
- テーブルが ready に至るまでは `/api/services/{id}/stream` が 404 を返してよい（README に記載）。
- ARIB サービス名/SDT デコード：`siketyan/arib-rs`（MIT、STD-B24 text / STD-B10 TS SI tables）を第一候補とし、実 TS で早期検証する。動かなければ手書き SI パーサにフォールバックする。
- スキャンのスケジュール（定期 re-scan）は行わない。結果は `services.json` に永続化し、起動時はキャッシュが有効ならチューナーに触れない。再スキャンは webui の **Re-scan** ボタン（`POST /webui/api/services/scan`）で手動実行する（§8.2）。
- ARIB テキストのデコードは「0x21..0x7E だけなら ASCII」というショートカットをしない。初期の GL は 2 バイト漢字であり、`25 46 25 6C 45 6C` は `%F%lEl` ではなく「テレ東」になる。現在の designation を知っているのはデコーダだけなので、常に arib デコーダを通す。
- `SiParser` は PID ごとに最後にパースしたセクションをキャッシュし、同一バイト列の再送では CRC・パース・ARIB デコードを省略する。キャッシュヒットでも PAT の network id スタンプと NIT の副作用（network id / TSID 集合の更新）は再適用する。

### 8.1 チャンネル探索 API（mirakurun 本家の `PUT /api/config/channels/scan` 互換）

- 目的: `channels.yml` に未登録のチャンネルを列挙して追加する。既存の登録済みエントリは維持し、探索は未登録チャンネルを実際にチューニングして SDT/NIT から名前とサービスを取得する。
- 既存 channels.yml の **登録済みエントリは維持**する（`disabled: true` のエントリは再スキャン対象）。`refresh` オプションが真のときは既存エントリを列挙し直して上書きする。refresh で既存エントリを置換するときも、手書きの `routes` と `extra-args` は保持する（scan が発見するのはサービスであり、経路設定ではないため）。
- エラー（`no_signal` / `tuner failed to start` / `no tuner for type` / `tuner busy` / preempted）はそのチャンネルを**スキップして継続**する。分類と再試行規則は §8 の表を参照。dry run でもログ出力のみ。
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
- **早期終了**: 探索と起動時スキャンは、1 チャンネルにつき PAT + PMT + 完全な SDT-actual + NIT-actual が揃った時点でそのチャンネルの読み取りを打ち切る。SDT-actual の完了だけでは打ち切らない（NIT-actual が未着のまま終わると network id が SDT の `original_network_id` に落ちるため）。SDT 完了後は `SI_GRACE_AFTER_SDT`（1.5 秒）だけ残りのコアテーブルを待つ（実測で PAT は約 100 ms、PMT/SDT は約 200〜500 ms、NIT-actual は約 0.4〜0.8 秒で繰り返す。700 ms では NIT の 1 周期を取りこぼしうるため 1.5 秒とする）。猶予内に NIT-actual が来なければ SDT の `original_network_id` を使う。1 チャンネルが `scan_channel_timeout_secs`（既定 60 秒）の予算を最後まで使うことはない。
- **並列度**: その type の**有効チューナー数**（`tuner_count_for`）を上限に並列でスキャンする。1 台なら従来どおり直列、N 台なら 1 パスが概ね「チャンネル数 / N」の時間で終わる。結果は設定順に畳み込むため、進捗テキストと再試行キューは完了順に依存しない。
- 起動時スキャンのパスでは、busy / 奪取（preempted）のチャンネルは他のチャンネルを塞がず再試行キューへ回る。奪取されたチャンネルでも、部分受信の SDT から得られたサービスは捨てずに反映する。

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

### 8.2 サービスキャッシュ（`services.json`）

- スキャン結果は `config_dir/services.json` に永続化する。起動時はこのキャッシュを読み、`/api/services` とサービスストリームが直ちに使える状態にする。
- スキーマ:
  ```json
  {"version":1,"fingerprint":"<hex>","generated_at":"2026-11-09T12:34:56.789Z","services":[ ... ]}
  ```
  - `version`: 現在 `1`。未知のバージョンは無視してスキャンし、成功後に上書きする。
  - `fingerprint`: 後述の設定フィンガープリント（FNV-1a 64 の 16 桁 hex）。
  - `generated_at`: RFC3339 UTC（ミリ秒精度）。
  - `services`: `ServiceRecord` の配列（`network_id` / `tsid` / `service_id` / `service_type` / `name` / `remote_control_key_id` / `channel` / `pmt_pid` / `has_logo_data`）。順序は `ServiceTable::snapshot()`（network → tsid → service id → channel）で安定。
- **無期限**: キャッシュは時間で失効しない。フィンガープリントが変わったとき、または手動 Re-scan が成功したときだけ置き換わる。放送再編でサービス名/SID だけが変わった場合は、設定が同じならキャッシュはそのまま使われ、Re-scan が必須。
- **フィンガープリント**: 有効（`disabled` でない）`channels.yml` エントリだけを対象に、`(type, channel)` でソートした各エントリの `type` / `channel` / `extra-args` / `routes`（route ごとの `tuner`・`channel` 上書き・`extra-args` 上書きを含む）を、長さプレフィックス付きで連結した文字列を FNV-1a 64 でハッシュする。スキーマ定数 `"unituner-services-v1"` も入力に含む。`DefaultHasher` / `RandomState` は Rust バージョン間で安定でないため使わない。`disabled` エントリと `tuners.yml` は含めない（Tuner 定義ではなく放送の性質で決まるため）。チャンネルの追加・削除・disable・`routes` / `extra-args` 変更で自動的に再スキャンされる。
- **起動時**: `services.json` が読め、`version == 1` かつフィンガープリントが一致すれば、チューナーを一切起動せずにキャッシュを復元して終了する（`startup scan skipped: service cache is valid`）。`services: []` も「その設定にはスキャン対象が無い」という有効な結果として受理する。ファイル欠落・破損・バージョン不一致・フィンガープリント不一致は warn して通常のスキャンにフォールバックし、完了後に上書きする。
- **書き込み**: `services.json.tmp` → `fsync` → `rename`。Unix では `0600` で作成し、既存ファイルのモードは保持する。部分的 JSON は残らない。書き込み失敗は警告のみで、メモリ上のサービス表は有効なまま。
- **起動時スキャンの永続化**: 最初のパスで 1 件以上見つかった、または再試行対象が残らず **`NoTuner` / `TunerFailed` / `Failed` が無い**（全チャンネルが成功または無信号）場合にキャッシュを書く。以後の再試行パスは、表が実際に変わったときだけ書き換える。全チャンネルが busy の初回パスや、チューナーが 1 台も起動できなかった初回パスでは、空のキャッシュを「有効」として残さないよう書き込まない（次回起動時に再プローブする）。キャッシュが書かれた後の再起動では busy チャンネルの再試行は行われない（無期限キャッシュの仕様）。
- **手動 Re-scan** (`POST /webui/api/services/scan`):
  - 他の webui mutation と同じ CSRF / Host / JSON Content-Type ガードを通る。
  - 共有 `scan_lock`（`PUT /api/config/channels/scan` と同一）を取得する。実行中なら **409** `{"code":409,"reason":"Already Scanning"}`。
  - 有効な `channels.yml` の全エントリを **ちょうど 1 パス** スキャンし、進捗を `text/plain` でストリーミングする（無限 backoff はしない）。
  - パス完了後に、**有効な**チャンネルに属するレコードだけを残す（削除・disable されたチャンネルのレコードを落とす）。busy / 無信号 / チューナー起動失敗のチャンネルは有効なままなので、以前のレコードを保持する。スキャンに成功したチャンネルでは、そのチャンネルから消えたサービスも（通常の探索と同じ `retain_channel` で）落とす。
  - その後 `services.json` を書き、`**RE-SCAN COMPLETE**` と件数を出力する。サマリは `**RE-SCAN COMPLETE**: <table 件数> services (<scanned> channels scanned, <found> found, <removed> stale records dropped; <no signal> no signal, <busy> busy, <tuner failed> tuner failed; <cache note>)` の 1 行で、`no signal` / `busy`（preempted を含む）/ `tuner failed`（`NoTuner` を含む）の内訳を必ず出す。**1 件もサービスが無く、全チャンネルが `NoTuner` / `TunerFailed` / `Failed`** の場合は空のキャッシュを書かない（`did not write ... (no tuner could be started)` と表示する）。既存レコードがある場合は通常どおり書き、有効チャンネルのレコードは落とさない。
  - `channels.yml` は一切書かない（チャンネル探索 API とは別物）。
  - クライアントが途中で切断した場合、キャッシュは書かず、チャンネル単位の retain も適用しない（メモリ上の表には途中までの upsert が残りうる）。終了時の retain と保存は行わない。

## 9. HTTP API（mirakurun 互換サブセット）

| エンドポイント | 仕様 |
|---|---|
| `GET /api/version` | `{"current":"<unituner ver>","latest":"<同じ>"}`、`Server: unituner/<ver>` |
| `GET /api/status` | ミニマム JSON（konomiTV 依存しないが mirakurun 互換向けに持ち、version は unituner） |
| `GET /api/channels` | **有効な**チャンネル一覧（`disabled: true` を除く） |
| `GET /api/channels/{type}` | type 絞り込み（同じく `disabled: true` を除く） |
| `GET /api/channels/{type}/{channel}` | チャンネル詳細（services は簡易 embed）。`disabled: true` は 404 |
| `GET /api/channels/{type}/{channel}/stream` | **pipeline 通過後の TS をそのまま返す**（channel stream） |
| `GET /api/channels/{type}/{channel}/services/{id}/stream` | 内蔵 TSFilter 適用 |
| `GET /api/services` | サービス一覧（id, serviceId, networkId, tsid, name, type, logoId:0, hasLogoData:false, remoteControlKeyId, channel{type,channel}）。**有効なチャンネル上のサービスだけ** |
| `GET /api/services/{id}` | サービス詳細。有効なチャンネル上のサービスだけ（それ以外は 404 `Service Not Found`） |
| `GET /api/services/{id}/stream` | 内蔵 TSFilter 適用（`{id}` は ServiceItemId または sid） |
| `GET /api/tuners` | index, name, types, isAvailable, isFree, isUsing, isFault:false, users:[{date,id,priority,provider}] |
| `GET /api/tuners/{index}` | 同上（単体） |
| `GET /api/programs` | **200 `[]` 固定**（EPG 非実装の明示） |
| `GET / HEAD /api/iptv/playlist` | mirakurun 互換 M3U。EPG 非実装のため `url-tvg` なし。service type 1 / 173 のみ |
| `PUT /api/config/channels/scan` | チャンネル探索（8.1）。`text/plain` ストリーミング、完了まで応答が返る |
| `GET /api/config/channels/scan` | **404**（`async` 非対応のため） |

- Stream 系は HEAD も可。`X-Mirakurun-Tuner-User-ID` ヘッダを付与する。
- service stream（`/api/services/{id}/stream` と `/api/channels/{t}/{c}/services/{id}/stream`）は内蔵 TSFilter を通す。EIT (PID `0x0012`) は `server.yml` の `service_stream_keep_eit` が `true` のときだけ加工せず通る（既定 `false`）。channel stream は raw passthrough のままで、この設定の影響を受けない。
- `decode` クエリは 4.1 通り（無し / `1` / `true` は pipeline、`0` / `false` は全段 bypass、それ以外は 400）。
- `/api/services` の一覧・詳細・サービス stream は同じ可視性判定を使う。サービス表のレコードのうち、`(channel.type, channel.channel)` が `channels.yml` に存在して **有効**（`disabled: true` でない）なものだけを一覧・詳細に出す。`disabled: true`、`channels.yml` から削除済み、未バインド (`channel: None`) のレコードは一覧に出さず、詳細は 404 `Service Not Found`（stream も 404）。サービスキャッシュ (`services.json`) は全レコードを保持し、scan/retain の挙動は変えない。
- `X-Mirakc-Tuner` / `X-Mirakurun-Tuner` は 6.3 通り。
- `/api/channels` は 1 リクエストにつきサービス表のスナップショット（`ServiceTable::list()`）を 1 回だけ取り、各チャンネルの services はそこから該当分をフィルタして埋める（チャンネル数ぶんの全件 clone + sort をしない）。`/api/channels/{type}/{channel}` は `list_by_channel` で該当チャンネル分だけを取得する。
- チャンネル一覧 / 詳細は `disabled: true` のエントリを返さない（mirakc と同じ）。無効なチャンネルは配信できない資源なので、stream 系の 404 と同じ扱いにする。`channels.yml` にはエントリを残し、起動時スキャンは従来どおり対象外、webui の GET/PUT round-trip も `disabled` を保持する。応答に `disabled` フィールドは追加しない（そもそも返さないため）。
- `/api/iptv/playlist` は mirakurun の `playlist.ts` 互換。先頭は `#EXTM3U` のみで、`url-tvg` は付けない（EPG 非実装）。`/api/iptv/xmltv` と `/api/iptv/epg` は 404 のまま。
  - 掲載対象は service type `0x01` (1) と `0xAD` (173) のみ。ラジオ (0x02)・データ (0xC0) などは出さない。ロゴ非実装のため `tvg-logo` も出さない。
  - エントリは `#KODIPROP:mimetype=video/mp2t`、`#EXTINF:-1 tvg-id="<ServiceItemId>" group-title="<channel type>",<service name>`、`http://<Host>/api/services/<ServiceItemId>/stream`。名前の `\n` / `\r` は空白に置換し、それ以外のエスケープはしない（mirakurun と同じ）。
  - `Host` ヘッダがあればその authority を `http://` の後に使う。無ければ `server.yml` の `addr` を `<host>:<port>` として使う。`0.0.0.0:40772` / `:40772` のようなワイルドカード bind は `127.0.0.1:40772` に読み替える。
  - 並び順は GR → BS → CS → SKY → その他の type のアルファベット順、各グループ内はチャンネル値の自然順（両方整数なら数値、そうでなければ辞書順）、最後に serviceId。空のサービス表でも `#EXTM3U` を返す。Content-Type は `application/x-mpegURL; charset=utf-8`。
- 未実装（404 `{code,reason}`）: `/api/programs/{id}(/stream)`, jobs/events/log, config CRUD（mirakurun 互換外。`PUT /api/config/channels/scan` のみ例外）, `/api/config/channels/scan` の GET ポーリング, `/api/restart`, HDHR/websocket, EPG (`/api/iptv/xmltv`、`/api/iptv/epg`), logos, `/rpc`, `/docs`, mirakurun 静的 web UI。
  - `hasLogoData:false` なので KonomiTV は `/api/services/{id}/logo` を呼ばない。

## 10. webui（フル管理 UI）

- 埋め込み静的アセット + vanilla JS。フロントエンドビルド工程なし。axum で配信。
- 認証なし（`allowed_cidrs` + `allowed_hosts`）。`allowed_hosts` は `Host` 許可リストで、空なら loopback / `allowed_cidrs` 内の IP / `localhost` のみ受理する（DNS リバインディング対策）。ただし `Host` が無い/空のリクエストは受理する（BonDriver_Mirakurun 等のネイティブクライアント用。ブラウザは authority 無しで送れないため対策は弱まらない）。存在するが許可外の `Host` は 403。単一バイナリに埋め込むため、遠隔コード実行面であることは README に明記する。
- 編集対象：**tuners / channels / pipelines**（追加・編集・削除）。**server.yml は閲覧のみ**。
  - Channels タブは `routes`（順序・`tuner`・route ごとの `channel` / `extra-args` 上書き）と `extra-args` も編集できる。route の追加・削除・並べ替えに対応する。
- 内部 API は mirakurun 互換外の独自名前空間 `/webui/api/*`：
  - `GET/PUT /webui/api/{tuners,channels,pipelines}`
  - `GET /webui/api/server-config`（閲覧）
  - `POST /webui/api/restart`
  - `POST /webui/api/services/scan`（有効チャンネルを 1 パス再スキャンし `services.json` を更新。§8.2）
- 保存フロー：入力バリデーション（3.6）→ `.bak` 世代管理付きコピー → tmp 書き込み → fsync → 原子的 rename。失敗時は書き戻さない。
  - 同時編集を防ぐ楽観排他（ETag / version）を持つ。1 世代のみの `.bak` は上書きで失われるため、時刻付き世代管理とするか否かは要定義。
- 反映：「再起動」ボタン → サーバ側で新 config を再検証 → **自己再exec**（`std::env::current_exe()`、systemd 非依存）。再検証失敗なら再起動せずエラー返却（壊れた設定で自己再exec してループしない）。
- CSRF ガード（変更系 API）は `Origin` と `Host` を比較する。`Host` が無い HTTP/2 では URI の authority を使い、ホスト名の比較は ASCII の大文字小文字を無視する。
- 再起動はチャンネルスキャン実行中なら **409 `Already Scanning`** を返す（スキャンが `channels.yml` を read-merge-write している最中の `exec` を避ける）。
- app bar の **Re-scan** ボタンは `POST /webui/api/services/scan` を呼び、進捗ログをモーダルにストリーミング表示する。完了後に Services タブと Status の件数を更新する。実行中の再クリックや他のスキャンは 409 になる。`channels.yml` は変更しない。
- ステータス表示用の `channels.yml` エントリ数は `(path, mtime, size)` をキーにキャッシュし、ファイルが変わったときだけ再パース・再検証する。
- `/api/config/{tuners,channels}`（Mirakurun 互換の mutation path）は実装しない。非対応は README 制限欄に明記する。

## 11. プロジェクト構成

```
unituner/
  Cargo.toml            # single binary crate
  src/main.rs           # clap: --config-dir (default /etc/unituner)
  src/config.rs         # server/tuners/channels/pipelines YAML 読込 + validate (+ webui 保存/原子書き込み/.bak)
  src/tsframe.rs        # 188/204 packetizer (204→188 正規化は pipeline 出力側)
  src/si.rs             # PAT/PMT/SDT 解析 (arib クレート呼出 or 手書き)
  src/tsfilter.rs       # 内蔵 TSFilter (PAT 書き換え + PID 通過制御)
  src/services.rs       # in-memory ServiceTable + services.json 永続化 + fingerprint
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
7. 起動時サービススキャン + チャンネル探索 API（priority −1000、実行中チャンネルは共有 TS から更新、アイドルなら一時チューン、既定 25s/ch（SI が揃えば早期終了）。探索結果は channels.yml へ追加のみ・手動再起動）
8. 統合テスト + README

## 13. 検証（end-to-end）

- `cargo test`（unit + 統合。配信バイト == 入力バイト、503、奪取、優先度同値は FCFS、scan→`/api/services` 反映、decode=0/false バイパス、内蔵 TSFilter の PID 絞り込みと PAT 単一 program 化）
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
- `X-Mirakc-Tuner` / `X-Mirakurun-Tuner` 同時指定時の優先規則
- per-IP/per-session cap の既定値
  - 確定: `max_subscribers_per_session: 32` / `max_subscribers_per_ip: 16` / `max_sessions: 16` / `max_subscriber_buffer_bytes: 2147483648`（2 GiB）。超過は 429。2 GiB は 32 購読者分（64 MiB × 32）にあたるため、通常は per-session 上限の方が先に効く。詳細は §7。
- `.bak` の世代管理方針
- `PUT /api/config/channels/scan` の `skipCh` / `minSubCh` / `maxSubCh` のカンマ区切り入力の正規化

## 15. README 制限欄に必ず書くこと

- `decode=0` / `decode=false` は pipeline 全段 bypass かつ正規化なし（mirakurun の B-25 スキップとは異なる）。TLV 源（BS4K 等）では pipeline が TLV→TS 変換を担うため、`0` / `false` は TS ではなく**生 TLV/MMT** を返す（TS-only 原則の許容済み例外）。`decode` のその他の値（`1` / `true` / 無し以外）は 400
- pipeline はチューナー出力（188 / 204 / TLV/MMT のいずれでも）を byte-for-byte で受け、**必ず MPEG-TS を出力**する。TLV/MMT → TS 変換は pipeline の責務で、unituner は pipeline 後の TS だけを正規化して扱う。`dantto4k` / `mmt2ts` およびその前段の `b61dec-remote` は TLV 入力前提
- priority の multi-header 集約は未対応（単一値前提）
- `X-Mirakc-Tuner` / `X-Mirakurun-Tuner` が service stream にも効く（mirakc-BS4K からの拡張）
- `/api/config/{tuners,channels}` の GET/PUT（設定 CRUD）は非対応（`PUT /api/config/channels/scan` のみ例外）
- service stream の EIT は既定で落とす。クライアントが EIT を直接読む場合は `server.yml` の `service_stream_keep_eit: true` にする。unituner は EIT を解析・スコープしないため、このとき全サービスの EIT（EIT-other 含む）が加工されずに流れる
- IPTV はプレイリスト（`/api/iptv/playlist`）のみ実装済み。EPG は非実装なので `url-tvg` を付けず、`/api/iptv/xmltv` / `/api/iptv/epg` は 404。ロゴも非実装なので `tvg-logo` を出さない
- webui は認証なし（allowed_cidrs + allowed_hosts のみ。LAN 内のみに配備すること）
- `Host` が無い/空のリクエストは webui / `/api/*` とも受理する（BonDriver_Mirakurun 等のネイティブクライアント用）。存在するが許可外の `Host` は 403。ブラウザは authority 無しで送れないため DNS リバインディング対策は弱まらない
- 初回起動など `services.json` が無い/無効なとき、スキャン完了までは `/api/services/{id}/stream` が 404 しうる（有効なキャッシュがあれば起動直後から配信できる）
- `services.json` は無期限キャッシュである。放送再編（サービス名/SID の変更）は設定フィンガープリントを変えないため、自動検出されず webui の **Re-scan**（`POST /webui/api/services/scan`）が必要
- `routes` を持つチャンネルは**列挙したチューナーだけ**を使う（type 非依存）。フォールバックは起動失敗のときだけで、起動 probe（既定 1000 ms）のタイムアウトは「起動成功」として扱う（no signal はフォールバックしない）。pinning（`X-Mirakc-Tuner` / `X-Mirakurun-Tuner`）は routes を無視する
- webui の Channels は `routes` / `extra-args` も編集できる（route の追加・削除・並べ替えを含む）。
- `PUT /api/config/channels/scan` は curl 等で直接叩く（チャンネル探索は webui には出さない。webui の Re-scan はサービス表専用で `channels.yml` を書かない）。`async` / `GET` ポーリングは非対応。channels.yml 書き換え後は **RESTART REQUIRED**（自動再起動しない）
- 探索 API は既存エントリを維持し、**追加のみ**行う（type 単位の完全置換はしない。`refresh=true` のときのみ既存を置換）
- スキャンの SDT 完了後の猶予（1.5 秒）は時間の上限であり保証ではない。SDT 完了から 1.5 秒以内に NIT-actual が送出されない多重では、そのチャンネルの network id は SDT の `original_network_id` 由来になる
- DRCS グリフからデコードしたサービス名は U+FFFD になる（DRCS は Unicode に対応付けがない）
