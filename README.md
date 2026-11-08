# unituner

チューナー共有と抽象化に特化した mirakurun 互換サーバ。外部コマンドのチューナーを排他制御し、mirakurun 互換 API のサブセットで MPEG-TS を配信する。EPG・録画・ロゴ・MMT/TLV の内部処理は扱わない。

実装仕様は [docs/SPEC.md](docs/SPEC.md)（日本語）、実機手順は [docs/TESTING.md](docs/TESTING.md)。

## 特徴

- **チューナー共有**: 同一 `(type, channel)` への要求は 1 本のプロセス木（チューナー + pipeline）に相乗りする。優先度に関係なく参加できる。
- **mirakurun 互換 API サブセット**: `/api/version` から `/api/tuners` まで。`/api/programs` は常に `[]`。
- **内蔵サービスフィルタ**: service stream は購読者ごとに独立したフィルタで PAT を 1 プログラムへ書き換え、対象 PMT/ES PID だけを通す。
- **`X-Mirakurun-Priority` による奪取**: `128` は無条件、それ以外は victim の最大 priority より真に大きいときのみ。
- **`X-Mirakc-Tuner` / `X-Mirakurun-Tuner` によるチューナー固定**: 指定チューナー以外へフォールバックしない。
- **pipeline による外部コマンド委譲**: 任意段数のコマンドを stdin→stdout で直列接続。シェルは介さない。
- **埋め込み web UI**: フロントエンドのビルド工程なし。設定の閲覧・編集と再起動。
- **起動時サービススキャン**: 起動直後にバックグラウンドで `channels.yml` を走査し、サービス表を構築する。チャンネル探索 API も持つ。

## ビルドと起動

```sh
cargo build --release
sudo ./target/release/unituner --config-dir /etc/unituner
```

- Rust 1.85 以上（`arib 0.3` が edition 2024 のため）。edition は 2021。
- `--config-dir` の既定は `/etc/unituner`。4 つの YAML を起動時に読み、検証に失敗したら非 0 で終了する。
- 待受アドレスは `server.yml` の `addr`。既定は `":40772"`（= `0.0.0.0:40772`）。
- ログは stderr。`UNITUNER_LOG`（未設定なら `RUST_LOG`）に `tracing` のフィルタを書く。既定は `info`。
- SIGINT / SIGTERM で全チューナーセッションを停止してから終了する。

## 設定

`--config-dir` 直下の 4 ファイルを読む。すべて必須。未知のフィールドは拒否する（`cmd:` のような typo は起動エラー）。検証は起動時 fail-fast で、問題はファイル名と該当項目付きで報告される。同じディレクトリにスキャン結果の `services.json` が自動生成される（手編集しない。後述の「サービスキャッシュ」を参照）。

### `server.yml`

```yaml
addr: ":40772"
allowed_cidrs:
  - 10.0.0.0/8
  - 127.0.0.0/8
  - 172.16.0.0/12
  - 192.168.0.0/16
allowed_hosts: []                 # web UI が追加で受け付ける Host 名（任意）
idle_timeout_secs: 5
subscriber_buffer_chunks: 1024
chunk_size_bytes: 65536
max_subscribers_per_session: 32
max_subscribers_per_ip: 16
max_sessions: 16
max_subscriber_buffer_bytes: 2147483648
tuner_startup_probe_ms: 1000
tuner_readiness_timeout_secs: 30
scan_channel_timeout_secs: 60
child_path: "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
child_home: "/var/lib/unituner"
child_tmpdir: "/tmp"
child_cwd: "/var/lib/unituner"
child_env: {}
```

| フィールド | 既定値 | 検証・挙動 |
|---|---|---|
| `addr` | `":40772"` | 空不可。`IP:port` として解釈できること。先頭 `:` は `0.0.0.0` を補う |
| `allowed_cidrs` | RFC1918 + loopback の 4 件 | 各要素が CIDR として妥当であること。空リストは全拒否 |
| `allowed_hosts` | `[]`（空） | web UI が `Host` として受け付けるホスト名の追加リスト。空なら loopback / `allowed_cidrs` 内の IP / `localhost` のみ受理。`Host` が無い/空のリクエストは受理する（BonDriver_Mirakurun 等のネイティブクライアント用。ブラウザは authority 無しで送れないため DNS リバインディング対策は弱まらない）。存在するが許可外の `Host` は 403。ポート・スキーム・ブラケットなしのホスト名か IP リテラル。逆プロキシや mDNS 名で公開する場合は必ず列挙する |
| `idle_timeout_secs` | `5` | 検証なし（0 も受理）。最後の購読者が抜けてからこの秒数後にプロセス木を止める |
| `subscriber_buffer_chunks` | `1024` | 1 以上。購読者ごとの bounded queue 容量（chunks） |
| `chunk_size_bytes` | `65536` | 1 以上。実際の chunk は 188 の倍数に切り下げる（65536 → 65424） |
| `max_subscribers_per_session` | `32` | 1 以上。1 セッションに同時接続できる購読者数。超えた要求は 429 |
| `max_subscribers_per_ip` | `16` | 1 以上。1 クライアント IP が全セッション合計で持てる購読者数。超えた要求は 429 |
| `max_sessions` | `16` | 1 以上。同時に存在できるセッション数（登録中の `Starting` / `Stopping` も含む）。超えた要求は 429。チューナー数が 16 を超える構成ではこの値も上げる |
| `max_subscriber_buffer_bytes` | `2147483648`（2 GiB） | 1 以上。全購読者キューの合計上限バイト数。1 購読者は `chunk_size_bytes × subscriber_buffer_chunks`（既定 ≈64 MiB）を worst-case で予約する。既定 2 GiB は 32 購読者分にあたり `max_subscribers_per_session: 32` と同じなので、通常は per-session 上限の方が先に効く。この予算は「最悪ケースの予約」であり実使用量ではない（実際のキューは配信中の分しか持たない）ため、2 GiB の予約は一般的なホストで許容できる。メモリが制約されたホストでは `max_subscriber_buffer_bytes` をさらに上げるか `subscriber_buffer_chunks` を下げる |
| `tuner_startup_probe_ms` | `1000` | 1 以上 10000 以下。`routes` を持つチャンネルの起動 probe 窓（ms）。窓内にチューナーが最初のデータを出せば起動成功、データ前に終了すれば起動失敗として次の route を試す。窓を過ぎても生きていれば（no signal でも）起動成功とみなす。`routes` を持たないチャンネルには効かない。**readiness gate とは別物**（下記） |
| `tuner_readiness_timeout_secs` | `30` | 1 以上 600 以下。最初のデータが来るまで待つ readiness gate（秒）。tune が 6–11 秒、デスクランブラの初回出力が 9–15 秒かかる構成でも既定で足りる。0 や 600 超は起動エラー |
| `scan_channel_timeout_secs` | `60` | 1 以上 600 以下。1 チャンネルの scan 予算（秒、acquire + SI 読み）。起動時スキャン・手動 Re-scan・探索 API のすべてで使う。`tuner_readiness_timeout_secs` より**厳密に大きい**こと（そうでなければ起動エラー）。acquire は `readiness + 5 秒` に制限し、残りを SI に使う |
| `child_path` | `/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin` | 子プロセスの `PATH`。空不可。サーバの `PATH` は継承しない |
| `child_home` | `/var/lib/unituner` | 子プロセスの `HOME`。空不可。未設定の `HOME` を継承させない（`dtv.names` 等の探索先を固定できる） |
| `child_tmpdir` | `/tmp` | 子プロセスの `TMPDIR`。空不可 |
| `child_cwd` | `/var/lib/unituner` | 子プロセスの作業ディレクトリ。`""` はサーバの作業ディレクトリを継承。存在しない値は起動時に warn し、各 spawn では `current_dir` を省略する（起動エラーにはしない） |
| `child_env` | `{}` | 子プロセスへ追加する環境変数。`PATH` / `HOME` / `TMPDIR` と同じ名前なら**こちらが優先**。名前は空でなく NUL・改行を含まないこと、値は NUL を含まないこと |

- `tuner_startup_probe_ms` は **route フォールバックの窓**、`tuner_readiness_timeout_secs` は **ストリーム開始までの待ち**。routes 使用時は probe のタイムアウトを「起動成功」として扱うため、readiness gate は実質使われない。
- 子プロセス（チューナーと pipeline の全段）は環境を継承しない。spawn ごとに `env_clear()` → `PATH` / `HOME` / `TMPDIR` → `child_env` の順に適用し、`child_cwd` が存在すれば `current_dir` を設定する。
- 遅いデスクランブラで `tuner failed to start: timed out waiting for the tuner to start` が出る場合は、実測に合わせて `tuner_readiness_timeout_secs` と `scan_channel_timeout_secs` を広げる（常に scan > readiness）。`no signal` はチューンできたが SI が無い場合であり、readiness タイムアウトとは別の結果。

`allowed_cidrs` はネットワーク制御であり認証ではない。API と web UI の両方に効く。

#### アクセス制限

- `allowed_cidrs` は TCP 接続元 IP の許可リスト。範囲外は API / web UI とも 403。
- `allowed_hosts` は web UI の `Host` 許可リスト。空なら loopback / `allowed_cidrs` 内の IP / `localhost` だけを受け付け、DNS リバインディングを防ぐ。名前や逆プロキシ経由で公開するなら、そのホスト名を列挙する。
- `Host` が無い/空のリクエストは受理する。BonDriver_Mirakurun のようなネイティブクライアントは `Host` を送らず、これを拒否するとチャンネル一覧は通っても選局・ストリームが 403 になるため。ブラウザは authority 無しでリクエストを送れない（HTTP/1.1 は `Host` 必須、HTTP/2 は `:authority`）ので、DNS リバインディング対策は弱まらない。**存在するが許可外**の `Host` は従来どおり 403。
- 購読者数とセッション数は `max_subscribers_per_session` / `max_subscribers_per_ip` / `max_sessions` / `max_subscriber_buffer_bytes` で制限する。上限を超えたストリーム要求は **429** `{"code":429,"reason":"Too Many Requests"}` を返し、既存の購読者とチューナーには影響しない（参加の判定はチューナー起動より前）。per-IP は `X-Forwarded-For` を無視し、TCP 接続元だけで数える。同時視聴を増やすには `max_subscriber_buffer_bytes` を上げるか `subscriber_buffer_chunks` を下げる。

### `tuners.yml`

```yaml
- name: px-s1ud
  types: [GR]
  command: "/usr/bin/recisdb tune -i /dev/dvb/adapter0/frontend0 -c T{channel} --no-decode -"
  disabled: false
```

| フィールド | 既定値 | 検証 |
|---|---|---|
| `name` | なし（必須） | 空不可・重複不可。`X-Mirakc-Tuner` の値になる |
| `types` | なし（必須） | 1 個以上。各要素が type 名正規表現に一致 |
| `command` | なし（必須） | 空不可。`{channel}` / `{type}` を使える |
| `disabled` | `false` | `true` のチューナーは選択対象外。pin で指定すると 400 |

### `channels.yml`

```yaml
- name: NHK総合
  type: GR
  channel: "27"
  extra-args: ""            # 任意・既定 ""。チューナーコマンドの {extra_args} に入る
  disabled: false
  routes:                   # 任意・既定 []（未指定は全チューナーから選択）
    - tuner: catv-tuner     # 必須: tuners.yml の name
      channel: "13"         # 任意: この route だけ channel を上書き
      extra-args: "--lnb 15"  # 任意: この route だけ extra-args を上書き
    - tuner: ptx-s110
```

| フィールド | 既定値 | 検証 |
|---|---|---|
| `name` | なし（必須） | 空不可・重複不可 |
| `type` | なし（必須） | `^[A-Za-z0-9][A-Za-z0-9_-]*$`（`GR`, `BS`, `CABLE_CATV` など任意名） |
| `channel` | なし（必須） | ASCII 英数字と `. _ + - : @ ,` のみ。`/` は URL パス 1 セグメントに収めるため拒否 |
| `extra-args` | `""` | 任意。`{extra_args}` に入る追加引数。`extra_args` 綴りも可。引用符・バックスラッシュ・改行は不可（standalone な argv 断片） |
| `disabled` | `false` | 一覧 API には出るが、起動時スキャンの対象外 |
| `routes` | `[]` | 任意。各要素は `tuner`（必須、`tuners.yml` の name で既存必須）と任意の `channel` / `extra-args`。同一チャンネル内でチューナー重複不可。`route.channel` は `channel` と同じ文字種検証を通る |

同じ `channel` 値でも `type` が違えば別エントリとして扱う。同じ `(type, channel)` の重複は起動エラー（fail-fast。mirakc-BS4K の warn + first wins は採用しない）。

#### ルート（`routes`）

同じ TS が複数の経路で取れるとき（例: CATV チューナーとローカルチューナー）や、片方が起動に失敗したときのフォールバック先を指定する。mirakc-BS4K の `channels[].routes` 互換で、`routes` はチャンネルタイプに依存しない。

- `routes` を書いたチャンネルは、**列挙したチューナーだけを書いた順に**試す。type 適合は従来どおり `tuners.types` で判定する。
- route ごとに、既存セッションの再利用 → フリーなチューナーの起動 → 下位 priority からの奪取、の順で検討し、使えなければ次の route へ進む。
- **フォールバックは起動失敗のときだけ**。チューナーコマンドが spawn できない、または最初のデータを出す前に終了した場合に次の route を試す。起動 probe（`tuner_startup_probe_ms`、既定 1000 ms）を過ぎても生きていれば、データが無くても（no signal でも）起動成功とみなしてフォールバックしない。
- 全 route が失敗すると **503** になる。
- `route.channel` / `route.extra-args` の上書きは、チューナーコマンドの `{channel}` / `{extra_args}` とセッションの同一性判定の両方に効く。
- セッションは実効 `(type, channel)` でキー付けされる。同じ実効 channel に解決される route は 1 セッションを共有し、その再利用は route の順序やチューナーより優先される（二重起動を防ぐため）。したがって同じ実効 channel の route は実効 `extra-args` も一致していなければならない。
- decode モードの衝突（**409**）と購読者/セッション上限（**429**）は terminal で、次の route を試さない。
- **pinning は routes に優先する**。`X-Mirakc-Tuner` / `X-Mirakurun-Tuner` を指定した要求は routes を無視し、そのチューナーだけを使う（フォールバック無し）。
- `disabled: true` のチャンネルは routes を試す前に 404。

### `pipelines.yml`

```yaml
pipelines:
  cas:
    - "arib-b25-stream-test"
types:
  GR: { pipeline: cas }
```

| 項目 | 既定値 | 検証 |
|---|---|---|
| `pipelines` | なし（必須） | 1 エントリ以上。各 pipeline は 1 コマンド以上、各コマンドは空不可 |
| `types` | 空（未バインド type は生 TS） | キーが type 名正規表現に一致し、値の `pipeline` が実在すること |
| `types.<type>.input` | `ts` | `ts` / `tlv` のみ。`tlv` は実在する pipeline へのバインドが必須で、その pipeline に TLV→TS 変換の実段（`passthrough` 以外）が 1 つ以上必要 |

`input` はチューナー出力の種類を宣言する。省略時は従来どおり `ts`（MPEG-TS）で、204→188 正規化を行ってから pipeline 1 段目へ渡す。BS4K のように **TLV/MMT** を出すチューナーは `input: tlv` を指定する。この場合 unituner は tuner stdout を一切正規化せず byte-for-byte で pipeline へ渡し、pipeline が出力する TS を pump 側で 188B に揃える。TLV に対して TS の `0x47` 同期バイト探索をかけるとストリームが破壊されるため、この区別は必須である。

### プレースホルダとコマンド実行

- チューナー `command` と pipeline の各段で `{channel}` / `{type}` / `{extra_args}` を使える。
- 置換は argv 分割の**前**に行う。`{channel}` は `27` / `BS13_1`、`{type}` は `GR` / `BS`、`{extra_args}` はチャンネル（`routes` 使用時は route）の実効値になる。
- 未知の `{...}` は空白にせず**そのまま残す**（`{chanel}` のような typo はコマンド側で失敗する）。
- `{extra_args}` が空のときは何も置換しない。引用符なしの `cmd {channel} {extra_args}` は `{extra_args}` のトークンが消え、引用した `"{extra_args}"` は空トークン 1 個になる（シェルと同じ）。
- `{channel}` の値は英数字と `. _ + - : @ ,` のみ、`{type}` は上記正規表現。URL パス由来の値にも同じ検証を適用する。`{extra_args}` は `--lnb 15` のように複数トークンへ展開する前提のため charset ホワイトリスト検証は行わないが、定義値に引用符・バックスラッシュ・改行は含められない。
- コマンドは argv 分割して**直接 spawn** する。`/bin/sh -c` は使わない。引用符・バックスラッシュは解釈するが、変数展開・glob・リダイレクトは行わない。
- チューナーと pipeline の全段は環境を継承しない。`server.yml` の `child_path` / `child_home` / `child_tmpdir` / `child_env` / `child_cwd` が spawn ごとに適用される（§`server.yml` の表を参照）。`dantto4k` 等が `dtv.names` を `HOME` から探す構成では `child_home` を固定できる。

### サービスキャッシュ（`services.json`）

スキャン結果は `--config-dir` 直下の `services.json` に保存される。起動時はこれを読み、`/api/services` とサービスストリームをチューナー起動なしで提供する。

```json
{"version":1,"fingerprint":"0123456789abcdef","generated_at":"2026-11-09T12:34:56.789Z","services":[...]}
```

- `version` は現在 `1`。未知のバージョン・破損 JSON・欠落は警告して通常スキャンにフォールバックし、成功後に上書きする。起動は失敗しない。
- `fingerprint` は**有効な** `channels.yml` エントリ（`type` / `channel` / `extra-args` / `routes` と route ごとの上書き）を `(type, channel)` 順に正規化して FNV-1a 64 でハッシュしたもの。`disabled` エントリと `tuners.yml` は含まない。
- チャンネルの追加・削除・disable、`routes` / `extra-args` の変更はフィンガープリントを変え、次回起動時に自動再スキャンする。放送再編でサービス名/SID だけが変わった場合は自動検出されない。
- **無期限キャッシュ**。時間では失効しない。フィンガープリント変更か、web UI の **Re-scan** ボタン（`POST /webui/api/services/scan`）でだけ置き換わる。有効なキャッシュがある再起動では、前回 busy だったチャンネルの再試行は行わない。
- 書き込みは `services.json.tmp` → `fsync` → `rename` で原子的。Unix では `0600` で作成し、既存のモードを保持する。書き込み失敗は警告のみで、メモリ上のサービス表は有効なまま。
- `services: []` も「スキャン対象が無かった」という有効な結果として扱う。ただし**全チャンネルがチューナー起動失敗**で表が空の場合は、空キャッシュを書かない（次回起動時に再プローブする）。
- 手動 Re-scan は有効チャンネルを 1 パスだけスキャンし、削除/disable されたチャンネルのレコードを落とす。busy・無信号・チューナー起動失敗のチャンネルは以前のレコードを保持する。`channels.yml` は変更しない。
  - 完了行は `**RE-SCAN COMPLETE**: N services (S channels scanned, F found, D stale records dropped; A no signal, B busy, C tuner failed; wrote ...)`。`busy` は preempted を含み、`tuner failed` は `NoTuner` を含む。
  - 全チャンネルがチューナー起動に失敗した場合は、チャンネルごとの行の後に `all N channels failed to start their tuner — check tuners.yml (command/device)` を 1 行追加する。一部だけ失敗したときは出さない。

## パイプライン

`pipelines.<name>` は stdin→stdout で直列につながる外部コマンドの配列。`input: ts`（既定）では 1 段目の stdin にチューナー出力（188/204 を 188 に正規化したもの）が入る。`input: tlv` では正規化せず TLV/MMT をそのまま渡す。最終段（pipeline 無しの場合は tuner）の出力は pump で 188B に正規化されて broadcast される。パイプライン通過後の TS だけが service 検出・フィルタ・配信に使われる。

```yaml
pipelines:
  cas:
    - "arib-b25-stream-test"
  cas4k:
    - "arib-b25-stream-test"
    - "ts-converter <args>"
  none:
    - "passthrough"       # 何もしない（生 TS をそのまま流す）
  mmt:                    # BS4K: TLV/MMT → MPEG-TS
    - "b61dec-remote <args>"   # 前段も TLV 入力前提
    - "dantto4k"               # または "mmt2ts"
types:
  GR:   { pipeline: cas }
  BS:   { pipeline: cas4k }
  BS4K: { pipeline: mmt, input: tlv }
```

- BS4K チューナーは TS ではなく TLV/MMT を出す。`input: tlv` を付けなければ unituner が TLV を TS として正規化し、`0x47` を探して捨ててしまうため、pipeline にはゴミしか届かず scan が `no signal` になる。`dantto4k` / `mmt2ts` のチェーンは `input: tlv` で使い、その前段の `b61dec-remote` も TLV を受ける。
- 型にバインドしない場合は生 TS をそのまま配信する（`types` に書かなければよい）。
- 段として `passthrough` とだけ書くと、外部プロセスを spawn しない疑似コマンドになる（前後の空白を除き ASCII 大文字小文字を無視して完全一致）。
  全段が `passthrough` ならバインドなしと同じ生 TS になり、混合チェーンではその段だけが除かれる。`passthrough` はプレースホルダ置換より前に判定される。`input: tlv` の型では全段 `passthrough` を指定できない（TLV→TS 変換が無いため起動エラー）。
- 各段は `setsid` で独立したプロセスグループになる。停止はグループ全体へ SIGTERM → 2 秒の猶予 → SIGKILL。
- 段の stderr はセッションラベルと段番号付きでログに出る（`error` / `fail` / `fatal` を含む行は `warn`）。
- コマンドが 1 つも起動できなかった場合、起動済みの段は kill され、セッションは作られない。

## API

ベースは `http://host:40772`。すべて JSON。エラーは `{"code":<status>,"reason":"…","errors":[]}` 形式（`errors` は通常空で、設定検証エラー時のみ issue の一覧が入る）。

| メソッド | パス | 内容 |
|---|---|---|
| GET | `/api/version` | `{"current":"<version>","latest":"<同じ>"}`（更新チェックなし） |
| GET | `/api/status` | 最小の mirakurun 風 status。EPG の走査情報は空、`streamInfos` は `[]` 固定 |
| GET | `/api/channels` | 設定済みチャンネル一覧（`disabled: true` も含む）。各要素に `services` を簡易埋め込み |
| GET | `/api/channels/{type}` | type で絞り込み。不正な type 名は 400 |
| GET | `/api/channels/{type}/{channel}` | チャンネル詳細。未設定は 404 |
| GET / HEAD | `/api/channels/{type}/{channel}/stream` | pipeline 通過後の TS（チャンネル全体） |
| GET / HEAD | `/api/channels/{type}/{channel}/services/{id}/stream` | そのチャンネル上の 1 サービス（内蔵フィルタ適用） |
| GET | `/api/services` | サービス一覧。`id` は `networkId * 100000 + serviceId` |
| GET | `/api/services/{id}` | サービス詳細。`{id}` は item id か sid |
| GET / HEAD | `/api/services/{id}/stream` | 1 サービス（内蔵フィルタ適用） |
| GET | `/api/tuners` | チューナー状態と利用者一覧 |
| GET | `/api/tuners/{index}` | 単体。数値でない・範囲外は 404 |
| GET | `/api/programs` | 常に `[]`（EPG 非実装） |
| GET | `/api/programs/{id}` | 404 |
| PUT | `/api/config/channels/scan` | チャンネル探索（`text/plain` ストリーミング） |
| GET | `/api/config/channels/scan` | 404（`async` 非対応） |

サービスの JSON は `logoId: 0`、`hasLogoData: false` を常に持つ（ロゴは扱わない）。`channel` は探索で紐付くまで `null`。

### 共通ヘッダ

- 全レスポンス（web UI 含む）に `Server: unituner/<version>`。
- ストリーム応答は `Content-Type: video/MP2T`、`Cache-Control: no-store`、`X-Mirakurun-Tuner-User-ID: <接続元 ip:port>`。
- `HEAD` はストリーム系 3 ルートで利用可。GET と同じパス・ヘッダ検証を行うが、セッションを取得しないためチューナーは動かない。

### 要求ヘッダ

| ヘッダ | 意味 |
|---|---|
| `X-Mirakurun-Priority` | 要求優先度。既定 `0`。整数でない値は 400。複数ある場合は最後の非空値を使う |
| `X-Mirakc-Tuner` | チューナー固定。`tuners.yml` の `name` |
| `X-Mirakurun-Tuner` | 同上の alias。両方ある場合は `X-Mirakc-Tuner` が優先 |
| `X-Mirakurun-Tuner-User-Provider` | `/api/tuners` の `users[].provider` に反映 |
| `X-Mirakurun-Tuner-User-Agent` | 同上（`agent`） |

pin の挙動:

- 未知の名前、`disabled: true`、type 不一致は 400。
- 固定先が busy の場合は優先度による奪取判定に従い、不可なら 503。別チューナーへのフォールバックはしない。
- 同じ `(type, channel)` が既に別チューナーで動いている場合、pin 付き要求は 503（既存セッションには相乗りしない）。
- pin はチャンネルの `routes` を無視する（そのチューナーだけを使い、フォールバックしない）。
- pin は 3 つの stream ルートすべてに効く（`/api/channels/.../stream`、`/api/channels/.../services/{id}/stream`、`/api/services/{id}/stream`）。mirakc-BS4K が channel stream のみ対応なのに対する拡張。

### 優先度と奪取

- 同じ `(type, channel)` の既存セッションには優先度に関係なく参加する。
- 空きチューナーがあればそれを使う。
- 空きがない場合、`priority >= 0` かつ victim の**最大** priority より真に大きければ奪取する。`128` は無条件。負の優先度とスキャン（`-1000`）は奪取しない。
- 奪取は **kill-first**。victim をレジストリから外してプロセスグループを停止し、デバイスが解放されてから代替を spawn する。spawn に失敗した場合は 503 になり、そのチャンネルは次回要求まで停止する。
- コメント付きの応答はしない。奪取・pipeline 異常終了・チューナー終了はいずれも、ヘッダ送信後の **EOF** として見える。

### `decode=0`

`?decode=0` は **pipeline 全段をバイパス**し、`input: ts`（既定）ではチューナー出力を正規化しただけの TS を配信する（mirakurun の B-25 のみスキップとは異なる）。service stream の内蔵フィルタは引き続き適用される。

`input: tlv` の型では pipeline が TLV→TS 変換そのものなので、`decode=0` は **生の TLV/MMT をそのまま配信する**。188B アラインも行われないため、mirakurun クライアントがこの型に `decode=0` を付けると TS ではなく TLV を受け取る点に注意する。

decode モードはセッションの属性であり、同一チャンネルに 2 つのプロセス木は作らない。既に逆のモードで配信中の場合、その要求は参加も奪取もせず **409**。

### 主なステータスコード

| コード | 条件 |
|---|---|
| 400 | 不正な type / channel 値、優先度ヘッダの不正、未知・disabled・type 不一致の pin、探索クエリの不正 |
| 403 | `allowed_cidrs` 外からの接続、web UI の変更系 API の CSRF チェック失敗 |
| 404 | 未知のチャンネル・サービス・プログラム・チューナー index・ルート、`GET /api/config/channels/scan` |
| 409 | decode モードの衝突、探索の二重実行（`Already Scanning`）、web UI 保存の `If-Match` 不一致 |
| 429 | 購読者数・セッション数の上限超過（`max_subscribers_per_session` / `max_subscribers_per_ip` / `max_sessions` / `max_subscriber_buffer_bytes`） |
| 503 | チューナーなし・全 busy・起動失敗・シャットダウン中 |

## Web UI

`http://host:40772/` を開く。認証はなく、`allowed_cidrs` と `allowed_hosts` が境界。同一バイナリに埋め込まれた 1 ページ（`/assets/style.css`, `/assets/app.js`）で、ビルド工程はない。

| タブ | 内容 |
|---|---|
| Status | チューナー / セッション / 件数（`/webui/api/status`） |
| Services | 探索で見つかったサービス一覧（表示のみ）。app bar の **Re-scan** で再スキャン |
| Channels | `channels.yml` の追加・編集・削除・保存 |
| Tuners | `tuners.yml` の追加・編集・削除・保存 |
| Pipelines | `pipelines.yml` と type バインドの編集・保存 |
| Server | `server.yml` の閲覧のみ（誤った `allowed_cidrs` でロックアウトしないため） |

内部 API:

| メソッド | パス |
|---|---|
| GET / PUT | `/webui/api/tuners` |
| GET / PUT | `/webui/api/channels` |
| GET / PUT | `/webui/api/pipelines` |
| GET | `/webui/api/server-config`（`Allow: GET, HEAD`） |
| POST | `/webui/api/restart` |
| POST | `/webui/api/services/scan`（`text/plain` 進捗ストリーム、409 で `Already Scanning`） |

**Re-scan** ボタンは app bar にあり、`POST /webui/api/services/scan` を `Content-Type: application/json`（body `{}`）で呼ぶ。進捗はモーダルに逐次表示され、完了時に Services タブと Status の件数を更新する。`channels.yml` は書かない。実行中の 2 本目は 409、途中で閉じると `services.json` は更新されない。

保存の挙動:

- 候補 YAML は他 3 ファイルのコピーと一緒に一時ディレクトリへ置かれ、起動時と同じ `Config::load` で検証される。不正なら 400 で、ファイルは書かれない。
- 書き込みは `.bak` コピー → `<file>.tmp` → `fsync` → `rename`。`.bak` は 1 世代のみ。
- `GET` は内容の FNV-1a ハッシュを `ETag` で返す。`PUT` に `If-Match` を付けると楽観排他になり、不一致は 409。
- 変更系（`PUT` / `POST`）は `Content-Type: application/json` が必須。`Origin` を送る場合は `Host` と一致していること（`Host` の無い HTTP/2 では URI の authority と比較し、ホスト名は ASCII 大文字小文字を無視する）。満たさない場合は 403。

再起動ボタンは `POST /webui/api/restart` を呼ぶ。

1. ディスク上の設定を再読み込み・再検証する。不正なら 400 で、何も停止しない。
2. 実行ファイルの存在を確認してから全セッションを停止する。
3. 自分自身を `execve` で置き換える。成功時はレスポンスボディなしで接続が切れる（ページはそれを成功として扱い、応答が戻るまでポーリングする）。
4. `exec` に失敗した場合はセッションを再開し、500 を返す。

チャンネルスキャン実行中は 409 `Already Scanning` を返し、何も停止しない（スキャンが `channels.yml` を読み書きしている最中の `exec` を避ける）。

```sh
# 変更系 API は JSON の Content-Type が必要
curl -s -X POST -H 'Content-Type: application/json' -d '{}' \
  http://127.0.0.1:40772/webui/api/restart
```

## チャンネルスキャン

`PUT /api/config/channels/scan` は指定 type のチャンネル番号を実際にチューニングし、SDT/NIT からサービス名とサービス一覧を取得して `channels.yml` に追記する。応答は `text/plain` のストリーミングで、完了まで接続が開いたまま進捗が流れる。

```sh
# GR 13-62 を探索して追記
curl -N -X PUT 'http://127.0.0.1:40772/api/config/channels/scan?type=GR&minCh=13&maxCh=62'

# 保存もサービス表の更新もせず結果だけ確認
curl -N -X PUT 'http://127.0.0.1:40772/api/config/channels/scan?type=BS&dryRun=true'

# 既存エントリも含めて再列挙して置換
curl -N -X PUT 'http://127.0.0.1:40772/api/config/channels/scan?type=GR&minCh=13&maxCh=62&refresh=true'
```

```text
channel scanning... (type: "GR")

channel: "24" (12/50) [24%] ... -> 4 services found.
Found 1 channels for 24
...
**RESTART REQUIRED** to apply changes.
```

| パラメータ | 既定値 | 内容 |
|---|---|---|
| `type` | 必須 | 任意の type 名。type 名正規表現に一致すること |
| `minCh` / `maxCh` | 13 / 62（`BS` / `CS` は 1 / 24） | 探索するチャンネル番号の範囲 |
| `skipCh` | なし | スキップする番号。カンマ区切り |
| `refresh` | `false` | `true` で既存エントリを置換 |
| `dryRun` | `false` | ファイル書き込みもサービス表更新もしない |
| `scanMode` | `Channel`（`BS` / `CS` は `Service`） | `Channel` は 1 チャンネル 1 エントリ、`Service` は 1 サービス 1 エントリ |
| `setDisabledOnAdd` | `false`（`BS` / `CS` は `true`） | 新規追加を `disabled: true` で書く |
| `channelNameFormat` | type 依存 | `{ch}` / `{ch00}` / `{subch}`、`Service` モードでは `{sid}` も使える |
| `useSubCh` | `false` | サブチャンネル展開。`BS` は常に展開 |
| `minSubCh` / `maxSubCh` | なし | サブチャンネル範囲。カンマ区切り |
| `async` | — | 無視される（常に同期実行） |

挙動:

- 既存エントリは維持し、**追加のみ**行う。他の type には触らない。
- `refresh=true` では、そのスキャンで実際に SI が読めたチャンネルだけを置換する。無信号・busy のチャンネルは既存エントリを保持する（失敗した更新で設定が消えない）。
- `disabled: true` のエントリは再スキャン対象。
- 失敗は種類ごとに区別して表示する（混同しない）:
  - `no signal` — チューンはできたが、期限までに使用可能な SI が無かった。
  - `tuner failed to start: <理由>` — チューナーコマンドの spawn 失敗、最初のデータ前の終了、または予算内に ready にならなかった（例: `timed out waiting for the tuner to start`）。spawn エラーは展開後の argv（`/nonexistent/tuner 27`）を表示する。デバイス未接続・パス誤り・コマンド異常が典型。**`no signal` とは別物**。
  - `skipped (no tuner for type <type>)` — その type をサポートする有効チューナーが無い（設定不備）。
  - `skipped (tuner busy)` / `skipped (preempted by a user request)` — 一時的な競合。
- その type の有効チューナーが複数なら、その台数を上限に並列でスキャンする（1 台なら従来どおり直列）。
- 候補は 1 リクエストあたり 512 まで（サブチャンネル展開を含む）。`skipCh` も 512 件まで。超過は 400。
- 1 チャンネルあたりの上限は `scan_channel_timeout_secs`（既定 60 秒、チューニング + SI 読み取り）。起動時スキャン・手動 Re-scan・探索 API のすべてで同じ値を使う。PAT・PMT・SDT-actual・NIT-actual が揃えば上限を待たずに終了する（SDT 完了後に残りのコアテーブルを待つ猶予は最大 1.5 秒（NIT-actual の 1 周期以上））。
- 同時実行は 1 本のみ。2 本目は 409 `Already Scanning`。
- 書き込みは `.tmp` + `rename` で原子的に行い、マージ後の一覧を起動時と同じ規則で検証してから書く。意味的に同一の一覧ならファイルを書き換えない。
- 完了時に `**RESTART REQUIRED**` を出す。**自動再起動はしない**（起動中のサーバはメモリ上の設定を保持し続ける）。
- クライアントが切断するとスキャンは中断し、`channels.yml` は書かれない。

### 起動時スキャン

リスン開始後、有効な `channels.yml` の全エントリを 1 回ずつチューニングし、見つかったサービスをメモリ上のサービス表へ upsert する。ただし `services.json` のスキーマバージョンとフィンガープリントが現在の設定に一致する場合は、**チューナーを起動せず**キャッシュを復元して即座に終了する。

- 優先度 `-1000` で動くため、利用者のセッションを奪取しない。busy なチューナーはスキップする。
- 有効なチューナーが複数ある type では、その台数を上限に並列でスキャンする（1 台なら従来どおり直列）。busy / 奪取されたチャンネルはそのパスを塞がず、再試行キューに回る。
- PAT・PMT・SDT-actual・NIT-actual が揃ったチャンネルは、`scan_channel_timeout_secs`（既定 60 秒）の予算を待たずに終了する（SPEC §8.1）。
- busy / 利用者に奪取されたチャンネルだけを 30 秒 → 60 秒 → 120 秒 → 240 秒 → 300 秒（上限）の backoff で再試行する。無信号・チューナー不在（`no tuner for type`）・起動失敗（`tuner failed to start`）は再試行しない（待っても状態が変わらず spin するだけ）。
- 1 パスで 1 チャンネル以上を試し、そのすべてが `NoTuner` / `TunerFailed` / `Failed` だった場合は、そのパスの終わりに `every channel failed to start its tuner; check tuners.yml (command/device path)` を 1 回 warn する（一部だけの失敗や 0 チャンネルでは出さない）。
- チューナーが 1 台も起動できなかった初回パスでは、空の `services.json` を「有効なキャッシュ」として書かない（次回起動時に再プローブする）。
- 最初のパスで 1 件以上見つかった、または再試行対象が残らない場合に `services.json` を書く。以後の再試行パスは表が変わったときだけ更新する。全チャンネル busy の初回パスでは空キャッシュを書かない。
- サービス表は削除しないため、失敗したチャンネルは前回の内容を保持する（ただし起動時の完了パス後、設定から消えたチャンネルのレコードは落とす）。
- サーバのシャットダウン時に停止する。

### スキャンが失敗したときの見分け方

- **全チャンネルが `tuner failed to start: ...`**: チューナーコマンド／デバイスを確認する。典型的には DVB デバイスのパス（例: `/dev/dvb/adapter0/frontend0`）が存在するか、コマンドが実行可能か、権限があるか。`/dev/dvb` が無い（機器未接続）状態では、実チューナーコマンドは即座に終了し、全チャンネルがこれになる。
- **`skipped (no tuner for type <type>)`**: `tuners.yml` にその type をサポートする有効なチューナーが無い。設定の `types` を確認する。
- **`tuner failed to start: timed out waiting for the tuner to start`**: readiness gate の既定 30 秒以内に最初のデータが来なかった。遅いデスクランブラ（`dantto4k` 等が 9–15 秒かかる構成）でも通常は足りるが、さらに遅い場合は `tuner_readiness_timeout_secs` を上げ、`scan_channel_timeout_secs` もそれより大きくする。
- **`no signal`**: チューナーは起動してデータを流したが、期限内に使用可能な SI（PAT/PMT/SDT/NIT）が無かった。アンテナ・受信状態・チャンネル番号を確認する。readiness タイムアウトとは別の結果。
- **`skipped (tuner busy)` / `skipped (preempted by a user request)`**: 他の利用と競合した。起動時スキャンは backoff で再試行する。

## 実機での確認

PX-S1UD（ISDB-T）と recisdb の例。`arib-b25-stream-test` は pipeline の 1 段目に置く。

```yaml
# tuners.yml — recisdb は T27 のようなプレフィックス付きチャンネル名を要求する。
# channel 値は "27" のまま API 互換を保ち、プレフィックスはプレースホルダの外に置く。
- name: px-s1ud
  types: [GR]
  command: "/usr/bin/recisdb tune -i /dev/dvb/adapter0/frontend0 -c T{channel} --no-decode -"
```

```yaml
# pipelines.yml
pipelines:
  cas:
    - "arib-b25-stream-test"
types:
  GR: { pipeline: cas }
```

```sh
BASE=http://127.0.0.1:40772

curl -s $BASE/api/version
curl -s $BASE/api/channels | jq 'length'
curl -sI $BASE/api/channels/GR/27/stream         # HEAD はチューナーを動かさない

# ストリームの 188 バイトアラインを確認
curl -s $BASE/api/channels/GR/27/stream | head -c 1000000 > ch.ts
python3 -c "d=open('ch.ts','rb').read(); print(len(d), len(d)%188, hex(d[0]))"
```

- recisdb の `tune` では `-t` を書かない（省略が無制限。`-t 0` は 1 バッファで終了する）。`--no-decode` で recisdb 自身のデコードを切り、デスクランブルは pipeline に任せる。
- パケットドロップはこの構成では**仕様**であり、異常ではない。追跡・報告しない。
- ISDB-T デバイスでは BS/CS は受信できない（`FE_SET_PROPERTY: Invalid argument`）。BS/CS を扱うには該当デバイスと別チューナーエントリを用意する。
- 詳しい手順と実測結果は [docs/TESTING.md](docs/TESTING.md)。

### 性能

PX-S1UD（ISDB-T、単一チューナー）+ recisdb + arib-b25-stream-test の構成での実測値。**参考値であり保証値ではない**（チャンネル・受信状態・ホスト性能で変わる）。

| 項目 | 実測 | 内訳 |
|---|---|---|
| 冷たいチャンネルへの切り替え（TTFB） | 約 2.0〜2.2 秒 | recisdb 自身が最初のバイトまでに約 2.05 秒。unituner のオーバーヘッドは約 0.0〜0.15 秒 |
| 配信中セッションへの再参加 | ほぼ即時（5 ms 未満） | 既存セッションへの相乗りだけで、チューナーを起動しない |
| 同じ冷チャンネルへの同時 3 要求 | 3 本とも同じ時刻に最初のバイト | spawn は 1 回だけ。1 本のセッションを共有する |
| 起動時スキャン（GR 12 チャンネル） | 約 38 秒（約 3.2 秒/チャンネル） | recisdb のチューンと SDT の繰り返し待ちが支配的。SI が揃うチャンネルは 60 秒の予算を使い切らない |
| 配信中セッションの奪取 | 冷たいチャンネルと同程度 | kill-first の victim 停止（SIGTERM → プロセスグループ消滅の確認）は約 50 ms 未満 |

- 待ち時間の大半は recisdb のチューニング（同調と最初の TS パケット）であり、unituner 側はプロセス起動・readiness gate（`tuner_readiness_timeout_secs`、既定 30 秒）・ヘッダ生成のみ。
- 「同時 3 要求」が同じ時刻に始まるのは `(type, channel)` 単位のセッション共有が効いているため。2 本目以降は spawn しない。
- 奪取は kill-first（victim を止めてデバイスを解放してから spawn）なので、冷たいチャンネルと同じチューン時間に victim の停止時間が加わる。

## 制限事項

- EPG・録画・タイムシフト・ロゴ（CDT）・MMT/TLV の内部処理・websocket・HDHR/xmltv は実装していない。TLV は pipeline 内の外部コマンドで MPEG-TS に変換してから渡す前提。
- `/api/programs` は常に `[]`、`/api/programs/{id}` は 404。
- `/api/config/{tuners,channels}` の GET/PUT（設定 CRUD）は非対応。例外は `PUT /api/config/channels/scan` のみ。
- チャンネル探索の `async` と `GET` ポーリングは非対応。`GET /api/config/channels/scan` は 404。
- 複数の `X-Mirakurun-Priority` は集約しない（最後の非空値を使う）。
- `decode=0` は pipeline **全段**バイパス。mirakurun の「B-25 デコーダのみスキップ」とは異なる。`input: tlv` の型では TS ではなく生 TLV/MMT を返す。
- `input: tlv` は tuner stdout を byte-for-byte で pipeline へ渡し、pipeline 出力の TS を pump で正規化する。TLV を TS 正規化してから pipeline に渡す構成は不可。`dantto4k` / `mmt2ts` とその前段の `b61dec-remote` は TLV 入力前提。
- web UI に認証はない。`allowed_cidrs` と `allowed_hosts` のみで、設定編集と再起動ができるため実質的に遠隔操作面になる。信頼できる LAN に置くこと。
- `services.json` が無い初回起動やキャッシュ無効時、スキャンが完了するまで `/api/services/{id}/stream` は 404 になりうる（有効なキャッシュがあれば起動直後から配信できる）。
- `services.json` は無期限キャッシュ。放送再編（サービス名/SID の変更）はフィンガープリントを変えないため自動検出されず、web UI の **Re-scan** が必要。busy だったチャンネルは、キャッシュが書かれた後の再起動では再試行されない。
- スキャンは専用の大型購読バッファを持たず、通常購読者と同じ `subscriber_buffer_chunks` を使う（SPEC §8 の専用バッファ要件は未実装）。
- スキャンの SDT 完了後の猶予（1.5 秒）は時間の上限であり保証ではない。SDT 完了から 1.5 秒以内に NIT-actual が送出されない多重では、そのチャンネルの network id は SDT の `original_network_id` 由来になる。
- DRCS グリフからデコードしたサービス名は U+FFFD になる（DRCS は Unicode に対応付けがない）。
- pin 指定時、同じ `(type, channel)` が別チューナーで配信中なら 503。pin が既存セッションのチューナーと一致する場合のみ相乗りする。
- チューナー出力のフレーミングは 188 / 204 のみ対応。192 は検出するが配信しない。
- 購読者数・セッション数には上限がある（`max_subscribers_per_session` / `max_subscribers_per_ip` / `max_sessions` / `max_subscriber_buffer_bytes`）。超過した要求は 429。per-IP は TCP 接続元のみで数え、`X-Forwarded-For` は無視する。
- 奪取後の spawn 失敗時は、そのチャンネルが次回要求まで停止する（kill-first のトレードオフ）。
- `routes` を持つチャンネルは**列挙したチューナーだけ**を使う（type 非依存）。フォールバックは起動失敗のときだけで、起動 probe（既定 1000 ms）のタイムアウトは「起動成功」として扱い、no signal ではフォールバックしない。pinning（`X-Mirakc-Tuner` / `X-Mirakurun-Tuner`）は routes を無視する。

## ライセンス

MIT。[Cargo.toml](Cargo.toml) を参照。

- 仕様: [docs/SPEC.md](docs/SPEC.md)
- 実機テスト手順: [docs/TESTING.md](docs/TESTING.md)
