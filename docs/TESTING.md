# unituner 実機テスト手順 (PX-S1UD + recisdb + arib-b25-stream-test)

本書は unituner を実チューナーで動かして確認するための手順書。SPEC (`docs/SPEC.md`) §3 / §8 / §15 と整合する。

- 対象ホスト: この Linux ホスト
- チューナー: PX-S1UD (ISDB-T, USB) → `/dev/dvb/adapter0/{frontend0,dvr0,demux0}`
- チューナーバックエンド: **recisdb**（`dvbv5-zap` は使わない）
- CAS: 外部 B25 デコーダ **`arib-b25-stream-test`**（pipeline 内の外部コマンド）

> **重要な前提**
> - パケット drop（"drop"）はこの構成では**正常**。追わない・報告しない。
> - チューナープロセスが終了すると、そのセッションの TS も終わる（EOF として見える）。

---

## 1. recisdb

### 1.1 インストール / バージョン確認

```sh
recisdb --version
# recisdb 1.3.2
which recisdb
# /usr/bin/recisdb
```

`recisdb` は v1.3.2 が `/usr/bin/recisdb` に入っている前提。バージョンが違う場合は
`recisdb --help` で `tune` のオプション名を必ず確認すること（後述の `-i` / `-d` 差分がある）。

### 1.2 デバイスパス

| 用途 | パス |
|---|---|
| フロントエンド（チューニング） | `/dev/dvb/adapter0/frontend0` |
| DVR（TS 読み出し） | `/dev/dvb/adapter0/dvr0` |
| demux | `/dev/dvb/adapter0/demux0` |

recisdb に渡すのは **frontend0 のパス**。

### 1.3 checksignal（ロック確認）

`checksignal` のデバイス指定は **`-d` / `--device`**。

```sh
recisdb checksignal -d /dev/dvb/adapter0/frontend0 -c T27
```

- ロックすると S/N 比 (dB) を連続出力し続ける（**自動終了しない**ので `timeout` を付ける）。
- ロックしないと数秒リトライ後に `ERROR: No signal.` で exit 255。

```sh
# 例: 13〜30ch を順に確認
for ch in $(seq 13 30); do
  printf '%s: ' "T$ch"
  timeout 12 recisdb checksignal -d /dev/dvb/adapter0/frontend0 -c "T$ch" 2>&1 \
    | grep -qE 'dB' && echo LOCK || echo no-signal
done
```

> 注: `-c 27` のような数字だけの指定は**不可**。`T27` / `C30` / `BS01_0` / `CS02` という
> recisdb のチャンネル名で指定する。

### 1.4 tune（TS 取得）

`tune` のデバイス指定は **`-i` / `--device`**（`checksignal` の `-d` とは異なる。`-d` を渡すと
`unexpected argument '-d' found` で即エラー）。

```sh
# 8 秒だけ取得してファイルへ
recisdb tune -i /dev/dvb/adapter0/frontend0 -c T27 -t 8 - > sample.ts

# 無制限に流し続ける（unituner の tuner command はこちら）
recisdb tune -i /dev/dvb/adapter0/frontend0 -c T27 --no-decode -
```

- 最後の `-` が「stdout へ出力」の指定。
- **`-t 0` は「無制限」ではない**。この実装では 1 バッファ（32KB）出力して即終了する。
  無制限にしたい場合は **`-t` 自体を省略する**（ログに `Recording duration: Infinite` と出る）。
- `-t -1` は引数エラー（`-1` がフラグ扱い）。負値で無制限にはできない。省略すること。
- `--no-decode` を付けると recisdb 自身の B-CAS デコードを無効化する。スマートカード
  リーダーが無い環境では付けないと
  `Failed to initialize the decoder. (BCAS_CARD_ERROR_NO_SMART_CARD_READER)` が出て
  自動的にデコード無効へフォールバックする（動作は継続するが、ログが汚れる）。
  unituner 側は CAS を外部 B25 デコーダ（`arib-b25-stream-test`）に任せるので
  `--no-decode` を付ける。
- 出力は 188 バイト固定の MPEG-TS（204/192 ではない）。先頭から `0x47`。

### 1.5 チャンネルフォーマット

| 種別 | 例 | 意味 |
|---|---|---|
| 地上波 (ISDB-T) | `T27` | 地上デジタル 27ch |
| CATV | `C30` | CATV 30ch |
| BS (ISDB-S) | `BS01_0` | BS01ch のサブチャンネル 0 |
| CS (ISDB-S) | `CS02` | CS02ch |

- プレフィックスは必須。`T`/`C` は terrestrial / CATV、`BS`/`CS` は衛星。
- BS/CS はサブチャンネル表記（`_0`）を取る。このホストは ISDB-T チューナー（PX-S1UD）なので
  `BS`/`CS` は `FE_SET_PROPERTY: Invalid argument` となり**受信できない**（想定どおり）。

### 1.6 このホストでの実測結果（2026-11-05）

| チャンネル | 結果 | S/N |
|---|---|---|
| T13 | no signal | — |
| T14 | no signal | — |
| T16 | no signal | — |
| T20 | no signal | — |
| **T24** | **LOCK** | 39.7〜41.2 dB |
| **T25** | **LOCK** | 42.7〜44.3 dB |
| **T26** | **LOCK** | 41.2〜42.7 dB |
| **T27** | **LOCK**（最良） | 44.3〜45.8 dB |
| T28 / T29 / T30 / T62 | no signal | — |
| C30 | no signal | — |
| BS01_0 / CS02 | 非対応デバイス | `FE_SET_PROPERTY: Invalid argument` |

→ **T24〜T27 の 4 波のみ受信可能**。最も安定するのは T27。受信した TS から確認したサービス:

| ch | 局 | サービス ID (sid) |
|---|---|---|
| T24 | テレビ朝日 | 1064, 1065, 1066 (+ 1448 データ) |
| T25 | 日本テレビ | 1040, 1041, 1044 (+ 1424 データ) |
| T26 | NHK Eテレ | 1032, 1033, 1034 (+ 1416 データ) |
| T27 | NHK総合 | 1024, 1025 (+ 1408 データ) |

TSID はすべて `32736`。

### 1.7 キャプチャの検証

```sh
recisdb tune -i /dev/dvb/adapter0/frontend0 -c T27 -t 8 - > sample.ts
ls -l sample.ts
xxd -l 4 -s 0   sample.ts   # 47 ...
xxd -l 4 -s 188 sample.ts   # 47 ...
xxd -l 4 -s 376 sample.ts   # 47 ...
python3 -c "d=open('sample.ts','rb').read(); print(len(d), len(d)%188, d[0], d[188], d[376])"
```

実測: 8 秒で **17,661,952 バイト**（約 16.8 MiB、93946 パケット）。
`0/188/376` すべて `0x47`、全 93946 パケットで同期が崩れない（`bad sync = 0`）。

> 末尾に 104 バイトの端数（書き込み途中の不完全パケット）が残るのは正常。
> `-t` の終了が書き込み途中に来るためで、完全なパケットはすべて 188 アラインされている。
> unituner 側の 204→188 正規化も、この端数は捨てて完全パケットのみ扱う。

---

## 2. テスト用 config tree

`/tmp/opencode/unituner-test/config/` に配置済み。SPEC §3 と同じ 4 ファイル構成。
値はこのホスト向け（recisdb / PX-S1UD / GR 4 局）。CAS 段の実行コマンドは環境依存なので、
本書では外部 B25 デコーダ（`arib-b25-stream-test`）を例にしている。

### 2.1 `server.yml`

```yaml
addr: ":40772"

allowed_cidrs:
  - 10.0.0.0/8
  - 127.0.0.0/8
  - 172.16.0.0/12
  - 192.168.0.0/16

idle_timeout_secs: 5
subscriber_buffer_chunks: 1024
chunk_size_bytes: 65536
```

### 2.2 `tuners.yml`

```yaml
- name: px-s1ud
  types: [GR]
  command: "/usr/bin/recisdb tune -i /dev/dvb/adapter0/frontend0 -c T{channel} --no-decode -"
  disabled: false
```

### 2.3 `pipelines.yml`

```yaml
pipelines:
  cas:
    - "arib-b25-stream-test"

types:
  GR:
    pipeline: cas
```

### 2.4 `channels.yml`

```yaml
- name: "27"            # 物理チャンネル番号を推奨（NHK総合）
  type: GR
  channel: "27"
  disabled: false

- name: "26"            # 物理チャンネル番号を推奨（NHK Eテレ）
  type: GR
  channel: "26"
  disabled: false

- name: "25"            # 物理チャンネル番号を推奨（日本テレビ）
  type: GR
  channel: "25"
  disabled: false

- name: "24"            # 物理チャンネル番号を推奨（テレビ朝日）
  type: GR
  channel: "24"
  disabled: false
```

`name` は**物理チャンネル番号を推奨**する。`name` は設定の識別子でしかなく（一覧・詳細 API と web UI の表示、重複判定）、放送局名やサービス名は SDT から scan が取得する。物理番号は `channel` と対応し、改称・中継・サブチャンネルで変わらないので、長期運用で設定がぶれない。放送局名を書いても動作は変わらない（推奨であり必須ではない）。`name` は `type` をまたいで全体で一意なので、同じ番号が別 `type` にある場合は `C27` のように `type` を前置する。探索 API が生成する `name` は SDT のサービス名由来であり、物理番号に揃えたい場合は生成後に手で編集する。

### 2.5 なぜ `T{channel}` と書くのか

unituner の `{channel}` プレースホルダは `channels.yml` の `channel` 値を**そのまま**埋める。
recisdb は `T27` のような**プレフィックス付き**チャンネル名しか受け付けないため、

- `channel: "27"` + `command: "... -c T{channel} ..."` → `-c T27` ✅
- `channel: "T27"` + `command: "... -c {channel} ..."` → これでも動くが、`{channel}` の
  許容文字（ASCII 英数字と `. _ + - : @ ,`）に収まるものの、API パスが
  `/api/channels/GR/T27/stream` となり mirakurun 互換の「チャンネル番号」とずれる。

**プレフィックスをプレースホルダの外に出す**ことで、
API 上は `channel = "27"`（mirakurun 互換の素直な値）を保ちつつ、コマンドには `T27` が渡る。
BS/CS を追加する場合は `channel: "BS01_0"` のようにプレフィックス込みの値にし、
その type 用の tuner を別エントリで定義する（`-c {channel}` とする）のがよい。

### 2.6 起動確認

```sh
unituner --config-dir /tmp/opencode/unituner-test/config
# INFO loading configuration config_dir=/tmp/opencode/unituner-test/config
# INFO configuration loaded tuners=1 channels=4 pipelines=1 bindings=1 addr=:40772 ...
```

（現時点のフェーズ 1 実装は config 読込・検証まで。サーバ起動以降は §4 のチェックリストで確認する。）

---

## 3. arib-b25-stream-test（CAS / デスクランブル）

### 3.1 インストール

mirakurun と同じく npm のグローバルパッケージとして入れる。

```sh
sudo npm install arib-b25-stream-test -g --unsafe-perm
```

- コマンド名は `arib-b25-stream-test`。
- 引数なしで起動し、stdin のスクランブル TS を stdout へデスクランブルして書く。
- unituner は pipeline の 1 段としてこれを spawn する。

### 3.2 unituner での使い方

unituner は `arib-b25-stream-test` を argv 分割して直接 spawn し、stdin/stdout を
前段・後段に直列接続する（フィルタとして動く）。

```sh
cat sample.ts | arib-b25-stream-test > desc.ts
```

- 既定は無音（標準出力には TS のみ）。統計が必要ならデコーダのドキュメントを参照する。
- 出力は 188 バイト固定の MPEG-TS。

### 3.3 確認方法

```sh
# exit code 0、出力が 188 の倍数であること
python3 -c "d=open('desc.ts','rb').read(); print(len(d), len(d)%188, d[0])"
```

- 出力を `ffmpeg` で 1 フレーム抜き出し、正常な映像が得られることを目視確認する。
- これにより pipeline が「クリア TS がそのまま通るだけ」ではなく、実際に復号して
  いることが確認できる。
- 一部 ECM の復号に失敗しても異常ではない。**エラー扱いしない**（§5 参照）。

### 3.4 デコーダが無い場合の代替（ドキュメント用）

`arib-b25-stream-test` が入っていない環境では、pipeline を `cat` に差し替えて
「TS が素通りする経路」としてテストする:

```yaml
pipelines:
  cas:
    - "cat"

types:
  GR:
    pipeline: cas
```

この場合 `decode` あり / `decode=0` ありの差はバイト列としては現れない（どちらも同じ生 TS）。
pipeline の spawn / 直列連結 / backpressure / kill の経路だけを検証する目的で使う。
**実 CAS の検証には使えない**点に注意。デコーダを用意できたら §2.3 の形に戻す。

---

## 4. 手動エンドツーエンド・チェックリスト

サーバ実装ができたら、以下を上から順に実行する。`BASE=http://127.0.0.1:40772`。

### 4.1 起動

```sh
BASE=http://127.0.0.1:40772
unituner --config-dir /tmp/opencode/unituner-test/config
```

### 4.2 基本メタデータ

```sh
# 自己識別: Server ヘッダが unituner/<ver>、ボディに "Mirakurun" を含まない
curl -si $BASE/api/version
# {"current":"...","latest":"..."} かつ current == latest

curl -s $BASE/api/status | jq .

# チャンネル一覧（4 件。type GR の 24/25/26/27）
curl -s $BASE/api/channels | jq .

# type 絞り込み
curl -s $BASE/api/channels/GR | jq 'length'   # => 4

# チャンネル詳細（services が簡易 embed される）
curl -s $BASE/api/channels/GR/27 | jq .

# チューナー一覧（index/name/types/isFree/isUsing/users）
curl -s $BASE/api/tuners | jq .
```

### 4.3 チャンネルストリーム

```sh
# 先頭 1MB を取得して 188 アラインを確認
curl -s $BASE/api/channels/GR/27/stream | head -c 1000000 > /tmp/opencode/unituner-test/capture/api_ch.ts
python3 -c "d=open('/tmp/opencode/unituner-test/capture/api_ch.ts','rb').read(); \
  print(len(d), len(d)%188, hex(d[0]), hex(d[188]), hex(d[376]))"

# HEAD も可（Stream 系は HEAD 対応）
curl -sI $BASE/api/channels/GR/27/stream

# X-Mirakurun-Tuner-User-ID が付与されている
curl -sI $BASE/api/channels/GR/27/stream | grep -i x-mirakurun-tuner-user-id
```

期待: `len(d)%188 == 0`、`0/188/376` がすべて `0x47`。
pipeline 通過後なので、この時点で CAS 復号済みの TS になっている。

### 4.4 サービス一覧 / サービスストリーム

```sh
curl -s $BASE/api/services | jq .
# 各要素: id, serviceId, networkId, tsid, name, type, logoId:0, hasLogoData:false,
#         remoteControlKeyId, channel{type,channel}

curl -s $BASE/api/services | jq -r '.[] | "\(.id)\t\(.name)\t\(.channel.type)/\(.channel.channel)"'

# SID 直指定でストリーム取得（内蔵 TSFilter 適用）
SID=$(curl -s $BASE/api/services | jq -r '.[0].serviceId')
curl -s "$BASE/api/services/$SID/stream" | head -c 1000000 > /tmp/opencode/unituner-test/capture/api_svc.ts
python3 -c "d=open('/tmp/opencode/unituner-test/capture/api_svc.ts','rb').read(); \
  print(len(d), len(d)%188)"
```

内蔵 TSFilter の確認（fixture 無しでも実 TS で目視可能）:

- PAT の program が **1 件だけ**になっている。
- 他サービスの ES PID が落ちている。
- PAT の CRC32 (MPEG-2) が正しい（`ffprobe` が PAT を読める / パケットが壊れていない）。

```sh
ffprobe -v error -show_programs -of compact /tmp/opencode/unituner-test/capture/api_svc.ts | head
```

### 4.5 decode=0 の比較

SPEC §4.1: `decode=0` は **pipeline 全段をバイパス**して生 TS を配信する
（mirakurun の B-25 のみスキップとは異なる）。service stream の TSFilter は適用される。

```sh
# 通常（pipeline 通過 = 復号済み）
curl -s "$BASE/api/channels/GR/27/stream"        | head -c 2000000 > ch_dec.ts

# decode=0（pipeline バイパス = 暗号化された生 TS）
curl -s "$BASE/api/channels/GR/27/stream?decode=0" | head -c 2000000 > ch_raw.ts

# 同一でないことを確認（同じチャンネル・同じ長さでも中身が違う）
cmp -s ch_dec.ts ch_raw.ts && echo "NG: identical" || echo "OK: different"

# 生 TS は ffmpeg でデコードできない（スクランブルされたまま）ことを目視確認
ffmpeg -v error -i ch_raw.ts -map 0:v:0 -frames:v 1 /dev/null 2>&1 | head
```

期待: 復号済みはフレームが出る、`decode=0` 側は映像にならない（PMT は読めても ES が
スクランブルされたまま）。※ `cat` スタンドイン構成（§3.4）では差が出ないので、
このチェックは `arib-b25-stream-test` を入れた構成でのみ意味を持つ。

サービスストリームでも同様:

```sh
curl -s "$BASE/api/services/$SID/stream?decode=0" | head -c 1000000 > svc_raw.ts
```

### 4.6 チャンネル探索 API（SPEC §8.1）

```sh
# GR 13-62 を探索 (text/plain ストリーミング。完了まで応答が返る)
curl -N -X PUT "$BASE/api/config/channels/scan?type=GR&minCh=13&maxCh=62"

# no_signal のチャンネルはスキップして継続すること
# 進捗例:
#   channel scanning... (type: "GR")
#   channel: "24" ... -> N services found.
#   **RESTART REQUIRED** to apply changes.

# 保存せず確認
curl -N -X PUT "$BASE/api/config/channels/scan?type=GR&minCh=13&maxCh=62&dryRun=true"

# GET は 404（async 非対応）
curl -s -o /dev/null -w '%{http_code}\n' "$BASE/api/config/channels/scan"   # => 404

# 二重実行は 409
# （1 本目を流したまま別シェルで）
curl -s -X PUT "$BASE/api/config/channels/scan?type=GR&minCh=13&maxCh=62" | jq .
# => {"code":409,"reason":"Already Scanning"}
```

確認ポイント:

- 探索後に `channels.yml` へ**追加のみ**される（既存 4 エントリは維持）。
- 他 type は変更されない。
- `**RESTART REQUIRED**` が出る（自動再起動しない）。
- 探索中の通常ストリーム要求は priority -1000 の scan セッションと共存する。

### 4.7 503 / 優先度 / pinning

チューナーは 1 本しかないので、同時に別チャンネルを引くと競合する。

```sh
# 1 本目を保持（T27 を視聴中）
curl -s "$BASE/api/channels/GR/27/stream" | head -c 50000000 > /dev/null &

# 別チャンネル (T24) を priority 0 で要求 → 奪取不可なら 503
curl -s -o /dev/null -w '%{http_code}\n' "$BASE/api/channels/GR/24/stream"
# => 503  {"code":503,"reason":"Tuner Resource Unavailable"}

# priority が victim より真に大きければ奪取できる
curl -s -o /dev/null -w '%{http_code}\n' \
  -H 'X-Mirakurun-Priority: 10' "$BASE/api/channels/GR/24/stream"
# => 200（1 本目は EOF で切られる）

# priority 128 は無条件奪取（mirakc GRAB 互換）
curl -s -o /dev/null -w '%{http_code}\n' \
  -H 'X-Mirakurun-Priority: 128' "$BASE/api/channels/GR/26/stream"

# 同一 (type, channel) は priority 関係なく相乗り（新規 spawn しない）
curl -s "$BASE/api/channels/GR/24/stream" -o /dev/null -w '%{http_code}\n'
# => 200 かつ /api/tuners の users が 2 人、isUsing は 1 のまま
curl -s $BASE/api/tuners | jq '.[0] | {isUsing, users}'
```

pinning:

```sh
# 正しい tuner 名 → 起動
curl -s -o /dev/null -w '%{http_code}\n' \
  -H 'X-Mirakc-Tuner: px-s1ud'    "$BASE/api/channels/GR/26/stream"   # 200
curl -s -o /dev/null -w '%{http_code}\n' \
  -H 'X-Mirakurun-Tuner: px-s1ud' "$BASE/api/channels/GR/26/stream"   # 200（alias）

# 未知の tuner 名 → 400
curl -s -o /dev/null -w '%{http_code}\n' \
  -H 'X-Mirakc-Tuner: nope' "$BASE/api/channels/GR/26/stream"         # 400

# type 不一致（GR tuner に BS チャンネル）→ 400
curl -s -o /dev/null -w '%{http_code}\n' \
  -H 'X-Mirakc-Tuner: px-s1ud' "$BASE/api/channels/BS/BS01_0/stream"  # 400
```

### 4.8 idle timeout / プロセス終了

```sh
# 最後の購読者が抜けて 5 秒 (idle_timeout_secs) 後に子プロセス群が消えること
curl -s "$BASE/api/channels/GR/27/stream" | head -c 500000 > /dev/null
sleep 6
pgrep -af 'recisdb|arib-b25-stream-test'   # => 何も出ない
curl -s $BASE/api/tuners | jq '.[0] | {isUsing, isFree, users}'
# => isUsing:false, isFree:true, users:[] を期待
```

### 4.9 再起動後の反映

`channels.yml` を書き換えた後は手動再起動が必要（SPEC §8.1 / §10）。

```sh
# webui の再起動ボタン相当
curl -s -X POST $BASE/webui/api/restart
# あるいはプロセスを落として起動し直す
```

---

## 5. 注意事項（必読）

1. **パケット drop は想定内**
   この構成（USB チューナー + 単一 frontend）では TS パケットの drop が発生する。
   これは異常ではない。**drop を追跡・報告しない**。ログに drop が出ても無視してよい。

2. **CAS は外部 B25 デコーダ**
   unituner は B-CAS/B25/C-CAS を内蔵しない。デスクランブルは pipeline 内の
   `arib-b25-stream-test`（外部コマンド）が行う。`recisdb` 側は `--no-decode` で
   自前デコードを切る。復号できない ECM があっても異常ではない（§3.3）。

3. **テスト用 config はリポジトリに入れない**
   テスト用 config は `/tmp/opencode/unituner-test/config/` に置き、リポジトリの
   `docs/` には YAML の例だけを載せる。

4. **チューナープロセスが終了するとセッションも終わる**
   チューナーコマンド（または pipeline 段）が exit すると、そのセッションの TS は
   終了する。クライアントからは headers 送信後の **EOF** として見える。
   奪取 kill / pipeline クラッシュ / tuner 終了を区別する通知はしない（SPEC §6.2）。

5. **受信できるのは T24〜T27 のみ**
   他のチャンネルは no signal。探索 API のテストでは no_signal スキップの確認に使える。
   BS/CS はこのデバイスでは受信不可（`FE_SET_PROPERTY: Invalid argument`）。

6. **`-t 0` は無制限ではない**
   recisdb の `tune -t` は省略で無制限、`0` で 1 バッファ出力して終了。
   unituner の tuner command では `-t` を**書かない**こと。

---

## 6. 参照

- `docs/SPEC.md` §3 (config), §4 (データパス / decode=0), §6 (競合・pinning), §8 (スキャン)
- `arib-b25-stream-test`: mirakurun と同じ外部 B25 デコーダ（stdin/stdout フィルタ）
- recisdb: `recisdb --help`, `recisdb tune --help`
