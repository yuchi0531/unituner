# unituner POLICY

> 何を引き受け、何を引き受けず、迷ったときどちらへ倒すかの方針。
> 実装の詳細は [SPEC.md](SPEC.md)、実機手順は [TESTING.md](TESTING.md)。
> SPEC と衝突したら実装については SPEC が正、新しい要求をどう扱うかについては本書が正。

## 0. 一文でいうと

**チューナー共有と抽象化だけを引き受ける mirakurun 互換サーバ。互換は「クライアントが動くこと」で測り、自分の身元は偽らない。放送側の複雑さは全部 pipeline の外部コマンドへ追い出し、unituner は TS だけを扱う。設定は明示・検証・原子的に保存し、失敗は原因が分かる形で出す。最後は実機で確かめる。**

## 1. スコープ

やる:

- 外部コマンドチューナーの共有・排他・奪取（`(type, channel)` 単位の 1 本のプロセス木）
- mirakurun 互換 API のサブセット（`/api/version` 〜 `/api/tuners`、ストリーム 3 種）
- 内蔵サービスフィルタ（service stream の PAT 書き換え + PID 絞り込み）
- チャンネル探索 API（curl で叩く）とサービス表（永続キャッシュ + 手動 Re-scan）
- 管理 web UI（設定の閲覧・編集と再起動）

やらない:

- EPG、録画、タイムシフト、ロゴ（CDT）、番組情報
- MMT/TLV の内部処理、B-CAS / B25 / C-CAS の内蔵デコード
- 永続 DB、hot reload、認証、マルチノード

**追加要求を受けたときの第一基準**: それは「チューナー共有の正しさ」に効くか。
効かなければ unituner の仕事ではなく、pipeline の外部コマンドか別ツールの仕事として切り分ける。

例外は例外として明示する。`decode=0` が生 TLV を流すのは TS-only 原則の**許容済み例外**であり、黙って隠さず README と SPEC の制限欄に書く。

## 2. 互換性

- **互換は API 形状の互換であって、身元の詐称ではない。**
  `Server: unituner/<version>`、`/api/version` は `current == latest`、レスポンスボディに `Mirakurun` を出さない（mirakc と同じ正直な自己識別）。
- ただし**ワイヤプロトコルの識別子は残す**。`X-Mirakurun-Tuner-User-ID` / `X-Mirakurun-Priority` / `X-Mirakurun-Tuner` は「相手がそう呼ぶから」であって、自己紹介ではない。
- **互換の判定は実クライアント**。EPGStation、BonDriver_Mirakurun、KonomiTV が動くことが基準。
  - 例: `Host` が無い/空の要求を 403 にしないのは BonDriver_Mirakurun がそう送るため。`Host` が存在して許可外なら 403（DNS リバインディング対策は維持）。
- **互換仕様はソースで確認する**。mirakc / mirakc-BS4K / Hotarun / mirakurun 本家のコードを読み、記憶や推測で決めない。
  - 例: 奪取条件は「victim の**最大** priority より真に大きい」。最小 priority を見ると高優先度ユーザーを巻き込むので誤り。
- **拡張は既定で無効**。`routes` 未指定・pinning 未指定・pipeline 未指定は、すべて従来挙動に落ちる。新機能を足すときは「書かなければ今までどおり」を必ず保証する。
- 差分は制限として書く。mirakc-BS4K の `X-Mirakc-Tuner` を service stream にも広げたこと、`X-Mirakurun-Tuner` alias を足したこと等は README に明記する。

## 3. 責務の境界（TS-only 原則）

- **unituner が解釈するのは MPEG-TS だけ**。SI 解析、フィルタ、配信はすべて 188 バイトパケット前提。
- **pipeline はチューナー出力を byte-for-byte で受け、必ず TS を出力する。**
  TLV/MMT → TS 変換、デスクランブル、不要 PID 除去、再パケット化は pipeline の責務。
- **204→188 正規化は pipeline の後段に 1 箇所だけ**。pump が最終段の出力に適用してから broadcast する。
  入力側で正規化しない。TLV に `0x47` を探してストリームを壊すのが、BS4K が `no signal` になった実因だった。
- **入力フレーミングを設定で宣言しない**。`input: ts|tlv` のような項目は廃止した。
  設定を増やすより「pipeline は何を受けても TS を出す」という 1 つの契約で吸収する。
- **`decode=0` は pipeline 全段バイパス + 正規化なし**。生バイトをそのまま返す。
  TLV 源では TS ではなく TLV が流れる。これは許容済み例外として明示する（上記）。
- 外部 B25 デコーダの例は **`arib-b25-stream-test`** と表記する。デスクランブル実装の固有名をドキュメントに出さない（実装は差し替え可能な部品として扱う）。

## 4. 設定

- YAML 4 分割（`server` / `tuners` / `channels` / `pipelines`）+ 生成物 `services.json`（手編集しない）。
- **起動時 fail-fast**。問題はファイル名と該当項目付きで報告し、非 0 で落ちる。
- **未知フィールドは拒否**（`cmd:` のような typo を黙って無視しない）。
- **意味レベルの検証**を行う。型の正規表現は 3 ファイル横断で適用、参照先の存在、空 command の拒否、`scan > readiness` の順序。
- **webui の保存も起動時と同じ検証を通す**。UI から壊れた設定を書けない。
- **保存は原子的**: `.bak` → tmp 書き込み → fsync → rename。失敗時は書き戻さない。パーミッションは 0600 / 既存 mode を維持。
- **既定値は「運用に耐える」を優先**し、資源上限はテストでピン留めする。
  - 例: 購読者バッファ予算 256 MiB は同時 4 人で尽きたので **2 GiB**（= 32 購読者 × 64 MiB）へ。既定で詰まらせない。
- **設定を増やす前に 3 回考える**: 既定で吸収できないか、pipeline で解決できないか、環境変数や実行時引数で足りないか。
  増やすなら「検証 + README 記載 + テスト」を同じ変更にセットで入れる。
- **子プロセスの環境は継承しない**。`PATH` / `HOME` / `TMPDIR` / cwd を明示する。
  サービス実行と手動実行で `dtv.names` の探索先が変わる、といった差で壊れないため。

## 5. 資源と失敗

- **予約は原子的**。pick → reserve → spawn を 1 つのロッカーで行い、二重 spawn / 二重 kill を起こさない。
- **キャンセルでリークしない**。HTTP の切断、スキャンのタイムアウト、クライアントのタブ閉じで `Starting` のままチューナーを占有しない（Drop ガード / detached spawn + oneshot）。
- **奪取は readiness 確認後に確定**。spawn 失敗で victim を道連れにしない。停止は setsid したプロセスグループへ SIGTERM → 猶予 → SIGKILL。
- **上限は 4 層**: per-session / per-IP / 全体セッション数 / 合計バイト。超過は 429。
- **失敗は分類する**: `no signal` / `tuner failed to start` / `busy` / `preempted` / `no tuner for type` / decode 競合。
  「チューナーが起動していない」を「電波が無い」と混ぜない。ユーザーには展開後の argv 付きで理由を出す。
- **リトライは一時的なものだけ**。busy / preempted は backoff、起動失敗はリトライしない。全滅時は集約ヒントを 1 行出す。
- **静的に決まるエラーは GET と HEAD で一致**させる。HEAD が 200 を返して GET が 503 になる、という嘘をつかない。
- **設定外のチャンネルはチューナーを起こさない**（404）。`channels.yml` は資源の allowlist であり、飾りではない。
- **ログは原因が分かる形**にする。展開後の argv、スキャンの内訳、どの設定を疑うべきか。

## 6. 状態とキャッシュ

- **起動のたびに全チャンネルを舐めない**。`services.json` を読み、fingerprint が一致すればチューナーに触らず即座に配信可能にする。
- **fingerprint は「放送の性質を変える設定」だけ**。enabled な `channels.yml` の `(type, channel, extra-args, routes)`。`disabled` は除外、`tuners.yml` は含めない（チューナー定義の変更でサービス表は変わらない）。
- **キャッシュは無期限**。放送再編は手動 Re-scan で追随する。期限付き自動更新は YAGNI。
- **更新は upsert のみ**。busy / 無信号のチャンネルは既存レコードを保持し、設定から消えたチャンネルだけフルパス完了後に落とす。
- **中途半端に書かない**。クライアント切断・shutdown 時は retain もキャッシュ書込も行わない。

## 7. API と UI の分け方

- mirakurun 互換は `/api/*`。独自機能は `/webui/api/*` に閉じる。互換名前空間を独自用途で汚さない。
- **チャンネル探索は curl 用**（`PUT /api/config/channels/scan`）。webui には出さない。
- **webui の Re-scan はサービス表専用**で `channels.yml` を書かない。探索と混同しないよう別ボタン・別ダイアログにする。
- **webui はルート配信**（`host:port/`）。`/web` のような脇道を作らない。
- 見た目は Material Design 2。**ダーク既定 + トグル**（localStorage、未保存時は OS 設定に追従、描画前適用でちらつきなし）。Restart はヘッダー内、Channels は型別サブタブ、Services は物理 ch 列。
- webui は認証なし。**`allowed_cidrs` / `allowed_hosts` が境界**であり、LAN 内前提であることを README に書く。

## 8. 検証

- **実機で確かめる**。PX-S1UD + recisdb。ドロップは仕様であり、それ自体を不具合として扱わない。
- fake チューナー（`cat` / `head -c` / `sleep` / 存在しないコマンド）で回帰を固定する。実機が無い日でも壊れたことが分かるように。
- 変更時の最低ライン: `cargo test` / `cargo clippy -D warnings` / `cargo fmt --check`。
- 実機で見るもの: 188 整列、先頭 `0x47`、tuner の解放、スキャン結果の一致、`services.json` の更新。
- **主張はコードで確認する**。ユーザーの指摘が正しいか分からないときは、同意も反論もせずにまずコードを読み、再現し、修正余地があれば直す。
- テストは「通った」だけでは不十分。クリティカルな経路は実機相当の end-to-end で 1 回通す。

## 9. 進め方

- 大きな変更は**並列サブエージェント**。ただしファイルが重ならない単位で分割し、最後に統合して全体テストを回す。
- 優先順は「動かして壊れる層」→「結果の正しさ」→「資源・安全性」→「保守性」。
- コミットは小さく、コード + テスト + docs（SPEC / README）を同じ変更に含める。
- **push は指示があるまでしない**。リポジトリは private。
- ドキュメントは日本語。コマンド・識別子・ログは原文のまま。

## 10. 判断に迷ったときの順序

1. **実クライアントは動くか**（互換の実測）
2. **unituner の責務か**（TS-only / チューナー共有）
3. **設定を増やさず既定で吸収できないか**
4. **失敗したとき原因が分かるか**
5. **資源をリークしないか**（キャンセル・タイムアウト・異常終了）
6. **実機で再現・検証できるか**
7. **制限としてドキュメントに書けるか**（隠さず書けるなら許容、書けないなら設計が悪い）

## 11. アンチパターン（やってはいけない）

- 互換のために `Mirakurun` と名乗る（身元の詐称）
- pipeline の入力側で 204→188 正規化する（TLV を壊す）
- 「チューナー起動失敗」を `no signal` に丸める（原因が見えなくなる）
- 取得のキャンセルで `Starting` 予約を残す（チューナー恒久占有）
- 設定に無いチャンネルでチューナーを起動する（allowlist の無効化）
- HEAD で楽観的な 200 を返す（クライアントの事前判定を壊す）
- 暗黙のフォールバック（pinning / routes で指定した以外へ勝手に流れる）
- 無制限バッファ・無制限購読者（DoS の入口）
- webui に探索とサービス再スキャンを混ぜる（`channels.yml` の二重書き込み）
- テストが緑だからと実機を見ない（ドロップ・遅延・権限は実機でしか出ない）

## 12. 主な決定と根拠（記録）

| 決定 | 根拠 |
|---|---|
| `Server: unituner/<version>`、`/api/version` は current == latest | 互換でも身元を偽らない。mirakc が同じ方式で実用上動いている |
| `Host` 空は許可、存在して許可外は 403 | BonDriver_Mirakurun が Host を送らない。ブラウザは authority 必須なので rebinding 対策は弱まらない |
| 204→188 正規化は pipeline 後段のみ | 入力側で正規化すると TLV を TS と誤認して破壊する（BS4K の no signal の実因） |
| `input`（`ts` / `tlv`）を廃止 | unituner は TS のみ扱う。TLV→TS は pipeline の契約で吸収する |
| 購読者バッファ既定 2 GiB | 256 MiB は同時 4 人で上限に達した。既定で運用を詰まらせない |
| `services.json` + fingerprint、無期限 | 起動のたびの全 ch スキャンは無駄で遅い。放送再編は手動 Re-scan |
| チャンネル探索は curl、サービス Re-scan は webui | 探索は `channels.yml` を書き、Re-scan は `services.json` を書く。別物を混ぜない |
| `routes` 未指定は従来挙動 | 新機能は「書かなければ今までどおり」を守る |
| `passthrough` 疑似コマンド | 生 TS 運用を設定で表現できる。空 pipeline を特別扱いしない |
| 子プロセスの環境を固定 | サービス実行と手動実行の差（PATH/HOME）で外部コマンドが壊れる |
| readiness 30 秒 / scan 60 秒 | tune 6–11 秒 + デスクランブラ初回 9–15 秒の実測に合わせる |
