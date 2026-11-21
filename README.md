# shard_receipt — 离线分片回执接收与校验

只读跨站收件器的离线模拟：同一份模拟资料的认证分片经由多条相互独立的链路到达，
渠道可以延迟、重复交付，某一来源也可能签出彼此不一致的分片。本程序不打开网络，
用整数向量和可信标记代替真实报文与验签，对每份资料清单输出**可以独立复算**的
收齐（release）或隔离（hold）回执。

* 运行环境：Windows 原生 Python 3.12，纯标准库，CPU 即可。
* 所有线性关系都在 **GF(257)** 上；求解只用高斯消元，**从不枚举数据向量**
  （典型规模 32 行 / 12 个数据符号 / 6 个来源域，257^12 的枚举被明确禁止）。
* 输出为确定性 UTF-8 JSON（键排序、LF 行尾、UTF-8 字节写 stdout）。

## 命令

```bash
# 对每份清单、每个事件前缀验证能否重建，输出解释轨迹
python -m shard_receipt explain CASE.json

# 用 SQLite 原子登记资料代次、接收回执与发布回执，重放操作序列
python -m shard_receipt reuse CASE.json --db PATH

# 运行断言套件
python -m unittest discover -s tests -v

# 重新生成 cases/ 下的可运行案例（确定性，输出字节稳定）
python tools/generate_cases.py
```

结构错误以退出码 2 失败，stderr 上输出带 JSON 路径的错误对象，例如
`{"error": {"path": "manifests[0].deadline", "message": "missing required key 'deadline'"}}`。

## 数据模型

### 资料清单（manifest）

清单绑定资料 ID、当前代次、源域集合、收件域、版本截止与内容承诺摘要：

```json
{
  "manifest_id": "M-DOC1-G1",
  "doc_id": "DOC-1",
  "generation": 1,
  "source_domains": ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"],
  "recipient_domain": "inbox-1",
  "deadline": 100,
  "commitment": "sha256:<64 hex>",
  "data_symbols": 12,
  "segments": [{"name": "head", "start": 0, "length": 4}, ...]
}
```

`segments` 可选；缺省时整份资料视为单段。若给出，必须按顺序恰好铺满
`[0, data_symbols)`。

### 分片回执（receipt，explain 的 events 元素 / reuse 的 submit 载荷）

```json
{
  "type": "receipt",            // 仅 explain 事件流需要
  "receipt_id": "rcpt-001",
  "time": 3,
  "link": "link-alpha-1-0",
  "source_instance": "alpha-1",
  "source_domain": "alpha",
  "doc_id": "DOC-1",
  "generation": 1,
  "shard_index": 0,
  "trusted": true,
  "row": [121, 5, ...],         // 生成行向量，长度 = data_symbols，按 mod 257 理解
  "payload": 42                 // 载荷符号：row·x ≡ payload (mod 257)
}
```

每个分片给出一条线性方程 `row·x = payload (mod 257)`，各行可覆盖不同的资料段
（行向量只在对应段内非零）。跨资料、跨代次、跨源域清单、跨收件窗的分片不能拼用：
引擎只把 `doc_id`、`generation` 匹配且 `source_domain` 在清单声明集合内的回执纳入
该清单的方程组，其余计入 `out_of_scope`。

### 分类规则（每份清单独立执行）

1. `trusted = false`：只记录，不参与发布。
2. **同号异值**：同一来源实例 + 代次 + 分片编号签下两组不同的 `(row, payload)`，
   该来源实例在此代次的**所有**分片移入待复核集合（quarantine），并记录
   `quarantined_sources`。
3. **完全重复**的回执（同实例、同编号、同行、同载荷，哪怕经不同链路到达）只记一次。
4. `time > deadline` 的可信分片**留档但不可用于发布**（`archived_late`）。
5. 其余为可用分片（`usable`）。

### 解释（interpretation）与放行规则

输入允许**最多一个来源域**在任一解释下不可靠。对可用分片逐一评估：

* 不剔除任何域（无故障解释）；
* 剔除每一个出现的来源域（单域剔除解释）。

对每个解释，把余下可信分片的方程做 GF(257) 高斯消元：

* `inconsistent`：方程矛盾。**无解的解释不进入结果集合**，但矛盾证据保留
  （见下）。
* `underdetermined`：一致但欠秩，解不唯一。
* `unique`：唯一解；再校验其规范编码承诺摘要是否与清单一致。

放行（`release`）当且仅当：**至少一个解释有解，且所有有解解释均为唯一解并同
清单承诺一致**。即任何一个一致但欠秩的解释、或唯一解但承诺不符的解释都会阻断
发布。这也是"单域孤证无法放行"的来源：只有一个域供货时，剔除该域的解释留下
空方程组——一致但欠秩。

### 证据（可独立复算）

| 情形 | 证据 |
|---|---|
| 无解（`inconsistent`） | `conflict`：所用行 ID 组合及系数，矛盾等式 `0 = b (mod 257)`。验证：按系数对原始行求线性组合，左端为零向量、右端非零。 |
| 欠秩（`underdetermined`） | `under_rank`：秩 + 一个**非零零空间向量** `v`，满足对每行 `row·v ≡ 0`。 |
| 承诺不符 | `commitment_mismatch`：期望与实算摘要。 |
| 放行 | `witness`：一组分片 ID、其来源域、解向量与承诺摘要。任何人可用这些行重解方程组、重算摘要独立复算。 |

见证选择的破平顺序：**不同来源域数 → 分片数 → 升序回执 ID**（基的行数恒等于
符号数，故前两条起决定作用）。

单段足够与全包来源相容**分别验证**：`segment_checks` 对每个声明段只取支撑完全
落在段内的分片投影求解，报告该段是否被唯一确定；`cross_source_compatibility`
报告无故障解释下全部可信分片是否互相相容。

## 规范编码与承诺摘要

数据向量 `x = [x0, ..., x(n-1)]`（各符号取 mod 257 的最小剩余，0..256）的规范
编码为定长字节串：

```
enc(x) = LE16(x0) || LE16(x1) || ... || LE16(x(n-1))     (每符号 2 字节小端)
commitment = "sha256:" || HEX( SHA256( enc(x) ) )
```

例：`[0, 1, 256]` 编码为 `00 00 01 00 00 01`（hex）。见
`shard_receipt/canon.py`，测试 `TestCommitment` 锁定该格式。

## explain 输入/输出

输入 `CASE.json`：`{"case_id", "data_symbols", "manifests": [...], "events": [...]}`。
输出对**每个事件前缀**（0..len(events)）给出每份清单的完整评估：

```json
{
  "command": "explain",
  "case_id": "case-explain-1",
  "field_modulus": 257,
  "prefixes": [
    {"prefix_events": 36,
     "manifests": [{
        "manifest_id": "M-DOC1-G1",
        "decision": "release" | "hold",
        "reasons": ["ok" | "no_consistent_interpretation" |
                    "underdetermined:<域或<none>>" | "commitment_mismatch:<...>"],
        "counts": {"usable": .., "duplicates": .., "quarantined": ..,
                   "archived_late": .., "untrusted": .., "out_of_scope": ..},
        "quarantined_sources": [...],
        "interpretations": [{"excluded_domain": null|"beta", "status": ..,
                             "rank": .., "evidence": ..?}, ...],
        "segment_checks": [...],
        "cross_source_compatible": true|false,
        "witness": null | {"receipt_ids": [...], "source_domains": [...],
                           "solution": [...], "commitment": "sha256:..."}
     }]}
  ]
}
```

## reuse 输入/输出

输入 `CASE.json`：`{"case_id", "data_symbols", "ops": [...]}`。支持的操作：

| op | 说明 |
|---|---|
| `register_manifest` | 登记资料代次（`request_id` + `manifest`）。 |
| `submit` | 登记接收回执（`request_id` + `receipt`）。同事务内检测同号异签；一旦检出，隔离该来源实例并**重核所有未发布清单**（写入 `rechecks`）。 |
| `concurrent_submit` | 多线程并发提交（各自独立连接、`BEGIN IMMEDIATE` 串行化），用于复现并发写入下的来源异签。 |
| `release` | 评估并登记发布回执；结果引用原始回执提交号（`receipt_submissions`）。 |
| `flag_quarantine` | 发布后的隔离标志**独立记录**，不改写已发布回执；仍会重核未发布清单。 |
| `restart` | 关闭并重开数据库，模拟进程退出与重启。 |
| `status` | 回读某代次的登记、回执、发布、隔离与重核记录。 |

幂等性锚定在调用方 `request_id` 上：相同请求重放（包括进程重启后的重放）按
**原提交号**回读，返回 `replayed: true` 与原始记录，不会二次入账。已发布回执
永远不被改写。

## 复跑步骤

```bash
# 1. 重新生成案例（可选；输出确定性，应与仓库内文件一致）
python tools/generate_cases.py

# 2. explain：两次运行输出必须逐字节相同
python -m shard_receipt explain cases/case_explain_1.json > out1.json
python -m shard_receipt explain cases/case_explain_1.json > out2.json
fc out1.json out2.json

# 3. reuse：删掉旧库后重放；在同一库上再跑一遍验证重放幂等
del reuse.db*
python -m shard_receipt reuse cases/case_reuse_1.json --db reuse.db > run1.json
python -m shard_receipt reuse cases/case_reuse_1.json --db reuse.db > run2.json
# run2 中所有 submit/register/release 均应为 replayed: true 且提交号不变

# 4. 断言套件
python -m unittest discover -s tests -v
```

## 案例说明

* `cases/case_explain_1.json`：6 来源域、12 符号、每域 6 个段内行。`beta-1`
  签错两个载荷（唯一可靠解释是剔除 beta）；`gamma-2` 同号异签被整体隔离；
  一个 alpha 分片经第二链路重复交付；一个 delta 分片越过截止留档；一个
  epsilon 分片不可信。同一资料 ID 的第二代次事件混在流中验证代次隔离。
* `cases/case_reuse_1.json`：登记两个代次 → 提交分片 → 重启 → 重放请求 →
  并发异签（delta-1）触发隔离与未发布清单重核 → 发布 → 重放发布 → 欠料代次
  发布被拒（欠秩证据）→ 发布后隔离标志独立记录 → 重启回读。

## 代码结构

```
shard_receipt/
  gf257.py    GF(257) 高斯消元；唯一解 / 零空间向量 / 矛盾行组合证据
  canon.py    规范编码与 sha256 承诺摘要
  engine.py   分类、解释扫描、放行裁决、见证选择、分段检查
  model.py    CASE.json 结构校验（错误带 JSON 路径）
  store.py    SQLite 原子登记、幂等重放、并发异签重核
  __main__.py CLI
tools/generate_cases.py   确定性案例生成器
cases/                    可运行案例
tests/test_shard_receipt.py
```
