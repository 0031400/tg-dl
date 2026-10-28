# tgdl

Telegram 文件下载器。输入只有一种格式：**`chat_id:message_id`**。

| 引擎 | 传输 | 说明 |
| --- | --- | --- |
| `fast` | 原生 MTProto（Telethon，`upload.getFile`） | **默认引擎**，单文件多连接并行，大文件通常比 TDLib 快 2~5 倍 |
| `tdlib` | TDLib 1.8（`tdjson.dll`，ctypes） | 基线，行为与官方客户端一致 |

要求：Python ≥ 3.11、[uv](https://docs.astral.sh/uv/)、一个可用的 `tdjson` 动态库（仅 `tdlib` 引擎需要）。

## 快速开始

```bash
cd C:/Users/Administrator/file/tg-dl
uv sync

# 1. 写配置（api_id / api_hash 从 https://my.telegram.org 申请）
uv run tgdl init --api-id 123456 --api-hash 0123456789abcdef0123456789abcdef

# 2. 登录（两个引擎各自一个会话；只用 fast 就 --engine fast）
uv run tgdl login --engine all

# 3. 看消息里是什么文件
uv run tgdl info 8913476869:6733

# 4. 下载
uv run tgdl download 8913476869:6733 -o out.mp4
uv run tgdl download -1001234567890:42 -o C:/Users/Administrator/file/downloads/     # 给已存在的目录就用原文件名

# 5. 同一个文件实测两边速度
uv run tgdl bench 8913476869:6733 -c 4,8,16,32

# 6. 退出登录
uv run tgdl logout
```

`tdjson` 路径默认 `C:\Users\Administrator\file\tdlib\bin\tdjson.dll`（可用 `--tdjson` 或环境变量 `TGDL_TDJSON` 覆盖）。
配置查找顺序：`--config` → `$TGDL_CONFIG` → `./.tgdl/config.toml` → `~/.tgdl/config.toml`。

## 输入格式

```
chat_id:message_id
```

- **chat_id**：频道/超级群是 `-100xxxxxxxxxx`，普通群是 `-xxxxxxxxxx`，用户是正数。
- **message_id**：MTProto 的原始 id（如 `6733`）和 TDLib 的 id（`6733 << 20`）都接受，程序会自动识别并转换
  （TDLib 把服务端 id 存在高 20 位以上，见 `td/telegram/MessageId.h`）。

两个引擎拿到同一个 `chat_id:message_id` 后各自去取消息：

- `fast`：`messages.getMessages` 拿消息 → 从 `document` / `photo` 里取
  `id / access_hash / file_reference / dc_id` → 多连接并行 `upload.getFile`；
- `tdlib`：`getMessage`（本地库）→ 查不到就用 `getChatHistory` 回源 → `downloadFile`。

因为每次都是从消息里现取定位信息，**不存在 file_reference 过期的问题**，照片也能直接下。

## 为什么 fast 能比 TDLib 快

不是玄学，是 TDLib 自己给自己设了上限（源码 `C:\Users\Administrator\file\td-1.8.0`）：

1. **全局在途预算只有 2 MiB**
   `td/telegram/files/ResourceManager.h` → `MAX_RESOURCE_LIMIT = 1 << 21`，
   整个客户端所有文件加载共享这 2 MiB 的"在途数据"额度。

2. **分片被自动放大到 512 KiB**
   `td/telegram/files/PartsManager.cpp` → 从 64 KiB 起翻倍直到分片数 ≤ `MAX_PART_COUNT`(4000)，
   上限 `MAX_PART_SIZE = 512 KiB`。

   于是 1 GiB 文件：part_size = 512 KiB，在途分片 ≈ 2 MiB / 512 KiB ≈ **4 个并发请求**。
   这就是单文件下载吃不满带宽的根本原因。

3. **单 DC 连接数上限 10**（`ConnectionCreator.cpp`），所有请求复用这 10 条连接。

`fast` 引擎绕开这套调度：直接对目标 DC 发 `upload.getFile`，默认 **16 个并发**、
固定 **512 KiB** 分片（可用 `-c/--chunk` 调到 32/64 并发与 1 MiB），每个 worker 持有独立文件句柄
按偏移写盘，无 per-part fsync、无分片哈希校验。文件不在本会话的 DC 上时会自动借导出连接
（`_borrow_exported_sender`），不会 `FILE_MIGRATE`。

> **`--connections` 是"在途请求数"，不是 TCP 连接数。** 这些请求共用一条 TCP 连接，
> 靠 MTProto 多路复用并发。实测（同一个 556 MB 文件，各跑 32 MB）：
>
> | 配置 | 总在途 | 吞吐 | FLOOD_WAIT |
> | --- | --- | --- | --- |
> | 1 TCP × 16 | 16 | **5.5 MB/s** | 0 |
> | 4 TCP × 4 | 16 | 4.3 MB/s | 0 |
> | 2 TCP × 16 | 32 | 3.0 MB/s | 18 次 / 20s |
> | 4 TCP × 16 | 64 | 2.6 MB/s | 56 次 / 112s |
>
> 结论：**单连接 + 16 在途就是最优点**。TCP 数不影响吞吐，在途数超过 ~24 反而因为
> Telegram 按账号限流（`FLOOD_WAIT`）而变慢，连接还会被服务端重置。`-c` 超过 24 时 CLI 会提示。

### FLOOD_WAIT 是自适应的，不是死循环

Telegram 的 flood 限制按**账号**算。如果撞上以后还保持原并发猛发，就会陷入
「限流 → 睡 → 突发 → 再限流」，用户看到的就是刷屏的 `FLOOD_WAIT`。

`fast` 引擎里有一个 `AdaptiveLimiter`（同 TDLib 的 `ResourceManager` 思路）：

- 撞到限流 → 在途额度**砍半**（有地板，最低 `-c/4`），并记一个**全局冷却时间点**，
  所有 worker 在这之前都不再发请求（避免睡醒后集体突发）；
- 连续成功 → 额度慢慢涨回 `-c` 给的上限。

想让并发**钉死**在一个值、完全不做调节，加 `--fixed`：撞限流只做全局冷却，不降额度也不涨。
同一个 189 MB 文件实测（`-c 16`）：

| 模式 | 耗时 | 速度 |
| --- | --- | --- |
| 自适应 + 地板（默认） | **41.5s** | **4.57 MiB/s** |
| `--fixed -c 16` | 44.9s | 4.21 MiB/s |
| `--engine tdlib` | 46.1s | 4.11 MiB/s |
| 自适应但无地板（旧） | 49.5s | 3.83 MiB/s |

实测 96 MB：改之前 32 MB 就能触发 56 次（累计等 112 秒）；改之后 96 MB 只有 4 次、每次 1 秒，
并发自动从 16 降到 1 再涨回 9，下载全程没有卡顿。所以偶尔看到一两条 `FLOOD_WAIT` 是正常的
自动降速，不是出错。

### 多 auth key 叠加（`--auth-keys`）

Telegram 的 flood 预算按 **auth key** 算，不是按账号。所以可以用多条独立 auth key 并行，
把限流额度摊开。关键在于：**不需要重复登录**——从当前会话 `exportAuthorization` 拿到 token，
再对目标 DC 做一次全新 DH 握手导入即可，每条都是独立的 key。

```bash
uv run tgdl download 8913476869:6799 --auth-keys 3 -c 16
```

同一个 189 MB 文件实测：

| 配置 | 耗时 | 速度 |
| --- | --- | --- |
| `--engine tdlib` | 46.1s | 4.11 MiB/s |
| `--auth-keys 1`（默认） | 41.5s | 4.57 MiB/s |
| **`--auth-keys 3`** | **28.3s** | **6.68 MiB/s** |
| `--auth-keys 5` | 32.4s | 5.84 MiB/s |

3 条是最优点：每条多约 2 秒的 DH 握手，5 条时握手开销（~8s）已经吃掉收益，
而且链路本身也接近饱和。**小文件（几十 MB 以内）不要开**，握手成本盖过收益。

**auth key 全部存在 `.tgdl/authkeys.json`**，主 key 和派生 key 地位完全平等——
都只是「某个 DC 的一条授权」。`fast.session` 里**不再保留密钥**（只留实体缓存和同步游标）。

```json
{
  "home_dc": 5,
  "home_ip": "91.108.56.196",
  "home_port": 443,
  "keys": {"4": ["<hex>", "..."], "5": ["<hex>"]}
}
```

`home_*` 只回答「没有 key 时从哪个 DC 开始连」。所以：

> **换机器 / 换账号 = 替换这一个文件。** 把 `authkeys.json` 拷到另一台机器的 `.tgdl/` 下
> 就等于登录了，不用验证码。实测把 `fast.session` 整个删掉、只留 `authkeys.json`，
> `tgdl whoami` 和下载都照常工作。

`tgdl login` 做的就是这件事：登录成功后把 key 写进 `authkeys.json`，并让 session 文件
永远不落盘密钥（`KeylessSession` 覆写了写库逻辑，无条件写 NULL）。
老版本升级会自动迁移：发现 `fast.session` 里还有 key 而 `authkeys.json` 没有 home，
就把它搬过去并清掉 session 里那份。

同一个文件：

| 场景 | 耗时 | 速度 |
| --- | --- | --- |
| `--auth-keys 3`，首次（要 DH） | 24.5s | 7.74 MiB/s |
| `--auth-keys 3`，key 已缓存 | **21.9s** | **8.66 MiB/s** |

#### 每次运行的实际流程

1. 看目标 DC（文件所在 DC）的缓存里有几条；
2. **先取缓存里的前 N 条直接连**（`MTProtoSender(AuthKey(...))`，不做 DH）；
3. 缓存不够 `--auth-keys - 1` 条时，只补差额：现场 DH 握手，**成功后立刻写回缓存**；
4. 缓存里某条失效（服务端重置过）会自动 `drop` 掉，改下一条或现场重生成。

日志会明确告诉你是复用还是新生成：

```
[fast] auth key DC4：缓存 4 条，需要 5 条 → 复用 4 条，新生成 1 条
[fast] 使用 6 条独立 auth key 并行下载（复用 4 条缓存，新生成 1 条，DC4 现有 5 条）
```

第二次同样的命令就变成：

```
[fast] auth key DC4：缓存 5 条，需要 5 条 → 全部复用，免 DH 握手
[fast] 使用 6 条独立 auth key 并行下载（复用 5 条缓存，新生成 0 条，DC4 现有 5 条）
```

`--auth-keys N` 指的是**独立 key 总数（含主会话那条）**，所以额外生成的是 `N-1` 条。
每个 DC 最多存 12 条，超出丢最旧的。

> **创建多条 key 的正确姿势**（踩过坑）：
> `auth.importAuthorization` **不能并发**——服务端只放行一条，其余全部返回
> `AuthBytesInvalidError`（"The provided authorization is invalid"）。
> 早期版本用 `asyncio.gather` 并发创建，`--auth-keys 10` 时 9 条里只成功 1 条。
> 现在的做法是 **并行 DH 握手 + 串行导出/导入**：
>
> | 阶段 | 耗时 |
> | --- | --- |
> | 并行 DH ×4 | 3.4s |
> | 串行导入 ×4（约 1.2s/条） | 4.9s |
> | 合计 | **7.3s（4/4 全部成功）** |
>
> 全串行要 14s，所以并行握手这一半还是省下来的。
>
> **key 不是越多越好。** 同一个 189MB 文件实测：
>
> | auth-keys | 耗时 | 速度 | FLOOD |
> | --- | --- | --- | --- |
> | 5 | 22.9s | 8.25 MiB/s | 1~2 次 |
> | 10 | 33.3s | 5.69 MiB/s | 4 次 |
>
> 新建出来的授权额度更紧，条数一多反而更容易撞限流。**3~5 条是最稳的区间**，
> 超过 8 条 CLI 会直接提示。

> **启动快在哪**：命中缓存时，连「主连接」也直接用缓存 key 建，不再走
> `_borrow_exported_sender`（那一步本身要做一次全新 DH，实测 3.4s）。
> 预检日志也从「借完连接之后」提前到了最前面：
>
> | 阶段 | 耗时 |
> | --- | --- |
> | `client.connect()`（会话 DC） | ~2.0s |
> | 取消息 `get_messages` | ~0.2s |
> | 建 N 条缓存 key 连接（并行） | ~1.9s |
>
> 所以 `[fast] auth key DC4：...` 这行大约 **2.1s** 出现（原来要等 5.6s）。

`authkeys.json` 是**唯一的凭据文件**，里面是裸的 auth key，和密码等价，别外传。
`tgdl doctor` 会按 DC 显示条数和主 DC（`auth key 11 条（DC4 10 条，DC5 1 条｜主 DC5）`）；
删掉这个文件就等于退出登录。

> 限制：`exportAuthorization` 不允许导出**当前会话所在 DC** 的授权，所以文件恰好在
> 会话自己的 DC 上时，额外 key 会创建失败并回退成单 key 下载（日志里会提示）。
> 文件在别的 DC（很常见，例如会话 DC5、文件 DC4）才能叠加。

### ⚠️ 必须装 cryptg，否则前功尽弃

Telethon 的 AES-IGE 加解密有三档实现：`cryptg`（C 扩展）→ `libssl` → **纯 Python 的 pyaes**。
没装 `cryptg` 时会静默退回 pyaes，**解密成为唯一瓶颈，吞吐死死卡在 300~400 KB/s，
而且加并发完全无效**——看起来就像"网络慢"。

实测同一个 556 MB 文件（DC4）：

| 配置 | 速度 |
| --- | --- |
| 纯 Python AES（pyaes），4/8/16 并发 | 0.33 MB/s（并发无提升） |
| 装 cryptg 后，8 并发 | 3.4 MB/s |
| 装 cryptg 后，16 并发 | 3.2 MB/s |
| TDLib 自带下载器 | 3.8 MB/s |

`cryptg` 已在依赖里，`tgdl doctor` 会检查这一项；缺失时 `fast` 引擎启动会直接告警。
**如果你看到几百 KB/s 且加并发没用，先查这个。**

> 实测注意：fast 每次运行要新建连接 + 取消息（约 1~2 秒固定开销），
> 所以**小文件（几百 KB 以内）会比 TDLib 慢**，倍速优势在大文件上才体现出来。
> `tgdl bench` 会自动清掉 TDLib 的本地缓存再比，避免缓存命中导致数据失真；
> 大文件加 `--mb 64` 只测前 64 MB，几十秒就能出结果。

## 命令一览

| 命令 | 作用 |
| --- | --- |
| `tgdl init` | 写 `./.tgdl/config.toml` |
| `tgdl login [--engine tdlib\|fast\|all]` | 登录（会话持久化在 `.tgdl/`） |
| `tgdl logout` | 退出登录并清理会话 |
| `tgdl whoami` | 两个引擎的登录状态 |
| `tgdl info <chat_id:message_id>` | 看文件名 / 大小 / 类型 / DC，不下载 |
| `tgdl download <chat_id:message_id> [-o 路径] [-c 16] [--chunk 512K] [--fixed] [--auth-keys 3] [--engine auto\|fast\|tdlib]` | 下载 |
| `tgdl bench <chat_id:message_id> [-c 4,8,16,32] [--mb 64]` | 同文件对比两个引擎（`--mb` 只测前 N MB） |
| `tgdl shell [--auth-keys 3] [-c 16] [--fixed] [--dc N]` | **交互模式**：先建连接池，再反复下载 / 中断 / 删除（测「服务态」速度） |

> **DC 会记进配置**：`download` / `shell` 跑过一次后，`.tgdl/config.toml` 会多出
> `session_dc = 5`（会话所在 DC）和 `pool_dc = 4`（上次下载用的 DC）。
> 下次 `shell` 启动就直接按 `pool_dc` 把连接池建好，不用等第一次 `dl` 才发现 DC 不对。
> 只改这两行，文件里的注释和其它键原样保留。
| `tgdl serve` | 本地服务，给油猴脚本用 |
| `tgdl doctor` | 自检配置 / dll / 会话 |

`--engine auto`（默认）：fast 会话已登录就用 fast，否则退回 tdlib。

## 交互模式（`tgdl shell`）

`download` 每次都要重连会话 DC、取消息、建 auth key 连接（约 2~4 秒）。
如果想测「常驻服务」的真实吞吐，用 `shell`——它只建一次连接池，之后每个任务都复用。

```bash
uv run tgdl shell --auth-keys 5 -c 16 --fixed
```

```
连接池就绪（1.89s）。输入 help 看命令。
  DC4｜5 条 auth key 连接｜在途 16｜分片 512.00 KiB｜固定并发｜建池 2.17s
tgdl>
```

| 命令 | 作用 |
| --- | --- |
| `dl <chat:mid> [-o 路径]` | 后台下载，立即返回（`st` 看进度）；不给 `-o` 就落到 `--dir` 目录并用原文件名 |
| `dlw <chat:mid> [-o 路径]` | 前台下载，带实时进度，Ctrl+C 中断 |
| `st` | 列出所有任务（含瞬时速度） |
| `stop [任务号]` | 中断下载，**保留已下分片**（同 target 再 `dl` 会续传） |
| `rm <任务号\|chat:mid\|路径>` | 正在跑就先中断，然后删文件 |
| `ls` | 列出下载目录 |
| `pool` | 连接池信息（DC / key 数 / 建池耗时 / 累计） |
| `q` | 退出并关掉连接池 |

同一个 189 MB 文件实测：

| 方式 | 耗时 | 速度 | 说明 |
| --- | --- | --- | --- |
| `download --auth-keys 5 --fixed` | 22.95s | 8.25 MiB/s | 含 2s 连会话 + 2s 建池 |
| **`shell` 池热后下载** | **14.09s** | **13.43 MiB/s** | 只有传输时间 |
| `shell` 中断后续传 | — | — | 从 13.50 MiB 接着下，不是从头 |

也就是说：**启动开销占了 CLI 单次下载的 1/3**，服务态能跑到 13 MiB/s。

> 注意：文件恰好在会话自己的 DC 上时，`exportAuthorization` 不允许导出本 DC，
> 派生不出额外 key，`shell` 会提示「只用 1 条连接」——这是 Telegram 的限制。

## 已知限制

- **fast 需要会话缓存在本地**。Telethon 只知道 id 的频道/群需要先有 `access_hash`，
  程序会在缓存缺失时扫一次会话列表；如果账号根本不在那个会话里，会直接报错。
- **tdlib 需要会话已在它的库里**。普通群（非超级群）如果 TDLib 没见过，无法凭空创建，
  请先在客户端打开一次该会话，或改用 `--engine fast`。
- **CDN 文件**：服务器返回 `FILE_CDN_REDIRECT` 时会自动切回直连重试（不实现 CDN 解密），
  极端情况下比 TDLib 走 CDN 慢，可加 `--engine tdlib` 对比。

## 想让 TDLib 自己也变快？

如果必须留在 TDLib 内部（比如要复用它已有的会话），可以改源码后重编。仓库里带了脚本：

```bash
uv run python scripts/patch_tdlib.py --td-src C:/Users/Administrator/file/td-1.8.0          # 打补丁（自动备份 .bak）
uv run python scripts/patch_tdlib.py --td-src C:/Users/Administrator/file/td-1.8.0 --revert # 撤销
```

它只改两处：`ResourceManager.h` 的 `MAX_RESOURCE_LIMIT`（2 MiB → 64 MiB）、
`ConnectionCreator.cpp` 的 `MAX_CONNECTIONS`（10 → 64）。
重编后把新的 `tdjson.dll` 换掉，`--engine tdlib` 就会跟着变快；`tgdl bench` 可以直接量出差异。

## 目录结构

```
src/tgdl/
  cli.py      # typer 命令行
  tdjson.py   # tdjson.dll 的 ctypes 绑定 + 登录/登出/取消息/下载
  fast.py     # 原生 MTProto 并行下载（Telethon）
  config.py   # config.toml / 环境变量
  util.py     # chat_id:message_id 解析、进度条、体积格式化
scripts/
  patch_tdlib.py   # 给 TDLib 源码打提速补丁
tests/
  test_target.py   # 目标解析与 message_id 转换单测（uv run --with pytest pytest -q）
```
