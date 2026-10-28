# tgdl 油猴脚本

在 Telegram Web（`/a` 和 `/k`）的图片 / 视频 / 文件消息上注入「⬇ tgdl」按钮，
点一下就丢进本地 `tgdl` 服务的下载队列，用多 auth key 并行下载。

## 组成

```
extension/monkey/tgdl.user.js   ← 油猴脚本（UI + 调用本地接口）
src/tgdl/server.py              ← 本地服务（tgdl serve）
```

脚本本身**不实现 MTProto**：浏览器没有裸 TCP，在 JS 里重写 AES-IGE / DH / SRP 登录
既没必要也不可靠。下载、登录、多 auth key 全部交给已经验证过的 Python 侧。

## 用法

1. 启动本地服务（会打印 token）：

   ```bash
   uv run tgdl serve
   # tgdl 本地服务已启动: http://127.0.0.1:8765
   # token: xxxxxxxxx
   ```

2. 装脚本：浏览器打开 <http://127.0.0.1:8765/tgdl.user.js>，
   油猴会自动拦截并弹出安装页，点「安装」即可。

   以后脚本更新了，重新打开这个地址点「更新」就行——不用再手动粘贴。
   （服务端在 `/tgdl.user.js` 直接吐 `extension/monkey/tgdl.user.js` 的内容）

3. 打开 https://web.telegram.org/a/ ，右下角出现 tgdl 面板 →
   「设置」里填 token → 保存（服务地址默认 `http://127.0.0.1:8765`）。

4. 在聊天里把鼠标移到图片/视频/文件上，右上角出现「⬇ tgdl」，点一下。

5. 「下载」标签里看进度；文件落在设置里的「下载目录」（默认 `.tgdl/downloads`）。

## 设置项

| 项 | 含义 |
| --- | --- |
| 下载目录 | 文件落盘位置 |
| 并行请求 | 一条 TCP 上的在途请求数（默认 16，别超过 24） |
| auth key 数 | 几条独立 auth key（默认 3；>1 会现场生成，无需额外登录） |
| 固定并发 | 勾上则不做自适应降速（撞限流只等待） |

这些值都会写进 `.tgdl/server.json`，下次启动沿用。

## 接口

除 `/api/ping` 外都要带 `X-Tgdl-Token` 头。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/ping` | 健康检查 |
| GET | `/api/state` | 配置 + 设置 + 任务 + 登录状态 |
| POST | `/api/settings` | 改设置 |
| POST | `/api/login` | 登录（`{phone}` → `{value}` 验证码 → `{value}` 两步密码） |
| POST | `/api/resolve` | `{username}` → `{chat_id}`，用于 `#@username` 形式的会话 |
| POST | `/api/tasks` | `{target: "chat:mid"}` 新建任务 |
| POST | `/api/tasks/cancel` | `{id}` 取消 |
| POST | `/api/tasks/clear` | 清掉已完成/失败 |

## 已知限制

- 只监听 `127.0.0.1`，并要求 token；token 在 `.tgdl/server.token`。
- 任务**顺序执行**：每个任务自己已经开了 N 个在途请求，再并行只会互相抢 flood 额度。
- 目前**只适配了 `/a`（webk）**，`/k` 未验证。`/a` 的实测结构：

  ```html
  <div class="Message message-list-item" data-message-id="6733">
    <div class="message-content ... media ...">
      <div class="content-inner">
        <div class="media-inner ...">   <!-- position:relative; overflow:hidden -->
          <img class="full-media">
        </div>
      </div>
    </div>
  </div>
  ```

  选择器：消息 `.Message[data-message-id]`，媒体 `.media-inner`，角标绝对定位在里面。
  实测注入后 4 条媒体消息（6733 / 6734 / 6789 / 6799）全部命中。

- **页面里的 `fetch` 到 `http://127.0.0.1` 会被 Chrome 的 Local Network Access 拦掉**
  （同源正常、到本地超时，CSP 的 `connect-src` 其实是允许的）。
  所以脚本必须用 `GM_xmlhttpRequest`——它走扩展上下文，不受页面策略限制。
  这也意味着**链路只能在真实 Tampermonkey 环境里验证**，用 CDP 往页面注入脚本测不出来。
  设置页的「测试连接」按钮就是为这个准备的。
