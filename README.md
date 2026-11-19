# pkg-cache：单进程 HTTP 包缓存代理 + 可控本地源站

面向构建工程的依赖包缓存代理：固定本地源站、按路径 GET 完整包，减少重复
回源流量；源包更新后按路径清除缓存。代理由 **FastAPI + aiohttp** 实现，
单进程单 worker 运行。

## 快速开始

```bash
docker compose up --build
```

* 代理：http://localhost:8080 （下载 API + `/docs` 清除入口）
* 源站：http://localhost:8081 （控制 API + `/docs`，自带 32MiB 包 `pkg-32m.bin` 与 1MiB 包 `pkg-1m.bin`）

## 使用

```bash
# 下载（首次 MISS 回源，之后 HIT 命中磁盘缓存；响应头带 X-Fill-ID）
curl -D - -OJ http://localhost:8080/packages/pkg-32m.bin

# 按路径清除（也可打开 http://localhost:8080/docs 在 Swagger UI 里执行）
curl -X POST http://localhost:8080/purge/pkg-32m.bin

# 查看代理内部状态
curl http://localhost:8080/cache/status

# 查看最近 50 次失败填充（最新在前），支持 ?limit=N
curl http://localhost:8080/cache/incidents

# 凭响应头里的 X-Fill-ID 精确定位某次失败填充
curl http://localhost:8080/cache/incidents/<X-Fill-ID>
```

源站控制（换内容 / 延迟 / 断流 / 回源次数）：

```bash
# 查看回源次数与当前包 sha256
curl http://localhost:8081/control/stats

# 更换包内容（重新生成随机字节，sha256 改变；代理在清除前仍发旧包）
curl -X POST http://localhost:8081/control/content/pkg-32m.bin

# 每个 256KiB 块延迟 100ms（模拟慢源站）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":100,"truncate_after_bytes":null,"chunk_size":262144}'

# 发送 200000 字节后断流（模拟上游故障）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":0,"truncate_after_bytes":200000,"chunk_size":65536}'

# 让源站对包请求直接回 503（模拟源站错误；404 由不存在的包天然产生）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"fail_status":503}'

# 清零回源计数
curl -X POST http://localhost:8081/control/reset
```

## 填充编号与失败事件（X-Fill-ID / incidents）

* **每次回源填充分配一个 `X-Fill-ID`**（32 位十六进制）。所有参与该填充的
  下载响应头都返回同一个编号，包括正文开始前就失败的错误响应（如源站 5xx）；
  缓存命中沿用已完成填充的编号；清除后重新填充产生新编号。
* **`GET /cache/incidents`** 返回最近 50 次失败填充（最新在前，可加
  `?limit=N`），每行含：`fill_id`、`path`（包路径）、`bytes_written`
  （失败前已写入缓存卷的字节）、`reason` 分类、`origin_status`、错误详情与
  时间戳。`GET /cache/incidents/{fill_id}` 按编号精确查询。
* **三类原因**：
  * `origin_status`：源站回了非 200/非 404 状态（如 500/503）；
  * `transport`：连接、超时或正文传输中断（含 Content-Length 半包）；
  * `local_write`：代理本地缓存卷打不开、写入/落盘失败。
* **记账规则**：同一次单飞失败只记一条（并发跟随者共享编号与记录）；
  源站 **404** 与**客户端自行断连**不算填充故障、不记录；清除（purge）不影响
  历史记录，旧填充的记录永远归属旧编号，之后新填充成功也不会覆盖；
  失败填充的半包立即删除、绝不入缓存，下一个请求自动重试回源。
* 事件只保留在**进程内存**中（deque 上限 50，最旧自动淘汰），进程重启即清空；
  磁盘上的完整包不受重启影响。

## 行为要点

* **单飞回源**：同路径并发未命中只回源一次；所有下载（包括中途加入的）
  都从字节 0 开始逐块收到原字节，首块在源站首块落盘后即可读出，不等整包。
* **慢客户端隔离**：填充只写磁盘，各读者按自己的节奏尾随 `.part-<代>` 文件；
  单客户端内存为一个读缓冲，不随包长增长（实测 32MiB 下载代理 RSS 增量约 4MiB）。
* **按路径清除**：清除返回后首个新下载重新回源，后续共用新填充/缓存；
  已开始的旧下载继续读完旧代文件；旧填充完成也不会重新登记；
  旧文件在最后一个读者结束后回收。
* **失败传播**：上游断流/非 200 使该代所有下载失败（客户端读到不完整
  响应），错误响应同样带 `X-Fill-ID`；半包立即删除、绝不入缓存，失败原因
  进入 `/cache/incidents`；下一个请求自动重试回源。
* **重启**：`proxy-cache` 卷保留完整包；启动时清理 `.part-*` 残件与孤儿
  meta。404 不做负缓存；失败事件仅在进程内保留，重启清空。

## 本地自测（无 Docker 时）

```bash
pip install -r proxy/requirements.txt -r origin/requirements.txt
pip install httpx pytest
python3 -m pytest tests/ -v
```

测试以真实子进程启动源站与代理，覆盖：单飞、首块延迟、慢客户端隔离、
清除语义（含填充中清除）、断流不缓存半包、重启保留/清理、内存有界、
404 与路径穿越；`test_incidents.py` 另覆盖 X-Fill-ID 透传（MISS/HIT/
错误响应/新代编号）、三类失败分类、单飞只记一条、404 与客户端断连不记账、
清除后成功不覆盖、50 条上限与本地写失败分类。`tests/run_server.py`
是测试专用启动器（本沙箱禁止进程间信号，用控制文件代替 SIGTERM 停服），
交付应用本身不含测试钩子。
