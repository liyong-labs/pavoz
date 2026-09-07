# ai_research 部署规范 (2026-09-07)

> **slim 部署 spec, 基于过去 2 周踩的 7 个坑**. **0 自动下载 / 0 env override / fail-fast**.
> 配套: [PRD.md](PRD.md) / [design.md](design.md) / [pavoz-requirements.md](pavoz-requirements.md)

## 1. 三层架构

```
ai_research client (本机 CC session)
   └→ kb-haomem MCP server (本机 stdio)
       └→ 50m haomem (kb ECS / unix socket /tmp/haomem.sock)
           └→ model snapshot (本地 /home/haomem/.cache/huggingface/...)
```

**外网唯一访问**: minimaxi api (anthropic-compatible, 付费).

## 2. 本机 ai_research 配置

### 2.1 settings.json (最小集, 14 env)

文件: `D:/Work/claude/ai_research/.claude/settings.json`

```json
{
  "env": {
    "ANTHROPIC_AUTH_TOKEN": "sk-cp-...",
    "ANTHROPIC_BASE_URL": "https://api.minimaxi.com/anthropic",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL": "MiniMax-M3[1m]",
    "ANTHROPIC_DEFAULT_OPUS_MODEL": "MiniMax-M3[1m]",
    "ANTHROPIC_DEFAULT_SONNET_MODEL": "MiniMax-M3[1m]",
    "ANTHROPIC_MODEL": "MiniMax-M3[1m]",
    "ANTHROPIC_REASONING_MODEL": "MiniMax-M3[1m]",
    "API_TIMEOUT_MS": "300000",
    "CAVEMAN_DEFAULT_MODE": "wenyan-full",
    "CLAUDE_CODE_ATTRIBUTION_HEADER": "0",
    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
    "CLAUDE_CODE_AUTO_COMPACT_WINDOW": "512000",
    "CLAUDE_CODE_EFFORT_LEVEL": "auto",
    "TEMPERATURE": "0.1",
    "HAOMEM_SERVER_URL": "https://hm.bitensor.com",
    "HAOMEM_TOKEN_PATH": "~/.haomem/server-token",
    "HAOMEM_AGENT_ID": "ai_research@local"
  },
  "permissions": {"defaultMode": "bypassPermissions"}
}
```

**为什么这些是必需的**:
- ANTHROPIC_*: CC session 调 LLM 用 (minimaxi api)
- CLAUDE_CODE_*: harness 行为开关
- TEMPERATURE/CAVEMAN: agent 模式
- HAOMEM_*: agent client 调 KB server 用 (URL + token path + agent_id)
- **没有**: HF_* / 任何 ai_research 不用的 env

### 2.2 agent.json (identity)

文件: `D:/Work/claude/ai_research/.claude/agent.json`

```json
{
  "agent": "ai_research@local",
  "project": "ai_research",
  "location": "local",
  "description": "ai_research (deep-research DAG) 本地 dev, prod 在 kb ECS"
}
```

### 2.3 scheduled_tasks.json (空, 待加)

```json
[]
```

### 2.4 .gitignore

```
.claude/settings.json
.claude/scheduled_tasks.json
__pycache__/
*.pyc
.ruff_cache/
.env
*.log
```

## 3. 本机 MCP server

### 3.1 文件

`C:/Users/Administrator/.claude/scripts/kb-mcp-server.py`

**关键代码** (3 处):
1. `ENDPOINT = os.environ.get("KB_ENDPOINT", "https://hm.bitensor.com")` (硬码新域名, 不依赖 env)
2. `_get_token()` 优先查 `~/.haomem/server-token`, fallback `~/.memsearch/server-token`
3. 启动 stderr log: `kb-mcp-server: ready, endpoint=..., token=...`

### 3.2 配置

`C:/Users/Administrator/.claude.json`:
```json
"kb-haomem": {
  "command": "python",
  "args": ["C:/Users/Administrator/.claude/scripts/kb-mcp-server.py"],
  "env": {"KB_ENDPOINT": "https://hm.bitensor.com"}
}
```

## 4. 本机 agent token

### 4.1 生成

```bash
DIGEST=$(python3 -c "
import hashlib
agent_id = 'ai_research'
fingerprint = 'local-2026-09-07'
key = f'{agent_id}:{fingerprint}'.encode()
print(hashlib.sha256(key).hexdigest()[:24])
")
echo "haomem-ai_research-${DIGEST}" > ~/.haomem/server-token
chmod 600 ~/.haomem/server-token
```

### 4.2 server 端 ACL 注册

```bash
ssh bitensor-com-root
python3 << 'EOF'
import hashlib, json, sys
agent_id = "ai_research"
fingerprint = "local-2026-09-07"
key = f"{agent_id}:{fingerprint}".encode()
digest = hashlib.sha256(key).hexdigest()[:24]
new_token = f"haomem-{agent_id}-{digest}"
path = "/home/haomem/data/token-acl.json"
with open(path) as f: acl = json.load(f)
acl["tokens"][new_token] = ["public-knowledge", "work-note-ai-research", "agent-profile", "profile-public"]
acl.setdefault("agents", {})[agent_id] = {
    "host": "local", "fingerprint": fingerprint,
    "scopes": ["public-knowledge", "work-note-ai-research", "agent-profile", "profile-public"],
    "created_at": "2026-09-07", "ttl_days": None,
}
with open(path, "w") as f: json.dump(acl, f, indent=2)
print(f"OK: tokens={len(acl['tokens'])}")
EOF

# nginx map 加一行 (硬码 Bearer token whitelist)
sed -i '/Bearer haomem-kb_guard/a\        "Bearer haomem-ai_research-04ee721a4895ddb0869935bc" 1;' \
    /etc/nginx/sites-enabled/hm.bitensor.com
nginx -t && systemctl restart nginx
```

## 5. 50m haomem server 配置

### 5.1 systemd unit

文件: `/home/haomem/.config/systemd/user/haomem.service`

```ini
[Unit]
Description=haomem HTTP daemon (ms.bitensor.com, user systemd)
After=network.target

[Service]
Type=simple
User=haomem
Group=haomem
WorkingDirectory=/home/haomem
Environment="HAOMEM_UDS=/tmp/haomem.sock"
Environment="TRANSFORMERS_OFFLINE=1"
Environment="HF_HUB_OFFLINE=1"
ExecStart=/home/haomem/venv/bin/python3 /home/haomem/haomem_serve.py
Restart=always
RestartSec=5
StandardOutput=append:/home/haomem/logs/daemon-stdout.log
StandardError=append:/home/haomem/logs/daemon-stderr.log

NoNewPrivileges=true

[Install]
WantedBy=default.target
```

**3 env 全部必要**:
- `HAOMEM_UDS`: unix socket 路径
- `TRANSFORMERS_OFFLINE=1`: transformers 库强制本地
- `HF_HUB_OFFLINE=1`: hub.py 强制本地 (双保险)
- **绝不能加** `HF_ENDPOINT=https://hf-mirror.com` (会让 service 重启触发下载)

### 5.2 service user = haomem

**为什么**: HF 默认 cache path = `~/.cache/huggingface` (`~` = service user HOME).
- memsearch user HOME = `/home/memsearch` (错路径)
- haomem user HOME = `/home/haomem` (对, model snapshot 在这)

**绝不能改 HF cache path via env** (HF 铁律, env 不生效).

### 5.3 model snapshot

位置: `/home/haomem/.cache/huggingface/hub/models--qihoo360--Zhinao-ChineseModernBert-Embedding/snapshots/a476584b3d44d0af3d998c948f38bfc6b97e40a7/`

**绝不能自动重下**: `TRANSFORMERS_OFFLINE=1` + `HF_HUB_OFFLINE=1` 强制本地.

**失败行为**: snapshot 缺失 → 启动失败 → systemd Restart=always 循环 + stderr 持续报错 + **不连外网**.

## 6. 启动顺序 (运维 runbook)

### 6.1 一次性 setup

```bash
# A. 本机 MCP 配置 (一次性)
#    ~/.claude/scripts/kb-mcp-server.py: ENDPOINT hard-code (已)
#    ~/.claude.json kb-haomem.env.KB_ENDPOINT=https://hm.bitensor.com (已)

# B. 本机 agent token (一次性)
echo "haomem-ai_research-04ee721a4895ddb0869935bc" > ~/.haomem/server-token
chmod 600 ~/.haomem/server-token

# C. 50m server ACL (一次性)
ssh bitensor-com-root "python3 /tmp/add-ai-research-token.py"

# D. 50m nginx map (一次性)
ssh bitensor-com-root "sed -i '/Bearer haomem-kb_guard/a ...' /etc/nginx/sites-enabled/hm.bitensor.com && nginx -t && systemctl restart nginx"

# E. 50m systemd unit (一次性, 已)
#    TRANSFORMERS_OFFLINE=1 + HF_HUB_OFFLINE=1 + User=haomem
```

### 6.2 启动

```bash
# 1. 50m haomem 起
ssh bitensor-com-root "systemctl --user daemon-reload && systemctl --user restart haomem.service"
sleep 10
ssh bitensor-com-root "curl -s --unix-socket /tmp/haomem.sock http://localhost/health"
# → status:ok + 7 collections + 15169 chunks

# 2. 本机 CC session 重启
#    Ctrl+R / 重开终端
#    /mcp → kb-haomem ✔ connected · 5 tools

# 3. 验证 MCP tool
#    kb_health() → status:ok
#    kb_search(query="...", collection="public-knowledge") → 200 + results
```

### 6.3 验证清单

```bash
# 1. 本机 settings.json HF-free
grep -i 'HF_' D:/Work/claude/ai_research/.claude/settings.json
# → 无输出 (正确)

# 2. 50m systemd unit OFFLINE
ssh bitensor-com-root "cat /home/haomem/.config/systemd/user/haomem.service | grep -E 'OFFLINE|User'"
# → User=haomem + TRANSFORMERS_OFFLINE=1 + HF_HUB_OFFLINE=1

# 3. 50m health
ssh bitensor-com-root "curl -s --unix-socket /tmp/haomem.sock http://localhost/health"

# 4. 本机 token
cat ~/.haomem/server-token
# → haomem-ai_research-04ee721a4895ddb0869935bc

# 5. MCP server endpoint
grep ENDPOINT C:/Users/Administrator/.claude/scripts/kb-mcp-server.py
# → "https://hm.bitensor.com" (硬码 default)

# 6. nginx map has ai_research
ssh bitensor-com-root "grep haomem-ai_research /etc/nginx/sites-enabled/hm.bitensor.com"
```

## 7. 失败模式 + 报警

| 失败 | 检测 | 行为 |
|---|---|---|
| MCP server 启动 race | harness mark failed | stderr 必须 "kb-mcp-server: ready" |
| haomem 启动失败 (model 缺) | /health 返 degraded 或 5xx | stderr 报 "snapshot not found", Restart=always 循环 |
| 自动下载 model | stderr 无 "Downloading" 日志 | **绝不应发生** (OFFLINE=1 阻断) |
| kb_search 500 | collection alias 漏 | `_COLLECTION_ALIASES` 加 (已加 public-knowledge → public_knowledge) |
| minimaxi 401 | session-start hook retry | 检查 ANTHROPIC_* env 是否匹配全局 |
| token 过期 | server 返 401 | 用户重生成 (跑 4.1 脚本) |

## 8. 不学 (反模式)

1. ❌ 抄模板 env 不审查 → HF 残留
2. ❌ systemd env 改 HF cache path → HF 铁律不生效
3. ❌ service 自动下载 model → 持续扣钱
4. ❌ 项目 settings.json 放全局已有 env → 覆盖冲突
5. ❌ 改 MCP server 端不修 server 端 → 跑半天白搞
6. ❌ systemd unit 改坏不备份 → 改坏无回滚 (ops-flow-not-simplify)
7. ❌ service user 跑 memsearch 但 cache 在 haomem → 路径错 → 自动下载

## 9. 关联 work-note

- `D:/Work/claude/docs/work-note/2026-09-07-50m-haomem-hf-offline-decision.md`: OFFLINE=1 决策
- `D:/Work/claude/docs/work-note/2026-09-07-memsearch-haomem-rename-nightly.md`: rename 全栈
- `D:/Work/claude/docs/work-note/2026-09-07-kb-mcp-server-fix.md`: MCP server endpoint 跟随 rename
- `D:/Work/claude/docs/work-note/2026-09-07-ai-research-mcp-failed-fix.md`: 401 retry 调试

## 10. 后续 (cron alert, 待 user 拍)

- [ ] 加 cron: `curl /health`, 失败发邮件 / 飞书 / IM
- [ ] cache 备份 (50m 换 /home 盘时)
- [ ] model snapshot 校验脚本 (启动前 safetensors sha256 验证)
