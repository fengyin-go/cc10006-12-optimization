<template>
  <section class="page" data-module="watermark">
    <header class="page-head">
      <div>
        <h2>水位边界与迟到治理</h2>
        <p class="page-desc">
          超窗数据进入隔离清单而非静默丢弃；重复事件以首次计算为准；检查点恢复后已修补窗口不回退。
          隔离数量与处理结果同步到质量台账与流处理作业状态。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" :disabled="loading" @click="runDemo">回放完整场景</button>
        <button class="btn" type="button" :disabled="loading" @click="settleBatch">正常+超窗同时结算</button>
        <button class="btn" type="button" :disabled="loading" @click="saveCheckpoint">保存检查点</button>
        <button class="btn ghost" type="button" :disabled="loading" @click="resetAll">重置引擎</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <div class="stat-row">
      <article v-for="item in statCards" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <div class="wm-grid">
      <article class="wm-panel">
        <h3>事件接入（可构造迟到/重复事件）</h3>
        <form class="wm-form" @submit.prevent="ingestEvent">
          <label><span>事件标识</span><input v-model="form.event_id" placeholder="如 EVT-LATE" required /></label>
          <label><span>事件时间</span><input v-model="form.event_time" placeholder="2026-11-12 10:08" required /></label>
          <label><span>来源标识</span><input v-model="form.source_id" placeholder="src-late-1" required /></label>
          <label><span>取值</span><input v-model.number="form.value" type="number" /></label>
          <label><span>预置失败次数</span><input v-model.number="form.fail_times" type="number" min="0" /></label>
          <button class="btn primary" type="submit">接入事件</button>
        </form>
        <form class="wm-form inline" @submit.prevent="advanceTick">
          <label><span>只推进水位到</span><input v-model="tickTime" placeholder="2026-11-12 10:25" required /></label>
          <button class="btn" type="submit">推进水位</button>
          <button class="btn" type="button" @click="settleDue">按水位结算到期窗口</button>
        </form>
      </article>

      <article class="wm-panel">
        <h3>对账结论（隔离区 + 正式窗口 = 输入事件数）</h3>
        <ul class="wm-checks">
          <li v-for="(passed, name) in reconcile.checks" :key="name" :class="passed ? 'ok' : 'bad'">
            <span>{{ passed ? '✓' : '✗' }}</span>{{ name }}
          </li>
        </ul>
        <p class="wm-line">
          输入去重事件 <b>{{ reconcile.distinct_input_events }}</b> ＝ 正式窗口
          <b>{{ reconcile.formal_window_events }}</b> ＋ 隔离主记录
          <b>{{ reconcile.quarantine_primary_records }}</b>
          ；重复副本 {{ reconcile.duplicate_arrival_markers }} 次均未二次计算。
        </p>
      </article>
    </div>

    <article class="wm-panel">
      <h3>窗口结果（含边界口径与上游原始事件追溯）</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>窗口</th><th>状态</th><th>采用边界</th><th>修订版本</th>
            <th>聚合值</th><th>事件数</th><th>修补来源</th><th>上游原始事件（点击展开）</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="win in state.windows" :key="win.window_start">
            <td>{{ win.window_start }} ~ {{ win.window_end }}</td>
            <td><span :class="['badge', win.status]">{{ win.status }}</span></td>
            <td>{{ win.boundary ?? '—' }}</td>
            <td>rev-{{ win.revision }}</td>
            <td>{{ win.result_sum }}</td>
            <td>{{ win.result_count }}</td>
            <td>{{ win.patched_by.join('、') || '—' }}</td>
            <td>
              <button class="link" type="button" @click="toggle(`w-${win.window_start}`)">
                {{ expanded[`w-${win.window_start}`] ? '收起明细' : `查看 ${win.events.length} 条原始事件` }}
              </button>
              <ul v-if="expanded[`w-${win.window_start}`]" class="wm-events">
                <li v-for="evt in win.events" :key="evt.event_id">
                  {{ evt.event_id }} · 来源 {{ evt.source_id }} · 事件时间 {{ evt.event_time }} · 值 {{ evt.value }}
                </li>
              </ul>
              <ul v-if="win.duplicate_markers?.length" class="wm-events dup">
                <li v-for="(m, i) in win.duplicate_markers" :key="i">
                  重复副本仅登记来源：{{ m.source_id }}（第 {{ m.arrival_seq }} 次到达，未计算）
                </li>
              </ul>
            </td>
          </tr>
          <tr v-if="!state.windows.length"><td colspan="8" class="empty-state">暂无窗口，可先接入事件或回放完整场景</td></tr>
        </tbody>
      </table>
    </article>

    <article class="wm-panel">
      <h3>隔离清单（超窗不静默丢弃，可人工恢复/回算，失败重试中断按原因登记）</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>隔离编号</th><th>原始事件</th><th>归属窗口</th><th>落后水位(分)</th>
            <th>原因</th><th>状态</th><th>失败/尝试</th><th>重复来源副本</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="q in state.quarantine" :key="q.quarantine_id">
            <td>{{ q.quarantine_id }}</td>
            <td>
              <button class="link" type="button" @click="toggle(`q-${q.quarantine_id}`)">
                {{ q.event_id }}
              </button>
              <pre v-if="expanded[`q-${q.quarantine_id}`]" class="wm-raw">{{ JSON.stringify(q.original_event, null, 2) }}</pre>
            </td>
            <td>{{ q.window_start }} ~ {{ q.window_end }}</td>
            <td>{{ q.lag_minutes }}</td>
            <td>{{ q.reason }}</td>
            <td><span :class="['badge', q.status]">{{ q.status }}</span></td>
            <td>
              <span v-for="(a, i) in q.attempts" :key="i" :class="['attempt', a.ok ? 'ok' : 'bad']">
                {{ a.action }}/{{ a.ok ? '成功' : a.reason }}
              </span>
              <span v-if="!q.attempts.length">—</span>
            </td>
            <td>
              <span v-if="!q.duplicate_sources.length">—</span>
              <ul v-else class="wm-events dup">
                <li v-for="(m, i) in q.duplicate_sources" :key="i">{{ m.source_id }}（第 {{ m.arrival_seq }} 次）</li>
              </ul>
            </td>
            <td class="row-actions">
              <button class="link" type="button" @click="recompute(q.quarantine_id)">回算恢复</button>
              <button class="link" type="button" @click="abort(q.quarantine_id)">登记中断</button>
            </td>
          </tr>
          <tr v-if="!state.quarantine.length"><td colspan="9" class="empty-state">隔离清单为空，超窗事件会出现在这里而不是被丢弃</td></tr>
        </tbody>
      </table>
    </article>

    <div class="wm-grid">
      <article class="wm-panel">
        <h3>检查点（恢复后已修补窗口保持修补结果）</h3>
        <ul class="wm-cp">
          <li v-for="cp in state.checkpoints" :key="cp.checkpoint_id">
            {{ cp.checkpoint_id }} · {{ cp.label }} · 水位 {{ cp.watermark ?? '—' }}
            <button class="link" type="button" @click="restore(cp.checkpoint_id)">从此恢复</button>
          </li>
          <li v-if="!state.checkpoints.length" class="empty-state">暂无检查点</li>
        </ul>
      </article>
      <article class="wm-panel">
        <h3>质量台账流水（跨模块同步）</h3>
        <ol class="wm-ledger">
          <li v-for="row in state.ledger.slice().reverse().slice(0, 12)" :key="row.seq">
            <span :class="['tag', row.reason ? 'bad' : 'ok']">{{ row.reason || '正常' }}</span>
            {{ row.action }}：{{ row.detail }}
          </li>
        </ol>
      </article>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import { request } from '@/api/client'

interface WindowRow {
  window_start: string
  window_end: string
  status: string
  boundary: string | null
  revision: number
  result_sum: number
  result_count: number
  patched_by: string[]
  duplicate_markers?: Array<{ source_id: string; arrival_seq: number }>
  events: Array<{ event_id: string; source_id: string; event_time: string; value: number }>
}
interface QuarantineRow {
  quarantine_id: string
  event_id: string
  window_start: string
  window_end: string
  lag_minutes: number
  reason: string
  status: string
  duplicate_sources: Array<{ source_id: string; arrival_seq: number }>
  attempts: Array<{ action: string; ok: boolean; reason: string }>
  original_event: Record<string, unknown>
}
interface EngineState {
  watermark: string | null
  metrics: Record<string, number | Record<string, number>>
  windows: WindowRow[]
  quarantine: QuarantineRow[]
  checkpoints: Array<{ checkpoint_id: string; label: string; watermark: string | null }>
  ledger: Array<{ seq: number; action: string; reason: string; detail: string }>
  reconcile: {
    distinct_input_events: number
    formal_window_events: number
    quarantine_primary_records: number
    duplicate_arrival_markers: number
    checks: Record<string, boolean>
    all_passed: boolean
  }
}

const ENDPOINT = '/api/watermark'
const loading = ref(false)
const errorMessage = ref('')
const expanded = reactive<Record<string, boolean>>({})
const tickTime = ref('2026-11-12 10:25')
const form = reactive({ event_id: 'EVT-LATE', event_time: '2026-11-12 10:08', source_id: 'src-late-1', value: 99, fail_times: 0 })

const state = reactive<EngineState>({
  watermark: null,
  metrics: {},
  windows: [],
  quarantine: [],
  checkpoints: [],
  ledger: [],
  reconcile: {
    distinct_input_events: 0,
    formal_window_events: 0,
    quarantine_primary_records: 0,
    duplicate_arrival_markers: 0,
    checks: {},
    all_passed: false,
  },
})

const reconcile = computed(() => state.reconcile)
const statCards = computed(() => {
  const m = state.metrics as Record<string, number>
  return [
    { label: '当前水位', value: state.watermark ?? '未推进' },
    { label: '隔离事件数', value: m.quarantine_total ?? 0 },
    { label: '待处理隔离', value: m.quarantine_pending ?? 0 },
    { label: '重复副本（仅登记）', value: m.duplicate_arrivals ?? 0 },
    { label: '已修补窗口', value: m.patched_windows ?? 0 },
    { label: '回算成功/失败', value: `${m.recompute_success ?? 0}/${m.recompute_failure ?? 0}` },
    { label: '对账结果', value: state.reconcile.all_passed ? '全部通过' : '存在异常' },
  ]
})

function toggle(key: string) {
  expanded[key] = !expanded[key]
}

async function call(path: string, init?: RequestInit, reloadAfter = true) {
  loading.value = true
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}${path}`, init)
    const payload = await response.json()
    if (payload.ok === false) {
      errorMessage.value = `${payload.reason ?? '操作未生效'}：${payload.message ?? ''}`
    }
    if (reloadAfter) await reload()
    return payload
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '水位治理接口调用失败'
  } finally {
    loading.value = false
  }
}

async function reload() {
  const response = await request(`${ENDPOINT}/state`)
  if (!response.ok) {
    errorMessage.value = '水位治理状态读取失败'
    return
  }
  const payload = await response.json()
  Object.assign(state, payload.data)
}

function runDemo() {
  void call('/demo/run', { method: 'POST' })
}
function resetAll() {
  void call('/reset', { method: 'POST' })
}
function settleDue() {
  void call('/settle', { method: 'POST' })
}
function settleBatch() {
  void call('/settle-batch', { method: 'POST' })
}
function saveCheckpoint() {
  void call('/checkpoint', {
    method: 'POST',
    body: JSON.stringify({ label: `手动检查点 ${new Date().toLocaleTimeString()}` }),
  })
}
function restore(checkpointId: string) {
  void call('/restore', { method: 'POST', body: JSON.stringify({ checkpoint_id: checkpointId }) })
}
function recompute(quarantineId: string) {
  void call('/quarantine/recompute', { method: 'POST', body: JSON.stringify({ quarantine_id: quarantineId }) })
}
function abort(quarantineId: string) {
  void call('/quarantine/abort', { method: 'POST', body: JSON.stringify({ quarantine_id: quarantineId, reason: '人工中断' }) })
}
function ingestEvent() {
  void call('/ingest', { method: 'POST', body: JSON.stringify({ ...form }) })
}
function advanceTick() {
  void call('/tick', { method: 'POST', body: JSON.stringify({ event_time: tickTime.value }) })
}

onMounted(reload)
</script>

<style scoped>
.wm-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px; }
.wm-panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; margin-bottom: 12px; }
.wm-panel h3 { margin: 0 0 10px; font-size: 14px; }
.wm-form { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; }
.wm-form.inline { margin-top: 10px; }
.wm-form label span { display: block; font-size: 12px; color: var(--muted); margin-bottom: 2px; }
.wm-form input { padding: 6px 8px; border: 1px solid var(--border); border-radius: 6px; min-width: 130px; }
.wm-checks { list-style: none; padding: 0; margin: 0; font-size: 13px; display: grid; grid-template-columns: 1fr; gap: 4px; }
.wm-checks li.ok { color: #067647; }
.wm-checks li.bad { color: #b42318; }
.wm-line { font-size: 12px; color: var(--muted); margin: 8px 0 0; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; background: #eef2f7; }
.badge.已修补, .badge.已回算 { background: #e6f4ea; color: #067647; }
.badge.已结算 { background: #e8f0fe; color: #1f6feb; }
.badge.回算失败, .badge.已中断 { background: #fde8e8; color: #b42318; }
.badge.待处理 { background: #fff4e0; color: #b54708; }
.wm-events { list-style: none; padding: 6px 0 0; margin: 0; font-size: 12px; color: var(--muted); }
.wm-events.dup li { color: #b54708; }
.wm-raw { font-size: 11px; background: #f6f8fb; padding: 6px; border-radius: 6px; margin: 4px 0 0; }
.attempt { display: inline-block; margin-right: 4px; font-size: 11px; padding: 1px 6px; border-radius: 8px; }
.attempt.ok { background: #e6f4ea; color: #067647; }
.attempt.bad { background: #fde8e8; color: #b42318; }
.wm-cp, .wm-ledger { margin: 0; padding-left: 18px; font-size: 12px; }
.wm-cp li { margin-bottom: 6px; }
.wm-ledger li { margin-bottom: 4px; }
.tag { display: inline-block; padding: 0 6px; border-radius: 8px; font-size: 11px; margin-right: 4px; }
.tag.ok { background: #e6f4ea; color: #067647; }
.tag.bad { background: #fde8e8; color: #b42318; }
</style>
