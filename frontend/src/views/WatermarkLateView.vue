<template>
  <section class="page" data-testid="watermark-late-detail">
    <header class="page-head">
      <div>
        <h2>水位迟到分布 · {{ distribution?.watermark['水位名称'] ?? `水位 ${watermarkId}` }}</h2>
        <p class="page-desc">
          读取水位记录识别迟到事件，冻结迟到旁路版本并确定一份可执行补数范围。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" @click="reload">刷新</button>
        <RouterLink class="btn" to="/watermark_track">返回水位追踪</RouterLink>
        <RouterLink class="btn" to="/late-bypass/backfill">查看补数任务</RouterLink>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="late-error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="late-notice">{{ notice }}</p>

    <div v-if="distribution" class="stat-row">
      <article class="stat-card">
        <span class="stat-label">当前水位</span>
        <strong class="stat-value">{{ formatTs(Number(distribution.watermark['当前水位'])) }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">水位区间</span>
        <strong class="stat-value small">{{ distribution.scopes.current.interval_label }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">当前口径迟到数</span>
        <strong class="stat-value">{{ distribution.scopes.current.late_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">回看口径迟到数</span>
        <strong class="stat-value">{{ distribution.scopes.lookback.late_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">重复到达去重</span>
        <strong class="stat-value">{{ distribution.scopes.current.duplicate_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">旁路版本</span>
        <strong class="stat-value">{{ distribution.versions.length }}</strong>
      </article>
    </div>

    <template v-if="distribution">
      <section class="panel" data-testid="rule-hit">
        <h3>阈值判定与 4 号口径条件范围</h3>
        <div class="detail-grid">
          <div>
            <span class="muted">判定规则</span>
            <strong>{{ distribution.rule_hit.rule }}（{{ distribution.rule_hit.policy }}）</strong>
          </div>
          <div>
            <span class="muted">命中结果</span>
            <strong :class="distribution.rule_hit.matched ? 'hit' : 'miss'">
              {{ distribution.rule_hit.matched ? '命中' : '未命中' }} · {{ distribution.rule_hit.reason }}
            </strong>
          </div>
          <div>
            <span class="muted">迟到截止（水位 - 允许延迟）</span>
            <strong>{{ formatTs(distribution.rule_hit.condition_range.cutoff) }}</strong>
          </div>
          <div>
            <span class="muted">条件范围</span>
            <strong class="mono">{{ distribution.rule_hit.condition_range.expression }}</strong>
          </div>
        </div>
      </section>

      <section class="panel">
        <h3>迟到分布（当前口径）</h3>
        <div class="analysis-chart" data-testid="late-distribution">
          <div
            v-for="bucket in distribution.distribution"
            :key="bucket.bucket"
            class="analysis-bar"
            :style="{ height: `${barHeight(bucket.count)}px` }"
            :title="`${bucket.bucket}：${bucket.count} 条`"
          >
            <span>{{ bucket.bucket }}（{{ bucket.count }}）</span>
          </div>
        </div>
        <table class="data-table">
          <thead>
            <tr><th>分桶（×允许延迟）</th><th>迟到事件数</th><th>事件ID</th></tr>
          </thead>
          <tbody>
            <tr v-for="bucket in distribution.distribution" :key="`row-${bucket.bucket}`">
              <td>{{ bucket.bucket }}</td>
              <td>{{ bucket.count }}</td>
              <td class="mono">{{ bucket.event_ids.join('、') || '—' }}</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="scope-grid">
        <article
          v-for="scope in scopeList"
          :key="scope.scope"
          class="panel scope-card"
          :data-testid="`scope-${scope.scope}`"
        >
          <h3>{{ scope.scope_label }}</h3>
          <p class="muted">
            区间 {{ formatTs(scope.interval.start) }} ~ {{ formatTs(scope.interval.end) }}，
            截止 {{ formatTs(scope.interval.cutoff) }}（允许延迟 {{ scope.interval.allowed }} 秒）
          </p>
          <div class="detail-grid">
            <div><span class="muted">迟到事件</span><strong>{{ scope.late_count }}</strong></div>
            <div><span class="muted">待旁路（新增）</span><strong>{{ scope.candidate_count }}</strong></div>
            <div><span class="muted">已旁路跳过</span><strong>{{ scope.already_bypassed_count }}</strong></div>
            <div><span class="muted">迟到率 / 阈值</span><strong>{{ pct(scope.late_ratio) }} / {{ pct(scope.threshold) }}</strong></div>
          </div>
          <details class="event-list">
            <summary>迟到事件明细（{{ scope.late_events.length }}）</summary>
            <table class="data-table">
              <thead>
                <tr><th>事件ID</th><th>事件时间</th><th>到达时间</th><th>迟到秒</th></tr>
              </thead>
              <tbody>
                <tr v-for="event in scope.late_events" :key="`${scope.scope}-${event.event_id}-${event.arrival_time}`">
                  <td>{{ event.event_id }}</td>
                  <td>{{ event.event_time_iso }}</td>
                  <td>{{ event.arrival_time_iso }}</td>
                  <td>{{ event.lateness }}</td>
                </tr>
              </tbody>
            </table>
          </details>
          <div class="page-actions">
            <button
              class="btn primary"
              type="button"
              :data-testid="`freeze-${scope.scope}`"
              :disabled="busy"
              @click="freeze(scope.scope)"
            >
              冻结{{ scope.scope_label }}
            </button>
          </div>
        </article>
      </section>

      <section class="panel">
        <h3>同时提交两种口径</h3>
        <p class="muted">
          同一次提交只落一次记录；重复点击返回已接收的同一组版本。两种口径分别形成独立版本，
          并写入迟到旁路数据当前结果，再由责任审核人确定其中一份可执行范围。
        </p>
        <form class="filter-bar" @submit.prevent="submitGroup">
          <label class="filter-item">
            <span>提交请求号</span>
            <input v-model="requestId" data-testid="group-request-id" placeholder="同一请求号只记一次" />
          </label>
          <button class="btn primary" data-testid="submit-group" type="submit" :disabled="busy">
            同时提交两种口径
          </button>
        </form>
        <p v-if="lastGroup" class="mono" data-testid="group-result">
          {{ lastGroup }}
        </p>
      </section>

      <section class="panel">
        <h3>迟到旁路数据当前结果</h3>
        <table class="data-table">
          <thead>
            <tr><th>数据源</th><th>口径</th><th>当前版本</th><th>水位区间</th><th>补数范围</th><th>可执行</th></tr>
          </thead>
          <tbody>
            <tr v-for="row in distribution.current_result" :key="`current-${row.id}`">
              <td>{{ row.数据源 }}</td>
              <td>{{ row.口径名称 }}</td>
              <td>{{ row.当前版本号 }}</td>
              <td>{{ row.水位区间 }}</td>
              <td>{{ row.补数范围 }}</td>
              <td>{{ row.可执行 ? '是' : '候选' }}</td>
            </tr>
            <tr v-if="!distribution.current_result.length">
              <td colspan="6" class="empty-state">尚未冻结旁路版本</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="panel">
        <h3>旁路版本与补数范围</h3>
        <table class="data-table">
          <thead>
            <tr>
              <th>版本号</th><th>口径</th><th>水位区间</th><th>补数范围</th>
              <th>迟到事件数</th><th>阈值命中</th><th>状态</th><th>作业</th><th>回执</th><th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="version in distribution.versions" :key="version.id" :data-testid="`version-${version.id}`">
              <td>{{ version.版本号 }}</td>
              <td>{{ version.口径名称 }}</td>
              <td>{{ version.水位区间 }}</td>
              <td>{{ version.补数范围 }}</td>
              <td>{{ version.迟到事件数 }}</td>
              <td>{{ version.阈值命中 }}</td>
              <td>{{ version.status }}</td>
              <td>{{ version.作业ID ? `#${version.作业ID} ${version.作业状态}` : '未生成' }}</td>
              <td>{{ version.回执数 }}</td>
              <td class="row-actions">
                <button
                  v-if="version.status === '待确认'"
                  class="link"
                  type="button"
                  :data-testid="`confirm-${version.id}`"
                  :disabled="busy"
                  @click="confirm(version.id)"
                >
                  确认可执行范围
                </button>
                <button
                  v-if="version.作业ID"
                  class="link"
                  type="button"
                  @click="goBackfill(version.作业ID)"
                >
                  查看作业
                </button>
                <button
                  v-if="['部分失败', '已完成'].includes(version.status)"
                  class="link"
                  type="button"
                  :data-testid="`reopen-${version.id}`"
                  :disabled="busy"
                  @click="reopen(version.id)"
                >
                  故障重开
                </button>
              </td>
            </tr>
            <tr v-if="!distribution.versions.length">
              <td colspan="10" class="empty-state">尚未冻结旁路版本，可用上方按钮冻结</td>
            </tr>
          </tbody>
        </table>
      </section>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'

import { lateBypassApi, type Distribution } from '@/api/late-bypass'

const route = useRoute()
const router = useRouter()
const watermarkId = computed(() => Number(route.params.id))
const distribution = ref<Distribution | null>(null)
const errorMessage = ref('')
const notice = ref('')
const busy = ref(false)
const requestId = ref('')
const lastGroup = ref('')

const scopeList = computed(() => {
  if (!distribution.value) return []
  return [distribution.value.scopes.current, distribution.value.scopes.lookback]
})

function formatTs(value: number) {
  if (!Number.isFinite(value) || value <= 0) return String(value)
  return new Date(value * 1000).toISOString().replace('T', ' ').slice(0, 19)
}

function pct(value: number) {
  return `${(value * 100).toFixed(1)}%`
}

function barHeight(count: number) {
  const max = Math.max(1, ...(distribution.value?.distribution.map((item) => item.count) ?? [1]))
  return Math.max(8, Math.round((count / max) * 120))
}

function newRequestId(prefix: string) {
  return `${prefix}-${watermarkId.value}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`
}

async function reload() {
  errorMessage.value = ''
  try {
    distribution.value = await lateBypassApi.distribution(watermarkId.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '迟到分布读取失败'
  }
}

function describe(result: { created?: boolean; idempotent?: boolean; reason?: string; version?: { 版本号: string } }) {
  if (result.version) {
    return result.created
      ? `已冻结 ${result.version.版本号}`
      : `${result.idempotent ? '重复提交未新增版本' : '未新增版本'}：${result.version.版本号}`
  }
  return result.reason ?? '未产生新版本'
}

async function freeze(scope: 'current' | 'lookback') {
  busy.value = true
  notice.value = ''
  errorMessage.value = ''
  try {
    const result = await lateBypassApi.freeze({
      watermark_id: watermarkId.value,
      scope,
      request_id: newRequestId(`freeze-${scope}`),
    })
    notice.value = describe(result)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '冻结旁路版本失败'
  } finally {
    busy.value = false
  }
}

async function submitGroup() {
  busy.value = true
  notice.value = ''
  errorMessage.value = ''
  try {
    const result = await lateBypassApi.submitGroup({
      watermark_id: watermarkId.value,
      request_id: requestId.value.trim() || newRequestId('group'),
    })
    const count = result.versions?.length ?? 0
    lastGroup.value = result.idempotent
      ? `重复提交：同一次提交只落一次记录，复用 ${count} 个版本`
      : `已接收整组结果：${count} 个口径版本（${(result.versions ?? []).map((item) => item.口径名称).join('、')}）`
    notice.value = lastGroup.value
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '整组提交失败'
  } finally {
    busy.value = false
  }
}

async function confirm(versionId: number) {
  busy.value = true
  notice.value = ''
  errorMessage.value = ''
  try {
    const result = await lateBypassApi.confirm(versionId, {
      request_id: newRequestId('confirm'),
    })
    notice.value = result.job
      ? `已生成补数作业 #${result.job.id}，范围 ${result.job.补数范围}`
      : result.reason ?? '补数范围已确认'
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '确认补数范围失败'
  } finally {
    busy.value = false
  }
}

async function reopen(versionId: number) {
  busy.value = true
  notice.value = ''
  errorMessage.value = ''
  try {
    const result = await lateBypassApi.reopen(versionId, {
      request_id: newRequestId('reopen'),
    })
    notice.value = result.created
      ? '已按同一口径重开，补数范围不变'
      : result.reason ?? '重复重开请求未重复记录'
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '故障重开失败'
  } finally {
    busy.value = false
  }
}

function goBackfill(jobId: number) {
  void router.push({ path: '/late-bypass/backfill', query: { job: String(jobId) } })
}

onMounted(reload)
watch(watermarkId, reload)
</script>
