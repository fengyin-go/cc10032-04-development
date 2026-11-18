<template>
  <section class="page" data-testid="late-audit">
    <header class="page-head">
      <div>
        <h2>阈值核对</h2>
        <p class="page-desc">
          按水位区间核对迟到旁路数据与补数任务清单：哪些事件已旁路、哪些还未旁路、哪些补数范围已覆盖。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" @click="reload">刷新</button>
        <RouterLink class="btn" to="/late-bypass/backfill">补数任务</RouterLink>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="audit-error">{{ errorMessage }}</p>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">核对水位数</span>
        <strong class="stat-value">{{ auditSummary.checked ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">一致</span>
        <strong class="stat-value">{{ auditSummary.consistent ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">未旁路事件</span>
        <strong class="stat-value">{{ auditSummary.uncovered_events ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">补数作业</span>
        <strong class="stat-value">{{ auditSummary.jobs ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">阈值命中</span>
        <strong class="stat-value">{{ auditSummary.threshold_hits ?? 0 }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>水位区间起（epoch 秒）</span>
        <input v-model="start" data-testid="audit-start" placeholder="不限" />
      </label>
      <label class="filter-item">
        <span>水位区间止（epoch 秒）</span>
        <input v-model="end" data-testid="audit-end" placeholder="不限" />
      </label>
      <button class="btn" data-testid="audit-query" type="submit">核对</button>
      <button class="btn ghost" type="button" @click="reset">重置</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th>水位</th><th>数据源</th><th>水位区间</th><th>迟到截止</th>
          <th>迟到事件</th><th>已旁路</th><th>未旁路</th><th>迟到率 / 阈值</th><th>阈值命中</th>
          <th>旁路版本</th><th>补数作业</th><th>核对结论</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="row in rows"
          :key="row.水位记录ID"
          :data-testid="`audit-${row.水位记录ID}`"
          :data-state="row.state"
        >
          <td>#{{ row.水位记录ID }} {{ row.水位名称 }}</td>
          <td>{{ row.数据源 }}</td>
          <td>{{ row.水位区间 }}</td>
          <td>{{ row.迟到截止 }}</td>
          <td>{{ row.迟到事件数 }}</td>
          <td>{{ row.已旁路事件数 }}</td>
          <td>
            {{ row.未旁路事件数 }}
            <span v-if="row.未旁路事件.length" class="muted mono">（{{ row.未旁路事件.join('、') }}）</span>
          </td>
          <td>{{ pct(row.迟到率) }} / {{ row.阈值百分比 }}%</td>
          <td>{{ row.阈值命中 }}</td>
          <td>
            {{ row.旁路版本数 }}
            <span class="muted">{{ row.旁路版本.join('、') }}</span>
          </td>
          <td>
            {{ row.补数作业数 }}
            <span v-if="row.补数作业.length" class="muted">
              · 覆盖 {{ row.补数范围覆盖水位区间 ? '是' : '否' }}
            </span>
          </td>
          <td :class="row.state === '一致' ? 'ok-text' : 'warn-text'">{{ row.state }}</td>
        </tr>
        <tr v-if="!rows.length">
          <td colspan="12" class="empty-state">暂无水位记录</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { RouterLink } from 'vue-router'

import { lateBypassApi, type AuditRow } from '@/api/late-bypass'

const rows = ref<AuditRow[]>([])
const auditSummary = ref<Record<string, number>>({})
const start = ref('')
const end = ref('')
const errorMessage = ref('')

function pct(value: number) {
  return `${(value * 100).toFixed(1)}%`
}

async function reload() {
  errorMessage.value = ''
  try {
    const params: Record<string, string> = {}
    if (start.value.trim()) params.start = start.value.trim()
    if (end.value.trim()) params.end = end.value.trim()
    const payload = await lateBypassApi.audit(params)
    rows.value = payload.rows
    auditSummary.value = payload.summary
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '核对失败'
  }
}

function reset() {
  start.value = ''
  end.value = ''
  void reload()
}

onMounted(reload)
</script>
