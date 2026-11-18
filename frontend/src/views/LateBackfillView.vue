<template>
  <section class="page" data-testid="late-backfill">
    <header class="page-head">
      <div>
        <h2>迟到补数任务</h2>
        <p class="page-desc">
          只展示由迟到旁路版本生成的补数作业：读取旁路数据、按来源水位执行，并保留被覆盖的事件范围。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" @click="reload">刷新</button>
        <RouterLink class="btn" to="/late-bypass/audit">阈值核对</RouterLink>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="backfill-error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="backfill-notice">{{ notice }}</p>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">旁路版本</span>
        <strong class="stat-value">{{ summary?.versions ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">补数作业</span>
        <strong class="stat-value">{{ summary?.jobs ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已完成作业</span>
        <strong class="stat-value">{{ summary?.completed_jobs ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">作业回执</span>
        <strong class="stat-value">{{ summary?.receipts ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">可执行范围</span>
        <strong class="stat-value">{{ summary?.executable_ranges ?? 0 }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>数据源</span>
        <select v-model="source" data-testid="job-source-filter">
          <option value="">全部数据源</option>
          <option v-for="item in sources" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>
      <button class="btn" data-testid="job-query" type="submit">查询</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th>作业</th><th>旁路版本</th><th>口径</th><th>来源水位</th><th>水位区间</th>
          <th>补数范围</th><th>范围覆盖</th><th>规则命中</th><th>覆盖事件范围</th><th>状态</th><th>回执</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="job in jobs"
          :key="job.作业ID"
          :data-testid="`job-${job.作业ID}`"
          :class="{ highlight: String(job.作业ID) === highlightJob }"
        >
          <td>#{{ job.作业ID }} {{ job.作业名称 }}</td>
          <td>{{ job.版本号 }}</td>
          <td>{{ job.口径名称 }}</td>
          <td>{{ job.水位记录ID }}</td>
          <td>{{ job.水位区间 }}</td>
          <td>{{ job.补数范围 }}</td>
          <td>{{ job.范围覆盖 }}</td>
          <td>{{ job.规则命中 }}（{{ pct(job.迟到率) }} / {{ job.阈值百分比 }}%）</td>
          <td>
            {{ job.覆盖事件范围 || '—' }}
            <span class="muted">（{{ job.覆盖事件数 }} 条）</span>
          </td>
          <td>{{ job.作业状态 }} · {{ job.进度比例 }}</td>
          <td>{{ job.回执数 }} {{ job.最近回执号 ? `· ${job.最近回执号}` : '' }}</td>
          <td class="row-actions">
            <button
              v-if="job.作业状态 !== '已完成'"
              class="link"
              type="button"
              :data-testid="`execute-${job.作业ID}`"
              :disabled="busy"
              @click="execute(job.作业ID)"
            >
              执行补数
            </button>
            <button class="link" type="button" @click="loadReceipts(job.旁路版本ID)">
              查看回执
            </button>
          </td>
        </tr>
        <tr v-if="!jobs.length">
          <td colspan="12" class="empty-state">
            暂无迟到补数作业；请先在水位页面冻结旁路版本并确认可执行范围
          </td>
        </tr>
      </tbody>
    </table>

    <section class="panel">
      <h3>作业回执（保留被覆盖的事件范围）</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>回执号</th><th>作业</th><th>版本号</th><th>口径</th><th>水位区间</th>
            <th>补数范围</th><th>覆盖事件范围</th><th>覆盖事件</th><th>状态</th><th>执行时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="receipt in receipts" :key="receipt.id" :data-testid="`receipt-${receipt.id}`">
            <td>{{ receipt.回执号 }}</td>
            <td>#{{ receipt.作业ID }}</td>
            <td>{{ receipt.版本号 }}</td>
            <td>{{ receipt.口径名称 }}</td>
            <td>{{ receipt.水位区间 }}</td>
            <td>{{ receipt.补数范围 }}</td>
            <td class="mono">{{ receipt.覆盖事件范围 }}</td>
            <td>{{ receipt.覆盖事件数 }}（{{ receipt.覆盖事件ID列表.join('、') }}）</td>
            <td>{{ receipt.状态 }}</td>
            <td>{{ receipt.执行时间 ?? '—' }}</td>
          </tr>
          <tr v-if="!receipts.length">
            <td colspan="10" class="empty-state">执行补数后生成回执</td>
          </tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { RouterLink, useRoute } from 'vue-router'

import { lateBypassApi, type JobRow, type Receipt, type Summary } from '@/api/late-bypass'

const route = useRoute()
const jobs = ref<JobRow[]>([])
const receipts = ref<Receipt[]>([])
const summary = ref<Summary | null>(null)
const source = ref('')
const errorMessage = ref('')
const notice = ref('')
const busy = ref(false)
const highlightJob = computed(() => String(route.query.job ?? ''))
const sources = computed(() => [...new Set(jobs.value.map((job) => job.数据源))])

function pct(value: number) {
  return `${(value * 100).toFixed(1)}%`
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await lateBypassApi.jobs(source.value || undefined)
    jobs.value = payload.items
    summary.value = payload.summary
    if (route.query.job) {
      const matched = payload.items.find((job) => String(job.作业ID) === highlightJob.value)
      if (matched) await loadReceipts(matched.旁路版本ID)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补数任务读取失败'
  }
}

async function loadReceipts(versionId: number) {
  try {
    const payload = await lateBypassApi.receipts({ version_id: String(versionId) })
    receipts.value = payload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执读取失败'
  }
}

async function execute(jobId: number) {
  busy.value = true
  notice.value = ''
  errorMessage.value = ''
  try {
    const result = await lateBypassApi.execute(jobId, {
      request_id: `execute-${jobId}-${Date.now()}`,
    })
    notice.value = result.receipt
      ? `回执 ${result.receipt.回执号}：覆盖事件 ${result.receipt.覆盖事件范围}（${result.receipt.覆盖事件数} 条）`
      : result.reason ?? '作业已执行'
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '执行补数失败'
  } finally {
    busy.value = false
  }
}

onMounted(reload)
</script>
