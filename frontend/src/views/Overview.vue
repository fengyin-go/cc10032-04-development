<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <section v-if="lateBypass" class="panel" data-testid="overview-late-bypass">
      <h3>迟到旁路链路</h3>
      <div class="detail-grid">
        <div><span class="muted">旁路版本</span><strong>{{ lateBypass.versions }}</strong></div>
        <div><span class="muted">待确认版本</span><strong>{{ lateBypass.pending_versions }}</strong></div>
        <div><span class="muted">已旁路事件</span><strong>{{ lateBypass.bypassed_events }}</strong></div>
        <div><span class="muted">补数作业 / 完成</span><strong>{{ lateBypass.jobs }} / {{ lateBypass.completed_jobs }}</strong></div>
        <div><span class="muted">作业回执</span><strong>{{ lateBypass.receipts }}</strong></div>
        <div><span class="muted">阈值命中</span><strong>{{ lateBypass.threshold_hits }}</strong></div>
      </div>
      <div class="page-actions">
        <RouterLink class="btn" to="/late-bypass/backfill">迟到补数任务</RouterLink>
        <RouterLink class="btn" to="/late-bypass/audit">阈值核对</RouterLink>
      </div>
    </section>

    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { RouterLink } from 'vue-router'

import { fetchJson } from '@/api/client'
import type { Summary } from '@/api/late-bypass'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
  late_bypass?: Summary
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])
const lateBypass = ref<Summary | null>(null)

onMounted(async () => {
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
    lateBypass.value = payload.late_bypass ?? null
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = [{"name": "流式源端", "created": 0, "pending": 0, "abnormal": 0}, {"name": "窗口配置", "created": 0, "pending": 0, "abnormal": 0}, {"name": "计算算子", "created": 0, "pending": 0, "abnormal": 0}, {"name": "状态存储", "created": 0, "pending": 0, "abnormal": 0}, {"name": "水位追踪", "created": 0, "pending": 0, "abnormal": 0}, {"name": "告警策略", "created": 0, "pending": 0, "abnormal": 0}, {"name": "滑动指标", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流关联", "created": 0, "pending": 0, "abnormal": 0}, {"name": "结果落地", "created": 0, "pending": 0, "abnormal": 0}, {"name": "补数任务", "created": 0, "pending": 0, "abnormal": 0}, {"name": "CEP规则", "created": 0, "pending": 0, "abnormal": 0}, {"name": "吞吐管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "检查点", "created": 0, "pending": 0, "abnormal": 0}, {"name": "故障恢复", "created": 0, "pending": 0, "abnormal": 0}, {"name": "指标血缘", "created": 0, "pending": 0, "abnormal": 0}, {"name": "迟到处理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "任务拓扑", "created": 0, "pending": 0, "abnormal": 0}, {"name": "数据采样", "created": 0, "pending": 0, "abnormal": 0}, {"name": "调试追踪", "created": 0, "pending": 0, "abnormal": 0}, {"name": "聚合落库", "created": 0, "pending": 0, "abnormal": 0}]
  }
})
</script>
