/** 迟到旁路链路接口：水位迟到分布、旁路版本、补数作业/回执、阈值核对。 */
import { fetchJson, request } from './client'

export type LateEvent = {
  event_id: string
  数据源: string
  event_time: number
  event_time_iso: string
  arrival_time: number
  arrival_time_iso: string
  lateness: number
}

export type LateBucket = { bucket: string; count: number; event_ids: string[] }

export type ScopeAnalysis = {
  scope: 'current' | 'lookback'
  scope_label: string
  interval: {
    start: number
    end: number
    cutoff: number
    allowed: number
    window: number
    previous: number
  }
  interval_label: string
  late_count: number
  interval_event_count: number
  duplicate_count: number
  already_bypassed_count: number
  candidate_count: number
  late_ratio: number
  max_lateness: number
  avg_lateness: number
  threshold: number
  threshold_hit: boolean
  buckets: LateBucket[]
  event_ids: string[]
  late_events: LateEvent[]
  candidates: LateEvent[]
}

export type Version = {
  id: number
  版本号: string
  数据源: string
  水位记录ID: number
  水位名称: string
  口径: 'current' | 'lookback'
  口径名称: string
  水位区间: string
  区间起始: number
  区间结束: number
  迟到截止: number
  补数范围: string
  范围起始: number
  范围结束: number
  迟到事件数: number
  区间事件数: number
  重复到达数: number
  已旁路跳过数: number
  迟到率: number
  阈值百分比: number
  阈值命中: string
  最大迟到秒: number
  事件ID列表: string[]
  status: string
  作业ID: number | null
  作业状态: string
  作业进度: string
  回执数: number
  最近回执: Receipt | null
}

export type Receipt = {
  id: number
  回执号: string
  作业ID: number
  作业名称: string
  旁路版本ID: number
  版本号: string
  数据源: string
  水位记录ID: number
  口径名称: string
  水位区间: string
  覆盖事件范围: string
  覆盖事件数: number
  覆盖事件ID列表: string[]
  补数范围: string
  尝试次数: number
  状态: string
  执行时间?: string
}

export type WatermarkRow = {
  id: number
  水位名称: string
  数据源: string
  当前水位: number
  水位区间: string
  上水位: number
  延迟阈值: number
  迟到截止: number
  当前口径迟到数: number
  回看口径迟到数: number
  迟到率: number
  阈值百分比: number
  阈值命中: boolean
  重复到达数: number
  已旁路数: number
  待旁路数: number
  旁路版本数: number
  旁路版本: string[]
  补数作业数: number
  补数作业: {
    作业ID: number
    作业名称: string
    作业状态: string
    补数范围: string
    覆盖事件范围: string
    版本号: string
  }[]
  回执数: number
  来源关系: {
    水位记录ID: number
    旁路版本ID: number
    版本号: string
    口径: string
    作业ID: number | null
    回执数: number
  }[]
  status: string
  pending: boolean
  abnormal: boolean
}

export type Distribution = {
  watermark: Record<string, string | number | boolean>
  policy: Record<string, string | number | boolean>
  scopes: Record<'current' | 'lookback', ScopeAnalysis>
  distribution: LateBucket[]
  rule_hit: {
    rule: string
    policy: string
    allowed_lateness: number
    threshold: number
    late_ratio: number
    matched: boolean
    reason: string
    condition_range: {
      watermark: number
      cutoff: number
      start: number
      end: number
      expression: string
    }
  }
  versions: Version[]
  current_result: CurrentResult[]
}

export type CurrentResult = {
  id: number
  数据源: string
  口径: 'current' | 'lookback'
  口径名称: string
  当前版本ID: number
  当前版本号: string
  水位区间: string
  补数范围: string
  可执行: boolean
  status: string
}

export type JobRow = {
  作业ID: number
  作业名称: string
  作业状态: string
  进度比例: string
  补数范围: string
  旁路版本ID: number
  版本号: string
  数据源: string
  水位记录ID: number
  口径: string
  口径名称: string
  水位区间: string
  范围覆盖: string
  覆盖事件范围: string
  覆盖事件数: number
  回执数: number
  最近回执号: string
  完成状态: string
  规则命中: string
  迟到率: number
  阈值百分比: number
}

export type AuditRow = {
  水位记录ID: number
  水位名称: string
  数据源: string
  水位: number
  水位区间: string
  迟到截止: number
  迟到事件数: number
  已旁路事件数: number
  未旁路事件数: number
  未旁路事件: string[]
  迟到率: number
  阈值百分比: number
  阈值命中: string
  旁路版本数: number
  旁路版本: string[]
  补数作业数: number
  补数作业: {
    作业ID: number
    作业状态: string
    补数范围: string
    版本号: string
    覆盖: boolean
    回执数: number
  }[]
  补数范围覆盖水位区间: boolean
  state: string
  status: string
  pending: boolean
  abnormal: boolean
}

export type Todo = {
  id: number
  module: string
  title: string
  watermark_id: number | null
  version_id: number | null
  job_id: number | null
  status: string
}

export type Summary = {
  watermarks: number
  versions: number
  pending_versions: number
  executable_ranges: number
  bypassed_events: number
  jobs: number
  completed_jobs: number
  receipts: number
  threshold_hits: number
  scopes: { current: number; lookback: number }
}

export type ActionEnvelope = {
  ok: boolean
  created?: boolean
  accepted?: boolean
  idempotent?: boolean
  reason?: string
  version?: Version
  versions?: Version[]
  job?: { id: number; 作业名称: string; 补数范围: string; status: string }
  receipt?: Receipt
  message?: string
}

async function send<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) {
    const detail = await response.json().catch(() => null)
    const message = detail?.detail?.message ?? detail?.detail ?? `接口返回 ${response.status}`
    throw new Error(String(message))
  }
  return (await response.json()) as T
}

async function post<T>(path: string, body: Record<string, unknown>): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  if (!response.ok) {
    const detail = await response.json().catch(() => null)
    const message = detail?.detail?.message ?? detail?.detail ?? `接口返回 ${response.status}`
    throw new Error(String(message))
  }
  return (await response.json()) as T
}

export const lateBypassApi = {
  watermarks: () =>
    send<{ items: WatermarkRow[]; total: number; summary: Summary }>('/api/late-bypass/watermarks'),
  distribution: (id: number) => send<Distribution>(`/api/late-bypass/watermarks/${id}/distribution`),
  versions: (params: Record<string, string> = {}) =>
    send<{ items: Version[]; total: number }>(
      `/api/late-bypass/versions?${new URLSearchParams(params).toString()}`,
    ),
  versionDetail: (id: number) =>
    send<{ version: Version; events: unknown[]; job: JobRow | null; receipts: Receipt[]; range_covers_interval: boolean }>(
      `/api/late-bypass/versions/${id}`,
    ),
  freeze: (body: { watermark_id: number; scope: string; request_id?: string }) =>
    post<ActionEnvelope>('/api/late-bypass/versions', body),
  submitGroup: (body: { watermark_id: number; request_id?: string; scopes?: string[] }) =>
    post<ActionEnvelope & { group_key?: string }>('/api/late-bypass/versions/submit-group', body),
  confirm: (id: number, body: { request_id?: string } = {}) =>
    post<ActionEnvelope>(`/api/late-bypass/versions/${id}/confirm`, body),
  reopen: (id: number, body: { request_id?: string } = {}) =>
    post<ActionEnvelope>(`/api/late-bypass/versions/${id}/reopen`, body),
  voidVersion: (id: number, body: { request_id?: string } = {}) =>
    post<ActionEnvelope>(`/api/late-bypass/versions/${id}/void`, body),
  jobs: (source?: string) =>
    send<{ items: JobRow[]; total: number; summary: Summary }>(
      `/api/late-bypass/jobs${source ? `?source=${encodeURIComponent(source)}` : ''}`,
    ),
  execute: (jobId: number, body: { request_id?: string } = {}) =>
    post<ActionEnvelope>(`/api/late-bypass/jobs/${jobId}/execute`, body),
  receipts: (params: Record<string, string> = {}) =>
    send<{ items: Receipt[]; total: number }>(
      `/api/late-bypass/receipts?${new URLSearchParams(params).toString()}`,
    ),
  audit: (params: Record<string, string> = {}) =>
    send<{ summary: Record<string, number>; rows: AuditRow[] }>(
      `/api/late-bypass/audit?${new URLSearchParams(params).toString()}`,
    ),
  todos: (module?: string) =>
    send<{ items: Todo[]; total: number }>(
      `/api/late-bypass/todos${module ? `?module=${module}` : ''}`,
    ),
  summary: () => send<Summary>('/api/late-bypass/summary'),
}

export { fetchJson }
