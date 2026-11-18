import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'
import WatermarkLateView from '@/views/WatermarkLateView.vue'
import LateBackfillView from '@/views/LateBackfillView.vue'
import LateAuditView from '@/views/LateAuditView.vue'

const router = createRouter({
  history: createWebHistory(),
  // 迟到旁路链路页面要排在动态模块路由之前，避免被 /:moduleKey 吃掉。
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/late-bypass/backfill', name: 'late-backfill', component: LateBackfillView },
    { path: '/late-bypass/audit', name: 'late-audit', component: LateAuditView },
    {
      path: '/late-bypass/watermark/:id',
      name: 'late-watermark',
      component: WatermarkLateView,
      props: true,
    },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
