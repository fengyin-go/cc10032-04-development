"""迟到旁路链路的示例数据。

把水位记录补成可计算的真实数值，并准备一批带重复到达的事件，
供「读取水位记录 → 识别迟到事件 → 生成迟到旁路版本/补数范围」使用。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# 水位记录的可计算字段覆盖：保留原字段名，只把示例值替换成数值/时间。
WATERMARK_OVERRIDES: dict[int, dict[str, Any]] = {
    1: {
        "水位名称": "订单流水位",
        "数据源": "order_stream",
        "当前水位": 1788307200.0,
        "事件时间": "2026-09-02 00:00:00",
        "延迟阈值": 60.0,
        "空转时长": 0,
        "溢出状态": "否",
        "status": "正常",
        "pending": False,
        "abnormal": False,
        "_previous_watermark": 1788303600.0,
        "_window_size": 3600.0,
    },
    2: {
        "水位名称": "支付流水位",
        "数据源": "payment_stream",
        "当前水位": 1788310800.0,
        "事件时间": "2026-09-02 01:00:00",
        "延迟阈值": 30.0,
        "空转时长": 12,
        "溢出状态": "否",
        "status": "延迟",
        "pending": True,
        "abnormal": True,
        "_previous_watermark": 1788307200.0,
        "_window_size": 3600.0,
    },
    3: {
        "水位名称": "库存流水位",
        "数据源": "inventory_stream",
        "当前水位": 1788393600.0,
        "事件时间": "2026-09-03 00:00:00",
        "延迟阈值": 120.0,
        "空转时长": 0,
        "溢出状态": "是",
        "status": "溢出",
        "pending": True,
        "abnormal": True,
        "_previous_watermark": 1788390000.0,
        "_window_size": 3600.0,
    },
    4: {
        "水位名称": "曝光流水位",
        "数据源": "exposure_stream",
        "当前水位": 1788314400.0,
        "事件时间": "2026-09-02 02:00:00",
        "延迟阈值": 45.0,
        "空转时长": 8,
        "溢出状态": "否",
        "status": "延迟",
        "pending": True,
        "abnormal": True,
        "_previous_watermark": 1788310800.0,
        "_window_size": 3600.0,
    },
}


# 迟到处理策略：允许延迟/阈值必须可计算，并绑定到具体数据源。
LATE_HANDLER_OVERRIDES: dict[int, dict[str, Any]] = {
    1: {
        "策略名称": "订单迟到旁路策略",
        "数据源": "order_stream",
        "允许延迟": 60,
        "迟到动作": "bypass",
        "旁路输出": "late_bypass",
        "计数方式": "event",
        "阈值百分比": 20,
        "标记字段": "_late_mark",
        "status": "已启用",
        "pending": False,
        "abnormal": False,
    },
    2: {
        "策略名称": "支付迟到旁路策略",
        "数据源": "payment_stream",
        "允许延迟": 30,
        "迟到动作": "bypass",
        "旁路输出": "late_bypass",
        "计数方式": "event",
        "阈值百分比": 15,
        "标记字段": "_late_mark",
        "status": "已启用",
        "pending": True,
        "abnormal": True,
    },
    3: {
        "策略名称": "库存迟到旁路策略",
        "数据源": "inventory_stream",
        "允许延迟": 120,
        "迟到动作": "bypass",
        "旁路输出": "late_bypass",
        "计数方式": "event",
        "阈值百分比": 60,
        "标记字段": "_late_mark",
        "status": "已停用",
        "pending": False,
        "abnormal": False,
    },
    4: {
        "策略名称": "曝光迟到旁路策略",
        "数据源": "exposure_stream",
        "允许延迟": 45,
        "迟到动作": "bypass",
        "旁路输出": "late_bypass",
        "计数方式": "event",
        "阈值百分比": 40,
        "标记字段": "_late_mark",
        "status": "已启用",
        "pending": True,
        "abnormal": False,
    },
}

# 补数任务示例改成可读位点，避免列表里出现「补数任务样例1」这种占位值。
BACKFILL_OVERRIDES: dict[int, dict[str, Any]] = {
    1: {
        "作业名称": "订单指标历史补数",
        "目标指标": "order_metrics",
        "补数范围": "1788303600-1788307200",
        "并行度": "4",
        "起始位点": "1788303600",
        "截止位点": "1788307200",
        "进度比例": "100%",
        "status": "已完成",
        "pending": False,
        "abnormal": False,
    },
    2: {
        "作业名称": "支付指标历史补数",
        "目标指标": "payment_metrics",
        "补数范围": "1788307200-1788310800",
        "并行度": "3",
        "起始位点": "1788307200",
        "截止位点": "1788310800",
        "进度比例": "40%",
        "status": "补数中",
        "pending": True,
        "abnormal": False,
    },
    3: {
        "作业名称": "库存指标历史补数",
        "目标指标": "inventory_metrics",
        "补数范围": "1788390000-1788393600",
        "并行度": "2",
        "起始位点": "1788390000",
        "截止位点": "1788393600",
        "进度比例": "0%",
        "status": "待执行",
        "pending": True,
        "abnormal": False,
    },
}


def _event(
    idx: int,
    event_id: str,
    source: str,
    watermark_id: int,
    event_time: float,
    arrival_delta: float = 0.0,
    amount: float = 10.0,
    region: str = "region_a",
) -> dict[str, Any]:
    return {
        "id": idx,
        "event_id": event_id,
        "数据源": source,
        "watermark_id": watermark_id,
        "event_time": event_time,
        "event_time_iso": _iso(event_time),
        "arrival_time": event_time + arrival_delta,
        "arrival_time_iso": _iso(event_time + arrival_delta),
        "payload": {
            "user_id": f"u{idx % 5}",
            "amount": amount,
            "region": region,
        },
    }


# 事件时间围绕各自水位产生：
#   - 每个源的当前区间都有迟到事件，也保留按时到达的事件用于迟到率；
#   - 每个源还准备了上一水位区间的事件，供「回看上一水位合并补报」口径使用；
#   - evt_order_0003 故意重复到达一次，用于验证事件级去重。
WATERMARK_EVENTS: list[dict[str, Any]] = [
    # order_stream: 当前水位 1788307200，上水位 1788303600，延迟阈值 60
    # 当前窗口迟到（截止 1788307140），evt_order_0003 重复到达一次
    _event(1, "evt_order_0001", "order_stream", 1, 1788307020.0, 120.0, 18.0, "north"),
    _event(2, "evt_order_0002", "order_stream", 1, 1788306960.0, 180.0, 22.0, "north"),
    _event(3, "evt_order_0003", "order_stream", 1, 1788306900.0, 240.0, 35.0, "east"),
    _event(4, "evt_order_0003", "order_stream", 1, 1788306900.0, 700.0, 35.0, "east"),
    _event(5, "evt_order_0004", "order_stream", 1, 1788306800.0, 340.0, 12.0, "east"),
    # 当前窗口按时到达
    _event(6, "evt_order_0005", "order_stream", 1, 1788307150.0, 5.0, 9.5, "west"),
    _event(7, "evt_order_0006", "order_stream", 1, 1788307160.0, 4.0, 7.0, "west"),
    _event(8, "evt_order_0007", "order_stream", 1, 1788307180.0, 3.0, 6.0, "south"),
    _event(9, "evt_order_0008", "order_stream", 1, 1788307190.0, 2.0, 5.0, "south"),
    _event(10, "evt_order_0009", "order_stream", 1, 1788307195.0, 1.0, 4.5, "south"),
    # 上一水位窗口：只有回看口径才合并补报
    _event(11, "evt_order_0010", "order_stream", 1, 1788301000.0, 3600.0, 3.0, "old"),
    _event(12, "evt_order_0011", "order_stream", 1, 1788302000.0, 700.0, 6.0, "old"),
    _event(13, "evt_order_0012", "order_stream", 1, 1788303000.0, 100.0, 8.0, "old"),
    _event(14, "evt_order_0013", "order_stream", 1, 1788303400.0, 50.0, 5.0, "old"),

    # payment_stream: 当前水位 1788310800，上水位 1788307200，延迟阈值 30
    _event(15, "evt_pay_0001", "payment_stream", 2, 1788310740.0, 30.0, 31.0, "east"),
    _event(16, "evt_pay_0002", "payment_stream", 2, 1788310700.0, 70.0, 28.0, "east"),
    _event(17, "evt_pay_0003", "payment_stream", 2, 1788310650.0, 120.0, 19.0, "west"),
    _event(18, "evt_pay_0004", "payment_stream", 2, 1788310780.0, 5.0, 16.0, "west"),
    _event(19, "evt_pay_0005", "payment_stream", 2, 1788310790.0, 3.0, 12.0, "south"),
    _event(20, "evt_pay_0006", "payment_stream", 2, 1788310795.0, 2.0, 15.0, "south"),
    _event(21, "evt_pay_0007", "payment_stream", 2, 1788310798.0, 1.0, 14.0, "south"),
    _event(22, "evt_pay_0008", "payment_stream", 2, 1788310799.0, 1.0, 13.0, "south"),
    _event(23, "evt_pay_0009", "payment_stream", 2, 1788310799.5, 0.5, 11.0, "south"),
    _event(24, "evt_pay_0010", "payment_stream", 2, 1788304000.0, 300.0, 55.0, "old"),
    _event(25, "evt_pay_0011", "payment_stream", 2, 1788305000.0, 2300.0, 45.0, "old"),
    _event(26, "evt_pay_0012", "payment_stream", 2, 1788306000.0, 100.0, 33.0, "old"),
    _event(27, "evt_pay_0013", "payment_stream", 2, 1788307000.0, 60.0, 27.0, "old"),

    # inventory_stream: 当前水位 1788393600，上水位 1788390000，延迟阈值 120
    _event(28, "evt_inv_0001", "inventory_stream", 3, 1788393400.0, 80.0, 66.0, "north"),
    _event(29, "evt_inv_0002", "inventory_stream", 3, 1788393300.0, 180.0, 52.0, "north"),
    _event(30, "evt_inv_0003", "inventory_stream", 3, 1788393200.0, 280.0, 44.0, "east"),
    _event(31, "evt_inv_0004", "inventory_stream", 3, 1788393100.0, 380.0, 21.0, "east"),
    _event(32, "evt_inv_0005", "inventory_stream", 3, 1788393500.0, 10.0, 18.0, "west"),
    _event(33, "evt_inv_0006", "inventory_stream", 3, 1788393550.0, 5.0, 11.0, "west"),
    _event(34, "evt_inv_0007", "inventory_stream", 3, 1788393570.0, 3.0, 10.0, "west"),
    _event(35, "evt_inv_0008", "inventory_stream", 3, 1788393590.0, 2.0, 9.0, "west"),
    _event(36, "evt_inv_0009", "inventory_stream", 3, 1788393595.0, 1.0, 8.0, "west"),
    _event(37, "evt_inv_0010", "inventory_stream", 3, 1788390000.0 + 3600.0 - 3000.0, 900.0, 70.0, "old"),
    _event(38, "evt_inv_0011", "inventory_stream", 3, 1788388000.0, 300.0, 30.0, "old"),
    _event(39, "evt_inv_0012", "inventory_stream", 3, 1788389000.0, 150.0, 25.0, "old"),
    _event(40, "evt_inv_0013", "inventory_stream", 3, 1788389500.0, 60.0, 20.0, "old"),

    # exposure_stream（4号水位）：当前水位 1788314400，上水位 1788310800，延迟阈值 45
    _event(41, "evt_exp_0001", "exposure_stream", 4, 1788314300.0, 55.0, 8.0, "north"),
    _event(42, "evt_exp_0002", "exposure_stream", 4, 1788314250.0, 105.0, 9.0, "north"),
    _event(43, "evt_exp_0003", "exposure_stream", 4, 1788314200.0, 155.0, 7.0, "east"),
    _event(44, "evt_exp_0004", "exposure_stream", 4, 1788314100.0, 255.0, 6.0, "east"),
    _event(45, "evt_exp_0005", "exposure_stream", 4, 1788314370.0, 5.0, 11.0, "west"),
    _event(46, "evt_exp_0006", "exposure_stream", 4, 1788314380.0, 4.0, 13.0, "west"),
    _event(47, "evt_exp_0007", "exposure_stream", 4, 1788314390.0, 2.0, 12.0, "south"),
    _event(48, "evt_exp_0008", "exposure_stream", 4, 1788314395.0, 2.0, 14.0, "south"),
    _event(49, "evt_exp_0009", "exposure_stream", 4, 1788314398.0, 1.0, 15.0, "south"),
    _event(50, "evt_exp_0010", "exposure_stream", 4, 1788307600.0, 3400.0, 4.0, "old"),
    _event(51, "evt_exp_0011", "exposure_stream", 4, 1788309000.0, 600.0, 5.0, "old"),
    _event(52, "evt_exp_0012", "exposure_stream", 4, 1788310200.0, 100.0, 6.0, "old"),
    _event(53, "evt_exp_0013", "exposure_stream", 4, 1788310600.0, 50.0, 7.0, "old"),
]


LATE_SEED_ROWS: dict[str, list[dict[str, Any]]] = {
    "watermark_event": WATERMARK_EVENTS,
    "late_bypass": [],
    "late_bypass_event": [],
    "late_bypass_current": [],
    "backfill_receipt": [],
    "late_todo": [],
    "late_submission": [],
}
