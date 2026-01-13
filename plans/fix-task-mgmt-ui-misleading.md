---
task_id: fix-task-mgmt-ui-misleading
type: BUG
complexity: L
current_phase: P0.C
completed_phases: [P0, P0.B]
next_action: CREATE_BRANCH
created_at: 2026-01-13
updated_at: 2026-01-13
---

# 计划: 修复任务管理页面 UI 逻辑误导问题

## 问题定义
**类型**: BUG-L
**背景**: 任务管理页面中，“待下载”任务数量显示在“爬虫”任务下，导致用户误认为爬虫负责处理待下载队列。实际上，爬虫是生产者，下载转换器才是待下载任务的消费者。
**范围**:
- 修改 `webui/pages/3_任务管理.py` 中的 `tasks_meta` 定义。
- 将 `pending_downloads` 的显示从 `crawler` 任务移至 `converter` 任务。
- 调整 `converter` 的 UI，使其能同时显示“待下载”和“待转换”的数量。
- 移除 `crawler` 下不相关的队列统计，或改为显示更有意义的信息（如目标年份）。
**完成标准**:
- “爬虫”任务下方不再显示“待下载”数量。
- “下载转换”任务下方清晰显示“待下载”和“待转换”的任务数量。
- UI 布局保持整齐。

## 调研结论
- `webui/pages/3_任务管理.py` 使用 `tasks_meta` 字典驱动 UI 渲染。
- `crawler` 的逻辑是主动抓取，不消耗队列。
- `converter` 执行两个步骤：下载（消耗 `pending_downloads`）和转换（消耗 `pending_converts`）。

## 实现方案
1. 修改 `tasks_meta`：
    - `crawler`: 移除 `queue_label` 和 `queue_count`。
    - `converter`: 调整结构以支持多个队列统计，或者合并显示。
2. 更新循环渲染逻辑，支持多行统计。
