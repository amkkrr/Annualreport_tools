---
task_id: crawler-date-optimization
type: REFAC
complexity: L
branch: refactor/crawler-date-optimization
current_phase: PC
completed_phases: [P0, P0.B, P0.C, 实现]
next_action: IMPLEMENTATION_DONE
created_at: 2026-01-13
updated_at: 2026-01-13
---

## 问题定义
**类型**: REFAC-L
**背景**: 爬虫在抓取 N 年年报时，会盲目生成 N+1 年全年的日期分片（365天），导致无效请求且不符合 A 股发布逻辑。
**范围**:
- 修改 `DateRangeGenerator.generate_daily_ranges`，加入当前时间截断。
- 优化默认截止日期，使其更符合 A 股年报发布周期（1月-4月）。
**完成标准**:
- 不再生成未来日期的抓取请求。
- 默认抓取截止日期优化为次年 5 月 1 日。
