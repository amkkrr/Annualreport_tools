# WebUI Crawler Logic Optimization Specification

## 1. 目标
优化 WebUI 中爬虫及其他后台任务的管理逻辑，提升稳定性、灵活性和交互体验。

## 2. 变更内容

### 2.1 递归清理进程 (webui/components/task_runner.py)
- **问题**: `stop_task` 目前仅终止父进程，可能导致子进程残留。
- **方案**:
    - 使用 `psutil.Process(pid).children(recursive=True)` 获取所有子进程。
    - 按照“先子后父”的顺序进行 `terminate()`。
    - 增加超时处理，若 `terminate()` 失败则执行 `kill()`。

### 2.2 动态参数透传 (webui/pages/3_任务管理.py)
- **问题**: 爬虫年份固定在 `config.yaml` 中，不便灵活调整。
- **方案**:
    - 在“爬取链接”任务容器中增加 `st.multiselect` 或 `st.number_input` 用于选择目标年份。
    - 默认加载 `config.yaml` 中的年份。
    - 点击“启动”时，将选择的年份作为 `extra_args` 传递给 `task_runner.start_task`（例如：`["--use-config", "--year", "2023", "2024"]`）。

### 2.3 高效日志刷新 (webui/pages/3_任务管理.py)
- **问题**: `time.sleep(5) + st.rerun()` 导致全页面重绘。
- **方案**:
    - 使用 `st.empty()` 创建日志占位符。
    - 实现局部刷新逻辑，仅在日志内容变化时更新占位符。

### 2.4 PID 管理优化 (webui/components/pid_manager.py)
- **问题**: 现有的 `get_process` 逻辑已较为健壮，但可增加对 `Zombie` 进程的显式处理。
- **方案**: 确保在 `get_process` 中遇到 `Zombie` 状态时自动清理 PID 文件。

## 3. 验收标准
1. 启动爬虫任务后，在系统进程管理器中可看到完整的进程树。
2. 停止任务后，该任务下的所有子进程均被成功清理。
3. 在 WebUI 选择不同年份启动爬虫，日志输出应显示正在爬取指定的年份。
4. 日志刷新时，页面其他组件（如按钮、下拉框）状态不应闪烁。
