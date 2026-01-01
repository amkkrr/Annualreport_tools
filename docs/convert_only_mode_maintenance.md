# 纯转换模式（Convert-Only Mode）维护文档

对应脚本：`2.pdf_batch_converter.py`

目标：在不依赖 Excel/下载环节的前提下，对目录内 PDF 执行批量转换（支持递归、跳过、强制覆盖、多进程、可选删除源 PDF）。

## 1. 代码结构（程序即文档）

纯转换模式的核心对象与入口：

- `ConvertOnlyConfig`：纯转换模式配置（`pdf_dir/txt_dir/recursive/delete_pdf/force/processes/file_pattern`）
- `ConvertOnlyResult`：单文件处理结果（`status/pdf_path/txt_path/backend/error`）
- `ConvertOnlyProcessor.run()`：主流程（扫描、启动多进程、主进程汇总日志与统计）

与之配套的转换核心：

- `PDFToTextConverter`：只负责“PDF -> TXT”转换，不包含下载逻辑
- `PDFConversionResult`：单次转换结果（`success/backend/error`）

## 2. 多进程契约

- worker 入口：`_convert_only_worker(...) -> ConvertOnlyResult`
- worker 输入：字符串化路径与只读配置（保证可序列化）
- worker 输出：结构化 `ConvertOnlyResult`，主进程负责统一日志与统计，避免多进程输出交错

主进程使用 `Pool.imap_unordered(...)`，可以边处理边输出结果，同时保持日志集中在主进程。

## 3. 扫描与匹配策略

- 扫描函数：`_scan_pdf_files(pdf_dir, recursive, file_pattern)`
- 匹配函数：`_matches_file_pattern(path, file_pattern)`
  - 只会把扩展名为 `.pdf/.PDF` 的文件当作 PDF
  - `file_pattern` 对文件名进行近似 glob 匹配（大小写不敏感）

如需支持“对相对路径匹配”（例如 `子目录/*年报*.pdf`），可将 `_matches_file_pattern` 的匹配对象从 `path.name` 改为 `path.relative_to(pdf_dir).as_posix()`，并在用户文档中说明差异。

## 4. 输出路径规则与冲突避免

目标路径计算：`_resolve_txt_path(pdf_path, pdf_dir, txt_dir, recursive)`

- `txt_dir is None`：输出到 PDF 同目录
- `txt_dir is not None` 且 `recursive=True`：保留相对目录结构，避免不同子目录同名 PDF 导致覆盖

## 5. 跳过/覆盖与有效性检查

- 有效性检查：`_is_valid_txt_file(path)`（最小规则：非空）
- 默认行为：TXT 已存在且有效 -> `skipped`
- `force=True`：忽略“已存在跳过”，重新转换并覆盖
- 写入策略：`PDFToTextConverter` 会先写入临时文件（`*.tmp`），仅当输出非空时才原子替换为目标 TXT，避免失败时留下半成品
- 若目标 TXT 已存在但为 0 字节：worker 会尝试删除该空文件，避免后续误判

如需更严格的有效性（例如编码可读、最小字节阈值、包含关键页标记），扩展 `_is_valid_txt_file` 即可。

## 6. delete_pdf 安全策略

删除动作在主进程中执行：

- 仅当 `status == "success"` 且 TXT 通过最小有效性检查时删除 PDF
- `skipped/failed` 不删除

主进程删除失败会记录 warning，不会中止整体任务。

## 7. 磁盘空间不足（Errno 28）

worker 对 `OSError(errno=28)` 直接抛出异常；主进程捕获后打印错误并终止处理，避免生成不完整 TXT 或产生误删风险。

## 8. 扩展转换后端

新增后端建议按以下步骤：

1. 在 `PDFToTextConverter` 中新增 `_convert_with_xxx(pdf_path, txt_path) -> PDFConversionResult`
2. 在 `convert_pdf_to_txt(...)` 中按优先级插入尝试顺序
3. 在用户文档中补充“回退机制”说明（以及依赖安装）
