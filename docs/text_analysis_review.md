# 3.text_analysis.py 代码审查报告

**审查日期**: 2026-01-01
**文件路径**: `/opt/Annualreport_tools/3.text_analysis.py`
**审查状态**: 待修复

---

## 概述

该文件实现了多进程年报文本关键词分析器，用于：
- 读取 TXT 文件并使用 jieba 进行中文分词
- 统计指定关键词出现次数
- 将结果保存到 Excel 文件

代码结构清晰，采用 dataclass 配置和面向对象设计，符合项目规范。

---

## 问题列表

### 🔴 P0 - 需要修复

#### 1. 分词逻辑不一致

**位置**: Line 34-41

**现状**:
```python
words = [word for word in jieba.cut(content) if word.strip()]
content_non = re.sub(r"[^\u4e00-\u9fa5]", "", content)
words_non = [word for word in jieba.cut(content_non) if word.strip()]

for idx, keyword in enumerate(keywords):
    keyword_counts[idx] = words.count(keyword)  # 用 words

total_words = len(words_non)  # 用 words_non
```

**问题**:
- 关键词统计使用 `words`（包含英文/数字/标点的分词结果）
- 总词数使用 `words_non`（纯中文分词结果）
- 两者口径不一致，可能导致词频比例计算错误
- 如果关键词含非中文字符（如 "AI"、"5G"），在 `words_non` 中会被过滤

**建议修复**:
```python
# 方案1: 统一使用原始分词结果
words = [word for word in jieba.cut(content) if word.strip()]
keyword_counts = [words.count(kw) for kw in keywords]
total_words = len(words)

# 方案2: 统一使用纯中文分词结果（如果关键词全为中文）
content_non = re.sub(r"[^\u4e00-\u9fa5]", "", content)
words = [word for word in jieba.cut(content_non) if word.strip()]
keyword_counts = [words.count(kw) for kw in keywords]
total_words = len(words)
```

---

#### 2. 关键词计数效率低

**位置**: Line 38-39

**现状**:
```python
for idx, keyword in enumerate(keywords):
    keyword_counts[idx] = words.count(keyword)
```

**问题**:
- 每个关键词调用一次 `list.count()`，时间复杂度 O(n*k)
- 当文件词数多、关键词多时性能下降明显

**建议修复**:
```python
from collections import Counter

word_counter = Counter(words)
keyword_counts = [word_counter.get(kw, 0) for kw in keywords]
```

---

### 🟡 P1 - 建议改进

#### 3. 结果类型缺少注解

**位置**: Line 147

**现状**:
```python
def _write_result_row(self, result) -> None:
```

**建议修复**:
```python
from typing import Tuple

AnalysisResult = Tuple[str, str, str, int, List[int]]

def _write_result_row(self, result: AnalysisResult) -> None:
```

---

#### 4. 年份正则过于宽松

**位置**: Line 115

**现状**:
```python
match = re.match(r".*([12]\d{3}).*", os.path.basename(path))
```

**问题**:
- 匹配范围 1000-2999，过于宽泛
- 可能误匹配如 `report_2999_backup` 这样的路径

**建议修复**:
```python
match = re.match(r".*((19|20)\d{2}).*", os.path.basename(path))
```

---

#### 5. 硬编码的 worksheet 名称

**位置**: Line 74

**现状**:
```python
self.worksheet = self.workbook.add_sheet("公众号凌小添")
```

**问题**:
- 硬编码的推广信息不适合通用工具

**建议修复**:
```python
# 方案1: 添加配置项
@dataclass(frozen=True)
class AnalyzerConfig:
    # ... 其他字段
    sheet_name: str = "关键词分析"

# 方案2: 使用通用名称
self.worksheet = self.workbook.add_sheet("关键词分析")
```

---

#### 6. 输出风格不统一

**位置**: Line 182 vs Line 189

**现状**:
```python
print(f"\r当前进度: {progress:.2f}%", end="", flush=True)  # 用 print
logging.info("Excel 文件保存成功：%s", ...)                  # 用 logging
```

**建议修复**:
```python
# 方案1: 使用 tqdm 进度条
from tqdm import tqdm

with Pool(processes=worker_count) as pool:
    for result in tqdm(pool.imap_unordered(_analyze_task, iterator),
                       total=total_files, desc="分析进度"):
        # ...

# 方案2: 统一使用 logging
logging.info("当前进度: %.2f%%", progress)
```

---

### 🔵 P2 - 长期改进

#### 7. xlwt 库已过时

**位置**: Line 17, 73

**现状**:
```python
import xlwt
self.workbook = xlwt.Workbook(encoding="utf-8")
```

**问题**:
- `xlwt` 只支持旧版 `.xls` 格式
- 单个 sheet 最多 65536 行，大规模分析可能超限
- 库已停止维护

**建议修复**:
```python
# 迁移到 openpyxl
from openpyxl import Workbook

self.workbook = Workbook()
self.worksheet = self.workbook.active
self.worksheet.title = "关键词分析"

# 写入方式调整
self.worksheet.cell(row=self.next_row, column=col, value=data)

# 保存为 xlsx
self.workbook.save(self.config.output_path)  # 输出文件名改为 .xlsx
```

---

## 代码亮点

- ✅ 使用 `dataclass(frozen=True)` 保证配置不可变
- ✅ 多进程设计合理，使用 `imap_unordered` 提高吞吐
- ✅ 年份过滤支持范围配置，灵活实用
- ✅ 增量保存机制（chunk_size）防止数据丢失
- ✅ 异常处理覆盖文件读取失败场景
- ✅ `_extend_jieba_dict` 一次性注入词典，避免重复操作

---

## 修复优先级

| 优先级 | 问题 | 影响 |
|--------|------|------|
| P0 | 分词逻辑不一致 | 统计结果可能不准确 |
| P0 | 关键词计数效率低 | 大文件处理性能差 |
| P1 | 结果类型缺少注解 | 代码可维护性 |
| P1 | 年份正则过于宽松 | 可能误匹配 |
| P1 | 硬编码 worksheet 名称 | 通用性差 |
| P1 | 输出风格不统一 | 代码一致性 |
| P2 | xlwt 库过时 | 格式限制、兼容性 |

---

## 修复检查清单

- [ ] 统一分词逻辑，确保关键词统计与总词数口径一致
- [ ] 使用 Counter 优化关键词计数
- [ ] 添加 AnalysisResult 类型别名
- [ ] 收紧年份正则为 `(19|20)\d{2}`
- [ ] 将 worksheet 名称改为可配置或通用名称
- [ ] 统一使用 logging 或 tqdm 处理进度输出
- [ ] （可选）迁移 xlwt 到 openpyxl
