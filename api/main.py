# main.py
# encoding:utf-8
import io
import logging
import os
import re
import glob
import tempfile
import zipfile
import shutil
from copy import copy
from typing import List, Dict, Optional
from collections import OrderedDict

import pandas as pd
import py7zr
import xlsxwriter
import openpyxl
from openpyxl import load_workbook

try:
    from python_calamine import CalamineWorkbook
    _HAS_CALAMINE = True
except Exception:  # pragma: no cover
    CalamineWorkbook = None
    _HAS_CALAMINE = False

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse



logging.basicConfig(level=logging.INFO)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
#                       首页：返回前端页面
# ============================================================

INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8"/>
  <title>Excel 批量拆分 / 合并工具</title>
  <style>
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      margin: 40px;
      color: #222;
    }
    h1 { font-size: 24px; margin-bottom: 12px; }
    label { display: block; margin: 12px 0 4px; font-weight: 600; }
    input[type=text], input[type=file], input[type=number] {
      padding: 6px 8px; width: 320px;
    }
    button { margin-top: 20px; padding: 8px 20px; font-size: 16px; cursor: pointer; }
    #msg { margin-top: 20px; color: #e00; white-space: pre-wrap; }
    .modeBox {
      margin-bottom: 20px;
      padding: 12px 16px;
      border: 1px solid #ddd;
      border-radius: 6px;
      background: #fafafa;
    }
    .modeBox label { display: block; margin: 6px 0; font-weight: 500; }
    .hint { color:#666; font-size: 13px; margin: 4px 0 8px; line-height: 1.5; }
    form { max-width: 720px; }
    form > button { background: #2c7be5; color: #fff; border: none; border-radius: 4px; }
    form > button:hover { background: #1a68d1; }
  </style>
</head>
<body>
  <h1>Excel 批量拆分 / 合并工具</h1>

  <!-- 模式切换 -->
  <div class="modeBox">
    <label><input type="radio" name="mode" value="split" checked onchange="toggleMode()"> PM质量分拆分模式</label>
    <label><input type="radio" name="mode" value="merge" onchange="toggleMode()"> PM质量分合并模式</label>
    <label><input type="radio" name="mode" value="cra" onchange="toggleMode()"> CRA量表合并模式（横表）</label>
    <label><input type="radio" name="mode" value="cra_vertical" onchange="toggleMode()"> CRA量表合并模式（竖表）</label>
  </div>

  <!-- PM质量分拆分模式 -->
  <form id="splitForm" enctype="multipart/form-data">
    <div class="hint">
      按工作表名称提取数据文件指定列，并按最后一列（分组列）拆分成多个 Excel，
      套用模板样式后打包为 7z 下载。
    </div>

    <label>数据文件（必填）</label>
    <input type="file" name="data_file" required/>

    <label>模板文件（必填）</label>
    <input type="file" name="template_file" required/>

    <label>工作表名称</label>
    <input type="text" name="sheet_name" value="02-项目汇总表"/>

    <label>要提取的列（1 起始，逗号分隔）</label>
    <input type="text" name="usecols" value="4,5,6,9,11"/>

    <label>列名所在行（1 起始）</label>
    <input type="number" name="header_row" value="1" min="1"/>

    <label>写入起始行（1 起始）</label>
    <input type="number" name="data_start" value="4" min="1"/>

    <button type="submit">生成并下载（.7z）</button>
  </form>

  <!-- PM质量分合并模式 -->
  <form id="mergeForm" enctype="multipart/form-data" style="display:none;">
    <div class="hint">
      上传 zip / 7z 压缩包（内含 Excel），程序会读取每个文件中以「HS」开头的工作表，
      去掉前 3 行与第 1 列后纵向堆叠，输出为单个 xlsx。
    </div>

    <label>上传 zip / 7z 压缩包</label>
    <input type="file" name="archive_file" accept=".zip,.7z" required/>

    <button type="submit">下载合并结果（.xlsx）</button>
  </form>

  <!-- CRA量表合并模式（横表） -->
  <form id="craForm" enctype="multipart/form-data" style="display:none;">
    <div class="hint">
      读取压缩包内每个 Excel 的「CRA填写表单」工作表，并依据《填写指南》《分数目录》
      做宽松匹配与分值改写；按工作内容横向展开，输出一张宽表 xlsx。
      <br>压缩包内如包含文件名带「模板」或「template」的 xlsm/xlsx，将自动作为模板读取
      《填写指南》和《分数目录》；否则使用第一个数据文件。
    </div>

    <label>上传 zip / 7z 压缩包</label>
    <input type="file" name="archive_file" accept=".zip,.7z" required/>

    <button type="submit">下载 CRA 横表合并结果（.xlsx）</button>
  </form>

  <!-- CRA量表合并模式（竖表） -->
  <form id="craVerticalForm" enctype="multipart/form-data" style="display:none;">
    <div class="hint">
      逐个读取压缩包内每个 Excel 的「CRA填写表单」工作表，保留第一行第 2、4、6 列作为固定表头，
      从第 3 行开始取数，按第 8 列（次数）过滤掉 0 值行；所有文件的有效行纵向堆叠成一张明细表。
      <br>输出为竖向明细 xlsx（首列为「来源文件」）。
    </div>

    <label>上传 zip / 7z 压缩包</label>
    <input type="file" name="archive_file" accept=".zip,.7z" required/>

    <button type="submit">下载 CRA 竖表合并结果（.xlsx）</button>
  </form>

  <div id="msg"></div>

  <script>
    const msg = document.getElementById('msg');

    function toggleMode() {
      const mode = document.querySelector('input[name="mode"]:checked').value;
      document.getElementById('splitForm').style.display       = (mode === 'split')        ? 'block' : 'none';
      document.getElementById('mergeForm').style.display       = (mode === 'merge')        ? 'block' : 'none';
      document.getElementById('craForm').style.display         = (mode === 'cra')          ? 'block' : 'none';
      document.getElementById('craVerticalForm').style.display = (mode === 'cra_vertical') ? 'block' : 'none';
      msg.textContent = '';
    }

    /* ---------- PM质量分拆分模式 ---------- */
    document.getElementById('splitForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      msg.textContent = '';
      const fd = new FormData(e.target);
      try {
        const r = await fetch('/process', { method: 'POST', body: fd });
        if (!r.ok) throw new Error(await r.text());
        downloadBlob(await r.blob(), 'processed_excels.7z');
      } catch (err) { msg.textContent = '出错：' + err.message; }
    });

    /* ---------- PM质量分合并模式 ---------- */
    document.getElementById('mergeForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      msg.textContent = '';
      const fd = new FormData(e.target);
      try {
        const r = await fetch('/merge', { method: 'POST', body: fd });
        if (!r.ok) throw new Error(await r.text());
        downloadBlob(await r.blob(), 'merged.xlsx');
      } catch (err) { msg.textContent = '出错：' + err.message; }
    });

    /* ---------- CRA量表合并模式（横表） ---------- */
    document.getElementById('craForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      msg.textContent = '';
      const fd = new FormData(e.target);
      try {
        const r = await fetch('/merge_cra', { method: 'POST', body: fd });
        if (!r.ok) throw new Error(await r.text());
        downloadBlob(await r.blob(), 'merged_cra.xlsx');
      } catch (err) { msg.textContent = '出错：' + err.message; }
    });

    /* ---------- CRA量表合并模式（竖表） ---------- */
    document.getElementById('craVerticalForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      msg.textContent = '';
      const fd = new FormData(e.target);
      try {
        const r = await fetch('/merge_cra_vertical', { method: 'POST', body: fd });
        if (!r.ok) throw new Error(await r.text());
        downloadBlob(await r.blob(), 'merged_cra_vertical.xlsx');
      } catch (err) { msg.textContent = '出错：' + err.message; }
    });

    /* ---------- 公共下载函数 ---------- */
    function downloadBlob(blob, fileName) {
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = fileName;
      a.click();
      URL.revokeObjectURL(url);
    }
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return INDEX_HTML

# ============================================================
#                       通用：压缩包解压工具
# ============================================================

def decode_zip_filename(raw: str) -> str:
    """尝试多种编码解码 ZIP 中的文件名"""
    try:
        raw_bytes = raw.encode('cp437')
    except UnicodeEncodeError:
        return raw
    for encoding in ['gbk', 'utf-8', 'cp437']:
        try:
            return raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw


def extract_archive(archive_bytes: bytes, file_name: str) -> Dict[str, bytes]:
    """解压 zip / 7z，返回 {内层文件名: 字节}（仅 Excel 文件）"""
    ret: Dict[str, bytes] = {}
    lower_name = (file_name or '').lower()

    if lower_name.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as z:
            for info in z.infolist():
                decoded_name = decode_zip_filename(info.filename)
                if decoded_name.lower().endswith((".xls", ".xlsx", ".xlsm")):
                    ret[decoded_name] = z.read(info)
    elif lower_name.endswith(".7z"):
        with py7zr.SevenZipFile(io.BytesIO(archive_bytes), mode="r") as z:
            for fname, bio in z.readall().items():
                if fname.lower().endswith((".xls", ".xlsx", ".xlsm")):
                    ret[fname] = bio.read()
    else:
        raise ValueError("只支持 .zip / .7z 压缩包")

    if not ret:
        raise ValueError("压缩包内未找到 Excel 文件")
    return ret


def extract_archive_to_dir(archive_bytes: bytes, file_name: str,
                           target_dir: str) -> List[str]:
    """解压 zip / 7z 到 target_dir，返回解压出的 Excel 文件绝对路径列表"""
    lower = (file_name or '').lower()
    extracted: List[str] = []

    def safe_path(base_dir: str, name: str) -> Optional[str]:
        name = name.replace('\\', '/')
        parts = [p for p in name.split('/') if p and p not in ('.', '..')]
        if not parts:
            return None
        return os.path.join(base_dir, *parts)

    if lower.endswith('.zip'):
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as z:
            for info in z.infolist():
                if info.is_dir():
                    continue
                name = decode_zip_filename(info.filename)
                if not name.lower().endswith(('.xls', '.xlsx', '.xlsm')):
                    continue
                out = safe_path(target_dir, name)
                if not out:
                    continue
                os.makedirs(os.path.dirname(out), exist_ok=True)
                with open(out, 'wb') as f:
                    f.write(z.read(info))
                extracted.append(out)
    elif lower.endswith('.7z'):
        with py7zr.SevenZipFile(io.BytesIO(archive_bytes), mode='r') as z:
            for fname, bio in z.readall().items():
                if not fname.lower().endswith(('.xls', '.xlsx', '.xlsm')):
                    continue
                out = safe_path(target_dir, fname)
                if not out:
                    continue
                os.makedirs(os.path.dirname(out), exist_ok=True)
                with open(out, 'wb') as f:
                    f.write(bio.read())
                extracted.append(out)
    else:
        raise ValueError("只支持 .zip / .7z 压缩包")

    if not extracted:
        raise ValueError("压缩包内未找到 Excel 文件")
    return extracted


# ============================================================
#                原有 /merge 接口（保持不变）
# ============================================================

@app.post("/merge")
async def merge_archive(archive_file: UploadFile = File(...)):
    try:
        archive_bytes = await archive_file.read()
        file_map = extract_archive(archive_bytes, archive_file.filename)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"解压失败: {e}")

    all_frames: List[pd.DataFrame] = []
    for file_name, file_bytes in file_map.items():
        index_pos = file_name.rfind("/")
        base = file_name
        if index_pos != -1:
            base = file_name[index_pos + 1:]
        file_key = base.split("-")[4] if len(base.split("-")) > 4 else base
        if file_key.lower().endswith(".xlsx"):
            file_key = file_key[:-5]
        logging.error(f"file name is {file_name}, file key is {file_key}")

        try:
            xl = pd.ExcelFile(io.BytesIO(file_bytes), engine="calamine")
            hs_sheets = [s for s in xl.sheet_names if str(s).startswith("HS")]
            for sheet in hs_sheets:
                df = pd.read_excel(xl, sheet_name=sheet, header=None)
                if df.shape[1] <= 1:
                    continue
                df = df.dropna(axis=1, how="all").iloc[3:, 1:]
                df = df[df.iloc[:, 0].notna()]
                df.insert(0, "Sheet", sheet)
                df.insert(0, "File", file_key)
                all_frames.append(df)
        except Exception as e:
            logging.warning("读取 %s 失败: %s", file_name, e)

    if not all_frames:
        raise HTTPException(status_code=400, detail="未找到任何符合条件的 Sheet")

    final = pd.concat(all_frames, ignore_index=True)
    out_io = io.BytesIO()
    final.to_excel(out_io, index=False)
    out_io.seek(0)
    return StreamingResponse(
        out_io,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=merged.xlsx"}
    )


# ============================================================
#               新增：CRA 月度工作量表合并功能
# ============================================================

SHEET_FORM = 'CRA填写表单'
SHEET_GUIDE = '填写指南'
SHEET_SCORE = '分数目录'

GUIDE_START_ROW = 3
GUIDE_END_ROW = 57

MAX_ROW = 5000
MAX_COL = 300

COUNT_MODE = 'times'

SEPARATOR = '_'
VISIT_SEP = ','

DROP_PLACEHOLDER_ONE = True

TARGET_B_VALUES = ('肿瘤项目监查/访视', '非肿瘤项目监查/访视')

H_COL_CHECK_ROWS = 100

_SCORE_CATALOG: Dict = {}


# ---------- 通用小工具 ----------

def norm(s):
    if s is None:
        return ''
    return re.sub(r'\s+', '', str(s).strip())


def canon_key(s):
    if s is None:
        return ''
    s = str(s).replace('*', '-')
    s = s.replace('（', '(').replace('）', ')')
    s = s.replace('：', ':')
    return re.sub(r'\s+', '', s.strip())


def text(v):
    if v is None:
        return ''
    if isinstance(v, float) and v == int(v) and abs(v) < 1e15:
        return str(int(v))
    return str(v).strip()


def to_num(v):
    if v is None or v == '':
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip())
    except ValueError:
        return None


def strip_paren(s):
    return re.sub(r'[（(][^）)]*[）)]\s*$', '', str(s)).strip()


def join_parts(parts, sep=SEPARATOR):
    return sep.join([p for p in parts if p])


# ---------- openpyxl 读 H 列（回退方案） ----------

def _read_h_col_with_openpyxl(path, sheet_name, max_row):
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    except Exception:
        return None
    try:
        if sheet_name not in wb.sheetnames:
            return None
        ws = wb[sheet_name]
        n_row = min(ws.max_row or 0, max_row)
        if n_row <= 0:
            return None
        results = []
        for i, row in enumerate(
                ws.iter_rows(min_row=1, max_row=n_row,
                             min_col=8, max_col=8, values_only=True),
                start=1):
            results.append((i, row[0] if row else None))
        return results
    finally:
        wb.close()


# ---------- 读取工作表 ----------

def _need_openpyxl_fallback(rows, check_rows=H_COL_CHECK_ROWS):
    if len(rows) <= 2:
        return False
    data = rows[2:]
    if not any(len(r) > 7 for r in data):
        return False
    if not any(len(r) > 1 and text(r[1]) for r in data):
        return False
    for r in data[:check_rows]:
        if len(r) > 7:
            v = r[7]
            if v is not None and str(v).strip() != '':
                return False
    return True


def _merge_h_col(rows, h_col_data):
    if not h_col_data:
        return rows
    merged = list(rows)
    for excel_row, value in h_col_data:
        idx = excel_row - 1
        if idx >= len(merged):
            continue
        if value is None or str(value).strip() == '':
            continue
        r = list(merged[idx])
        while len(r) <= 7:
            r.append(None)
        r[7] = value
        merged[idx] = tuple(r)
    return merged


def read_sheet(path, sheet_name, max_col=MAX_COL, max_row=MAX_ROW, check_h_col=False):
    if not _HAS_CALAMINE:
        return None
    try:
        wb = CalamineWorkbook.from_path(path)
    except Exception:
        return None
    if sheet_name not in wb.sheet_names:
        return None
    rows = wb.get_sheet_by_name(sheet_name).to_python(
        skip_empty_area=False,
        nrows=max_row,
    )
    if not rows:
        return None
    if max_col:
        rows = [r[:max_col] for r in rows]
    if check_h_col and _need_openpyxl_fallback(rows):
        h_data = _read_h_col_with_openpyxl(path, sheet_name, max_row)
        if h_data:
            rows = _merge_h_col(rows, h_data)
    return rows


# ---------- 读取《填写指南》 ----------

def build_guide_index(guide_file):
    rows = read_sheet(guide_file, SHEET_GUIDE, max_col=6, max_row=GUIDE_END_ROW)
    if not rows:
        return [], {}

    order_keys, seen = [], set()
    match_map = {}

    for r in range(GUIDE_START_ROW - 1, min(GUIDE_END_ROW, len(rows))):
        row = rows[r]
        b_col = row[1] if len(row) > 1 else None
        c_col = row[2] if len(row) > 2 else None

        if not text(b_col):
            continue

        key = text(b_col)
        nkey = canon_key(key)
        if nkey not in seen:
            seen.add(nkey)
            order_keys.append(key)

        for alias in (c_col, b_col, strip_paren(key)):
            na = canon_key(alias)
            if na:
                match_map.setdefault(na, key)

    return order_keys, match_map


def resolve_order(key, match_map):
    nk = canon_key(key)
    if nk in match_map:
        return match_map[nk]
    return match_map.get(canon_key(strip_paren(key)))


# ---------- 读取《分数目录》 ----------

def read_score_catalog(template_file):
    rows = read_sheet(template_file, SHEET_SCORE, max_col=4, max_row=MAX_ROW)
    if not rows:
        return {}

    start = 0
    if len(rows[0]) > 1 and norm(rows[0][1]) == '项目编号':
        start = 1

    catalog = {}
    for r in rows[start:]:
        if len(r) < 4:
            continue
        proj = text(r[1])
        grade = text(r[2])
        score = text(r[3])
        if proj:
            catalog.setdefault(proj, (grade, score))
    return catalog


# ---------- 单行改写 ----------

def _apply_score_catalog_to_row(row, catalog):
    if not catalog or len(row) < 4:
        return row

    b = text(row[1])
    if b not in TARGET_B_VALUES:
        return row

    proj = text(row[3])
    if not proj:
        return row

    info = catalog.get(proj)
    if not info:
        return row

    grade, score = info
    if not grade:
        return row

    row_list = list(row)
    row_list[1] = f'{b}（{grade}类）'

    if score != '':
        s = str(score)
        try:
            row_list[2] = int(s) if '.' not in s else float(s)
        except (ValueError, TypeError):
            row_list[2] = score
    return tuple(row_list)


# ---------- 处理单个文件 ----------

def extract_basic_info(first_row):
    info = OrderedDict()
    for i in range(0, min(16, len(first_row)), 2):
        k = text(first_row[i])
        v = text(first_row[i + 1]) if i + 1 < len(first_row) else ''
        if k:
            info[k] = v
    return info


def _cell(row, idx):
    return text(row[idx]) if idx < len(row) else ''


def summarize_group(valid):
    if not valid:
        return 0, []

    valid = sorted(valid, key=lambda x: (_cell(x[1], 3), _cell(x[1], 4)))

    if COUNT_MODE == 'rows':
        count = len(valid)
    else:
        total = sum(n for n, _ in valid)
        count = int(total) if float(total).is_integer() else round(total, 4)

    rule = norm(_cell(valid[0][1], 6))

    if rule == '访视列计数':
        merged = OrderedDict()
        for n, r in valid:
            proj, center = _cell(r, 3), _cell(r, 4)
            remark = _cell(r, 5)
            slot = merged.setdefault((proj, center), {'visits': [], 'remark': []})

            width = int(n) if float(n).is_integer() and n > 0 else 0
            row_visits = []
            for c in range(8, min(8 + width, len(r))):
                v = text(r[c])
                if v:
                    row_visits.append(v)

            if DROP_PLACEHOLDER_ONE and remark and row_visits \
                    and set(row_visits) == {'1'}:
                row_visits = []

            slot['visits'].extend(row_visits)
            if remark:
                slot['remark'].append(remark)

        lines = []
        for (proj, center), slot in merged.items():
            visits = slot['visits']
            extra = VISIT_SEP.join(slot['remark'])
            detail = VISIT_SEP.join(visits)
            parts = [proj, center]
            if detail:
                parts.append(detail)
            if extra and extra != detail:
                parts.append(extra)
            lines.append(join_parts(parts))
        return count, lines

    lines = []
    for _, r in valid:
        parts = []
        for idx in (3, 4, 5, 8):
            v = _cell(r, idx)
            parts.append(v if v else '#')
        lines.append(join_parts(parts))
    return count, lines


def process_file(file_path):
    rows = read_sheet(file_path, SHEET_FORM, check_h_col=True)
    if not rows or len(rows) < 3:
        raise ValueError('工作表内容为空或格式不符')

    row_dict = extract_basic_info(rows[0])

    data_raw = rows[2:]

    if _SCORE_CATALOG:
        data_raw = [_apply_score_catalog_to_row(r, _SCORE_CATALOG)
                    for r in data_raw]

    data = [r for r in data_raw if len(r) > 1 and text(r[1])]
    if not data:
        return row_dict

    groups = OrderedDict()
    for r in data:
        key = canon_key(text(r[1]))
        groups.setdefault(key, []).append(r)

    for _group_key, group in groups.items():
        work_name = text(group[0][1])

        valid = []
        for r in group:
            n = to_num(r[7]) if len(r) > 7 else None
            if n is not None and n != 0:
                valid.append((n, r))

        count, lines = summarize_group(valid)

        row_dict[f'{work_name}_count'] = count
        row_dict[f'{work_name}_备注'] = '\n'.join([l for l in lines if l])

    return row_dict


# ---------- 构建输出规格 ----------

def build_output_specs(order_keys, match_map, ordered_names):
    specs = []

    if order_keys and match_map:
        wn_by_nk = {}
        for wn in ordered_names:
            wn_by_nk[canon_key(wn)] = wn

        used_wn = set()
        for gk in order_keys:
            nk = canon_key(gk)
            wn = wn_by_nk.get(nk)
            if wn is not None:
                specs.append((gk, wn, True))
                used_wn.add(wn)
            else:
                specs.append((gk, gk, False))

        for wn in ordered_names:
            if wn not in used_wn:
                specs.append((wn, wn, True))
    else:
        for wn in ordered_names:
            specs.append((wn, wn, True))

    return specs


# ---------- 流式写出 ----------

def _write_output_streaming(all_rows, basic_keys, output_specs, output_file,
                            max_width=42):
    headers = ['源文件', '所属文件夹'] + basic_keys
    for dname, _, _ in output_specs:
        headers.append(f'{dname}_count')
        headers.append(f'{dname}_备注')

    def row_to_values(row_dict):
        vals = [row_dict.get('源文件', ''), row_dict.get('所属文件夹', '')]
        for k in basic_keys:
            vals.append(row_dict.get(k, ''))
        for _dname, lkey, has_data in output_specs:
            if has_data:
                vals.append(row_dict.get(f'{lkey}_count', ''))
                vals.append(row_dict.get(f'{lkey}_备注', ''))
            else:
                vals.append(0)
                vals.append('')
        return vals

    wb = xlsxwriter.Workbook(output_file, {'constant_memory': True})
    ws = wb.add_worksheet('Sheet1')

    wrap_fmt = wb.add_format({'text_wrap': True, 'valign': 'top'})

    for i, h in enumerate(headers):
        if h.endswith('_count'):
            width = min(max(len(h) * 2, 10), max_width)
        else:
            width = min(max(len(h), 12), max_width)
        ws.set_column(i, i, width, wrap_fmt)

    for c, h in enumerate(headers):
        ws.write(0, c, h)

    for r, row_dict in enumerate(all_rows, start=1):
        vals = row_to_values(row_dict)
        for c, v in enumerate(vals):
            ws.write(r, c, v)

    ws.freeze_panes(1, 2)
    wb.close()


# ---------- CRA 合并主流程 ----------

def _find_template_file(files: List[str]) -> Optional[str]:
    for f in files:
        base = os.path.basename(f)
        low = base.lower()
        if '模板' in base or 'template' in low:
            return f
    return None


def cra_merge_files(files: List[str], output_file: str,
                    template_file: Optional[str] = None):
    global _SCORE_CATALOG

    out_abs = os.path.abspath(output_file)
    tpl_abs = os.path.abspath(template_file) if template_file else None

    data_files: List[str] = []
    for f in files:
        base = os.path.basename(f)
        if base.startswith('~$') or base.startswith('.'):
            continue
        fab = os.path.abspath(f)
        if fab == out_abs:
            continue
        if tpl_abs and fab == tpl_abs:
            continue
        if not f.lower().endswith(('.xlsm', '.xlsx', '.xls')):
            continue
        data_files.append(f)
    data_files.sort()

    if not data_files:
        raise ValueError('未找到待合并的 Excel 数据文件')

    if template_file and os.path.isfile(template_file):
        guide_source = template_file
        logging.info(f'使用模板文件读取《填写指南》和《{SHEET_SCORE}》: {template_file}')
    else:
        guide_source = data_files[0]
        logging.info(f'未指定模板文件，回退到从第一个数据文件读取《填写指南》: {guide_source}')

    order_keys, match_map = build_guide_index(guide_source)
    if order_keys:
        logging.info(
            f'从《填写指南》第 {GUIDE_START_ROW}~{GUIDE_END_ROW} 行 B 列读取到 '
            f'{len(order_keys)} 个工作内容，将按此顺序排列列')
    else:
        logging.info('警告：未能读取《填写指南》，将按数据在表单中出现的顺序排列列')

    score_catalog = read_score_catalog(guide_source)
    _SCORE_CATALOG = score_catalog or {}
    if score_catalog:
        logging.info(f'从「{SHEET_SCORE}」读取到 {len(score_catalog)} 条项目分值')
    else:
        logging.info(f'提示：未读取到「{SHEET_SCORE}」，跳过 B 列分值改写')

    all_rows = []
    basic_keys = []
    for f in data_files:
        try:
            row = process_file(f)
        except Exception as e:
            logging.warning(f'处理失败，已跳过：{f} -> {type(e).__name__}: {e}')
            continue
        logging.info(f'处理: {f}')
        for k in row:
            if not (k.endswith('_count') or k.endswith('_备注')) \
                    and k not in basic_keys:
                basic_keys.append(k)
        row['源文件'] = os.path.basename(f)
        row['所属文件夹'] = os.path.basename(os.path.dirname(f))
        all_rows.append(row)

    if not all_rows:
        raise ValueError('没有生成任何数据')

    work_names = []
    seen_wn = set()
    for row in all_rows:
        for k in row:
            if k.endswith('_count'):
                wn = k[:-6]
                if wn not in seen_wn:
                    seen_wn.add(wn)
                    work_names.append(wn)

    if order_keys and match_map:
        resolved = {wn: resolve_order(wn, match_map) for wn in work_names}
        ordered_names, used = [], set()
        for gk in order_keys:
            for wn in work_names:
                if wn in used:
                    continue
                if resolved.get(wn) == gk:
                    ordered_names.append(wn)
                    used.add(wn)
        leftovers = [wn for wn in work_names if wn not in used]
        if leftovers:
            logging.info(
                f'提示：以下 {len(leftovers)} 个工作内容在《填写指南》中未找到，'
                f'已附在末尾: {leftovers}')
        ordered_names.extend(leftovers)
    else:
        ordered_names = list(work_names)

    output_specs = build_output_specs(order_keys, match_map, ordered_names)

    if order_keys:
        no_data_count = sum(1 for _, _, has in output_specs if not has)
        if no_data_count:
            logging.info(
                f'提示：指南中有 {no_data_count} 个工作内容在所有数据文件中'
                f'都没有出现，将输出为空列（_count=0，_备注为空）')

    _write_output_streaming(all_rows, basic_keys, output_specs, output_file)

    total_cols = len(basic_keys) + 2 + len(output_specs) * 2
    logging.info(
        f'处理完成，共 {len(all_rows)} 行 / {total_cols} 列，结果已保存至 {output_file}')
    return all_rows


@app.post("/merge_cra")
async def merge_cra(archive_file: UploadFile = File(...)):
    """接收 zip / 7z（内含 CRA 填写表单等 Excel），返回合并后的汇总 xlsx"""
    try:
        archive_bytes = await archive_file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"读取上传文件失败: {e}")

    if not archive_bytes:
        raise HTTPException(status_code=400, detail="上传的文件为空")

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            files = extract_archive_to_dir(
                archive_bytes, archive_file.filename or '', tmpdir)
        except Exception as e:
            logging.exception("解压失败")
            raise HTTPException(status_code=400, detail=f"解压失败: {e}")

        template_path = _find_template_file(files)
        out_path = os.path.join(tmpdir, 'merged_cra.xlsx')

        try:
            cra_merge_files(files, out_path, template_path)
        except Exception as e:
            logging.exception("CRA 合并失败")
            raise HTTPException(status_code=400, detail=f"合并失败: {e}")

        if not os.path.isfile(out_path):
            raise HTTPException(status_code=500, detail="未生成合并结果")

        with open(out_path, 'rb') as f:
            out_bytes = f.read()

    return StreamingResponse(
        io.BytesIO(out_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=merged_cra.xlsx"}
    )


# ============================================================
#        新增：CRA 量表合并模式（竖表）
# ============================================================

SHEET_FORM_CRA = 'CRA填写表单'


def _cra_vertical_read_one(path: str, max_cols: int = 300,
                           filter_col: int = 8,
                           filter_row_start: int = 3) -> Optional[pd.DataFrame]:
    """读取单个 CRA 填写表单，返回竖表格式的 DataFrame"""
    try:
        df = pd.read_excel(
            path,
            sheet_name=SHEET_FORM_CRA,
            engine='openpyxl',
            header=None,
        )
    except ValueError:
        logging.info(f'  [跳过] 无 "{SHEET_FORM_CRA}" 工作表: {path}')
        return None
    except Exception as e:
        logging.warning(f'  [错误] 读取失败: {path} -> {e}')
        return None

    if df.empty or df.shape[0] < filter_row_start:
        logging.info(f'  [跳过] 数据行不足: {path}')
        return None

    header_vals = [
        df.iat[0, i] if df.shape[1] > i else None for i in (1, 3, 5)
    ]

    data = df.iloc[filter_row_start - 1:, :].reset_index(drop=True)

    if data.shape[1] < filter_col:
        logging.info(f'  [跳过] 列数不足 {filter_col}: {path}')
        return None

    col = pd.to_numeric(data.iloc[:, filter_col - 1], errors='coerce').fillna(0)
    data = data[col != 0].reset_index(drop=True)

    if data.empty:
        logging.info(f'  [跳过] 过滤后无数据: {path}')
        return None

    data = data.iloc[:, :max_cols]

    prefix = pd.DataFrame(
        [header_vals] * len(data),
        columns=['第1行第2列', '第1行第4列', '第1行第6列'],
        index=data.index,
    )

    result = pd.concat([prefix, data], axis=1)
    result.insert(0, '来源文件', os.path.basename(path))
    return result


def _cra_vertical_merge(files: List[str], output_file: str,
                        max_cols: int = 300) -> pd.DataFrame:
    """CRA 量表合并（竖表）：把多个文件的 CRA 填写表单纵向堆叠"""
    data_files = []
    for f in files:
        base = os.path.basename(f)
        if base.startswith('~$') or base.startswith('.'):
            continue
        if not f.lower().endswith(('.xlsm', '.xlsx')):
            continue
        data_files.append(f)
    data_files.sort()

    if not data_files:
        raise ValueError('未找到待合并的 Excel 数据文件')

    frames = []
    for idx, path in enumerate(data_files, 1):
        logging.info(f'[{idx}/{len(data_files)}] 处理: {path}')
        r = _cra_vertical_read_one(path, max_cols=max_cols)
        if r is not None:
            frames.append(r)
            logging.info(f'    -> 有效行数 {len(r)}')

    if not frames:
        raise ValueError('没有可合并的数据')

    merged = pd.concat(frames, ignore_index=True, sort=False)
    merged.to_excel(output_file, index=False)
    logging.info(f'合并完成，共 {merged.shape[0]} 行 / {merged.shape[1]} 列')
    return merged


@app.post("/merge_cra_vertical")
async def merge_cra_vertical(archive_file: UploadFile = File(...)):
    """接收 zip / 7z（内含 CRA 填写表单），返回竖表格式的合并 xlsx"""
    try:
        archive_bytes = await archive_file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"读取上传文件失败: {e}")

    if not archive_bytes:
        raise HTTPException(status_code=400, detail="上传的文件为空")

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            files = extract_archive_to_dir(
                archive_bytes, archive_file.filename or '', tmpdir)
        except Exception as e:
            logging.exception("解压失败")
            raise HTTPException(status_code=400, detail=f"解压失败: {e}")

        out_path = os.path.join(tmpdir, 'merged_cra_vertical.xlsx')

        try:
            _cra_vertical_merge(files, out_path)
        except Exception as e:
            logging.exception("CRA 竖表合并失败")
            raise HTTPException(status_code=400, detail=f"合并失败: {e}")

        if not os.path.isfile(out_path):
            raise HTTPException(status_code=500, detail="未生成合并结果")

        with open(out_path, 'rb') as f:
            out_bytes = f.read()

    return StreamingResponse(
        io.BytesIO(out_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=merged_cra_vertical.xlsx"}
    )

# ============================================================
#                       原有 /process 接口
# ============================================================

def parse_numeric_positions(usecols_str: str) -> List[int]:
    parts = [p.strip() for p in usecols_str.split(',') if p.strip() != '']
    if not parts:
        return []
    positions = []
    for p in parts:
        if not p.isdigit():
            raise ValueError(f"非法列索引: {p}. 请使用逗号分隔的正整数，例如 '1,4,5'.")
        n = int(p)
        if n < 1:
            raise ValueError(f"列索引必须 >= 1: {p}")
        positions.append(n - 1)
    return positions


def get_df_by_position_stream(data_content: bytes, sheet_name: str,
                              positions: List[int], header: int = 0):
    bio = io.BytesIO(data_content)
    df_all = pd.read_excel(bio, sheet_name=sheet_name, header=header,
                           engine='calamine')
    max_idx = df_all.shape[1] - 1
    for pos in positions:
        if pos > max_idx:
            raise IndexError(f"请求的列索引 {pos + 1} 超出表格列数 {max_idx + 1}")
    selected = df_all.iloc[:, positions].copy()
    del df_all
    return selected


def copy_style_no_fill(src_cell, dst_cell):
    dst_cell.font = copy(src_cell.font)
    dst_cell.border = copy(src_cell.border)
    dst_cell.alignment = copy(src_cell.alignment)
    dst_cell.number_format = copy(src_cell.number_format)
    dst_cell.protection = copy(src_cell.protection)


@app.get("/health")
def read_root():
    return {"message": "Welcome to Excel Processor"}


@app.post("/process")
async def process(
    data_file: UploadFile = File(...),
    template_file: UploadFile = File(...),
    sheet_name: str = Form("02-项目汇总表"),
    usecols: str = Form("4,5,6,9,11"),
    header_row: int = Form(1),
    data_start: int = Form(4),
):
    try:
        positions = parse_numeric_positions(usecols)
    except ValueError as e:
        logging.error(f"Invalid positions: {e}")
        raise HTTPException(status_code=400, detail=str(e))

    data_content = await data_file.read()
    template_content = await template_file.read()

    try:
        df = get_df_by_position_stream(data_content, sheet_name, positions,
                                       header=header_row - 1)
    except Exception as e:
        logging.error(f"Error reading Excel data: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"读取数据文件失败: {e}")

    logging.info("df shape is {}".format(df.shape))
    if df.empty:
        raise HTTPException(status_code=400,
                            detail="未从指定列和工作表中提取到任何数据。")

    group_col = df.columns[-1]

    seven_zip_buffer = io.BytesIO()
    with py7zr.SevenZipFile(seven_zip_buffer, mode='w') as archive:
        for k_value, sub_df in df.groupby(group_col, sort=False):
            safe_name = str(k_value).replace('/', '_')
            out_io = io.BytesIO()
            tpl_io = io.BytesIO(template_content)
            wb = load_workbook(tpl_io)
            ws_tpl = wb['A'] if 'A' in wb.sheetnames else wb[wb.sheetnames[0]]

            first_col = df.columns[0]
            for d_value, mini_df in sub_df.groupby(first_col, sort=False):
                sheet_name_d = str(d_value)
                if len(sheet_name_d) > 31:
                    sheet_name_d = sheet_name_d[:31]

                if sheet_name_d in wb.sheetnames:
                    wb.remove(wb[sheet_name_d])
                ws = wb.copy_worksheet(ws_tpl)
                ws.title = sheet_name_d

                for r_idx, (_, row) in enumerate(mini_df.iterrows(),
                                                 start=data_start):
                    v1 = row.iloc[1] if len(row) > 1 else None
                    v2 = row.iloc[2] if len(row) > 2 else None
                    v3 = row.iloc[3] if len(row) > 3 else None
                    if v1 is not None:
                        c = ws.cell(row=r_idx, column=1, value=v1)
                        copy_style_no_fill(
                            ws_tpl.cell(row=data_start, column=1), c)
                    if v2 is not None:
                        c = ws.cell(row=r_idx, column=2, value=v2)
                        copy_style_no_fill(
                            ws_tpl.cell(row=data_start, column=2), c)
                    if v3 is not None:
                        c = ws.cell(row=r_idx, column=3, value=v3)
                        copy_style_no_fill(
                            ws_tpl.cell(row=data_start, column=3), c)

            if ws_tpl.title in wb.sheetnames:
                try:
                    wb.remove(ws_tpl)
                except Exception:
                    pass
            wb.save(out_io)
            out_io.seek(0)
            archive.writestr(out_io.read(), str(safe_name) + '.xlsx')

    seven_zip_buffer.seek(0)
    logging.info("Processing finished. Sending response.")
    return StreamingResponse(
        seven_zip_buffer,
        media_type="application/x-7z-compressed",
        headers={"Content-Disposition": "attachment; filename=processed_excels.7z"}
    )
