# encoding:utf-8
import io
from copy import copy
from typing import List

import pandas as pd
from fastapi import FastAPI, UploadFile, Form
from fastapi.responses import StreamingResponse
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
import py7zr 

app = FastAPI()

def parse_numeric_positions(usecols_str: str) -> List[int]:
    """
    将 "1,4,5" 解析为 0 起始的整数列表 [0,3,4]
    抛出 ValueError 如果包含非正整数
    """
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
        positions.append(n - 1)  # 转为 0-based
    return positions

def get_df_by_position_stream(data_content: bytes, sheet_name: str, positions: List[int], header: int = 0):
    """
    读取整个 sheet，然后按列位置选择所需列返回 DataFrame。
    为降低内存，尽量只保留必要列。 pandas.read_excel 不支持只按位置读取，
    因此读取时先加载全部列头行，然后按 usecols 字母再读取数据是一种减少内存的方法。
    这里为通用性选择直接读取全部并切片，但随后释放不必要引用以降低内存驻留。
    """
    bio = io.BytesIO(data_content)
    # header 参数：传入的是 pandas.read_excel 的 header（0-based 行索引或 None）
    df_all = pd.read_excel(bio, sheet_name=sheet_name, header=header, engine='calamine')
    max_idx = df_all.shape[1] - 1
    for pos in positions:
        if pos > max_idx:
            raise IndexError(f"请求的列索引 {pos+1} 超出表格列数 {max_idx+1}")
    selected = df_all.iloc[:, positions].copy()
    # 释放大 DataFrame 引用（尽量降低内存）
    del df_all
    return selected

def copy_style_no_fill(src_cell, dst_cell):
    dst_cell.font = copy(src_cell.font)
    dst_cell.border = copy(src_cell.border)
    dst_cell.alignment = copy(src_cell.alignment)
    dst_cell.number_format = copy(src_cell.number_format)
    dst_cell.protection = copy(src_cell.protection)
    # 不复制 fill（颜色）

@app.get("/health")
def read_root():
    return {"message": "Welcome to Excel Processor"}

@app.post("/process")
async def process_files(
    data_file: UploadFile,  # 必传：数据 Excel 文件
    template_file: UploadFile | None = None,  # 可选：模板 Excel 文件
    sheet_name: str = Form("02-项目汇总表"),  # 可选参数，默认值
    usecols: str = Form("4,5,6,9,11"),  # 现在默认用数字，逗号分隔，1-based
    header_row: int = Form(1),  # 可选，1-based header 行（1 表示第一行是列名）
    data_start: int = Form(4)   # 可选，写入模板的起始行
):
    if not template_file:
        return {"error": "模板文件是必需的，请上传。"}

    # 解析数字列到 0-based positions
    try:
        positions = parse_numeric_positions(usecols)
    except ValueError as e:
        return {"error": str(e)}

    # 读取上传文件内容（小优化：先读取模板以便并发使用）
    data_content = await data_file.read()
    template_content = await template_file.read()

    # 读取 DataFrame（按位置）
    try:
        # pandas header 参数使用 0-based，user 给的是 1-based header_row
        df = get_df_by_position_stream(data_content, sheet_name, positions, header=header_row - 1)
    except Exception as e:
        return {"error": str(e)}

    # 为后续在行迭代中按位置访问，获取列名映射（保持 DataFrame 列名但我们通过位置取值）
    col_names = list(df.columns)  # 列名顺序与 positions 对应

    # 减少内存：对 groupby 使用 sort=False 并在循环中删除子 DataFrame 引用
    seven_zip_buffer = io.BytesIO()
    with py7zr.SevenZipFile(seven_zip_buffer, mode='w') as archive:
        for k_value, sub_df in df.groupby(group_col, sort=False):
            safe_name = str(k_value).replace('/', '_')
            out_io = io.BytesIO()

            tpl_io = io.BytesIO(template_content)
            wb = load_workbook(tpl_io)

            if 'A' not in wb.sheetnames:
                ws_tpl = wb[wb.sheetnames[0]]
            else:
                ws_tpl = wb['A']

            first_col = col_names[0]
            for d_value, mini_df in sub_df.groupby(first_col, sort=False):
                sheet_name_d = str(d_value)
                if sheet_name_d in wb.sheetnames:
                    ws_existing = wb[sheet_name_d]
                    wb.remove(ws_existing)
                ws = wb.copy_worksheet(ws_tpl)
                ws.title = sheet_name_d

                for r_idx, (_, row) in enumerate(mini_df.iterrows(), start=data_start):
                    try:
                        v1 = row.iloc[0] if len(col_names) > 0 else None
                        v2 = row.iloc[1] if len(col_names) > 1 else None
                        v3 = row.iloc[2] if len(col_names) > 2 else None
                    except Exception:
                        v1 = v2 = v3 = None

                    if v1 is not None:
                        e_cell = ws.cell(row=r_idx, column=1, value=v1)
                        copy_style_no_fill(ws_tpl.cell(row=data_start, column=1), e_cell)
                    if v2 is not None:
                        f_cell = ws.cell(row=r_idx, column=2, value=v2)
                        copy_style_no_fill(ws_tpl.cell(row=data_start, column=2), f_cell)
                    if v3 is not None:
                        i_cell = ws.cell(row=r_idx, column=3, value=v3)
                        copy_style_no_fill(ws_tpl.cell(row=data_start, column=3), i_cell)

            if ws_tpl.title in wb.sheetnames:
                try:
                    wb.remove(wb[ws_tpl.title])
                except Exception:
                    pass

            wb.save(out_io)
            out_io.seek(0)
            archive.writestr(f'{safe_name}.xlsx', out_io.read())
            out_io.close()
            del wb
            del tpl_io

    seven_zip_buffer.seek(0)


    return StreamingResponse(
        seven_zip_buffer,
        media_type="application/x-7z-compressed",
        headers={"Content-Disposition": "attachment; filename=processed_excels.7z"}
    )
