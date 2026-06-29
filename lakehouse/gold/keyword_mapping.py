# -*- coding: utf-8 -*-
r"""
Gold helper - Map keyword -> category.

Port từ apply_keyword_mapping.py cũ: chuẩn hóa text (bỏ dấu), nạp luật từ
reference/keyword_category_mapping.csv và trả về UDF phân loại.
"""

from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List

from pyspark.sql import functions as F
from pyspark.sql import types as T


def normalize_text(text: Any) -> str:
    if text is None:
        return ""
    text = str(text).strip().lower()
    text = "".join(
        ch for ch in unicodedata.normalize("NFD", text)
        if unicodedata.category(ch) != "Mn"
    )
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def load_mapping_rules(mapping_path: Path) -> List[Dict[str, Any]]:
    if not mapping_path.exists():
        raise FileNotFoundError(
            f"Không tìm thấy file mapping: {mapping_path}\n"
            "Hãy tạo keyword_category_mapping.csv trước khi chạy gold search trend."
        )

    rules: List[Dict[str, Any]] = []
    seen = set()
    with mapping_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"keyword", "category"}
        if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
            raise ValueError("File mapping phải có 2 cột: keyword, category")
        for row in reader:
            keyword = normalize_text(row.get("keyword", ""))
            category = str(row.get("category", "Khác")).strip() or "Khác"
            if not keyword or keyword in seen:
                continue
            seen.add(keyword)
            rules.append({
                "keyword": keyword,
                "category": category,
                "length": len(keyword),
                "single_token": " " not in keyword,
            })
    rules.sort(key=lambda x: x["length"], reverse=True)
    return rules


def make_categorize_udf(rules: List[Dict[str, Any]]):
    """Tạo UDF phân loại đóng gói sẵn `rules` (broadcast theo closure)."""

    def categorize(text: Any) -> str:
        normalized = normalize_text(text)
        if not normalized:
            return "Khác"
        for rule in rules:
            keyword = rule["keyword"]
            if rule["single_token"] and len(keyword) <= 4:
                pattern = rf"(?<!\w){re.escape(keyword)}(?!\w)"
                if re.search(pattern, normalized):
                    return rule["category"]
            elif keyword in normalized:
                return rule["category"]
        return "Khác"

    return F.udf(categorize, T.StringType())
