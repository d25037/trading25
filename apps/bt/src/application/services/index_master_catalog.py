"""
Index master seed catalog.

J-Quants v2 には index master 専用エンドポイントが見当たらないため、
index_master はローカル参照データを SoT として補完する。
"""

from __future__ import annotations

from datetime import UTC, datetime

_INDEX_CODE_NAME_PAIRS: tuple[tuple[str, str], ...] = (
    ("0000", "TOPIX"),
    ("0001", "東証二部総合指数"),
    ("0028", "TOPIX Core30"),
    ("0029", "TOPIX Large 70"),
    ("002A", "TOPIX 100"),
    ("002B", "TOPIX Mid400"),
    ("002C", "TOPIX 500"),
    ("002D", "TOPIX Small"),
    ("002E", "TOPIX 1000"),
    ("002F", "TOPIX Small500"),
    ("0040", "東証業種別 水産・農林業"),
    ("0041", "東証業種別 鉱業"),
    ("0042", "東証業種別 建設業"),
    ("0043", "東証業種別 食料品"),
    ("0044", "東証業種別 繊維製品"),
    ("0045", "東証業種別 パルプ・紙"),
    ("0046", "東証業種別 化学"),
    ("0047", "東証業種別 医薬品"),
    ("0048", "東証業種別 石油・石炭製品"),
    ("0049", "東証業種別 ゴム製品"),
    ("004A", "東証業種別 ガラス・土石製品"),
    ("004B", "東証業種別 鉄鋼"),
    ("004C", "東証業種別 非鉄金属"),
    ("004D", "東証業種別 金属製品"),
    ("004E", "東証業種別 機械"),
    ("004F", "東証業種別 電気機器"),
    ("0050", "東証業種別 輸送用機器"),
    ("0051", "東証業種別 精密機器"),
    ("0052", "東証業種別 その他製品"),
    ("0053", "東証業種別 電気・ガス業"),
    ("0054", "東証業種別 陸運業"),
    ("0055", "東証業種別 海運業"),
    ("0056", "東証業種別 空運業"),
    ("0057", "東証業種別 倉庫・運輸関連業"),
    ("0058", "東証業種別 情報・通信業"),
    ("0059", "東証業種別 卸売業"),
    ("005A", "東証業種別 小売業"),
    ("005B", "東証業種別 銀行業"),
    ("005C", "東証業種別 証券・商品先物取引業"),
    ("005D", "東証業種別 保険業"),
    ("005E", "東証業種別 その他金融業"),
    ("005F", "東証業種別 不動産業"),
    ("0060", "東証業種別 サービス業"),
    ("0070", "東証グロース市場250指数"),
    ("0075", "REIT"),
    ("0080", "TOPIX-17 食品"),
    ("0081", "TOPIX-17 エネルギー資源"),
    ("0082", "TOPIX-17 建設・資材"),
    ("0083", "TOPIX-17 素材・化学"),
    ("0084", "TOPIX-17 医薬品"),
    ("0085", "TOPIX-17 自動車・輸送機"),
    ("0086", "TOPIX-17 鉄鋼・非鉄"),
    ("0087", "TOPIX-17 機械"),
    ("0088", "TOPIX-17 電機・精密"),
    ("0089", "TOPIX-17 情報通信・サービスその他"),
    ("008A", "TOPIX-17 電力・ガス"),
    ("008B", "TOPIX-17 運輸・物流"),
    ("008C", "TOPIX-17 商社・卸売"),
    ("008D", "TOPIX-17 小売"),
    ("008E", "TOPIX-17 銀行"),
    ("008F", "TOPIX-17 金融（除く銀行）"),
    ("0090", "TOPIX-17 不動産"),
    ("0091", "JASDAQ INDEX"),
    ("0500", "東証プライム市場指数"),
    ("0501", "東証スタンダード市場指数"),
    ("0502", "東証グロース市場指数"),
    ("0503", "JPXプライム150指数"),
    ("8100", "TOPIX バリュー"),
    ("812C", "TOPIX500 バリュー"),
    ("812D", "TOPIXSmall バリュー"),
    ("8200", "TOPIX グロース"),
    ("822C", "TOPIX500 グロース"),
    ("822D", "TOPIXSmall グロース"),
    ("8501", "東証REIT オフィス指数"),
    ("8502", "東証REIT 住宅指数"),
    ("8503", "東証REIT 商業・物流等指数"),
)

_TOPIX_CODES = {
    "0000",
    "0001",
    "0028",
    "0029",
    "002A",
    "002B",
    "002C",
    "002D",
    "002E",
    "002F",
}
_MARKET_CODES = {
    "0070",
    "0075",
    "0091",
    "0500",
    "0501",
    "0502",
    "0503",
    "8501",
    "8502",
    "8503",
}
_STYLE_CODES = {"8100", "812C", "812D", "8200", "822C", "822D"}


def _in_hex_range(code: str, start_hex: str, end_hex: str) -> bool:
    try:
        value = int(code, 16)
    except ValueError:
        return False
    return int(start_hex, 16) <= value <= int(end_hex, 16)


def _resolve_category(code: str) -> str:
    if code in _TOPIX_CODES:
        return "topix"
    if code in _MARKET_CODES:
        return "market"
    if code in _STYLE_CODES:
        return "style"
    if _in_hex_range(code, "0040", "0060"):
        return "sector33"
    if _in_hex_range(code, "0080", "0090"):
        return "sector17"
    return "unknown"


def get_index_catalog_codes() -> set[str]:
    return {code for code, _name in _INDEX_CODE_NAME_PAIRS}


def build_index_master_seed_rows(
    *,
    existing_codes: set[str] | None = None,
) -> list[dict[str, str | None]]:
    created_at = datetime.now(UTC).isoformat()
    existing = existing_codes or set()
    rows: list[dict[str, str | None]] = []

    for code, name in _INDEX_CODE_NAME_PAIRS:
        if code in existing:
            continue
        rows.append({
            "code": code,
            "name": name,
            "name_english": None,
            "category": _resolve_category(code),
            "data_start_date": None,
            "created_at": created_at,
        })

    return rows
