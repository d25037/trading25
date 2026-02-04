/**
 * Index Master Data Constants
 *
 * Static master data for all supported indices from J-Quants API.
 * Based on: https://jpx.gitbook.io/j-quants-ja/api-reference/indices/indexcodes
 */

import type { IndexCategory } from '../schema/market-schema';
import { INDEX_CATEGORIES } from '../schema/market-schema';

/**
 * Index definition interface
 */
export interface IndexDefinition {
  code: string;
  name: string;
  nameEnglish: string | null;
  category: IndexCategory;
  dataStartDate: string; // YYYY-MM-DD
}

/**
 * All supported indices from J-Quants API
 */
export const INDEX_MASTER_DATA: IndexDefinition[] = [
  // ===== TOPIX and Size Indices =====
  { code: '0000', name: 'TOPIX', nameEnglish: 'TOPIX', category: INDEX_CATEGORIES.TOPIX, dataStartDate: '2008-05-07' },
  {
    code: '0028',
    name: 'TOPIX Core30',
    nameEnglish: 'TOPIX Core30',
    category: INDEX_CATEGORIES.TOPIX,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0029',
    name: 'TOPIX Large70',
    nameEnglish: 'TOPIX Large70',
    category: INDEX_CATEGORIES.TOPIX,
    dataStartDate: '2008-05-07',
  },
  {
    code: '002A',
    name: 'TOPIX 100',
    nameEnglish: 'TOPIX 100',
    category: INDEX_CATEGORIES.TOPIX,
    dataStartDate: '2008-05-07',
  },
  {
    code: '002B',
    name: 'TOPIX Mid400',
    nameEnglish: 'TOPIX Mid400',
    category: INDEX_CATEGORIES.TOPIX,
    dataStartDate: '2008-05-07',
  },
  {
    code: '002C',
    name: 'TOPIX 500',
    nameEnglish: 'TOPIX 500',
    category: INDEX_CATEGORIES.TOPIX,
    dataStartDate: '2008-05-07',
  },
  {
    code: '002D',
    name: 'TOPIX Small',
    nameEnglish: 'TOPIX Small',
    category: INDEX_CATEGORIES.TOPIX,
    dataStartDate: '2008-05-07',
  },
  {
    code: '002E',
    name: 'TOPIX 1000',
    nameEnglish: 'TOPIX 1000',
    category: INDEX_CATEGORIES.TOPIX,
    dataStartDate: '2008-05-07',
  },

  // ===== 33 Sector Indices =====
  {
    code: '0040',
    name: '水産・農林業',
    nameEnglish: 'Fishery, Agriculture & Forestry',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0041',
    name: '鉱業',
    nameEnglish: 'Mining',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0042',
    name: '建設業',
    nameEnglish: 'Construction',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0043',
    name: '食料品',
    nameEnglish: 'Foods',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0044',
    name: '繊維製品',
    nameEnglish: 'Textiles & Apparels',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0045',
    name: 'パルプ・紙',
    nameEnglish: 'Pulp & Paper',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0046',
    name: '化学',
    nameEnglish: 'Chemicals',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0047',
    name: '医薬品',
    nameEnglish: 'Pharmaceutical',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0048',
    name: '石油・石炭製品',
    nameEnglish: 'Oil & Coal Products',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0049',
    name: 'ゴム製品',
    nameEnglish: 'Rubber Products',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '004A',
    name: 'ガラス・土石製品',
    nameEnglish: 'Glass & Ceramics Products',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '004B',
    name: '鉄鋼',
    nameEnglish: 'Iron & Steel',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '004C',
    name: '非鉄金属',
    nameEnglish: 'Nonferrous Metals',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '004D',
    name: '金属製品',
    nameEnglish: 'Metal Products',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '004E',
    name: '機械',
    nameEnglish: 'Machinery',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '004F',
    name: '電気機器',
    nameEnglish: 'Electric Appliances',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0050',
    name: '輸送用機器',
    nameEnglish: 'Transportation Equipment',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0051',
    name: '精密機器',
    nameEnglish: 'Precision Instruments',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0052',
    name: 'その他製品',
    nameEnglish: 'Other Products',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0053',
    name: '電気・ガス業',
    nameEnglish: 'Electric Power & Gas',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0054',
    name: '陸運業',
    nameEnglish: 'Land Transportation',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0055',
    name: '海運業',
    nameEnglish: 'Marine Transportation',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0056',
    name: '空運業',
    nameEnglish: 'Air Transportation',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0057',
    name: '倉庫・運輸関連業',
    nameEnglish: 'Warehousing & Harbor Transportation',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0058',
    name: '情報・通信業',
    nameEnglish: 'Information & Communication',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0059',
    name: '卸売業',
    nameEnglish: 'Wholesale Trade',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '005A',
    name: '小売業',
    nameEnglish: 'Retail Trade',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '005B',
    name: '銀行業',
    nameEnglish: 'Banks',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '005C',
    name: '証券、商品先物取引業',
    nameEnglish: 'Securities & Commodity Futures',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '005D',
    name: '保険業',
    nameEnglish: 'Insurance',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '005E',
    name: 'その他金融業',
    nameEnglish: 'Other Financing Business',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '005F',
    name: '不動産業',
    nameEnglish: 'Real Estate',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0060',
    name: 'サービス業',
    nameEnglish: 'Services',
    category: INDEX_CATEGORIES.SECTOR33,
    dataStartDate: '2008-05-07',
  },

  // ===== Growth Market Index =====
  {
    code: '0070',
    name: '東証グロース市場250指数',
    nameEnglish: 'TSE Growth Market 250 Index',
    category: INDEX_CATEGORIES.GROWTH,
    dataStartDate: '2008-05-07',
  },

  // ===== REIT Index =====
  {
    code: '0075',
    name: '東証REIT指数',
    nameEnglish: 'TSE REIT Index',
    category: INDEX_CATEGORIES.REIT,
    dataStartDate: '2008-05-07',
  },

  // ===== TOPIX-17 Sector Indices =====
  {
    code: '0080',
    name: 'TOPIX-17 食品',
    nameEnglish: 'TOPIX-17 Foods',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0081',
    name: 'TOPIX-17 エネルギー資源',
    nameEnglish: 'TOPIX-17 Energy Resources',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0082',
    name: 'TOPIX-17 建設・資材',
    nameEnglish: 'TOPIX-17 Construction & Materials',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0083',
    name: 'TOPIX-17 素材・化学',
    nameEnglish: 'TOPIX-17 Raw Materials & Chemicals',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0084',
    name: 'TOPIX-17 医薬品',
    nameEnglish: 'TOPIX-17 Pharmaceutical',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0085',
    name: 'TOPIX-17 自動車・輸送機',
    nameEnglish: 'TOPIX-17 Automobiles & Transportation Equipment',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0086',
    name: 'TOPIX-17 鉄鋼・非鉄',
    nameEnglish: 'TOPIX-17 Steel & Nonferrous Metals',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0087',
    name: 'TOPIX-17 機械',
    nameEnglish: 'TOPIX-17 Machinery',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0088',
    name: 'TOPIX-17 電機・精密',
    nameEnglish: 'TOPIX-17 Electric Appliances & Precision Instruments',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0089',
    name: 'TOPIX-17 情報通信・サービスその他',
    nameEnglish: 'TOPIX-17 IT & Services, Others',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '008A',
    name: 'TOPIX-17 電力・ガス',
    nameEnglish: 'TOPIX-17 Electric Power & Gas',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '008B',
    name: 'TOPIX-17 運輸・物流',
    nameEnglish: 'TOPIX-17 Transportation & Logistics',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '008C',
    name: 'TOPIX-17 商社・卸売',
    nameEnglish: 'TOPIX-17 Commercial & Wholesale Trade',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '008D',
    name: 'TOPIX-17 小売',
    nameEnglish: 'TOPIX-17 Retail Trade',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '008E',
    name: 'TOPIX-17 銀行',
    nameEnglish: 'TOPIX-17 Banks',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '008F',
    name: 'TOPIX-17 金融（除く銀行）',
    nameEnglish: 'TOPIX-17 Financials (ex Banks)',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },
  {
    code: '0090',
    name: 'TOPIX-17 不動産',
    nameEnglish: 'TOPIX-17 Real Estate',
    category: INDEX_CATEGORIES.SECTOR17,
    dataStartDate: '2008-05-07',
  },

  // ===== Market Indices (Prime/Standard/Growth) =====
  {
    code: '0500',
    name: '東証プライム市場指数',
    nameEnglish: 'TSE Prime Market Index',
    category: INDEX_CATEGORIES.MARKET,
    dataStartDate: '2022-06-27',
  },
  {
    code: '0501',
    name: '東証スタンダード市場指数',
    nameEnglish: 'TSE Standard Market Index',
    category: INDEX_CATEGORIES.MARKET,
    dataStartDate: '2022-06-27',
  },
  {
    code: '0502',
    name: '東証グロース市場指数',
    nameEnglish: 'TSE Growth Market Index',
    category: INDEX_CATEGORIES.MARKET,
    dataStartDate: '2022-06-27',
  },

  // ===== REIT Sub-indices =====
  {
    code: '8501',
    name: '東証REITオフィス指数',
    nameEnglish: 'TSE REIT Office Index',
    category: INDEX_CATEGORIES.REIT,
    dataStartDate: '2008-05-07',
  },
  {
    code: '8502',
    name: '東証REIT住宅指数',
    nameEnglish: 'TSE REIT Residential Index',
    category: INDEX_CATEGORIES.REIT,
    dataStartDate: '2008-05-07',
  },
  {
    code: '8503',
    name: '東証REIT商業・物流等指数',
    nameEnglish: 'TSE REIT Commercial & Logistics Index',
    category: INDEX_CATEGORIES.REIT,
    dataStartDate: '2008-05-07',
  },

  // ===== Style Indices (Value/Growth) =====
  {
    code: '8100',
    name: 'TOPIX バリュー',
    nameEnglish: 'TOPIX Value',
    category: INDEX_CATEGORIES.STYLE,
    dataStartDate: '2009-02-09',
  },
  {
    code: '8200',
    name: 'TOPIX グロース',
    nameEnglish: 'TOPIX Growth',
    category: INDEX_CATEGORIES.STYLE,
    dataStartDate: '2009-02-09',
  },
  {
    code: '812C',
    name: 'TOPIX500 バリュー',
    nameEnglish: 'TOPIX500 Value',
    category: INDEX_CATEGORIES.STYLE,
    dataStartDate: '2009-02-09',
  },
  {
    code: '822C',
    name: 'TOPIX500 グロース',
    nameEnglish: 'TOPIX500 Growth',
    category: INDEX_CATEGORIES.STYLE,
    dataStartDate: '2009-02-09',
  },
  {
    code: '812D',
    name: 'TOPIXSmall バリュー',
    nameEnglish: 'TOPIX Small Value',
    category: INDEX_CATEGORIES.STYLE,
    dataStartDate: '2009-02-09',
  },
  {
    code: '822D',
    name: 'TOPIXSmall グロース',
    nameEnglish: 'TOPIX Small Growth',
    category: INDEX_CATEGORIES.STYLE,
    dataStartDate: '2009-02-09',
  },
];

/**
 * Get all index codes as an array
 */
export function getAllIndexCodes(): string[] {
  return INDEX_MASTER_DATA.map((index) => index.code);
}

/**
 * Get all index codes excluding TOPIX (0000)
 * TOPIX must be fetched from /indices/bars/daily/topix endpoint
 */
export function getAllIndexCodesExcludingTOPIX(): string[] {
  return INDEX_MASTER_DATA.filter((index) => index.code !== '0000').map((index) => index.code);
}

/**
 * Get index codes by category
 */
export function getIndexCodesByCategory(category: IndexCategory): string[] {
  return INDEX_MASTER_DATA.filter((index) => index.category === category).map((index) => index.code);
}

/**
 * Get index definition by code
 */
export function getIndexDefinition(code: string): IndexDefinition | undefined {
  return INDEX_MASTER_DATA.find((index) => index.code === code);
}

/**
 * Total number of indices
 */
export const TOTAL_INDEX_COUNT = INDEX_MASTER_DATA.length;
