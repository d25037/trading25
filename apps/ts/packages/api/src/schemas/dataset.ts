import { z } from '@hono/zod-openapi';
import { DATASET_PRESET_NAMES } from '@trading25/shared/dataset';

export const DatasetPresetSchema = z.enum(DATASET_PRESET_NAMES).openapi({
  description: 'Dataset preset configuration',
  example: 'primeMarket',
});

export const DatasetCreateRequestSchema = z
  .object({
    name: z.string().min(1).max(255).openapi({
      description: 'Dataset filename (will be created in XDG datasets directory)',
      example: 'prime.db',
    }),
    preset: DatasetPresetSchema.openapi({
      description: 'Preset configuration to use',
      example: 'primeMarket',
    }),
    overwrite: z.boolean().optional().default(false).openapi({
      description: 'Overwrite existing dataset',
      example: false,
    }),
    timeoutMinutes: z.number().int().min(1).max(120).optional().openapi({
      description: 'Job timeout in minutes (default: 35)',
      example: 30,
    }),
  })
  .openapi('DatasetCreateRequest');

export const DatasetJobStatusSchema = z.enum(['pending', 'running', 'completed', 'failed', 'cancelled']).openapi({
  description: 'Current job status',
  example: 'running',
});

export const DatasetJobProgressSchema = z
  .object({
    stage: z.string().openapi({ description: 'Current stage name', example: 'Fetching stock data' }),
    current: z.number().int().openapi({ description: 'Current progress count', example: 50 }),
    total: z.number().int().openapi({ description: 'Total items to process', example: 1000 }),
    percentage: z.number().openapi({ description: 'Progress percentage', example: 5.0 }),
    message: z.string().openapi({ description: 'Progress message', example: 'Processing 7203 (Toyota)' }),
  })
  .openapi('DatasetJobProgress');

export const DatasetJobResultSchema = z
  .object({
    success: z.boolean().openapi({ description: 'Whether creation was successful', example: true }),
    totalStocks: z.number().int().openapi({ description: 'Total stocks in dataset', example: 1000 }),
    processedStocks: z.number().int().openapi({ description: 'Number of stocks processed', example: 1000 }),
    warnings: z.array(z.string()).openapi({ description: 'Warning messages', example: [] }),
    errors: z.array(z.string()).openapi({ description: 'Error messages', example: [] }),
    outputPath: z.string().openapi({
      description: 'Path to created dataset',
      example: '/home/user/.local/share/trading25/datasets/prime.db',
    }),
  })
  .openapi('DatasetJobResult');

export const DatasetJobResponseSchema = z
  .object({
    jobId: z
      .string()
      .uuid()
      .openapi({ description: 'Unique job identifier', example: '123e4567-e89b-12d3-a456-426614174000' }),
    status: DatasetJobStatusSchema,
    preset: DatasetPresetSchema,
    name: z.string().openapi({ description: 'Dataset name', example: 'prime.db' }),
    progress: DatasetJobProgressSchema.optional(),
    result: DatasetJobResultSchema.optional(),
    startedAt: z.string().datetime().openapi({ description: 'Job start time' }),
    completedAt: z.string().datetime().optional().openapi({ description: 'Job completion time' }),
    error: z.string().optional().openapi({ description: 'Error message if failed' }),
  })
  .openapi('DatasetJobResponse', {
    description: 'Dataset creation job status and progress',
  });

export const CreateDatasetJobResponseSchema = z
  .object({
    jobId: z.string().uuid().openapi({ description: 'Unique job identifier' }),
    status: DatasetJobStatusSchema,
    name: z.string().openapi({ description: 'Dataset name' }),
    preset: DatasetPresetSchema,
    message: z.string().openapi({ description: 'Status message', example: 'Dataset creation job started' }),
    estimatedTime: z
      .string()
      .optional()
      .openapi({ description: 'Estimated completion time', example: '10-30 minutes' }),
  })
  .openapi('CreateDatasetJobResponse');

export const CancelDatasetJobResponseSchema = z
  .object({
    success: z.boolean().openapi({ description: 'Whether cancellation was successful' }),
    jobId: z.string().uuid().openapi({ description: 'Job ID' }),
    message: z.string().openapi({ description: 'Result message' }),
  })
  .openapi('CancelDatasetJobResponse');

export const DatasetValidationDetailsSchema = z.object({
  dateGapsCount: z.number().int().optional().openapi({ description: 'Number of missing trading days' }),
  fkIntegrity: z
    .object({
      stockDataOrphans: z.number().int(),
      marginDataOrphans: z.number().int(),
      statementsOrphans: z.number().int(),
    })
    .optional()
    .openapi({ description: 'Foreign key integrity issues' }),
  orphanStocksCount: z.number().int().optional().openapi({ description: 'Stocks without quote data' }),
  stockCountValidation: z
    .object({
      preset: z.string().nullable(),
      expected: z.object({ min: z.number().int(), max: z.number().int() }).nullable(),
      actual: z.number().int(),
      isWithinRange: z.boolean(),
    })
    .optional()
    .openapi({ description: 'Stock count validation result' }),
  dataCoverage: z
    .object({
      totalStocks: z.number().int().openapi({ description: 'Total stocks in dataset' }),
      stocksWithQuotes: z.number().int().openapi({ description: 'Stocks with quote data' }),
      stocksWithStatements: z.number().int().openapi({ description: 'Stocks with statements data' }),
      stocksWithMargin: z.number().int().openapi({ description: 'Stocks with margin data' }),
    })
    .optional()
    .openapi({ description: 'Data coverage details' }),
});

export const DatasetValidationSchema = z.object({
  isValid: z.boolean().openapi({ description: 'Whether dataset is valid' }),
  errors: z.array(z.string()).openapi({ description: 'Validation errors', example: [] }),
  warnings: z.array(z.string()).openapi({ description: 'Validation warnings', example: [] }),
  details: DatasetValidationDetailsSchema.optional().openapi({ description: 'Validation details' }),
});

export const StatementsFieldCoverageSchema = z
  .object({
    total: z.number().int().openapi({ description: 'Total statements records', example: 5000 }),
    totalFY: z.number().int().openapi({ description: 'FY (full year) records only', example: 1000 }),
    totalHalf: z.number().int().openapi({ description: 'FY + 2Q (half year) records', example: 2000 }),
    hasExtendedFields: z.boolean().openapi({ description: 'Whether extended fields exist in schema', example: true }),
    hasCashFlowFields: z
      .boolean()
      .openapi({ description: 'Whether cash flow extended fields exist in schema', example: true }),
    earningsPerShare: z.number().int().openapi({ description: 'Records with EPS data', example: 4900 }),
    profit: z.number().int().openapi({ description: 'Records with profit data', example: 4800 }),
    equity: z.number().int().openapi({ description: 'Records with equity data', example: 4700 }),
    nextYearForecastEps: z
      .number()
      .int()
      .openapi({ description: 'Records with next year forecast EPS (FY only)', example: 900 }),
    bps: z.number().int().openapi({ description: 'Records with BPS data (FY only)', example: 950 }),
    sales: z.number().int().openapi({ description: 'Records with sales data', example: 4800 }),
    operatingProfit: z.number().int().openapi({ description: 'Records with operating profit', example: 4700 }),
    ordinaryProfit: z
      .number()
      .int()
      .openapi({ description: 'Records with ordinary profit (J-GAAP only)', example: 2000 }),
    operatingCashFlow: z
      .number()
      .int()
      .openapi({ description: 'Records with operating cash flow (FY/2Q only)', example: 1800 }),
    dividendFY: z.number().int().openapi({ description: 'Records with dividend data (FY only)', example: 900 }),
    forecastEps: z.number().int().openapi({ description: 'Records with forecast EPS', example: 4000 }),
    investingCashFlow: z
      .number()
      .int()
      .openapi({ description: 'Records with investing cash flow (primarily FY/2Q)', example: 1800 }),
    financingCashFlow: z
      .number()
      .int()
      .openapi({ description: 'Records with financing cash flow (primarily FY/2Q)', example: 1800 }),
    cashAndEquivalents: z
      .number()
      .int()
      .openapi({ description: 'Records with cash and equivalents (primarily FY/2Q)', example: 1800 }),
    totalAssets: z.number().int().openapi({ description: 'Records with total assets (all periods)', example: 4000 }),
    sharesOutstanding: z
      .number()
      .int()
      .openapi({ description: 'Records with shares outstanding (all periods)', example: 4000 }),
    treasuryShares: z
      .number()
      .int()
      .openapi({ description: 'Records with treasury shares (all periods)', example: 3900 }),
  })
  .openapi('StatementsFieldCoverage');

export const DatasetInfoResponseSchema = z
  .object({
    name: z.string().openapi({ description: 'Dataset name', example: 'prime.db' }),
    path: z.string().openapi({ description: 'Full path to dataset' }),
    fileSize: z.number().int().openapi({ description: 'File size in bytes', example: 104857600 }),
    lastModified: z.string().datetime().openapi({ description: 'Last modification time' }),
    snapshot: z
      .object({
        preset: z
          .string()
          .nullable()
          .openapi({ description: 'Preset used to create this dataset', example: 'primeMarket' }),
        createdAt: z.string().datetime().nullable().openapi({ description: 'Dataset creation time' }),
      })
      .openapi({ description: 'Snapshot metadata (preset configuration and creation time)' }),
    stats: z.object({
      totalStocks: z.number().int().openapi({ description: 'Total stocks', example: 1000 }),
      totalQuotes: z.number().int().openapi({ description: 'Total quote records', example: 250000 }),
      dateRange: z.object({
        from: z.string().openapi({ description: 'Earliest date', example: '2015-01-01' }),
        to: z.string().openapi({ description: 'Latest date', example: '2025-01-01' }),
      }),
      hasMarginData: z.boolean().openapi({ description: 'Contains margin data' }),
      hasTOPIXData: z.boolean().openapi({ description: 'Contains TOPIX data' }),
      hasSectorData: z.boolean().openapi({ description: 'Contains sector index data' }),
      hasStatementsData: z.boolean().openapi({ description: 'Contains financial statements' }),
      statementsFieldCoverage: StatementsFieldCoverageSchema.nullable().openapi({
        description: 'Field coverage for extended financial metrics (null if no statements data)',
      }),
    }),
    validation: DatasetValidationSchema.openapi({ description: 'Validation results' }),
  })
  .openapi('DatasetInfoResponse');

export const DatasetSampleQuerySchema = z.object({
  size: z.coerce.number().int().min(1).max(10000).default(300).openapi({
    description: 'Sample size',
    example: 300,
  }),
  byMarket: z.coerce.boolean().optional().openapi({
    description: 'Stratify by market',
    example: false,
  }),
  bySector: z.coerce.boolean().optional().openapi({
    description: 'Stratify by sector',
    example: false,
  }),
  seed: z.coerce.number().int().optional().openapi({
    description: 'Random seed for reproducibility',
  }),
});

export const DatasetSampleResponseSchema = z
  .object({
    codes: z.array(z.string()).openapi({ description: 'Sampled stock codes', example: ['7203', '9984', '6758'] }),
    metadata: z.object({
      totalAvailable: z.number().int().openapi({ description: 'Total available stocks' }),
      sampleSize: z.number().int().openapi({ description: 'Actual sample size' }),
      stratificationUsed: z.boolean().openapi({ description: 'Whether stratification was used' }),
      marketDistribution: z.record(z.string(), z.number().int()).optional(),
      sectorDistribution: z.record(z.string(), z.number().int()).optional(),
    }),
  })
  .openapi('DatasetSampleResponse');

export const DatasetSearchQuerySchema = z.object({
  term: z.string().min(1).openapi({
    description: 'Search term (stock code or company name)',
    example: 'toyota',
  }),
  limit: z.coerce.number().int().min(1).max(100).default(20).openapi({
    description: 'Maximum results to return',
    example: 20,
  }),
  exact: z.coerce.boolean().optional().openapi({
    description: 'Exact match only',
    example: false,
  }),
});

export const SearchResultItemSchema = z.object({
  code: z.string().openapi({ description: 'Stock code', example: '7203' }),
  companyName: z.string().openapi({ description: 'Company name', example: 'トヨタ自動車' }),
  companyNameEnglish: z
    .string()
    .optional()
    .openapi({ description: 'Company name in English', example: 'Toyota Motor Corporation' }),
  marketName: z.string().openapi({ description: 'Market name', example: 'プライム' }),
  sectorName: z.string().openapi({ description: 'Sector name', example: '輸送用機器' }),
  matchType: z.enum(['code', 'name', 'english_name']).openapi({ description: 'How the result matched' }),
});

export const DatasetSearchResponseSchema = z
  .object({
    results: z.array(SearchResultItemSchema).openapi({ description: 'Search results' }),
    totalFound: z.number().int().openapi({ description: 'Total matches found', example: 5 }),
  })
  .openapi('DatasetSearchResponse');

export const DatasetListItemSchema = z
  .object({
    name: z.string().openapi({ description: 'Dataset filename', example: 'prime.db' }),
    fileSize: z.number().int().openapi({ description: 'File size in bytes', example: 104857600 }),
    lastModified: z.string().datetime().openapi({ description: 'Last modification time' }),
    preset: z
      .string()
      .nullable()
      .openapi({ description: 'Preset used to create this dataset', example: 'primeMarket' }),
    createdAt: z.string().datetime().nullable().openapi({ description: 'Dataset creation time' }),
  })
  .openapi('DatasetListItem');

export const DatasetListResponseSchema = z
  .object({
    datasets: z.array(DatasetListItemSchema).openapi({ description: 'List of datasets' }),
    totalCount: z.number().int().openapi({ description: 'Total number of datasets', example: 3 }),
  })
  .openapi('DatasetListResponse');

export const DatasetDeleteResponseSchema = z
  .object({
    success: z.boolean().openapi({ description: 'Whether deletion was successful' }),
    name: z.string().openapi({ description: 'Deleted dataset name' }),
    message: z.string().openapi({ description: 'Result message' }),
  })
  .openapi('DatasetDeleteResponse');

export type DatasetListItem = z.infer<typeof DatasetListItemSchema>;
export type DatasetListResponse = z.infer<typeof DatasetListResponseSchema>;
export type DatasetDeleteResponse = z.infer<typeof DatasetDeleteResponseSchema>;
export type DatasetPreset = z.infer<typeof DatasetPresetSchema>;
export type DatasetCreateRequest = z.infer<typeof DatasetCreateRequestSchema>;
export type DatasetJobStatus = z.infer<typeof DatasetJobStatusSchema>;
export type DatasetJobProgress = z.infer<typeof DatasetJobProgressSchema>;
export type DatasetJobResult = z.infer<typeof DatasetJobResultSchema>;
export type DatasetJobResponse = z.infer<typeof DatasetJobResponseSchema>;
export type CreateDatasetJobResponse = z.infer<typeof CreateDatasetJobResponseSchema>;
export type CancelDatasetJobResponse = z.infer<typeof CancelDatasetJobResponseSchema>;
export type DatasetValidationDetails = z.infer<typeof DatasetValidationDetailsSchema>;
export type DatasetValidation = z.infer<typeof DatasetValidationSchema>;
export type DatasetInfoResponse = z.infer<typeof DatasetInfoResponseSchema>;
export type DatasetSampleQuery = z.infer<typeof DatasetSampleQuerySchema>;
export type DatasetSampleResponse = z.infer<typeof DatasetSampleResponseSchema>;
export type DatasetSearchQuery = z.infer<typeof DatasetSearchQuerySchema>;
export type DatasetSearchResponse = z.infer<typeof DatasetSearchResponseSchema>;
