import { z } from '@hono/zod-openapi';

/**
 * Portfolio ID parameter schema
 */
export const PortfolioIdParamSchema = z
  .object({
    id: z
      .string()
      .regex(/^\d+$/, 'Portfolio ID must be a positive integer')
      .openapi({
        example: '1',
        description: 'Portfolio ID',
        param: {
          name: 'id',
          in: 'path',
        },
      }),
  })
  .openapi('PortfolioIdParam');

/**
 * Portfolio Item ID parameter schema (for ID-based endpoints)
 */
export const PortfolioItemIdParamSchema = z
  .object({
    id: z
      .string()
      .regex(/^\d+$/, 'Portfolio ID must be a positive integer')
      .openapi({
        example: '1',
        description: 'Portfolio ID',
        param: {
          name: 'id',
          in: 'path',
        },
      }),
    itemId: z
      .string()
      .regex(/^\d+$/, 'Item ID must be a positive integer')
      .openapi({
        example: '1',
        description: 'Portfolio Item ID',
        param: {
          name: 'itemId',
          in: 'path',
        },
      }),
  })
  .openapi('PortfolioItemIdParam');

/**
 * Create Portfolio request schema
 */
export const CreatePortfolioRequestSchema = z
  .object({
    name: z.string().min(1, 'Portfolio name is required').openapi({
      example: 'My Portfolio',
      description: 'Portfolio name (must be unique)',
    }),
    description: z.string().optional().openapi({
      example: 'Long-term holdings',
      description: 'Portfolio description',
    }),
  })
  .openapi('CreatePortfolioRequest');

/**
 * Update Portfolio request schema
 */
export const UpdatePortfolioRequestSchema = z
  .object({
    name: z.string().min(1).optional().openapi({
      example: 'Updated Portfolio',
      description: 'Updated portfolio name',
    }),
    description: z.string().optional().openapi({
      example: 'Updated description',
      description: 'Updated portfolio description',
    }),
  })
  .openapi('UpdatePortfolioRequest');

/**
 * Create Portfolio Item request schema
 */
export const CreatePortfolioItemRequestSchema = z
  .object({
    code: z
      .string()
      .regex(/^\d[0-9A-Z]\d[0-9A-Z]$/, 'Stock code must be 4 characters (e.g., 7203 or 285A)')
      .openapi({
        example: '7203',
        description: 'Stock code (4 characters)',
      }),
    companyName: z.string().min(1).optional().openapi({
      example: 'Toyota Motor',
      description: 'Company name (if not provided, will be fetched from JQuants API)',
    }),
    quantity: z.number().int().positive('Quantity must be greater than 0').openapi({
      example: 100,
      description: 'Number of shares',
    }),
    purchasePrice: z.number().positive('Purchase price must be greater than 0').openapi({
      example: 2500,
      description: 'Purchase price per share',
    }),
    purchaseDate: z
      .string()
      .regex(/^\d{4}-\d{2}-\d{2}$/, 'Purchase date must be in YYYY-MM-DD format')
      .openapi({
        example: '2024-01-01',
        description: 'Purchase date (YYYY-MM-DD)',
      }),
    account: z.string().optional().openapi({
      example: 'NISA',
      description: 'Account type (e.g., NISA, iDeCo)',
    }),
    notes: z.string().optional().openapi({
      example: 'Blue chip stock',
      description: 'Additional notes',
    }),
  })
  .openapi('CreatePortfolioItemRequest');

/**
 * Update Portfolio Item request schema
 */
export const UpdatePortfolioItemRequestSchema = z
  .object({
    quantity: z.number().int().positive().optional().openapi({
      example: 150,
      description: 'Updated number of shares',
    }),
    purchasePrice: z.number().positive().optional().openapi({
      example: 2600,
      description: 'Updated purchase price per share',
    }),
    purchaseDate: z
      .string()
      .regex(/^\d{4}-\d{2}-\d{2}$/)
      .optional()
      .openapi({
        example: '2024-02-01',
        description: 'Updated purchase date (YYYY-MM-DD)',
      }),
    account: z.string().optional().openapi({
      example: 'iDeCo',
      description: 'Updated account type',
    }),
    notes: z.string().optional().openapi({
      example: 'Updated notes',
      description: 'Updated notes',
    }),
  })
  .openapi('UpdatePortfolioItemRequest');

/**
 * Portfolio response schema
 */
export const PortfolioResponseSchema = z
  .object({
    id: z.number().openapi({
      example: 1,
      description: 'Portfolio ID',
    }),
    name: z.string().openapi({
      example: 'My Portfolio',
      description: 'Portfolio name',
    }),
    description: z.string().optional().openapi({
      example: 'Long-term holdings',
      description: 'Portfolio description',
    }),
    createdAt: z.string().openapi({
      example: '2024-01-01T00:00:00.000Z',
      description: 'Creation timestamp',
    }),
    updatedAt: z.string().openapi({
      example: '2024-01-01T00:00:00.000Z',
      description: 'Last update timestamp',
    }),
  })
  .openapi('PortfolioResponse');

/**
 * Portfolio Item response schema
 */
export const PortfolioItemResponseSchema = z
  .object({
    id: z.number().openapi({
      example: 1,
      description: 'Item ID',
    }),
    portfolioId: z.number().openapi({
      example: 1,
      description: 'Portfolio ID',
    }),
    code: z.string().openapi({
      example: '7203',
      description: 'Stock code',
    }),
    companyName: z.string().openapi({
      example: 'Toyota Motor',
      description: 'Company name',
    }),
    quantity: z.number().openapi({
      example: 100,
      description: 'Number of shares',
    }),
    purchasePrice: z.number().openapi({
      example: 2500,
      description: 'Purchase price per share',
    }),
    purchaseDate: z.string().openapi({
      example: '2024-01-01',
      description: 'Purchase date',
    }),
    account: z.string().optional().openapi({
      example: 'NISA',
      description: 'Account type',
    }),
    notes: z.string().optional().openapi({
      example: 'Blue chip stock',
      description: 'Additional notes',
    }),
    createdAt: z.string().openapi({
      example: '2024-01-01T00:00:00.000Z',
      description: 'Creation timestamp',
    }),
    updatedAt: z.string().openapi({
      example: '2024-01-01T00:00:00.000Z',
      description: 'Last update timestamp',
    }),
  })
  .openapi('PortfolioItemResponse');

/**
 * Portfolio with Items response schema
 */
export const PortfolioWithItemsResponseSchema = z
  .object({
    id: z.number().openapi({
      example: 1,
      description: 'Portfolio ID',
    }),
    name: z.string().openapi({
      example: 'My Portfolio',
      description: 'Portfolio name',
    }),
    description: z.string().optional().openapi({
      example: 'Long-term holdings',
      description: 'Portfolio description',
    }),
    createdAt: z.string().openapi({
      example: '2024-01-01T00:00:00.000Z',
      description: 'Creation timestamp',
    }),
    updatedAt: z.string().openapi({
      example: '2024-01-01T00:00:00.000Z',
      description: 'Last update timestamp',
    }),
    items: z.array(PortfolioItemResponseSchema).openapi({
      description: 'Portfolio items',
    }),
  })
  .openapi('PortfolioWithItemsResponse');

/**
 * Portfolio Summary response schema
 */
export const PortfolioSummaryResponseSchema = z
  .object({
    id: z.number().openapi({
      example: 1,
      description: 'Portfolio ID',
    }),
    name: z.string().openapi({
      example: 'My Portfolio',
      description: 'Portfolio name',
    }),
    description: z.string().optional().openapi({
      example: 'Long-term holdings',
      description: 'Portfolio description',
    }),
    stockCount: z.number().openapi({
      example: 5,
      description: 'Number of different stocks in portfolio',
    }),
    totalShares: z.number().openapi({
      example: 500,
      description: 'Total number of shares across all stocks',
    }),
    createdAt: z.string().openapi({
      example: '2024-01-01T00:00:00.000Z',
      description: 'Creation timestamp',
    }),
    updatedAt: z.string().openapi({
      example: '2024-01-01T00:00:00.000Z',
      description: 'Last update timestamp',
    }),
  })
  .openapi('PortfolioSummaryResponse');

/**
 * List Portfolios response schema
 */
export const ListPortfoliosResponseSchema = z
  .object({
    portfolios: z.array(PortfolioSummaryResponseSchema).openapi({
      description: 'List of portfolios with summary statistics',
    }),
  })
  .openapi('ListPortfoliosResponse');

/**
 * Delete success response schema
 */
export const DeleteSuccessResponseSchema = z
  .object({
    success: z.boolean().openapi({
      example: true,
      description: 'Operation success status',
    }),
    message: z.string().openapi({
      example: 'Portfolio deleted successfully',
      description: 'Success message',
    }),
  })
  .openapi('DeleteSuccessResponse');

/**
 * Portfolio Name and Stock Code parameter schema
 */
export const PortfolioNameStockCodeParamSchema = z
  .object({
    portfolioName: z
      .string()
      .min(1, 'Portfolio name is required')
      .openapi({
        example: 'My Portfolio',
        description: 'Portfolio name',
        param: {
          name: 'portfolioName',
          in: 'path',
        },
      }),
    code: z
      .string()
      .regex(/^\d[0-9A-Z]\d[0-9A-Z]$/, 'Stock code must be 4 characters (e.g., 7203 or 285A)')
      .openapi({
        example: '7203',
        description: 'Stock code (4 characters)',
        param: {
          name: 'code',
          in: 'path',
        },
      }),
  })
  .openapi('PortfolioNameStockCodeParam');

/**
 * Portfolio item deleted response (with item details)
 */
export const PortfolioItemDeletedResponseSchema = z
  .object({
    success: z.boolean().openapi({
      example: true,
      description: 'Operation success status',
    }),
    message: z.string().openapi({
      example: 'Stock removed successfully',
      description: 'Success message',
    }),
    deletedItem: PortfolioItemResponseSchema.openapi({
      description: 'The deleted portfolio item details',
    }),
  })
  .openapi('PortfolioItemDeletedResponse');

/**
 * Portfolio Name parameter schema (for name-based endpoints)
 */
export const PortfolioNameParamSchema = z
  .object({
    name: z
      .string()
      .min(1, 'Portfolio name is required')
      .openapi({
        example: 'My Portfolio',
        description: 'Portfolio name',
        param: {
          name: 'name',
          in: 'path',
        },
      }),
  })
  .openapi('PortfolioNameParam');

/**
 * Portfolio stock codes response schema
 * Returns list of stock codes in a portfolio
 */
export const PortfolioCodesResponseSchema = z
  .object({
    name: z.string().openapi({
      example: 'My Portfolio',
      description: 'Portfolio name',
    }),
    codes: z.array(z.string()).openapi({
      example: ['7203', '9984', '6758'],
      description: 'List of stock codes in the portfolio',
    }),
  })
  .openapi('PortfolioCodesResponse');
