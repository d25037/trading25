import { PortfolioService } from '../../services/portfolio-service';
import { createManagedService, createOpenAPIApp } from '../../utils';
import { createPortfolioCrudRoutes } from './portfolio-crud';
import { createPortfolioItemRoutes } from './portfolio-items';
import { createPortfolioStockRoutes } from './portfolio-stocks';

const getPortfolioService = createManagedService('PortfolioService', {
  factory: () => new PortfolioService(),
});

const portfolioApp = createOpenAPIApp();

// Mount portfolio CRUD routes (list, create, get, update, delete)
portfolioApp.route('/', createPortfolioCrudRoutes(getPortfolioService));

// Mount portfolio item routes (addItem, updateItem, deleteItem by ID)
portfolioApp.route('/', createPortfolioItemRoutes(getPortfolioService));

// Mount portfolio stock routes (updateStock, deleteStock, getPortfolioCodes by name+code)
portfolioApp.route('/', createPortfolioStockRoutes(getPortfolioService));

export default portfolioApp;
