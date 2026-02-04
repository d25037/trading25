import { WatchlistService } from '../../services/watchlist-service';
import { createManagedService, createOpenAPIApp } from '../../utils';
import { createWatchlistCrudRoutes } from './watchlist-crud';
import { createWatchlistItemRoutes } from './watchlist-items';
import { createWatchlistPricesRoutes } from './watchlist-prices';

const getWatchlistService = createManagedService('WatchlistService', {
  factory: () => new WatchlistService(),
});

const watchlistApp = createOpenAPIApp();

watchlistApp.route('/', createWatchlistCrudRoutes(getWatchlistService));
watchlistApp.route('/', createWatchlistItemRoutes(getWatchlistService));
watchlistApp.route('/', createWatchlistPricesRoutes(getWatchlistService));

export default watchlistApp;
