import { DatasetCreateForm } from './DatasetCreateForm';
import { DatasetList } from './DatasetList';

export function DatasetManager() {
  return (
    <div className="space-y-4">
      <DatasetCreateForm />
      <DatasetList />
    </div>
  );
}
