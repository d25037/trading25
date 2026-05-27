import type { AuthoringFieldSchema, SignalDefinition, SignalFieldDefinition } from '@trading25/api-clients/backtest';
import { useCallback, useId, useMemo } from 'react';
import { MetadataFieldControl } from '@/components/Backtest/MetadataFieldControl';
import { buildDefaultSignalParams, SignalFieldInputs } from '@/components/Backtest/SignalFieldInputs';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Label } from '@/components/ui/label';
import { deriveFundamentalParentFieldNames } from './authoringDocumentUtils';
import {
  addFundamentalSignalConfig,
  buildDefaultFundamentalConfig as buildDefaultFundamentalConfigFromFields,
  buildSignalOptions,
  getValueAtPath,
  hasValueAtPath,
  isPlainObject,
  normalizeSignalSection,
  removeFundamentalChildConfig,
  updateFundamentalChildConfig,
  updateFundamentalParentConfig,
  updateRegularSignalConfig,
} from './authoringUtils';

export type SignalSectionKey = 'entry_filter_params' | 'exit_trigger_params';

const FUNDAMENTAL_PARENT_FIELD_FALLBACK = ['enabled', 'period_type', 'use_adjusted'];

export const executionSemanticsLabels: Record<string, string> = {
  standard: 'Standard',
  next_session_round_trip: 'Next Session Round Trip',
  current_session_round_trip: 'Current Session Round Trip',
  overnight_round_trip: 'Overnight Round Trip',
};

function SignalCard({
  definition,
  signalConfig,
  disabled,
  onToggleEnabled,
  onFieldChange,
  onRemove,
}: {
  definition: SignalDefinition;
  signalConfig: Record<string, unknown>;
  disabled?: boolean;
  onToggleEnabled: (enabled: boolean) => void;
  onFieldChange: (field: SignalFieldDefinition, value: unknown) => void;
  onRemove: () => void;
}) {
  const enabledId = useId();
  const enabled = Boolean(
    signalConfig.enabled ?? definition.fields.find((field) => field.name === 'enabled')?.default ?? true
  );

  return (
    <Card className="border-border/60">
      <CardHeader className="pb-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="space-y-1">
            <CardTitle className="text-base">{definition.name}</CardTitle>
            <CardDescription>{definition.summary ?? definition.description}</CardDescription>
          </div>
          <div className="flex items-center gap-2">
            <Label htmlFor={enabledId} className="text-xs text-muted-foreground">
              Enabled
            </Label>
            <input
              id={enabledId}
              type="checkbox"
              aria-label={`${definition.name} enabled`}
              checked={enabled}
              disabled={disabled}
              onChange={(event) => onToggleEnabled(event.target.checked)}
            />
          </div>
        </div>
        {definition.when_to_use && definition.when_to_use.length > 0 ? (
          <ul className="list-disc space-y-1 pl-5 text-xs text-muted-foreground">
            {definition.when_to_use.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        ) : null}
      </CardHeader>
      <CardContent className="space-y-4">
        <SignalFieldInputs
          fields={definition.fields}
          values={signalConfig}
          excludeFields={['enabled']}
          disabled={disabled}
          onFieldChange={onFieldChange}
        />

        {definition.data_requirements.length > 0 ? (
          <div className="text-xs text-muted-foreground">
            Data requirements: {definition.data_requirements.join(', ')}
          </div>
        ) : null}

        {definition.pitfalls && definition.pitfalls.length > 0 ? (
          <div className="rounded-md bg-amber-500/5 p-3 text-xs text-amber-800">{definition.pitfalls.join(' ')}</div>
        ) : null}

        <div className="flex justify-end">
          <Button
            variant="ghost"
            className="text-destructive hover:text-destructive"
            onClick={onRemove}
            disabled={disabled}
          >
            Remove
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

function buildSignalParentFieldSchema(field: SignalFieldDefinition): AuthoringFieldSchema {
  return {
    path: field.name,
    section: 'shared_config',
    label: field.label ?? field.name,
    group: null,
    type: field.type === 'select' ? 'select' : field.type,
    widget:
      field.type === 'boolean'
        ? 'switch'
        : field.type === 'select'
          ? 'select'
          : field.type === 'number'
            ? 'number'
            : 'text',
    description: field.description,
    summary: null,
    default: field.default,
    options: field.options ?? [],
    constraints: field.constraints,
    placeholder: field.placeholder ?? null,
    unit: field.unit ?? null,
    examples: [],
    required: false,
    advanced_only: false,
  };
}

function NativeSelectField({
  label,
  ariaLabel,
  placeholder,
  disabled,
  optionGroups,
  options,
  onSelect,
}: {
  label: string;
  ariaLabel: string;
  placeholder: string;
  disabled?: boolean;
  optionGroups?: Array<{ key: string; label: string; options: Array<{ value: string; label: string }> }>;
  options?: Array<{ value: string; label: string }>;
  onSelect: (value: string) => void;
}) {
  const selectId = useId();

  return (
    <div className="min-w-64">
      <Label htmlFor={selectId} className="mb-1 block text-xs font-medium text-muted-foreground">
        {label}
      </Label>
      <select
        id={selectId}
        aria-label={ariaLabel}
        className="h-10 w-full rounded-md border border-input bg-background px-3 text-sm"
        defaultValue=""
        disabled={disabled}
        onChange={(event) => {
          const selectedValue = event.target.value;
          if (!selectedValue) {
            return;
          }
          onSelect(selectedValue);
          event.currentTarget.value = '';
        }}
      >
        <option value="">{placeholder}</option>
        {optionGroups?.map((group) => (
          <optgroup key={group.key} label={group.label}>
            {group.options.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </optgroup>
        ))}
        {options?.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </div>
  );
}

function FundamentalParentSettingsGrid({
  sectionKey,
  fields,
  values,
  onChange,
}: {
  sectionKey: SignalSectionKey;
  fields: SignalFieldDefinition[];
  values: Record<string, unknown>;
  onChange: (field: SignalFieldDefinition, value: unknown) => void;
}) {
  return (
    <div className="grid gap-3 lg:grid-cols-3">
      {fields.map((field) => (
        <MetadataFieldControl
          key={`${sectionKey}.fundamental.${field.name}`}
          field={buildSignalParentFieldSchema(field)}
          value={values[field.name] ?? field.default}
          onChange={(value) => onChange(field, value)}
        />
      ))}
    </div>
  );
}

function StrategyEditorSignalSection({
  sectionKey,
  title,
  draftConfig,
  definitionsByType,
  buildSelectableSignalOptions,
  fundamentalParentFieldNames,
  fundamentalDefinitionsByType,
  fundamentalDefinitions,
  exitSectionDisabled,
  currentExecutionMode,
  exitSignals,
  updateDraftAtPath,
  addRegularSignal,
  updateRegularSignalField,
  removeRegularSignal,
  addFundamentalSignal,
  fundamentalParentFields,
  updateFundamentalParentField,
  updateFundamentalChildField,
  removeFundamentalChild,
}: {
  sectionKey: SignalSectionKey;
  title: string;
  draftConfig: Record<string, unknown>;
  definitionsByType: Map<string, SignalDefinition>;
  buildSelectableSignalOptions: (sectionKey: SignalSectionKey) => ReturnType<typeof buildSignalOptions>;
  fundamentalParentFieldNames: string[];
  fundamentalDefinitionsByType: Map<string, SignalDefinition>;
  fundamentalDefinitions: SignalDefinition[];
  exitSectionDisabled: boolean;
  currentExecutionMode: string;
  exitSignals: Record<string, unknown>;
  updateDraftAtPath: (path: string, value: unknown) => void;
  addRegularSignal: (sectionKey: SignalSectionKey, signalType: string) => void;
  updateRegularSignalField: (
    sectionKey: SignalSectionKey,
    signalType: string,
    field: SignalFieldDefinition,
    value: unknown
  ) => void;
  removeRegularSignal: (sectionKey: SignalSectionKey, signalType: string) => void;
  addFundamentalSignal: (sectionKey: SignalSectionKey, childKey: string) => void;
  fundamentalParentFields: SignalFieldDefinition[];
  updateFundamentalParentField: (sectionKey: SignalSectionKey, field: SignalFieldDefinition, value: unknown) => void;
  updateFundamentalChildField: (
    sectionKey: SignalSectionKey,
    childKey: string,
    field: SignalFieldDefinition,
    value: unknown
  ) => void;
  removeFundamentalChild: (sectionKey: SignalSectionKey, childKey: string) => void;
}) {
  const section = normalizeSignalSection(draftConfig[sectionKey]);
  const regularSignalEntries = Object.entries(section).filter(
    ([signalKey, signalValue]) =>
      signalKey !== 'fundamental' && definitionsByType.has(signalKey) && isPlainObject(signalValue)
  );
  const availableOptions = buildSelectableSignalOptions(sectionKey);
  const fundamentalSection = normalizeSignalSection(section.fundamental);
  const configuredFundamentalChildren = Object.keys(fundamentalSection).filter(
    (key) => !fundamentalParentFieldNames.includes(key) && fundamentalDefinitionsByType.has(key)
  );
  const availableFundamentalSignals = fundamentalDefinitions.filter(
    (definition) =>
      !configuredFundamentalChildren.includes(definition.signal_type) &&
      (sectionKey === 'entry_filter_params' || !definition.exit_disabled)
  );
  const sectionDisabled = sectionKey === 'exit_trigger_params' && exitSectionDisabled;

  return (
    <Card className="border-border/60">
      <CardHeader className="pb-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="space-y-1">
            <CardTitle className="text-lg">{title}</CardTitle>
            <CardDescription>
              {sectionKey === 'entry_filter_params'
                ? 'Add signals from grouped categories and edit them visually.'
                : 'Exit signals are available only in standard execution mode.'}
            </CardDescription>
          </div>
          <NativeSelectField
            label="Add signal"
            ariaLabel={`Add ${title}`}
            placeholder="Select a signal…"
            disabled={sectionDisabled}
            optionGroups={availableOptions.map(({ category, signals }) => ({
              key: category.key,
              label: category.label,
              options: signals.map((definition) => ({
                value: definition.signal_type,
                label: definition.name,
              })),
            }))}
            onSelect={(signalType) => addRegularSignal(sectionKey, signalType)}
          />
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {sectionDisabled ? (
          <div className="rounded-lg border border-amber-500/20 bg-amber-500/10 p-4 text-sm text-amber-800">
            Execution policy <strong>{executionSemanticsLabels[currentExecutionMode] ?? currentExecutionMode}</strong>{' '}
            disables exit triggers. Save an empty object for <code>exit_trigger_params</code> or clear it now.
            {Object.keys(exitSignals).length > 0 ? (
              <div className="mt-3">
                <Button variant="outline" onClick={() => updateDraftAtPath('exit_trigger_params', {})}>
                  Clear Exit Config
                </Button>
              </div>
            ) : null}
          </div>
        ) : null}

        {regularSignalEntries.map(([signalKey, signalValue]) => {
          const definition = definitionsByType.get(signalKey);
          if (!definition || !isPlainObject(signalValue)) {
            return null;
          }
          return (
            <SignalCard
              key={signalKey}
              definition={definition}
              signalConfig={signalValue}
              disabled={sectionDisabled}
              onToggleEnabled={(enabled) =>
                updateRegularSignalField(
                  sectionKey,
                  signalKey,
                  { name: 'enabled', type: 'boolean', description: '' },
                  enabled
                )
              }
              onFieldChange={(field, value) => updateRegularSignalField(sectionKey, signalKey, field, value)}
              onRemove={() => removeRegularSignal(sectionKey, signalKey)}
            />
          );
        })}

        <Card className="border-dashed border-border/60">
          <CardHeader className="pb-4">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div className="space-y-1">
                <CardTitle className="text-base">Fundamental Signals</CardTitle>
                <CardDescription>Parent settings are shared. Child cards control individual factors.</CardDescription>
              </div>
              <NativeSelectField
                label="Add fundamental factor"
                ariaLabel={`Add ${title} fundamental signal`}
                placeholder="Select a factor…"
                disabled={sectionDisabled || availableFundamentalSignals.length === 0}
                options={availableFundamentalSignals.map((definition) => ({
                  value: definition.signal_type,
                  label: definition.name,
                }))}
                onSelect={(childKey) => addFundamentalSignal(sectionKey, childKey)}
              />
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            {Object.keys(fundamentalSection).length > 0 ? (
              <>
                <FundamentalParentSettingsGrid
                  sectionKey={sectionKey}
                  fields={fundamentalParentFields}
                  values={fundamentalSection}
                  onChange={(field, value) => updateFundamentalParentField(sectionKey, field, value)}
                />

                {configuredFundamentalChildren.map((childKey) => {
                  const definition = fundamentalDefinitionsByType.get(childKey);
                  const childConfig = normalizeSignalSection(fundamentalSection[childKey]);
                  if (!definition) return null;

                  return (
                    <SignalCard
                      key={`${sectionKey}.fundamental.${childKey}`}
                      definition={definition}
                      signalConfig={childConfig}
                      disabled={sectionDisabled}
                      onToggleEnabled={(enabled) =>
                        updateFundamentalChildField(
                          sectionKey,
                          childKey,
                          { name: 'enabled', type: 'boolean', description: '' },
                          enabled
                        )
                      }
                      onFieldChange={(field, value) => updateFundamentalChildField(sectionKey, childKey, field, value)}
                      onRemove={() => removeFundamentalChild(sectionKey, childKey)}
                    />
                  );
                })}
              </>
            ) : (
              <div className="rounded-lg bg-muted/40 p-4 text-sm text-muted-foreground">
                No fundamental filters configured in this section.
              </div>
            )}
          </CardContent>
        </Card>

        {regularSignalEntries.length === 0 && Object.keys(fundamentalSection).length === 0 ? (
          <div className="rounded-lg bg-muted/40 p-4 text-sm text-muted-foreground">
            No signals configured yet. Add a signal from the dropdown above.
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}

interface UseStrategyEditorSignalRendererParams {
  categories: Array<{ key: string; label: string }>;
  defaultSharedConfig: Record<string, unknown>;
  definitions: SignalDefinition[];
  draftConfig: Record<string, unknown>;
  rawSharedConfig: Record<string, unknown>;
  removeDraftPath: (path: string) => void;
  updateDraftAtPath: (path: string, value: unknown) => void;
}

export function useStrategyEditorSignalRenderer({
  categories,
  defaultSharedConfig,
  definitions,
  draftConfig,
  rawSharedConfig,
  removeDraftPath,
  updateDraftAtPath,
}: UseStrategyEditorSignalRendererParams) {
  const definitionsByType = useMemo(
    () => new Map(definitions.map((definition) => [definition.signal_type, definition])),
    [definitions]
  );
  const regularDefinitions = useMemo(
    () => definitions.filter((definition) => !definition.key.startsWith('fundamental_')),
    [definitions]
  );
  const fundamentalDefinitions = useMemo(
    () => definitions.filter((definition) => definition.key.startsWith('fundamental_')),
    [definitions]
  );
  const fundamentalDefinitionsByType = useMemo(
    () => new Map(fundamentalDefinitions.map((definition) => [definition.signal_type, definition])),
    [fundamentalDefinitions]
  );
  const fundamentalParentFieldNames = useMemo(
    () => deriveFundamentalParentFieldNames(fundamentalDefinitions, FUNDAMENTAL_PARENT_FIELD_FALLBACK),
    [fundamentalDefinitions]
  );
  const exitSignals = normalizeSignalSection(draftConfig.exit_trigger_params);
  const currentExecutionMode = String(
    hasValueAtPath(rawSharedConfig, 'execution_policy.mode')
      ? getValueAtPath(rawSharedConfig, 'execution_policy.mode')
      : (getValueAtPath(defaultSharedConfig, 'execution_policy.mode') ?? 'standard')
  );
  const exitSectionDisabled = currentExecutionMode !== 'standard';

  const buildSelectableSignalOptions = useCallback(
    (sectionKey: SignalSectionKey) => {
      return buildSignalOptions(
        normalizeSignalSection(draftConfig[sectionKey]),
        categories,
        regularDefinitions,
        sectionKey
      );
    },
    [categories, draftConfig, regularDefinitions]
  );

  const addRegularSignal = useCallback(
    (sectionKey: SignalSectionKey, signalType: string) => {
      const definition = definitionsByType.get(signalType);
      if (!definition) return;
      updateDraftAtPath(`${sectionKey}.${signalType}`, buildDefaultSignalParams(definition));
    },
    [definitionsByType, updateDraftAtPath]
  );

  const updateRegularSignalField = useCallback(
    (sectionKey: SignalSectionKey, signalType: string, field: SignalFieldDefinition, value: unknown) => {
      const nextSignal = updateRegularSignalConfig(
        normalizeSignalSection(draftConfig[sectionKey]),
        signalType,
        field,
        value
      );
      updateDraftAtPath(`${sectionKey}.${signalType}`, nextSignal);
    },
    [draftConfig, updateDraftAtPath]
  );

  const removeRegularSignal = useCallback(
    (sectionKey: SignalSectionKey, signalType: string) => {
      removeDraftPath(`${sectionKey}.${signalType}`);
    },
    [removeDraftPath]
  );

  const fundamentalParentFields = useMemo(() => {
    const firstDefinition = fundamentalDefinitions[0];
    if (!firstDefinition) return [];
    return firstDefinition.fields.filter((field) => fundamentalParentFieldNames.includes(field.name));
  }, [fundamentalDefinitions, fundamentalParentFieldNames]);

  const buildDefaultFundamentalConfig = useCallback(() => {
    return buildDefaultFundamentalConfigFromFields(fundamentalParentFields);
  }, [fundamentalParentFields]);

  const addFundamentalSignal = useCallback(
    (sectionKey: SignalSectionKey, childKey: string) => {
      const definition = fundamentalDefinitionsByType.get(childKey);
      if (!definition) return;

      const currentFundamental = normalizeSignalSection(normalizeSignalSection(draftConfig[sectionKey]).fundamental);
      const nextFundamental = addFundamentalSignalConfig(
        currentFundamental,
        childKey,
        definition,
        fundamentalParentFieldNames,
        buildDefaultFundamentalConfig()
      );
      updateDraftAtPath(`${sectionKey}.fundamental`, nextFundamental);
    },
    [
      buildDefaultFundamentalConfig,
      draftConfig,
      fundamentalDefinitionsByType,
      fundamentalParentFieldNames,
      updateDraftAtPath,
    ]
  );

  const updateFundamentalParentField = useCallback(
    (sectionKey: SignalSectionKey, field: SignalFieldDefinition, value: unknown) => {
      const currentFundamental = normalizeSignalSection(normalizeSignalSection(draftConfig[sectionKey]).fundamental);
      const nextFundamental = updateFundamentalParentConfig(
        currentFundamental,
        field,
        value,
        buildDefaultFundamentalConfig()
      );
      updateDraftAtPath(`${sectionKey}.fundamental`, nextFundamental);
    },
    [buildDefaultFundamentalConfig, draftConfig, updateDraftAtPath]
  );

  const updateFundamentalChildField = useCallback(
    (sectionKey: SignalSectionKey, childKey: string, field: SignalFieldDefinition, value: unknown) => {
      const currentFundamental = normalizeSignalSection(normalizeSignalSection(draftConfig[sectionKey]).fundamental);
      const nextFundamental = updateFundamentalChildConfig(
        currentFundamental,
        childKey,
        field,
        value,
        buildDefaultFundamentalConfig()
      );
      updateDraftAtPath(`${sectionKey}.fundamental`, nextFundamental);
    },
    [buildDefaultFundamentalConfig, draftConfig, updateDraftAtPath]
  );

  const removeFundamentalChild = useCallback(
    (sectionKey: SignalSectionKey, childKey: string) => {
      const currentFundamental = normalizeSignalSection(normalizeSignalSection(draftConfig[sectionKey]).fundamental);
      const { nextFundamental, shouldRemoveSection } = removeFundamentalChildConfig(
        currentFundamental,
        childKey,
        fundamentalParentFieldNames
      );

      if (shouldRemoveSection) {
        removeDraftPath(`${sectionKey}.fundamental`);
        return;
      }

      updateDraftAtPath(`${sectionKey}.fundamental`, nextFundamental);
    },
    [draftConfig, fundamentalParentFieldNames, removeDraftPath, updateDraftAtPath]
  );

  const renderSignalSection = useCallback(
    (sectionKey: SignalSectionKey, title: string) => (
      <StrategyEditorSignalSection
        sectionKey={sectionKey}
        title={title}
        draftConfig={draftConfig}
        definitionsByType={definitionsByType}
        buildSelectableSignalOptions={buildSelectableSignalOptions}
        fundamentalParentFieldNames={fundamentalParentFieldNames}
        fundamentalDefinitionsByType={fundamentalDefinitionsByType}
        fundamentalDefinitions={fundamentalDefinitions}
        exitSectionDisabled={exitSectionDisabled}
        currentExecutionMode={currentExecutionMode}
        exitSignals={exitSignals}
        updateDraftAtPath={updateDraftAtPath}
        addRegularSignal={addRegularSignal}
        updateRegularSignalField={updateRegularSignalField}
        removeRegularSignal={removeRegularSignal}
        addFundamentalSignal={addFundamentalSignal}
        fundamentalParentFields={fundamentalParentFields}
        updateFundamentalParentField={updateFundamentalParentField}
        updateFundamentalChildField={updateFundamentalChildField}
        removeFundamentalChild={removeFundamentalChild}
      />
    ),
    [
      addFundamentalSignal,
      addRegularSignal,
      buildSelectableSignalOptions,
      currentExecutionMode,
      definitionsByType,
      draftConfig,
      exitSectionDisabled,
      exitSignals,
      fundamentalDefinitions,
      fundamentalDefinitionsByType,
      fundamentalParentFieldNames,
      fundamentalParentFields,
      removeFundamentalChild,
      removeRegularSignal,
      updateDraftAtPath,
      updateFundamentalChildField,
      updateFundamentalParentField,
      updateRegularSignalField,
    ]
  );

  return {
    definitionsByType,
    fundamentalDefinitionsByType,
    fundamentalParentFieldNames,
    renderSignalSection,
  };
}
