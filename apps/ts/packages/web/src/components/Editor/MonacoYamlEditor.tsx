/**
 * Monaco YAML Editor Component
 *
 * YAMLシンタックスハイライト付きエディター
 */

import Editor, { type OnMount } from '@monaco-editor/react';

interface MonacoYamlEditorProps {
  value: string;
  onChange: (value: string) => void;
  height?: string;
  readOnly?: boolean;
  onMount?: OnMount;
}

export function MonacoYamlEditor({
  value,
  onChange,
  height = '400px',
  readOnly = false,
  onMount,
}: MonacoYamlEditorProps) {
  const handleChange = (newValue: string | undefined) => {
    onChange(newValue ?? '');
  };

  return (
    <div className="border rounded-md overflow-hidden">
      <Editor
        height={height}
        defaultLanguage="yaml"
        value={value}
        onChange={handleChange}
        onMount={onMount}
        theme="vs-dark"
        options={{
          readOnly,
          minimap: { enabled: false },
          scrollBeyondLastLine: false,
          fontSize: 13,
          lineNumbers: 'on',
          wordWrap: 'on',
          tabSize: 2,
          insertSpaces: true,
          automaticLayout: true,
          folding: true,
          renderLineHighlight: 'line',
          scrollbar: {
            vertical: 'auto',
            horizontal: 'auto',
          },
        }}
      />
    </div>
  );
}
