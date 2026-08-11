import {
  PRESET_IDS,
  TOOL_PRESETS,
  assessClientCompatibility,
  compileToolSelection,
  type ClientTarget,
  type ToolPresetId,
  type ToolSelection,
} from "./tool-presets.js";
import {
  assertPolicyUpdateCanUseBearer,
  type PersistedToolPolicy,
} from "./tool-policy.js";
import type { ToolDef } from "./tools/types.js";

export interface ToolSelectionPresetView {
  id: ToolPresetId;
  label: string;
  description: string;
  count: number;
  tool_ids: string[];
}

export interface ToolSelectionView {
  presets: ToolSelectionPresetView[];
  custom_tools: Array<{ id: string; label: string }>;
  default_preset: ToolPresetId;
}

export interface ParsedToolSelection {
  policy: PersistedToolPolicy;
  selection: ToolSelection;
  target: ClientTarget;
  selected_count: number;
}

export function compilePolicyUpdate(
  selection: ToolSelection,
  requestedTarget: ClientTarget | undefined,
  currentTarget: ClientTarget,
  registry: readonly ToolDef[],
  currentPolicy: PersistedToolPolicy,
): { policy: PersistedToolPolicy; target: ClientTarget } {
  const policy = compileToolSelection(selection, registry);
  const target = requestedTarget ?? currentTarget;
  assertPolicyUpdateCanUseBearer(
    currentPolicy,
    currentTarget,
    policy,
    target,
    registry.map((tool) => tool.name),
  );
  const count = policy.mode === "all" ? registry.length : policy.tool_ids.length;
  const compatibility = assessClientCompatibility(count, target);
  if (!compatibility.compatible) {
    throw new Error(compatibility.error ?? "Tool set is incompatible with the selected client.");
  }
  return { policy, target };
}

export function buildToolSelectionView(registry: readonly ToolDef[]): ToolSelectionView {
  return {
    presets: PRESET_IDS.map((id) => {
      const policy = compileToolSelection({ kind: "preset", preset_id: id }, registry);
      return {
        id,
        label: TOOL_PRESETS[id].label,
        description: TOOL_PRESETS[id].description,
        count: policy.mode === "all" ? registry.length : policy.tool_ids.length,
        tool_ids:
          policy.mode === "all"
            ? registry.map((tool) => tool.name)
            : policy.tool_ids,
      };
    }),
    custom_tools: registry.map((tool) => ({ id: tool.name, label: tool.name })),
    default_preset: "read_only_crm",
  };
}

export function parseToolSelectionForm(
  form: FormData,
  registry: readonly ToolDef[],
): ParsedToolSelection {
  const preset = String(form.get("toolPreset") ?? "read_only_crm");
  const target = form.get("targetClient") === "copilot_studio" ? "copilot_studio" : "generic";
  let selection: ToolSelection;

  if (preset === "custom") {
    selection = {
      kind: "custom",
      tool_ids: form.getAll("toolIds").map((value) => String(value)),
    };
  } else if ((PRESET_IDS as readonly string[]).includes(preset)) {
    selection = { kind: "preset", preset_id: preset as ToolPresetId };
  } else {
    throw new Error("Unknown tool preset.");
  }

  const policy = compileToolSelection(selection, registry);
  return {
    policy,
    selection,
    target,
    selected_count: policy.mode === "all" ? registry.length : policy.tool_ids.length,
  };
}
