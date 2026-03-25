# Atlas Glossary Specification

## Overview

An Atlas glossary file describes the metrics, tag keys, and tag values that exist in
a particular domain. The LSP uses glossary data to power completions, hover docs, and
validation for query expressions.

Glossary files are **composable**: the LSP merges all glossary files it finds on the
classpath, so different teams or libraries can ship their own fragments that get
combined at runtime.

## File format

Each glossary file is a JSON object. The top-level keys are:

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `id` | string | yes | Unique identifier for this glossary fragment (e.g. `"common-tags"`, `"myapp.custom-metrics"`) |
| `description` | string | no | Human-readable description of what this fragment covers |
| `metrics` | object | no | Metric definitions keyed by metric name |
| `tagKeys` | object | no | Tag key definitions keyed by tag key name |
| `tagValues` | object | no | Tag value definitions keyed by `"tagKey=tagValue"` |

All sections are optional so a fragment can contribute just metrics, just tags, or a
mix.

## Metrics

Each entry in `metrics` is keyed by the metric name (the value used with
`name,<metric>,:eq`).

```json
{
  "metrics": {
    "sys.cpu.utilization": {
      "description": "CPU utilization as a percentage (0-100).",
      "unit": "percent",
      "category": "system/cpu",
      "link": "https://example.com/glossary/system/cpu/",
      "type": "gauge",
      "tags": {
        "id": {
          "description": "CPU identifier (user, system, iowait, etc.)",
          "values": ["user", "system", "nice", "iowait", "irq", "softirq", "steal", "idle"],
          "valuesType": "examples"
        },
        "app": {
          "required": true
        }
      }
    }
  }
}
```

### Metric fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `description` | string | yes | What this metric measures |
| `unit` | string | no | Unit of measurement (e.g. `"seconds"`, `"bytes/second"`, `"percent"`, `"connections"`) |
| `category` | string | no | Slash-separated category for grouping (e.g. `"system/cpu"`, `"aws/load-balancers"`) |
| `link` | string | no | URL to detailed documentation |
| `type` | string | no | Conceptual type: `"counter"`, `"gauge"`, `"timer"`, `"distributionSummary"`, `"longTaskTimer"`, `"intervalCounter"` |
| `tags` | object | no | Tag keys that apply specifically to this metric (see below) |

### Metric-scoped tags

The `tags` object within a metric defines tag keys that are specific to (or have
specific semantics for) that metric. Each entry is keyed by tag key name:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `description` | string | no | What this tag means in the context of this metric (overrides global tag key description) |
| `required` | boolean | no | Whether this tag is always present on this metric |
| `values` | array of strings | no | List of values. Meaning depends on `valuesType`. Absent means free-form. |
| `valuesType` | string | no | `"enum"` (default) or `"examples"`. See [Values types](#values-types). |

## Tag keys

Each entry in `tagKeys` defines a globally known tag key. These are tag keys that
appear across many metrics (e.g. common infrastructure tags).

```json
{
  "tagKeys": {
    "app": {
      "description": "Application name.",
      "category": "infrastructure"
    },
    "region": {
      "description": "Cloud region where the instance is running.",
      "category": "infrastructure",
      "values": ["us-east-1", "us-west-2", "eu-west-1"],
      "valuesType": "examples"
    },
    "statistic": {
      "description": "Identifies the component of a conceptual type (count, totalTime, gauge, etc.).",
      "category": "type-system",
      "link": "https://netflix.github.io/atlas-docs/asl/conceptual-types/",
      "valuesType": "enum",
      "values": [
        "count", "totalTime", "totalOfSquares", "max",
        "gauge", "activeTasks", "duration", "totalAmount", "percentile"
      ]
    }
  }
}
```

### Tag key fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `description` | string | yes | What this tag key represents |
| `link` | string | no | URL to detailed documentation |
| `category` | string | no | Grouping category (e.g. `"infrastructure"`, `"type-system"`, `"application"`) |
| `values` | array of strings | no | List of values. Meaning depends on `valuesType`. Absent means free-form. |
| `valuesType` | string | no | `"enum"` (default) or `"examples"`. See [Values types](#values-types). |

## Values types

When a `values` array is present, the `valuesType` field controls how the LSP
interprets it:

| valuesType | Completions | Validation | Use when |
|------------|-------------|------------|----------|
| `"enum"` (default) | Offered as the complete set | Unknown values produce a **warning** diagnostic | The set is closed and exhaustive (e.g. `statistic`) |
| `"examples"` | Offered as suggestions alongside free-form input | No warning for unlisted values | The set is open-ended but common values are known (e.g. `app`, CPU `id` modes) |

If `valuesType` is omitted, it defaults to `"enum"` so that existing glossary
files that list values get validation automatically. Use `"examples"` explicitly
when the list is known to be non-exhaustive.

### Merge behavior

When merging `values` across fragments:
- Values arrays are always unioned (deduplicated).
- If fragments disagree on `valuesType` for the same key, `"examples"` wins
  (the more permissive interpretation), and the LSP logs an info-level message.

## Tag values

Each entry in `tagValues` provides documentation for a specific tag value. The key
is `"tagKey=tagValue"`.

This is useful for tag keys where values have non-obvious meaning.

```json
{
  "tagValues": {
    "region=us-east-1": {
      "description": "US East (N. Virginia)"
    },
    "statistic=totalOfSquares": {
      "description": "Sum of squared measurements. Used with count and totalTime to compute standard deviation."
    }
  }
}
```

### Tag value fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `description` | string | yes | What this specific value means |
| `link` | string | no | URL to detailed documentation |

## Composability

### Discovery

The LSP discovers glossary files from two sources, merged in order:

1. **Classpath**: all resources matching `atlas/glossary/*.json` under
   `META-INF/`. Libraries ship fragments by including files at
   `META-INF/atlas/glossary/<id>.json` in their jar.

2. **Configuration**: an optional list of file paths or URLs in the application
   config under `atlas.lsp.glossary.files`.

### Merge rules

When multiple fragments define the same key:

- **Metrics**: entries are merged by metric name. If the same metric appears in
  multiple fragments, fields are merged with later fragments winning on conflict.
  The `tags` sub-object is deep-merged (union of tag keys; per-key fields merged
  with later winning).

- **Tag keys**: entries are merged by tag key name. Fields from later fragments
  override earlier ones. `values` arrays are unioned (deduplicated).

- **Tag values**: entries are merged by compound key. Later descriptions override
  earlier ones.

The `id` field prevents accidental duplication. If two fragments have the same `id`,
the LSP logs a warning and uses the last one loaded.

### Example layout in a jar

```
mylib.jar
└── META-INF/
    └── atlas/
        └── glossary/
            └── mylib-metrics.json
```

### Example configuration

```hocon
atlas.lsp.glossary.files = [
  "/path/to/team-glossary.json"
]
```

## Full example

A single file covering a few common tags and one metric:

```json
{
  "id": "common",
  "description": "Common infrastructure tags and example metrics.",
  "tagKeys": {
    "app": {
      "description": "Application name.",
      "category": "infrastructure"
    },
    "cluster": {
      "description": "Specific deployment of an application, typically app-stack-detail.",
      "category": "infrastructure"
    },
    "region": {
      "description": "Cloud region where the instance is running.",
      "category": "infrastructure",
      "values": ["us-east-1", "us-west-2", "eu-west-1"],
      "valuesType": "examples"
    },
    "node": {
      "description": "Instance identifier.",
      "category": "infrastructure"
    },
    "statistic": {
      "description": "Component of a conceptual type (count, totalTime, gauge, etc.).",
      "category": "type-system",
      "link": "https://netflix.github.io/atlas-docs/asl/conceptual-types/",
      "valuesType": "enum",
      "values": [
        "count", "totalTime", "totalOfSquares", "max",
        "gauge", "activeTasks", "duration", "totalAmount", "percentile"
      ]
    }
  },
  "tagValues": {
    "statistic=count": {
      "description": "Rate of events per second."
    },
    "statistic=totalTime": {
      "description": "Total time in seconds spent on events per second."
    },
    "statistic=gauge": {
      "description": "Sampled value at a point in time."
    },
    "statistic=activeTasks": {
      "description": "Number of currently in-progress long tasks."
    }
  },
  "metrics": {
    "http.req.complete": {
      "description": "Timer for completed HTTP requests.",
      "unit": "seconds",
      "category": "http",
      "type": "timer"
    },
    "sys.cpu.utilization": {
      "description": "CPU utilization as a percentage.",
      "unit": "percent",
      "category": "system/cpu",
      "type": "gauge",
      "tags": {
        "id": {
          "description": "CPU mode (user, system, iowait, etc.).",
          "values": ["user", "system", "nice", "iowait", "irq", "softirq", "steal", "idle"],
          "valuesType": "examples"
        }
      }
    }
  }
}
```
