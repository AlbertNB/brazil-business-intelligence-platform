# Testing Conventions

## Overview

Tests are split across layers with distinct purposes:

- **Silver tests** guard the **fidelity to the source** — they verify that data arrived structurally intact and within the contracts defined by the originating system.
- **Gold tests** guard the **analytical utility** — they verify that transformations produced business entities that are correct, complete, and safe to consume downstream.

A failure in Silver means the source data is broken or unexpected. A failure in Gold means a transformation introduced a defect or a business rule was violated.

---

## Silver Layer

### Goal

Verify that raw data from external sources conforms to the technical and structural contracts of those sources. Silver tests answer: *did the data arrive correctly from the origin?*

### Test categories

#### Presence and uniqueness

Apply `not_null` and `unique` to every natural key column.

Apply `dbt_utils.unique_combination_of_columns` when the model's grain is defined by a composite key (e.g., snapshot models partitioned by a reference period).

```yaml
tests:
  - dbt_utils.unique_combination_of_columns:
      arguments:
        combination_of_columns: [business_key, reference_month]
```

#### Format and length

Use `dbt_utils.expression_is_true` to assert structural format rules defined by the source system (e.g., fixed-length identifiers, check digit validity).

```yaml
tests:
  - dbt_utils.expression_is_true:
      arguments:
        expression: "length(cast(identifier as string)) = N"
```

When a format rule is advisory (known exceptions exist in source data), apply `severity: warn` instead of failing the pipeline.

```yaml
tests:
  - dbt_utils.expression_is_true:
      arguments:
        expression: "{{ validation_macro('column') }}"
      config:
        severity: warn
```

#### Conditional presence

Use `dbt_utils.expression_is_true` for nullability rules that depend on other columns (e.g., at least one of two identifying fields must be present).

```yaml
tests:
  - dbt_utils.expression_is_true:
      arguments:
        expression: "field_a is not null or field_b is not null"
```

#### Metadata fields

Apply `not_null` to all Silver metadata fields: `_extraction_ts` / `_reference_month` / `_reference_date`, `_ingestion_ts`, and `_load_ts`.

---

## Gold Layer

### Goal

Verify that the analytical model correctly represents business concepts and is safe to use in reports and downstream models. Gold tests answer: *does the data correctly represent the business entity it models?*

### Test categories

#### Surrogate key integrity

Apply `not_null` and `unique` to every surrogate key (`_sk`). These are non-negotiable: a dimension with a duplicate or null surrogate key is broken by definition.

```yaml
- name: entity_sk
  tests: [not_null, unique]
```

Apply `not_null` and `unique` to every natural key (`_nk`) as well, since surrogate keys are derived from them.

#### Required business attributes

Apply `not_null` to any column that must be populated for the row to be analytically meaningful. This is a business judgment, not a source contract. A `null` here means the transformation failed to resolve an attribute the business requires.

#### Referential integrity

Use `dbt_utils.relationships` or `relationships` to assert that foreign keys in facts resolve to existing dimension rows.

```yaml
- name: entity_sk
  tests:
    - relationships:
        to: ref('dim_entity')
        field: entity_sk
```

#### Enumeration / accepted values

Use `accepted_values` for columns whose valid set is defined by the business domain (e.g., dimension type classifiers, status labels after translation). Silver only tests that a field arrived and has the correct format — whether a value is semantically valid is a business question that belongs here.

```yaml
- name: entity_status_description
  tests:
    - accepted_values:
        values: ['ACTIVE', 'INACTIVE', 'SUSPENDED']
```

For columns produced by a Silver `CASE WHEN` mapping (e.g., code → label), `not_null` implicitly covers enumeration correctness as long as the `CASE WHEN` has no `ELSE` clause. A source code outside the known set produces `NULL`, which the `not_null` test catches. Never add a catch-all `ELSE` that swallows unknown values, as it would silently hide new source codes that the transformation does not yet handle.

#### Metadata fields

Apply `not_null` to `_updated_at` on all Gold models.

---

## Summary

| Concern | Silver | Gold |
|---|---|---|
| Natural / composite key uniqueness | Yes | Yes (on `_nk`) |
| Surrogate key uniqueness | No | Yes (on `_sk`) |
| Format and length rules | Yes | No |
| Check digit / structural validation | Yes (warn ok) | No |
| Accepted values | No | Yes |
| Conditional presence (field_a or field_b) | Yes | No |
| Referential integrity (FK → dimension) | No | Yes |
| Metadata fields not_null | Yes | Yes |
