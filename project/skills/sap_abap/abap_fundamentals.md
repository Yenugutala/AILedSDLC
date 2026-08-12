# Skill: ABAP Fundamentals

## Overview
Core ABAP language constructs, data types, internal tables, and programming model for SAP development.

## Key Patterns

### Data Types
| Category | Examples |
|---|---|
| Numeric | `i` (integer), `p` (packed decimal), `f` (float), `decfloat16/34` |
| Character | `c` (fixed char), `string` (variable), `n` (numeric text) |
| Date/Time | `d` (YYYYMMDD), `t` (HHMMSS), `utclong` |
| Binary | `x` (raw hex), `xstring` (variable binary) |
| Reference | `REF TO` class or interface |

### Internal Tables
```abap
" Standard table — allows duplicates, sequential access
DATA lt_orders TYPE TABLE OF zorders_s.

" Sorted table — auto-sorted by key, binary search
DATA lt_sorted TYPE SORTED TABLE OF zorders_s WITH KEY order_id.

" Hashed table — unique key, O(1) read
DATA lt_hashed TYPE HASHED TABLE OF zorders_s WITH UNIQUE KEY order_id.
```
- Use `LOOP AT ... INTO` or `LOOP AT ... ASSIGNING FIELD-SYMBOL`
- `READ TABLE` with `BINARY SEARCH` on sorted tables for performance
- `DELETE ADJACENT DUPLICATES` after sorting

### Field Symbols and References
```abap
FIELD-SYMBOLS: <ls_order> TYPE zorders_s.
LOOP AT lt_orders ASSIGNING <ls_order>.
  <ls_order>-status = 'X'.  " Modifies table directly — no MODIFY needed
ENDLOOP.
```
- Field symbols avoid copying — use for large internal tables
- Data references (`REF TO`) enable dynamic programming

### Modularisation
- **Subroutines (FORM/ENDFORM)** — legacy; avoid in new code
- **Function Modules** — callable across program boundaries, RFC-enabled
- **Methods (CLASS/INTERFACE)** — preferred in modern ABAP OO
- **Macros** — code substitution at compile time; avoid (not debuggable)

### SELECT Performance
```abap
" Bad — N+1 queries
LOOP AT lt_orders INTO ls_order.
  SELECT SINGLE * FROM zitems WHERE order_id = ls_order-order_id INTO ls_item.
ENDLOOP.

" Good — single JOIN or FOR ALL ENTRIES
SELECT o~order_id i~item_id i~qty
  FROM zorders AS o
  JOIN zitems  AS i ON o~order_id = i~order_id
  INTO TABLE @lt_result.
```

## Best Practices
- Declare variables with `DATA` at the top of each scope
- Use `@` prefix for host variables in Open SQL (ABAP 7.40+)
- Prefer ABAP OO (classes/interfaces) over procedural for new development
- Use `TRY...CATCH` for exception handling instead of `SY-SUBRC` checks
- Activate only objects you have changed — avoid mass activation

## Common Pitfalls
- Selecting `*` when only a few fields are needed (performance)
- Using `SELECT` inside loops (N+1 problem)
- Not checking `SY-SUBRC` after database operations
- Hardcoding client `000` or `MANDT` in WHERE clauses

## Tools
- **SE80 / ADT (Eclipse)** — ABAP development workbench
- **ST05** — SQL trace for query analysis
- **SAT (Runtime Analysis)** — program performance profiling
- **SCI (Code Inspector)** — automated code quality checks
