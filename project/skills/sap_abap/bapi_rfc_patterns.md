# Skill: BAPI & RFC Patterns

## Overview
Calling and building SAP BAPIs and Remote Function Modules for integration and inter-system communication.

## Key Patterns

### RFC Types
| Type | Description |
|---|---|
| sRFC (Synchronous) | Waits for response; like a normal function call |
| aRFC (Asynchronous) | Fire-and-forget; no return values |
| tRFC (Transactional) | Exactly-once delivery; used for IDocs and BAPIs |
| bgRFC | Modern replacement for tRFC with better error handling |
| qRFC (Queued) | tRFC with ordering guarantee within a queue |

### Calling a BAPI
```abap
DATA: lt_return TYPE TABLE OF bapiret2,
      ls_header TYPE bapisdhead.

CALL FUNCTION 'BAPI_SALESORDER_CREATEFROMDAT2'
  EXPORTING order_header_in = ls_header
  TABLES    return          = lt_return.

" Check for errors
LOOP AT lt_return INTO DATA(ls_ret)
  WHERE type CA 'EA'.
  " Handle error
ENDLOOP.

" Commit — BAPIs do NOT auto-commit
CALL FUNCTION 'BAPI_TRANSACTION_COMMIT'
  EXPORTING wait = abap_true.
```

### Building a Remote-Enabled Function Module
- Set `Processing Type = Remote-Enabled Module` in SE37
- All EXPORTING/IMPORTING parameters must be pass-by-value
- Use only flat structures or tables with flat line types (no nested objects via RFC)
- Return `BAPIRET2` table for standardised error/success messages

### RFC Destination Configuration
- `SM59` — define RFC destinations (R/3, HTTP, TCP/IP)
- Test connections from SM59 before coding
- Use logical destinations to decouple host/port from code

### Error Handling
- Always check `RETURN` / `BAPIRET2` table after BAPI calls
- `TYPE = 'E'` (Error) or `TYPE = 'A'` (Abort) = failure
- `TYPE = 'S'` (Success) or `TYPE = 'W'` (Warning) = proceed
- Never assume success — always inspect the return table

## Best Practices
- Wrap BAPI calls in a dedicated ABAP class method for testability
- Use tRFC/bgRFC for BAPIs when exactly-once semantics are required
- Always call `BAPI_TRANSACTION_COMMIT` (with `WAIT = 'X'`) or `ROLLBACK_WORK` explicitly
- Document RFC interface versions — breaking changes require a new function group
- Use `CALL FUNCTION ... DESTINATION` for cross-system calls; handle `COMMUNICATION FAILURE`

## Common Pitfalls
- Forgetting to commit after BAPI — data never persisted
- Assuming BAPI success without checking return table
- Passing nested/deep structures over RFC (not supported in classic RFC)
- Not handling `SYSTEM_FAILURE` and `COMMUNICATION_FAILURE` exceptions

## Tools
- **SE37** — Function Module development
- **SM59** — RFC destination management
- **BAPI Explorer (transaction BAPI)** — browse all standard BAPIs
- **SRT_UTIL** — Web Services / SOAP RFC testing
