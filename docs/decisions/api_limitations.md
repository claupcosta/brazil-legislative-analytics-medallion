# API Limitations

## Overview

This document registers technical limitations identified during ingestion tests
using the Câmara dos Deputados Open Data API.

The objective is to preserve:
- transparency
- auditability
- architectural traceability
- engineering decision history

---

# Parliamentary Front Members Endpoint

## Endpoint Tested

```text
/frentes/{id}/membros
```

---

# Identified Behavior

During Bronze ingestion validation tests, the Câmara dos Deputados Open Data API
returned HTTP 400 responses for multiple parliamentary front identifiers.

The issue was reproduced consistently during controlled execution tests,
indicating instability or operational limitation in the provider endpoint.

---

# Technical Impact

Because of this behavior:

- multiple parliamentary fronts returned no valid member data;
- ingestion reliability could not be guaranteed;
- the pipeline execution time increased unnecessarily;
- the resulting dataset could become incomplete or inconsistent.

For this reason, the endpoint was not considered operationally reliable
for mandatory ingestion processing.

---

# Engineering Decision

The project adopted the following strategy:

| Decision | Description |
|---|---|
| Preserve notebook | The notebook was maintained as technical evidence and validation reference |
| Avoid global pipeline failure | HTTP 400 responses do not interrupt the full pipeline execution |
| Register warning | The execution is registered as `WARNING` in audit logs |
| Preserve Bronze structure | The Bronze table structure remains available for future compatibility |
| Document limitation | The behavior is formally documented for governance and transparency |
| Future replacement strategy | The endpoint may be replaced by `/frentes/{id}` in future iterations |

---

# Governance and Auditability

The limitation is registered through:

- audit pipeline logs;
- execution warnings;
- technical documentation;
- controlled validation tests.

This approach preserves:
- pipeline resilience;
- traceability;
- operational stability;
- engineering transparency.

---

# Conclusion

The issue was identified as a source-system limitation rather than an internal pipeline failure.

The project intentionally prioritizes:
- resilience;
- auditability;
- governance;
- operational reliability;
- transparent technical documentation.