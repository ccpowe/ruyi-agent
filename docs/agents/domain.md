# Domain docs

This is a single-context repository.

## Before exploring

- Read `CONTEXT.md` at the repository root for the domain language.
- Read relevant decisions under `docs/adr/` when that directory exists.
- If either source is absent, proceed without creating placeholder documents.

## Consumer rules

- Use the terms defined in `CONTEXT.md` in implementation plans, issues, tests,
  and architecture discussions.
- Do not replace glossary terms with synonyms that the glossary marks as
  avoided.
- If a needed concept is missing, first determine whether it is genuinely new
  domain language before adding it.
- Surface any conflict with an existing ADR explicitly rather than silently
  overriding the recorded decision.
