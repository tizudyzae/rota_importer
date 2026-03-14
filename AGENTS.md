# AGENTS.md

## Add-on versioning

When making any change that affects the Home Assistant add-on code, behaviour, UI, dependencies, build output, or configuration, always bump the version in `config.yaml`.

Rules:
- Update `config.yaml` in the same task as the code change
- Increment patch version only
- Example: `1.2.3` becomes `1.2.4`
- Do not skip this step
- Do not ask for confirmation