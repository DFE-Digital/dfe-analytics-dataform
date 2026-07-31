## Context

<!-- What problem does this solve, or what value does it add to the dfe-analytics-dataform or its consumers? -->

## What changed?

<!-- Briefly summarise the changes. Include any affected helpers, macros, assertions, configuration, dependencies, or generated outputs. -->

## Type of change

<!-- Select all that apply -->

* [ ] New package feature <!-- Adds new reusable Dataform functionality, such as a helper, include, assertion pattern, config option, or action generation utility. -->
* [ ] Change to existing package feature <!-- Changes behaviour, parameters, defaults, generated SQL/actions, or assumptions of existing package functionality. -->
* [ ] Bug fix <!-- Corrects package behaviour that was producing incorrect, incomplete, duplicated, unexpected, or inconsistent results. -->
* [ ] Refactor / performance <!-- Improves code structure, maintainability, compilation, runtime, cost, or query efficiency without intentionally changing behaviour. -->
* [ ] Documentation / metadata <!-- Adds or updates README content, usage examples, comments, metadata conventions, tags, or migration notes. -->
* [ ] Tests / validation <!-- Adds or updates tests, fixtures, assertions, validation logic, regression checks, or safeguards. -->
* [ ] Configuration / dependencies <!-- Updates Dataform config, package setup, workflow settings, npm dependencies, or shared package dependencies. -->
* [ ] Breaking change <!-- Changes package contracts in a way that may require consuming repos to update. -->
* [ ] Other: <!-- Use this for changes that do not fit the categories above. -->

## Downstream impact

<!-- Describe the expected impact on Dataform repos that consume this package. -->

* [ ] No expected downstream changes.
* [ ] Consuming repos may need to update package version.
* [ ] Consuming repos may need to update Dataform configuration.
* [ ] Consuming repos may need to update helper/include usage.
* [ ] Consuming repos may see changes in compiled SQL or generated actions.
* [ ] Consuming repos may see changes in materialised outputs.
* [ ] Migration guidance is included below.

### Migration guidance

<!-- Required for breaking changes or behaviour changes. Explain what consuming repos need to do, if anything. -->

## Notes for reviewers

<!-- Anything specific you would like reviewers to focus on, or any known trade-offs, risks, compatibility concerns, or release considerations. -->

## Reviewers Checklist

* [ ] The change belongs in the shared Dataform package rather than a consuming repo.
* [ ] The package interface is clear, reusable, and consistent with existing conventions.
* [ ] Generated SQL/actions, dependencies, tags, configs, assertions, or declarations are correct where relevant.
* [ ] Downstream impact has been considered and documented.
* [ ] Breaking changes are clearly flagged, with migration guidance where needed.
* [ ] Code is understandable and avoids unnecessary complexity or duplication.
* [ ] Relevant tests, fixtures, assertions, or regression checks are in place.
* [ ] Changes have been validated through Dataform compilation.
* [ ] README, usage examples, metadata, or comments are updated where relevant.
* [ ] Important assumptions, caveats, or non-obvious behaviours are documented.
