# Changelog

<!-- <START NEW CHANGELOG ENTRY> -->

## 0.2.0

([Full Changelog](https://github.com/QuantStack/Arbalister/compare/v0.1.0...d74ac8f76297b5507d509ced3fe5af3149333adb))

### New features added

- feat(client): Add data type in table header [#44](https://github.com/QuantStack/Arbalister/pull/44) ([@AntoinePrv](https://github.com/AntoinePrv))
- feat: Increase prefetching size [#42](https://github.com/QuantStack/Arbalister/pull/42) ([@AntoinePrv](https://github.com/AntoinePrv), [@claude](https://github.com/claude))
- feat(client): Handle http errors and toolbars errors [#37](https://github.com/QuantStack/Arbalister/pull/37) ([@AntoinePrv](https://github.com/AntoinePrv), [@claude](https://github.com/claude))
- feat: File info to return default file options [#34](https://github.com/QuantStack/Arbalister/pull/34) ([@AntoinePrv](https://github.com/AntoinePrv))
- feat(server): Add file info route [#33](https://github.com/QuantStack/Arbalister/pull/33) ([@AntoinePrv](https://github.com/AntoinePrv))
- feat: Add Sqlite toolbar [#30](https://github.com/QuantStack/Arbalister/pull/30) ([@AntoinePrv](https://github.com/AntoinePrv), [@claude](https://github.com/claude))
- perf: Merge schema and stats requests [#29](https://github.com/QuantStack/Arbalister/pull/29) ([@AntoinePrv](https://github.com/AntoinePrv), [@claude](https://github.com/claude))
- feat: Add mouse handling via selection model [#28](https://github.com/QuantStack/Arbalister/pull/28) ([@AntoinePrv](https://github.com/AntoinePrv))
- feat(server): Support setting CSV delimiter read option [#25](https://github.com/QuantStack/Arbalister/pull/25) ([@AnastasiaSliusar](https://github.com/AnastasiaSliusar), [@AntoinePrv](https://github.com/AntoinePrv))
- feat(client): Add Csv toolbar with delimiter option [#24](https://github.com/QuantStack/Arbalister/pull/24) ([@AntoinePrv](https://github.com/AntoinePrv))
- feat(client): Add error handling [#23](https://github.com/QuantStack/Arbalister/pull/23) ([@AnastasiaSliusar](https://github.com/AnastasiaSliusar), [@AntoinePrv](https://github.com/AntoinePrv))
- feat(client): Add SQLite file extension registration [#22](https://github.com/QuantStack/Arbalister/pull/22) ([@AntoinePrv](https://github.com/AntoinePrv), [@Copilot](https://github.com/Copilot))
- feat(server): Add support for Sqlite files in the server [#21](https://github.com/QuantStack/Arbalister/pull/21) ([@AntoinePrv](https://github.com/AntoinePrv))
- feat(client): Add data prefetching [#17](https://github.com/QuantStack/Arbalister/pull/17) ([@AntoinePrv](https://github.com/AntoinePrv))
- feat: Lazy load columns [#9](https://github.com/QuantStack/Arbalister/pull/9) ([@AntoinePrv](https://github.com/AntoinePrv))
- feat(client): Add file type icons [#8](https://github.com/QuantStack/Arbalister/pull/8) ([@AnastasiaSliusar](https://github.com/AnastasiaSliusar), [@AntoinePrv](https://github.com/AntoinePrv))

### Bugs fixed

- fix(client): Fix row chunk default size [#43](https://github.com/QuantStack/Arbalister/pull/43) ([@AntoinePrv](https://github.com/AntoinePrv))
- fix(client): Fix bounds on call to model emitChange [#40](https://github.com/QuantStack/Arbalister/pull/40) ([@AntoinePrv](https://github.com/AntoinePrv))
- fix(client): Avoid duplicate icon registration [#32](https://github.com/QuantStack/Arbalister/pull/32) ([@AntoinePrv](https://github.com/AntoinePrv))

### Maintenance and upkeep improvements

- ci: Change jupyter releaser commit message [#49](https://github.com/QuantStack/Arbalister/pull/49) ([@AntoinePrv](https://github.com/AntoinePrv))
- ci: Automatically label PR from conventional commit [#48](https://github.com/QuantStack/Arbalister/pull/48) ([@AntoinePrv](https://github.com/AntoinePrv), [@claude](https://github.com/claude))
- ci: Add npm trusted publishing [#47](https://github.com/QuantStack/Arbalister/pull/47) ([@AntoinePrv](https://github.com/AntoinePrv))
- dev: Add pixi tasks for ui tests [#46](https://github.com/QuantStack/Arbalister/pull/46) ([@AntoinePrv](https://github.com/AntoinePrv))
- test(client): Improve ArrowModel tests [#45](https://github.com/QuantStack/Arbalister/pull/45) ([@AntoinePrv](https://github.com/AntoinePrv))
- chore(client): Refactor DataModel with stronger types [#41](https://github.com/QuantStack/Arbalister/pull/41) ([@AntoinePrv](https://github.com/AntoinePrv))
- dev: Add task to download sample data [#36](https://github.com/QuantStack/Arbalister/pull/36) ([@AntoinePrv](https://github.com/AntoinePrv))
- chore: Refactor option to read options [#35](https://github.com/QuantStack/Arbalister/pull/35) ([@AntoinePrv](https://github.com/AntoinePrv))
- dev: Improve dev install [#31](https://github.com/QuantStack/Arbalister/pull/31) ([@AntoinePrv](https://github.com/AntoinePrv))
- dev: Add pixi run check-typescript [#26](https://github.com/QuantStack/Arbalister/pull/26) ([@AntoinePrv](https://github.com/AntoinePrv))
- dev: Add .gitattributes [#19](https://github.com/QuantStack/Arbalister/pull/19) ([@AntoinePrv](https://github.com/AntoinePrv))
- test(client): Add basic unit tests for client code [#18](https://github.com/QuantStack/Arbalister/pull/18) ([@AntoinePrv](https://github.com/AntoinePrv))
- ci: PR messages must start with a capital letter [#11](https://github.com/QuantStack/Arbalister/pull/11) ([@AntoinePrv](https://github.com/AntoinePrv))

### Documentation improvements

- doc: Add homepage URL [#6](https://github.com/QuantStack/Arbalister/pull/6) ([@AntoinePrv](https://github.com/AntoinePrv))

### Contributors to this release

The following people contributed discussions, new ideas, code and documentation contributions, and review.
See [our definition of contributors](https://github-activity.readthedocs.io/en/latest/#how-does-this-tool-define-contributions-in-the-reports).

([GitHub contributors page for this release](https://github.com/QuantStack/Arbalister/graphs/contributors?from=2025-12-10&to=2025-12-31&type=c))

@AnastasiaSliusar ([activity](https://github.com/search?q=repo%3AQuantStack%2FArbalister+involves%3AAnastasiaSliusar+updated%3A2025-12-10..2025-12-31&type=Issues)) | @AntoinePrv ([activity](https://github.com/search?q=repo%3AQuantStack%2FArbalister+involves%3AAntoinePrv+updated%3A2025-12-10..2025-12-31&type=Issues)) | @claude ([activity](https://github.com/search?q=repo%3AQuantStack%2FArbalister+involves%3Aclaude+updated%3A2025-12-10..2025-12-31&type=Issues)) | @Copilot ([activity](https://github.com/search?q=repo%3AQuantStack%2FArbalister+involves%3ACopilot+updated%3A2025-12-10..2025-12-31&type=Issues))

<!-- <END NEW CHANGELOG ENTRY> -->
