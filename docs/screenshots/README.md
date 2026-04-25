# Screenshots

Source images for the README. Capture from a real browser for best
visual quality (anti-aliasing, font hinting). Re-take whenever the UI
changes meaningfully.

## How to regenerate

```bash
dbt-features demo
# → opens your browser at http://127.0.0.1:8080
```

Then capture each of the four pages below at a **1280×800** viewport
(the README's max content width). Save as PNG into this directory.

| File                    | Path                                                                               |
|-------------------------|------------------------------------------------------------------------------------|
| `01-index.png`          | `/`                                                                                |
| `02-feature-group.png`  | `/groups/customer-features-daily/index.html`                                       |
| `03-feature.png`        | `/groups/customer-features-daily/features/orders-count-7d.html`                    |
| `04-lineage.png`        | `/lineage.html`                                                                    |

Dark mode is the default — these are the canonical shots. If you want
light-mode variants too, suffix with `-light` (e.g. `01-index-light.png`).
