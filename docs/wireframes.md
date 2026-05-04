# bird-bird UI Wireframes

These are text wireframes for the current frontend in [frontend/index.html](../frontend/index.html). The goal is to capture structure and flow, not final visual styling.

## Best Place To View These

The wireframes live here in `docs/` so they stay versioned with the rest of the project notes. Use this file as the canonical source, and link to it from the README.

If you want a more visual version later, the best follow-up is a vector image in `docs/` so it can be inserted into Microsoft Word directly. For now, the SVG below is the best image-format companion to this Markdown file.

## Visual Wireframe

Open [docs/wireframes.svg](wireframes.svg) for a diagram-style version of the same layout.

## Screen 1: Default State

When the page loads, the user has not selected a species yet.

```text
--------------------------------------------------------------
| Bird Migration Explorer                                     |
| Track seasonal movement patterns with weekly data.          |
|                                                            |
| [Species dropdown: Loading species...]   API status         |
--------------------------------------------------------------
| Timeline slider: disabled                                   |
| Waiting for weekly data...                                  |
--------------------------------------------------------------
| Migration Map                     | Weekly Trend Chart      |
| empty / placeholder map           | empty chart placeholder |
|                                   | Waiting for selection   |
--------------------------------------------------------------
| Collector Cards (Internal Preview)                          |
| Waiting for species selection...                            |
| empty collectible grid                                      |
--------------------------------------------------------------
```

## Screen 2: Species Selected

After the user chooses a species, the map, chart, and collectible cards all refresh from the API.

```text
--------------------------------------------------------------
| Bird Migration Explorer                                     |
| Choose a species and inspect route history by week.         |
|                                                            |
| [Species dropdown: selected bird]   Loaded / error status   |
--------------------------------------------------------------
| Timeline slider: active                                     |
| Week N of M (up to date label)                              |
--------------------------------------------------------------
| Migration Map                     | Weekly Trend Chart      |
| route polyline + stop markers     | yearly trend lines      |
| legend in corner                  | chart status line       |
--------------------------------------------------------------
| Collector Cards (Internal Preview)                          |
| 3-column cards on desktop, stacked on mobile                |
| tier, species, observations, dates, deployment id          |
--------------------------------------------------------------
```

## Screen 3: Mobile Layout

On smaller screens, the controls should stack vertically, then the map, chart, and cards should each become full-width sections.

```text
--------------------------------------------------------------
| Bird Migration Explorer                                     |
| Subtitle                                                    |
--------------------------------------------------------------
| Species dropdown                                            |
| API status                                                  |
--------------------------------------------------------------
| Timeline slider                                             |
| Timeline label                                              |
--------------------------------------------------------------
| Migration Map                                               |
--------------------------------------------------------------
| Weekly Trend Chart                                          |
--------------------------------------------------------------
| Collector Cards                                             |
--------------------------------------------------------------
```

## What The Current UI Already Does Well

- The page has a clear single-task flow: select a species, then inspect route, trend, and collectible views.
- The layout already supports responsive stacking.
- The status lines make the data loading state understandable.

## Small Gaps Worth Considering Later

- Add a dedicated empty-state illustration or helper text for the map and chart placeholders.
- Add a one-line explanation of the split control so users understand why the route mix changes.
- Consider a compact "How to read this page" block if non-technical users are the main audience.

## Suggested Viewing Options

1. Start with this Markdown file for discussion and review.
2. If you want visual sign-off, add a simple `docs/wireframes.html` next.
3. If the wireframe should become product-facing documentation, link it from the README and keep the detailed spec here.
4. If you need an image for Microsoft Word, use [docs/wireframes.svg](wireframes.svg).