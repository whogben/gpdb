# SVG Icon Guidelines

This document provides guidelines for creating SVG icons for schema visualization in the graph viewer.

## Requirements

### Size Limit
- Maximum file size: 20KB
- Recommended size: 1-5KB for optimal performance

### Dimensions
- Recommended viewBox: `0 0 24 24` or `0 0 32 32`
- Icons will be displayed at 32x32px in the graph viewer (node schemas on the node; edge schemas as a midpoint badge — Cytoscape has no `background-image` on edges)
- Icons should scale well to different sizes
- Loose viewBoxes (lots of empty space) or root `x`/`y` on `<svg>` are normalized in the admin UI via the `svg_icon_slot` macro and `svg-icon-slot.js` (`getBBox` tightening). The graph viewer runs the same `fitSvgMarkupToContent` step in `graph-viewer.js` before rasterizing to PNG so icons center like the schema pages. It then follows [Cytoscape.js guidance](https://js.cytoscape.org/#style/background-image): percent-encoded `data:image/svg+xml;charset=utf-8,...` (not base64) from the server, with explicit root `width`/`height` derived from `viewBox` when needed. If your icon omits `xmlns`, the server adds `xmlns="http://www.w3.org/2000/svg"` when building that data URI so WebKit can decode it in `<img>` / `Image()` (inline HTML SVG does not need this).

### Security

All SVGs are automatically sanitized before storage. The following are **not allowed**:

- JavaScript code (`<script>` tags)
- Event handlers (`onclick`, `onload`, `onmouseover`, etc.)
- External references (`<use href="external.svg">`)
- Data URIs in attributes
- CSS with `javascript:` protocol
- Unknown or dangerous elements

### Allowed Elements

Basic SVG elements:
- `svg` - Root element
- `path` - Custom shapes
- `circle` - Circles
- `rect` - Rectangles
- `ellipse` - Ellipses
- `line` - Lines
- `polyline` - Polylines
- `polygon` - Polygons
- `text` - Text elements
- `g` - Groups
- `defs` - Definitions
- `use` - Reusable elements (internal only)
- `symbol` - Symbol definitions
- `marker` - Arrowheads and markers
- `clipPath` - Clipping paths
- `mask` - Masks
- `pattern` - Patterns
- `gradient` - Gradients
- `stop` - Gradient stops
- `linearGradient` - Linear gradients
- `radialGradient` - Radial gradients

### Allowed Attributes

Presentation attributes:
- `fill` - Fill color
- `stroke` - Stroke color
- `stroke-width` - Stroke width
- `stroke-linecap` - Line cap style
- `stroke-linejoin` - Line join style
- `stroke-dasharray` - Dash pattern
- `stroke-dashoffset` - Dash offset
- `fill-opacity` - Fill opacity
- `stroke-opacity` - Stroke opacity
- `opacity` - Overall opacity
- `transform` - Transformations
- `d` - Path data
- `cx`, `cy` - Circle center
- `r` - Circle radius
- `rx`, `ry` - Ellipse radii
- `x`, `y` - Position
- `width`, `height` - Size
- `points` - Polygon points
- `x1`, `y1`, `x2`, `y2` - Line coordinates
- `text-anchor` - Text alignment
- `font-family` - Font family
- `font-size` - Font size
- `font-weight` - Font weight
- `font-style` - Font style
- `text-decoration` - Text decoration
- `letter-spacing` - Letter spacing
- `word-spacing` - Word spacing
- `writing-mode` - Writing mode
- `direction` - Text direction
- `dominant-baseline` - Baseline alignment
- `alignment-baseline` - Alignment baseline
- `baseline-shift` - Baseline shift
- `id` - Element ID
- `class` - CSS class
- `style` - Inline styles
- `href` - Reference (internal only)

## Best Practices

### Design Principles
- **Keep it simple**: Complex icons are harder to read and maintain
- **Use consistent style**: All icons should follow the same visual language
- **Optimize for small sizes**: Icons will be displayed at 32x32px
- **Consider context**: Icons should be recognizable in the graph viewer context
- **Test at scale**: Verify icons look good at different sizes

### Color Considerations
- **Theme support**: Icons should work in both light and dark themes
- **Use currentColor**: Where possible, use `fill="currentColor"` to inherit text color
- **Avoid hard colors**: Hard-coded colors may not match the theme
- **Test contrast**: Ensure icons are visible against different backgrounds

### Performance
- **Minimize paths**: Fewer path elements = better performance
- **Avoid filters**: Filters can impact rendering performance
- **Use simple shapes**: Basic shapes render faster than complex paths
- **Optimize SVG**: Remove unnecessary metadata and comments

### Accessibility
- **Use meaningful shapes**: Icons should represent their purpose
- **Consider color blindness**: Don't rely solely on color
- **Provide context**: Use aliases to supplement icons
- **Test with users**: Verify icons are understood by your audience

## Examples

### Simple Circle Icon
```xml
<svg viewBox="0 0 24 24" fill="currentColor">
  <circle cx="12" cy="12" r="10"/>
</svg>
```

### User Icon
```xml
<svg viewBox="0 0 24 24" fill="currentColor">
  <circle cx="12" cy="8" r="4"/>
  <path d="M12 14c-6.1 0-8 4-8 4v2h16v-2s-1.9-4-8-4z"/>
</svg>
```

### Document Icon
```xml
<svg viewBox="0 0 24 24" fill="currentColor">
  <path d="M14 2H6c-1.1 0-1.99.9-1.99 2L4 20c0 1.1.89 2 1.99 2H18c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z"/>
</svg>
```

### Arrow Icon (for edges)
```xml
<svg viewBox="0 0 24 24" fill="currentColor">
  <path d="M12 4l-1.41 1.41L16.17 11H4v2h12.17l-5.58 5.59L12 20l8-8z"/>
</svg>
```

## Testing

Before using an SVG icon in production:

1. **Validate size**: Ensure the SVG is under 20KB
2. **Test sanitization**: Verify the SVG passes sanitization
3. **Check rendering**: Test the icon in the graph viewer
4. **Verify themes**: Test in both light and dark themes
5. **Test at scale**: Verify the icon looks good at 32x32px
6. **Check accessibility**: Ensure the icon is recognizable

## Troubleshooting

### SVG Not Displaying
- Check that the SVG is valid XML
- Verify the SVG is under 20KB
- Ensure no JavaScript or event handlers are present
- Check that allowed elements and attributes are used

### Icon Looks Distorted
- Verify the viewBox is set correctly
- Check that the icon scales well
- Ensure the icon is designed for 32x32px display

### Colors Don't Match Theme
- Use `fill="currentColor"` instead of hard-coded colors
- Test in both light and dark themes
- Avoid using specific color values

### Performance Issues
- Simplify complex paths
- Remove unnecessary elements
- Optimize the SVG file size
- Consider using simpler shapes

## Resources

- [SVG Specification](https://www.w3.org/TR/SVG/)
- [SVG Accessibility](https://www.w3.org/TR/SVG-access/)
- [Icon Design Guidelines](https://material.io/design/iconography/system-icons.html)
