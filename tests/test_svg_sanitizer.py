"""
Tests for SVG sanitization utilities.
"""

from urllib.parse import unquote

import pytest

from gpdb.svg_sanitizer import (
    normalize_svg_icon_for_display,
    sanitize_svg,
    svg_markup_to_cytoscape_data_uri,
)


class TestSanitizeSVG:
    """Test cases for the sanitize_svg function."""

    def test_valid_svg_passes_through_unchanged(self):
        """Test that a valid SVG is sanitized and normalized for icon embedding."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100"><circle cx="50" cy="50" r="40" fill="red"/></svg>'
        result = sanitize_svg(svg)
        assert "<svg" in result
        assert 'viewBox="0 0 100 100"' in result
        assert 'preserveAspectRatio="xMidYMid meet"' in result
        assert "<circle" in result
        assert "cx=" in result
        assert "cy=" in result
        assert "r=" in result
        assert "fill=" in result

    def test_svg_markup_to_cytoscape_data_uri_percent_encoded(self):
        """Viewer payload uses percent-encoded UTF-8 data URIs (Cytoscape.js recommendation)."""
        uri = svg_markup_to_cytoscape_data_uri('<svg xmlns="http://www.w3.org/2000/svg"/>')
        assert uri is not None
        assert uri.startswith("data:image/svg+xml;charset=utf-8,")
        raw = unquote(uri.split(",", 1)[1])
        assert raw.lstrip().startswith("<svg")
        assert "<!DOCTYPE" not in raw

    def test_svg_markup_to_cytoscape_data_uri_adds_xmlns_for_bare_svg(self):
        """Bare <svg> (no xmlns) must get SVG namespace or WebKit img/Image rejects the data URL."""
        uri = svg_markup_to_cytoscape_data_uri('<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/></svg>')
        assert uri is not None
        raw = unquote(uri.split(",", 1)[1])
        assert 'xmlns="http://www.w3.org/2000/svg"' in raw

    def test_normalize_svg_icon_for_display_fixes_missing_viewbox(self):
        """Read-path helper: legacy stored SVG without viewBox still scales in UI."""
        legacy = '<svg width="800px" height="800px"><path d="M0 0 L10 10"/></svg>'
        out = normalize_svg_icon_for_display(legacy)
        assert 'viewBox="0 0 800 800"' in out
        assert 'preserveAspectRatio="xMidYMid meet"' in out

    def test_normalize_svg_icon_for_display_strips_root_x_y(self):
        """Root x/y shifts the viewport inside fixed slots; strip on read path."""
        skewed = (
            '<svg x="40" y="60" viewBox="0 0 100 100">'
            '<circle cx="50" cy="50" r="40"/></svg>'
        )
        out = normalize_svg_icon_for_display(skewed)
        assert 'x="40"' not in out
        assert 'y="60"' not in out
        assert "<circle" in out

    def test_svg_without_viewbox_gets_viewbox_from_dimensions(self):
        """Large pixel width/height without viewBox must scale in small UI slots."""
        svg = (
            '<svg xmlns="http://www.w3.org/2000/svg" width="800px" height="800px">'
            '<path d="M0 0 L800 800"/></svg>'
        )
        result = sanitize_svg(svg)
        assert 'viewBox="0 0 800 800"' in result
        assert 'preserveAspectRatio="xMidYMid meet"' in result
        assert 'width="800' not in result
        assert "<path" in result

    def test_svg_with_script_tags_removed(self):
        """Test that SVG with script tags has them removed."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><script>alert("xss")</script><circle cx="50" cy="50" r="40"/></svg>'
        result = sanitize_svg(svg)
        assert "<script>" not in result
        assert "</script>" not in result
        assert "<circle" in result

    def test_svg_with_onclick_handler_removed(self):
        """Test that SVG with onclick event handler has it removed."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40" onclick="alert(\'xss\')"/></svg>'
        result = sanitize_svg(svg)
        assert "onclick" not in result
        assert "<circle" in result

    def test_svg_with_onload_handler_removed(self):
        """Test that SVG with onload event handler has it removed."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg" onload="alert(\'xss\')"><circle cx="50" cy="50" r="40"/></svg>'
        result = sanitize_svg(svg)
        assert "onload" not in result
        assert "<circle" in result

    def test_svg_over_size_limit_raises_value_error(self):
        """Test that SVG exceeding size limit raises ValueError."""
        # Create an SVG that's larger than 20KB
        large_path = "M" + ",".join([f"{i},{i}" for i in range(10000)])
        svg = f'<svg xmlns="http://www.w3.org/2000/svg"><path d="{large_path}"/></svg>'
        
        with pytest.raises(ValueError, match="exceeds maximum size"):
            sanitize_svg(svg, max_size_kb=1)

    def test_malformed_xml_is_sanitized_gracefully(self):
        """Test that malformed XML is sanitized gracefully by bleach."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40" mismatched="value"'
        # bleach strips malformed elements and unknown attributes
        result = sanitize_svg(svg)
        assert "<svg" in result
        # The malformed circle element is stripped by bleach
        assert "mismatched" not in result

    def test_empty_svg_raises_value_error(self):
        """Test that empty SVG raises ValueError (not valid XML)."""
        svg = ""
        with pytest.raises(ValueError, match="not valid XML"):
            sanitize_svg(svg)

    def test_svg_with_dangerous_attributes_removed(self):
        """Test that SVG with dangerous attributes has them removed."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40" onmouseover="alert(\'xss\')" onerror="alert(\'xss\')"/></svg>'
        result = sanitize_svg(svg)
        assert "onmouseover" not in result
        assert "onerror" not in result
        assert "<circle" in result

    def test_svg_with_only_safe_elements_passes_through(self):
        """Test that SVG with only safe elements and attributes passes through."""
        svg = '''<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100">
            <defs>
                <linearGradient id="grad1" x1="0%" y1="0%" x2="100%" y2="0%">
                    <stop offset="0%" style="stop-color:rgb(255,255,0);stop-opacity:1" />
                    <stop offset="100%" style="stop-color:rgb(255,0,0);stop-opacity:1" />
                </linearGradient>
            </defs>
            <circle cx="50" cy="50" r="40" fill="url(#grad1)" stroke="black" stroke-width="2"/>
            <rect x="10" y="10" width="30" height="30" fill="blue" opacity="0.5"/>
        </svg>'''
        result = sanitize_svg(svg)
        assert "<svg" in result
        assert "<defs>" in result
        assert "<linearGradient" in result
        assert "<stop" in result
        assert "<circle" in result
        assert "<rect" in result

    def test_none_input_raises_value_error(self):
        """Test that None input raises ValueError."""
        with pytest.raises(ValueError, match="cannot be None"):
            sanitize_svg(None)

    def test_svg_with_text_element_passes_through(self):
        """Test that SVG with text element passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><text x="10" y="20" font-size="14">Hello</text></svg>'
        result = sanitize_svg(svg)
        assert "<text" in result
        assert "Hello" in result

    def test_svg_with_group_element_passes_through(self):
        """Test that SVG with group element passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><g transform="translate(10,10)"><circle cx="50" cy="50" r="40"/></g></svg>'
        result = sanitize_svg(svg)
        assert "<g" in result
        assert "transform" in result
        assert "<circle" in result

    def test_svg_with_path_element_passes_through(self):
        """Test that SVG with path element passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><path d="M10 10 L20 20 L30 10 Z" fill="green"/></svg>'
        result = sanitize_svg(svg)
        assert "<path" in result
        assert 'd="M10 10 L20 20 L30 10 Z"' in result

    def test_svg_with_polygon_element_passes_through(self):
        """Test that SVG with polygon element passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><polygon points="10,10 20,20 30,10" fill="blue"/></svg>'
        result = sanitize_svg(svg)
        assert "<polygon" in result
        assert "points=" in result

    def test_svg_with_ellipse_element_passes_through(self):
        """Test that SVG with ellipse element passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><ellipse cx="50" cy="50" rx="40" ry="20" fill="purple"/></svg>'
        result = sanitize_svg(svg)
        assert "<ellipse" in result
        assert "cx=" in result
        assert "cy=" in result
        assert "rx=" in result
        assert "ry=" in result

    def test_svg_with_line_element_passes_through(self):
        """Test that SVG with line element passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><line x1="10" y1="10" x2="100" y2="100" stroke="black" stroke-width="2"/></svg>'
        result = sanitize_svg(svg)
        assert "<line" in result
        assert "x1=" in result
        assert "y1=" in result
        assert "x2=" in result
        assert "y2=" in result

    def test_svg_with_polyline_element_passes_through(self):
        """Test that SVG with polyline element passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><polyline points="10,10 20,20 30,10 40,20" fill="none" stroke="red"/></svg>'
        result = sanitize_svg(svg)
        assert "<polyline" in result
        assert "points=" in result

    def test_svg_with_custom_max_size(self):
        """Test that custom max_size_kb parameter works correctly."""
        # Small SVG should pass with 1KB limit
        small_svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40"/></svg>'
        result = sanitize_svg(small_svg, max_size_kb=1)
        assert "<circle" in result

    def test_svg_with_style_attribute_preserved(self):
        """Test that style attribute is preserved."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40" style="fill: red; stroke: blue;"/></svg>'
        result = sanitize_svg(svg)
        assert "style=" in result
        assert "<circle" in result

    def test_svg_with_id_attribute_preserved(self):
        """Test that id attribute is preserved."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle id="my-circle" cx="50" cy="50" r="40"/></svg>'
        result = sanitize_svg(svg)
        assert 'id="my-circle"' in result
        assert "<circle" in result

    def test_svg_with_class_attribute_preserved(self):
        """Test that class attribute is preserved."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle class="my-class" cx="50" cy="50" r="40"/></svg>'
        result = sanitize_svg(svg)
        assert 'class="my-class"' in result
        assert "<circle" in result

    def test_svg_with_use_element_passes_through(self):
        """Test that SVG with use element passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><defs><circle id="c" cx="50" cy="50" r="40"/></defs><use href="#c" x="10" y="10"/></svg>'
        result = sanitize_svg(svg)
        assert "<use" in result
        assert "href=" in result

    def test_svg_with_symbol_element_passes_through(self):
        """Test that SVG with symbol element passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><defs><symbol id="sym"><circle cx="50" cy="50" r="40"/></symbol></defs><use href="#sym"/></svg>'
        result = sanitize_svg(svg)
        assert "<symbol" in result
        assert "<use" in result

    def test_svg_with_marker_element_passes_through(self):
        """Test that SVG with marker element passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><defs><marker id="arrow" markerWidth="10" markerHeight="10" refX="0" refY="3" orient="auto"><path d="M0,0 L0,6 L9,3 z" fill="black"/></marker></defs><line x1="10" y1="10" x2="100" y2="100" stroke="black" marker-end="url(#arrow)"/></svg>'
        result = sanitize_svg(svg)
        assert "<marker" in result
        assert "<line" in result

    def test_svg_with_clipPath_element_passes_through(self):
        """Test that SVG with clipPath element passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><defs><clipPath id="clip"><circle cx="50" cy="50" r="40"/></clipPath></defs><rect x="0" y="0" width="100" height="100" fill="red" clip-path="url(#clip)"/></svg>'
        result = sanitize_svg(svg)
        assert "<clipPath" in result
        assert "<rect" in result

    def test_svg_with_mask_element_passes_through(self):
        """Test that SVG with mask element passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><defs><mask id="mask"><circle cx="50" cy="50" r="40" fill="white"/></mask></defs><rect x="0" y="0" width="100" height="100" fill="red" mask="url(#mask)"/></svg>'
        result = sanitize_svg(svg)
        assert "<mask" in result
        assert "<rect" in result

    def test_svg_with_pattern_element_passes_through(self):
        """Test that SVG with pattern element passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><defs><pattern id="pattern" width="10" height="10" patternUnits="userSpaceOnUse"><circle cx="5" cy="5" r="3" fill="blue"/></pattern></defs><rect x="0" y="0" width="100" height="100" fill="url(#pattern)"/></svg>'
        result = sanitize_svg(svg)
        assert "<pattern" in result
        assert "<rect" in result

    def test_svg_with_radialGradient_element_passes_through(self):
        """Test that SVG with radialGradient element passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><defs><radialGradient id="grad" cx="50%" cy="50%" r="50%"><stop offset="0%" style="stop-color:rgb(255,255,0);stop-opacity:1"/><stop offset="100%" style="stop-color:rgb(255,0,0);stop-opacity:1"/></radialGradient></defs><circle cx="50" cy="50" r="40" fill="url(#grad)"/></svg>'
        result = sanitize_svg(svg)
        assert "<radialGradient" in result
        assert "<circle" in result

    def test_svg_with_multiple_event_handlers_removed(self):
        """Test that SVG with multiple event handlers has them all removed."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40" onclick="alert(1)" ondblclick="alert(2)" onmousedown="alert(3)" onmouseup="alert(4)" onmouseover="alert(5)" onmouseout="alert(6)" onmousemove="alert(7)" onmouseenter="alert(8)" onmouseleave="alert(9)"/></svg>'
        result = sanitize_svg(svg)
        assert "onclick" not in result
        assert "ondblclick" not in result
        assert "onmousedown" not in result
        assert "onmouseup" not in result
        assert "onmouseover" not in result
        assert "onmouseout" not in result
        assert "onmousemove" not in result
        assert "onmouseenter" not in result
        assert "onmouseleave" not in result
        assert "<circle" in result

    def test_svg_with_javascript_protocol_removed(self):
        """Test that SVG with javascript: protocol in href is removed."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><a href="javascript:alert(\'xss\')"><circle cx="50" cy="50" r="40"/></a></svg>'
        result = sanitize_svg(svg)
        assert "javascript:" not in result
        # The <a> tag is not in allowed elements, so it should be removed
        assert "<a>" not in result
        assert "<circle" in result

    def test_svg_with_data_uri_preserved(self):
        """Test that SVG with data URI in href is preserved (if element is allowed)."""
        # Note: <a> is not in allowed elements, so this tests that it's removed
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><a href="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="><circle cx="50" cy="50" r="40"/></a></svg>'
        result = sanitize_svg(svg)
        assert "<a>" not in result
        assert "<circle" in result

    def test_svg_with_foreignObject_removed(self):
        """Test that SVG with foreignObject element has it removed (not in allowed list)."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><foreignObject x="10" y="10" width="100" height="100"><body xmlns="http://www.w3.org/1999/xhtml"><p>Text</p></body></foreignObject><circle cx="50" cy="50" r="40"/></svg>'
        result = sanitize_svg(svg)
        assert "<foreignObject" not in result
        assert "<body" not in result
        assert "<circle" in result

    def test_svg_with_animation_elements_removed(self):
        """Test that SVG with animation elements has them removed (not in allowed list)."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40"><animate attributeName="r" from="40" to="20" dur="1s"/></circle></svg>'
        result = sanitize_svg(svg)
        assert "<animate" not in result
        assert "<circle" in result

    def test_svg_with_filter_element_removed(self):
        """Test that SVG with filter element has it removed (not in allowed list)."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><defs><filter id="blur"><feGaussianBlur in="SourceGraphic" stdDeviation="5"/></filter></defs><circle cx="50" cy="50" r="40" filter="url(#blur)"/></svg>'
        result = sanitize_svg(svg)
        assert "<filter" not in result
        assert "<feGaussianBlur" not in result
        assert "<circle" in result

    def test_svg_with_unknown_element_removed(self):
        """Test that SVG with unknown element has it removed."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><unknownElement><circle cx="50" cy="50" r="40"/></unknownElement></svg>'
        result = sanitize_svg(svg)
        assert "<unknownElement" not in result
        assert "<circle" in result

    def test_svg_with_unknown_attribute_removed(self):
        """Test that SVG with unknown attribute has it removed."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40" unknown-attr="value"/></svg>'
        result = sanitize_svg(svg)
        assert "unknown-attr" not in result
        assert "<circle" in result

    def test_svg_with_nested_elements_passes_through(self):
        """Test that SVG with nested safe elements passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><g><g><circle cx="50" cy="50" r="40"/></g></g></svg>'
        result = sanitize_svg(svg)
        assert "<svg" in result
        assert "<g>" in result
        assert "<circle" in result

    def test_svg_with_multiple_circles_passes_through(self):
        """Test that SVG with multiple circles passes through."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40" fill="red"/><circle cx="100" cy="100" r="30" fill="blue"/></svg>'
        result = sanitize_svg(svg)
        assert result.count("<circle") == 2

    def test_svg_with_whitespace_preserved(self):
        """Test that SVG with whitespace is preserved."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg">\n  <circle cx="50" cy="50" r="40"/>\n</svg>'
        result = sanitize_svg(svg)
        assert "<circle" in result
        assert "\n" in result

    def test_svg_with_comments_removed(self):
        """Test that SVG with comments has them removed."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><!-- This is a comment --><circle cx="50" cy="50" r="40"/></svg>'
        result = sanitize_svg(svg)
        assert "<!--" not in result
        assert "<circle" in result

    def test_svg_with_cdata_removed(self):
        """Test that SVG with CDATA sections has them removed."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40"/><![CDATA[<script>alert("xss")</script>]]></svg>'
        result = sanitize_svg(svg)
        assert "<![CDATA[" not in result
        assert "<circle" in result

    def test_svg_with_entity_references_preserved(self):
        """Test that SVG with entity references is preserved."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><text x="10" y="20"><Hello></text></svg>'
        result = sanitize_svg(svg)
        assert "<" in result
        assert ">" in result
        assert "<text" in result

    def test_svg_with_unicode_characters_preserved(self):
        """Test that SVG with Unicode characters is preserved."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><text x="10" y="20">Hello 世界 🌍</text></svg>'
        result = sanitize_svg(svg)
        assert "世界" in result
        assert "🌍" in result
        assert "<text" in result

    def test_svg_with_mixed_case_tags_normalized(self):
        """Test that SVG with mixed case tags is normalized."""
        svg = '<SVG xmlns="http://www.w3.org/2000/svg"><CIRCLE cx="50" cy="50" r="40"/></SVG>'
        result = sanitize_svg(svg)
        # bleach normalizes to lowercase
        assert "<svg" in result
        assert "<circle" in result

    def test_svg_without_namespace_still_works(self):
        """Test that SVG without namespace still works."""
        svg = '<svg><circle cx="50" cy="50" r="40"/></svg>'
        result = sanitize_svg(svg)
        assert "<svg" in result
        assert "<circle" in result

    def test_svg_with_self_closing_tags_preserved(self):
        """Test that SVG with self-closing tags is preserved."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40"/><rect x="10" y="10" width="20" height="20"/></svg>'
        result = sanitize_svg(svg)
        assert "<circle" in result
        assert "<rect" in result

    def test_svg_with_transform_attribute_preserved(self):
        """Test that SVG with transform attribute is preserved."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40" transform="rotate(45 50 50)"/></svg>'
        result = sanitize_svg(svg)
        assert "transform=" in result
        assert "<circle" in result

    def test_svg_with_opacity_attributes_preserved(self):
        """Test that SVG with opacity attributes is preserved."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40" fill-opacity="0.5" stroke-opacity="0.7" opacity="0.8"/></svg>'
        result = sanitize_svg(svg)
        assert "fill-opacity=" in result
        assert "stroke-opacity=" in result
        assert "opacity=" in result
        assert "<circle" in result

    def test_svg_with_stroke_attributes_preserved(self):
        """Test that SVG with stroke attributes is preserved."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><line x1="10" y1="10" x2="100" y2="100" stroke="black" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="5,5" stroke-dashoffset="2"/></svg>'
        result = sanitize_svg(svg)
        assert "stroke=" in result
        assert "stroke-width=" in result
        assert "stroke-linecap=" in result
        assert "stroke-linejoin=" in result
        assert "stroke-dasharray=" in result
        assert "stroke-dashoffset=" in result
        assert "<line" in result

    def test_svg_with_text_attributes_preserved(self):
        """Test that SVG with text attributes is preserved."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><text x="10" y="20" text-anchor="middle" font-family="Arial" font-size="14" font-weight="bold" font-style="italic" text-decoration="underline" letter-spacing="2" word-spacing="5">Hello</text></svg>'
        result = sanitize_svg(svg)
        assert "text-anchor=" in result
        assert "font-family=" in result
        assert "font-size=" in result
        assert "font-weight=" in result
        assert "font-style=" in result
        assert "text-decoration=" in result
        assert "letter-spacing=" in result
        assert "word-spacing=" in result
        assert "<text" in result

    def test_svg_with_writing_mode_attributes_preserved(self):
        """Test that SVG with writing mode attributes is preserved."""
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><text x="10" y="20" writing-mode="tb" direction="rtl" dominant-baseline="middle" alignment-baseline="central" baseline-shift="sub">Hello</text></svg>'
        result = sanitize_svg(svg)
        assert "writing-mode=" in result
        assert "direction=" in result
        assert "dominant-baseline=" in result
        assert "alignment-baseline=" in result
        assert "baseline-shift=" in result
        assert "<text" in result
