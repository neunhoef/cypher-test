use super::*;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;

/// Helper function to parse a Cypher query and return the parse result
fn parse_cypher_query(query: &str) -> *const cypher_parse_result_t {
    let c_query = CString::new(query).expect("Failed to create CString");
    let config = unsafe { cypher_parser_new_config() };
    assert!(!config.is_null(), "Failed to create parser config");

    let result = unsafe {
        cypher_uparse(
            c_query.as_ptr().cast::<c_char>(),
            c_query.as_bytes().len() as u64,
            ptr::null_mut(),
            config,
            u64::from(CYPHER_PARSE_DEFAULT),
        )
    };

    unsafe {
        cypher_parser_config_free(config);
    }

    result
}

/// Helper function to clean up parse result
fn cleanup_parse_result(result: *const cypher_parse_result_t) {
    if !result.is_null() {
        unsafe {
            cypher_parse_result_free(result as *mut cypher_parse_result_t);
        }
    }
}

// Helper function to find a map node in a parsed query
fn find_map_node_in_query(result: *const cypher_parse_result_t) -> Option<*const cypher_astnode_t> {
    if result.is_null() {
        return None;
    }

    let n_roots = unsafe { cypher_parse_result_nroots(result) };
    for i in 0..n_roots {
        let root = unsafe { cypher_parse_result_get_root(result, i) };
        if let Some(map_node) = find_map_node_recursive(root) {
            return Some(map_node);
        }
    }

    None
}

// Recursively search for a map node in the AST
fn find_map_node_recursive(node: *const cypher_astnode_t) -> Option<*const cypher_astnode_t> {
    if node.is_null() {
        return None;
    }

    let node_type = unsafe { cypher_astnode_type(node) };
    let cypher_map = unsafe { CYPHER_AST_MAP };

    if node_type == cypher_map {
        return Some(node);
    }

    // Recursively search children
    let n_children = unsafe { cypher_astnode_nchildren(node) };
    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(node, i) };
        if let Some(map_node) = find_map_node_recursive(child) {
            return Some(map_node);
        }
    }

    None
}

#[test]
fn test_pattern_vertex_creation() {
    let vertex = PatternVertex {
        identifier: "n".to_string(),
        label: Some("Person".to_string()),
        properties: {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props
        },
    };

    assert_eq!(vertex.identifier, "n");
    assert_eq!(vertex.label, Some("Person".to_string()));
    assert_eq!(
        vertex.properties.get("name"),
        Some(&Value::String("Alice".to_string()))
    );
}

#[test]
fn test_pattern_edge_creation() {
    let edge = PatternEdge {
        identifier: "r".to_string(),
        source: "a".to_string(),
        target: "b".to_string(),
        rel_type: Some("KNOWS".to_string()),
        properties: HashMap::new(),
        direction: RelationshipDirection::Outbound,
        min_depth: Some(1),
        max_depth: Some(1),
    };

    assert_eq!(edge.identifier, "r");
    assert_eq!(edge.source, "a");
    assert_eq!(edge.target, "b");
    assert_eq!(edge.rel_type, Some("KNOWS".to_string()));
    assert_eq!(edge.direction, RelationshipDirection::Outbound);
}

#[test]
fn test_extract_properties_null_input() {
    let result = extract_properties(ptr::null());
    assert_eq!(result, None);
}

#[test]
fn test_extract_properties_non_map_node() {
    // Test with a non-map AST node (should return None)
    let query = "MATCH (n) RETURN n";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            let n_roots = unsafe { cypher_parse_result_nroots(result) };
            if n_roots > 0 {
                let root = unsafe { cypher_parse_result_get_root(result, 0) };
                if !root.is_null() {
                    // Root node is not a map, should return None
                    let properties = extract_properties(root);
                    assert_eq!(properties, None);
                }
            }
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_extract_properties_empty_map() {
    let query = "MATCH (n {}) RETURN n";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            if let Some(map_node) = find_map_node_in_query(result) {
                let properties = extract_properties(map_node);
                assert!(properties.is_some());
                let props = properties.unwrap();
                assert_eq!(props.len(), 0);
            }
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_extract_properties_string_values() {
    let query = "MATCH (n {name: 'Alice', city: 'Wonderland'}) RETURN n";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            if let Some(map_node) = find_map_node_in_query(result) {
                let properties = extract_properties(map_node);
                assert!(properties.is_some());
                let props = properties.unwrap();
                assert_eq!(props.len(), 2);
                assert_eq!(props.get("name"), Some(&Value::String("Alice".to_string())));
                assert_eq!(
                    props.get("city"),
                    Some(&Value::String("Wonderland".to_string()))
                );
            }
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_extract_properties_integer_values() {
    let query = "MATCH (n {age: 30, count: 42}) RETURN n";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            if let Some(map_node) = find_map_node_in_query(result) {
                let properties = extract_properties(map_node);
                assert!(properties.is_some());
                let props = properties.unwrap();
                assert_eq!(props.len(), 2);
                assert_eq!(
                    props.get("age"),
                    Some(&Value::Number(serde_json::Number::from(30)))
                );
                assert_eq!(
                    props.get("count"),
                    Some(&Value::Number(serde_json::Number::from(42)))
                );
            }
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_extract_properties_boolean_values() {
    let query = "MATCH (n {active: true, deleted: false}) RETURN n";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            if let Some(map_node) = find_map_node_in_query(result) {
                let properties = extract_properties(map_node);
                assert!(properties.is_some());
                let props = properties.unwrap();
                assert_eq!(props.len(), 2);
                assert_eq!(props.get("active"), Some(&Value::Bool(true)));
                assert_eq!(props.get("deleted"), Some(&Value::Bool(false)));
            }
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_extract_properties_float_values() {
    let query = "MATCH (n {height: 5.9, weight: 70.5}) RETURN n";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            if let Some(map_node) = find_map_node_in_query(result) {
                let properties = extract_properties(map_node);
                assert!(properties.is_some());
                let props = properties.unwrap();
                assert_eq!(props.len(), 2);
                assert_eq!(
                    props.get("height"),
                    Some(&Value::Number(serde_json::Number::from_f64(5.9).unwrap()))
                );
                assert_eq!(
                    props.get("weight"),
                    Some(&Value::Number(serde_json::Number::from_f64(70.5).unwrap()))
                );
            }
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_extract_properties_mixed_types() {
    let query = "MATCH (n {name: 'Alice', age: 30, active: true, score: 95.5}) RETURN n";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            if let Some(map_node) = find_map_node_in_query(result) {
                let properties = extract_properties(map_node);
                assert!(properties.is_some());
                let props = properties.unwrap();
                assert_eq!(props.len(), 4);
                assert_eq!(props.get("name"), Some(&Value::String("Alice".to_string())));
                assert_eq!(
                    props.get("age"),
                    Some(&Value::Number(serde_json::Number::from(30)))
                );
                assert_eq!(props.get("active"), Some(&Value::Bool(true)));
                assert_eq!(
                    props.get("score"),
                    Some(&Value::Number(serde_json::Number::from_f64(95.5).unwrap()))
                );
            }
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_extract_properties_null_values() {
    let query = "MATCH (n {optional: null}) RETURN n";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            if let Some(map_node) = find_map_node_in_query(result) {
                let properties = extract_properties(map_node);
                assert!(properties.is_some());
                let props = properties.unwrap();
                assert_eq!(props.len(), 1);
                assert_eq!(props.get("optional"), Some(&Value::Null));
            }
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_extract_property_name_helper() {
    // Test the helper function directly with null input
    let result = extract_property_name(ptr::null());
    assert_eq!(result, None);
}

#[test]
fn test_extract_expression_value_helper() {
    // Test the helper function directly with null input
    let result = extract_expression_value(ptr::null());
    assert_eq!(result, None);
}

// Helper function to find MATCH clause in AST recursively
fn find_match_clause(node: *const cypher_astnode_t) -> Option<*const cypher_astnode_t> {
    if node.is_null() {
        return None;
    }

    let node_type = unsafe { cypher_astnode_type(node) };
    let cypher_match = unsafe { CYPHER_AST_MATCH };
    if node_type == cypher_match {
        return Some(node);
    }

    // Recursively search children
    let n_children = unsafe { cypher_astnode_nchildren(node) };
    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(node, i) };
        if let Some(match_node) = find_match_clause(child) {
            return Some(match_node);
        }
    }

    None
}

/// Helper function to parse a Cypher query and extract the match graph
fn parse_and_extract_match_graph(query: &str) -> Result<PatternGraph, GraphError> {
    let result = parse_cypher_query(query);

    if result.is_null() {
        cleanup_parse_result(result);
        return Err(GraphError::InvalidAstNode);
    }

    let n_errors = unsafe { cypher_parse_result_nerrors(result) };
    if n_errors > 0 {
        cleanup_parse_result(result);
        return Err(GraphError::InvalidAstNode);
    }

    let n_roots = unsafe { cypher_parse_result_nroots(result) };
    if n_roots == 0 {
        cleanup_parse_result(result);
        return Err(GraphError::InvalidAstNode);
    }

    let root = unsafe { cypher_parse_result_get_root(result, 0) };

    // Find the MATCH clause within the AST
    let match_clause = find_match_clause(root);

    let graph_result = match match_clause {
        Some(match_node) => make_match_graph(match_node),
        None => Err(GraphError::UnsupportedPattern),
    };

    cleanup_parse_result(result);
    graph_result
}

#[test]
fn test_connectivity_with_shared_vertices_regression() {
    // Regression test for the duplicate vertex issue
    // This query should create a connected graph with 3 vertices, not 4
    // The vertex v1 appears in both paths p1 and p2 and should be deduplicated
    let query = "MATCH p1 = (v1) -[e]-> (v2), p2 = (v1) -[f]-> (v3)";

    let graph_result = parse_and_extract_match_graph(query);
    assert!(
        graph_result.is_ok(),
        "Failed to parse query: {:?}",
        graph_result.err()
    );

    let graph = graph_result.unwrap();

    // Verify correct vertex count - should be 3, not 4 (v1 should be deduplicated)
    assert_eq!(
        graph.vertices.len(),
        3,
        "Expected 3 vertices, got {}",
        graph.vertices.len()
    );

    // Verify we have the expected vertices
    let vertex_ids: HashSet<String> = graph
        .vertices
        .iter()
        .map(|v| v.identifier.clone())
        .collect();
    let expected_ids: HashSet<String> = ["v1", "v2", "v3"].iter().map(|s| s.to_string()).collect();
    assert_eq!(
        vertex_ids, expected_ids,
        "Vertex identifiers don't match expected set"
    );

    // Verify edge count
    assert_eq!(
        graph.edges.len(),
        2,
        "Expected 2 edges, got {}",
        graph.edges.len()
    );

    // Verify path count
    assert_eq!(
        graph.paths.len(),
        2,
        "Expected 2 paths, got {}",
        graph.paths.len()
    );

    // Most importantly: verify the graph is connected
    assert!(
        graph.is_connected(),
        "Graph should be connected but connectivity check failed"
    );

    // Verify the edges connect the right vertices
    let edge_pairs: HashSet<(String, String)> = graph
        .edges
        .iter()
        .map(|e| (e.source.clone(), e.target.clone()))
        .collect();
    let expected_pairs: HashSet<(String, String)> = [
        ("v1".to_string(), "v2".to_string()),
        ("v1".to_string(), "v3".to_string()),
    ]
    .iter()
    .cloned()
    .collect();
    assert_eq!(
        edge_pairs, expected_pairs,
        "Edge connections don't match expected pairs"
    );
}

#[test]
fn test_connectivity_multiple_shared_vertices() {
    // Test a more complex case with multiple shared vertices
    let query =
        "MATCH p1 = (a) -[r1]-> (b) -[r2]-> (c), p2 = (a) -[r3]-> (d), p3 = (b) -[r4]-> (e)";

    let graph_result = parse_and_extract_match_graph(query);
    assert!(
        graph_result.is_ok(),
        "Failed to parse query: {:?}",
        graph_result.err()
    );

    let graph = graph_result.unwrap();

    // Should have 5 unique vertices: a, b, c, d, e
    assert_eq!(
        graph.vertices.len(),
        5,
        "Expected 5 vertices, got {}",
        graph.vertices.len()
    );

    // Verify we have the expected vertices
    let vertex_ids: HashSet<String> = graph
        .vertices
        .iter()
        .map(|v| v.identifier.clone())
        .collect();
    let expected_ids: HashSet<String> = ["a", "b", "c", "d", "e"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        vertex_ids, expected_ids,
        "Vertex identifiers don't match expected set"
    );

    // Should have 4 edges
    assert_eq!(
        graph.edges.len(),
        4,
        "Expected 4 edges, got {}",
        graph.edges.len()
    );

    // Should be connected
    assert!(
        graph.is_connected(),
        "Graph should be connected but connectivity check failed"
    );
}

#[test]
fn test_connectivity_single_vertex_multiple_paths() {
    // Edge case: single vertex appearing in multiple named paths
    let query = "MATCH p1 = (n), p2 = (n)";

    let graph_result = parse_and_extract_match_graph(query);
    assert!(
        graph_result.is_ok(),
        "Failed to parse query: {:?}",
        graph_result.err()
    );

    let graph = graph_result.unwrap();

    // Should have only 1 vertex (n should be deduplicated)
    assert_eq!(
        graph.vertices.len(),
        1,
        "Expected 1 vertex, got {}",
        graph.vertices.len()
    );
    assert_eq!(graph.vertices[0].identifier, "n");

    // No edges
    assert_eq!(
        graph.edges.len(),
        0,
        "Expected 0 edges, got {}",
        graph.edges.len()
    );

    // Single vertex is considered connected
    assert!(
        graph.is_connected(),
        "Single vertex graph should be connected"
    );
}

#[test]
fn test_connectivity_star_pattern() {
    // Test a star pattern where one central vertex connects to multiple others
    let query =
        "MATCH p1 = (center) -[r1]-> (a), p2 = (center) -[r2]-> (b), p3 = (center) -[r3]-> (c)";

    let graph_result = parse_and_extract_match_graph(query);
    assert!(
        graph_result.is_ok(),
        "Failed to parse query: {:?}",
        graph_result.err()
    );

    let graph = graph_result.unwrap();

    // Should have 4 unique vertices: center, a, b, c
    assert_eq!(
        graph.vertices.len(),
        4,
        "Expected 4 vertices, got {}",
        graph.vertices.len()
    );

    // Verify we have the expected vertices
    let vertex_ids: HashSet<String> = graph
        .vertices
        .iter()
        .map(|v| v.identifier.clone())
        .collect();
    let expected_ids: HashSet<String> = ["center", "a", "b", "c"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        vertex_ids, expected_ids,
        "Vertex identifiers don't match expected set"
    );

    // Should have 3 edges
    assert_eq!(
        graph.edges.len(),
        3,
        "Expected 3 edges, got {}",
        graph.edges.len()
    );

    // Should be connected
    assert!(
        graph.is_connected(),
        "Star pattern graph should be connected"
    );

    // Verify all edges have 'center' as source
    for edge in &graph.edges {
        assert_eq!(
            edge.source, "center",
            "All edges should originate from center vertex"
        );
    }
}
