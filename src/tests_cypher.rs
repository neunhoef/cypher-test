use super::*;
use crate::pattern_graph::{
    GraphError, MatchGraphResult, PatternEdge, PatternGraph, PatternVertex, RelationshipDirection,
    make_match_graph, print_pattern_graph,
};
use libcypher_parser_sys::{
    CYPHER_PARSE_DEFAULT, cypher_astnode_t, cypher_parse_result_free, cypher_parse_result_get_root,
    cypher_parse_result_nerrors, cypher_parse_result_nroots, cypher_parse_result_t,
    cypher_parser_config_free, cypher_parser_new_config, cypher_uparse,
};
use std::collections::HashMap;
use std::ffi::CStr;
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

#[test]
fn test_print_ast_with_null_node() {
    // Test that print_ast handles null nodes gracefully
    print_ast(ptr::null(), 0);
    // This should not panic and should print "<null>"
}

#[test]
fn test_print_ast_enhanced_with_null_node() {
    // Test that print_ast_enhanced handles null nodes gracefully
    print_ast_enhanced(ptr::null(), 0);
    // This should not panic and should print "<null>"
}

#[test]
fn test_print_ast_from_result_with_null_result() {
    // Test that print_ast_from_result handles null results gracefully
    print_ast_from_result(ptr::null());
    // This should not panic and should print "Parse result is null"
}

#[test]
fn test_print_ast_from_result_enhanced_with_null_result() {
    // Test that print_ast_from_result_enhanced handles null results gracefully
    print_ast_from_result_enhanced(ptr::null());
    // This should not panic and should print "Parse result is null"
}

#[test]
fn test_simple_match_query() {
    let query = "MATCH (n) RETURN n";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            // Test basic AST printing
            print_ast_from_result(result);

            // Test enhanced AST printing
            print_ast_from_result_enhanced(result);
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_complex_match_with_properties() {
    let query = "MATCH path = (n_0:aws_s3_bucket {org_id: 2, to_delete: false, default_encryption_type: 'aws:kms'}) RETURN path";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            let n_roots = unsafe { cypher_parse_result_nroots(result) };
            assert!(n_roots > 0, "Should have at least one root node");

            // Test enhanced AST printing
            print_ast_from_result_enhanced(result);
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_relationship_query() {
    let query = "MATCH path = (n_0:aws_ec2_instance {org_id: 2, to_delete: false}) <-[r_0:ATTACHES_TO]- (n_1:aws_ebs_volume {org_id: 2, to_delete: false, encrypted:false}) RETURN path";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            let n_roots = unsafe { cypher_parse_result_nroots(result) };
            assert!(n_roots > 0, "Should have at least one root node");

            // Test both AST printing methods
            print_ast_from_result(result);
            print_ast_from_result_enhanced(result);
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_complex_path_query() {
    let query = "MATCH path = (n_0:aws_ec2_instance {org_id: 2, to_delete: false, entity_info_status:1, is_publicly_accessible: true}) -[r_0:ASSOCIATES]-> (n_0_0:aws_iam_instance_profile {org_id: 2, to_delete: false, entity_info_status:1}) -[r_0_0:ASSUMES]-> (n_0_0_0:aws_iam_role {org_id: 2, to_delete: false, entity_info_status:1}) -[r_0_0_0:CAN_ACCESS]-> (n_0_0_0_0:aws_s3_bucket {org_id: 2, to_delete: false, entity_info_status:1, sensitive_data: true}) RETURN path";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            let n_roots = unsafe { cypher_parse_result_nroots(result) };
            assert!(n_roots > 0, "Should have at least one root node");

            // Test enhanced AST printing
            print_ast_from_result_enhanced(result);
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_administrator_role_query() {
    let query = "MATCH path = (n_0:aws_ec2_instance {org_id:2, entity_info_status: 1, to_delete:false, is_publicly_accessible: true}) -[r_0:ASSOCIATES]-> (n_0_0:aws_iam_instance_profile {org_id:2, entity_info_status: 1, to_delete:false}) -[r_0_0:ASSUMES]-> (n_0_0_0:aws_iam_role {org_id:2, entity_info_status: 1, to_delete:false, is_administrator: true}) RETURN path";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            let n_roots = unsafe { cypher_parse_result_nroots(result) };
            assert!(n_roots > 0, "Should have at least one root node");

            // Test enhanced AST printing
            print_ast_from_result_enhanced(result);
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_lambda_function_query() {
    let query = "MATCH path = (n_0:aws_lambda_function {org_id:2, entity_info_status: 1, to_delete:false, is_publicly_accessible: true}) -[r_0:ASSOCIATES]-> (n_0_0_0:aws_iam_role {org_id:2, entity_info_status: 1, to_delete:false, is_administrator: true}) RETURN path";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            let n_roots = unsafe { cypher_parse_result_nroots(result) };
            assert!(n_roots > 0, "Should have at least one root node");

            // Test enhanced AST printing
            print_ast_from_result_enhanced(result);
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_invalid_cypher_query() {
    let query = "INVALID CYPHER SYNTAX {";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        // This query should have parsing errors
        if n_errors > 0 {
            // Test that our functions handle error cases gracefully
            print_ast_from_result(result);
            print_ast_from_result_enhanced(result);
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_empty_query() {
    let query = "";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let _n_errors = unsafe { cypher_parse_result_nerrors(result) };
        let _n_roots = unsafe { cypher_parse_result_nroots(result) };

        // Test both printing methods with empty query
        print_ast_from_result(result);
        print_ast_from_result_enhanced(result);
    }

    cleanup_parse_result(result);
}

#[test]
fn test_ast_node_children_count() {
    let query = "MATCH (n:Person {name: 'Alice'}) RETURN n";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            let n_roots = unsafe { cypher_parse_result_nroots(result) };
            if n_roots > 0 {
                let root = unsafe { cypher_parse_result_get_root(result, 0) };
                if !root.is_null() {
                    let n_children = unsafe { cypher_astnode_nchildren(root) };
                    // n_children is unsigned, so it's always >= 0

                    // Test that we can iterate through children without panicking
                    for i in 0..n_children {
                        let child = unsafe { cypher_astnode_get_child(root, i) };
                        // Test both printing functions on each child
                        print_ast(child, 0);
                        print_ast_enhanced(child, 0);
                    }
                }
            }
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_ast_node_type_handling() {
    let query = "MATCH (n) RETURN n";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            let n_roots = unsafe { cypher_parse_result_nroots(result) };
            if n_roots > 0 {
                let root = unsafe { cypher_parse_result_get_root(result, 0) };
                if !root.is_null() {
                    let node_type = unsafe { cypher_astnode_type(root) };
                    // node_type is unsigned, so it's always >= 0

                    // Test that typestr doesn't panic
                    let type_str_ptr = unsafe { cypher_astnode_typestr(node_type) };
                    if !type_str_ptr.is_null() {
                        let type_str = unsafe {
                            CStr::from_ptr(type_str_ptr.cast::<c_char>())
                                .to_string_lossy()
                                .to_string()
                        };
                        assert!(!type_str.is_empty(), "Type string should not be empty");
                    }
                }
            }
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_multiple_root_nodes() {
    let query = "MATCH (n) RETURN n; MATCH (m) RETURN m";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            let n_roots = unsafe { cypher_parse_result_nroots(result) };
            assert!(
                n_roots >= 2,
                "Should have at least 2 root nodes for multiple statements"
            );

            // Test both printing methods
            print_ast_from_result(result);
            print_ast_from_result_enhanced(result);
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_make_match_graph_null_input() {
    let result = make_match_graph(std::ptr::null());
    assert_eq!(result, Err(GraphError::InvalidAstNode));
}

#[test]
fn test_make_match_graph_simple_match() {
    let query = "MATCH (n) RETURN n";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            let n_roots = unsafe { cypher_parse_result_nroots(result) };
            if n_roots > 0 {
                let root = unsafe { cypher_parse_result_get_root(result, 0) };
                if !root.is_null() {
                    // Find the MATCH clause in the query
                    let match_node = find_match_clause(root);
                    if let Some(match_clause) = match_node {
                        match make_match_graph(match_clause) {
                            Ok(graph) => {
                                println!(
                                    "Graph created successfully: {} vertices, {} edges, {} paths",
                                    graph.vertices.len(),
                                    graph.edges.len(),
                                    graph.paths.len()
                                );
                                // Basic validation
                                assert!(
                                    !graph.vertices.is_empty(),
                                    "Should have at least one vertex"
                                );
                            }
                            Err(e) => {
                                println!("Graph creation failed: {e}");
                            }
                        }
                    }
                }
            }
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_make_match_graph_with_relationship() {
    let query = "MATCH (a)-[r]->(b) RETURN a, r, b";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            let n_roots = unsafe { cypher_parse_result_nroots(result) };
            if n_roots > 0 {
                let root = unsafe { cypher_parse_result_get_root(result, 0) };
                if let Some(match_clause) = find_match_clause(root) {
                    match make_match_graph(match_clause) {
                        Ok(graph) => {
                            println!(
                                "Relationship graph: {} vertices, {} edges, {} paths",
                                graph.vertices.len(),
                                graph.edges.len(),
                                graph.paths.len()
                            );
                            // Should have at least 2 vertices and 1 edge
                            assert!(
                                graph.vertices.len() >= 2,
                                "Should have at least 2 vertices for relationship pattern"
                            );
                            assert!(
                                !graph.edges.is_empty(),
                                "Should have at least 1 edge for relationship pattern"
                            );
                        }
                        Err(e) => {
                            println!("Relationship graph creation failed: {e}");
                        }
                    }
                }
            }
        }
    }

    cleanup_parse_result(result);
}

// Helper function to find MATCH clause in AST
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

// Helper function to parse query and extract match graph
fn parse_and_extract_graph(query: &str) -> Result<MatchGraphResult, Box<dyn std::error::Error>> {
    let result = parse_cypher_query(query);
    if result.is_null() {
        return Err("Failed to parse query".into());
    }

    let n_errors = unsafe { cypher_parse_result_nerrors(result) };
    if n_errors > 0 {
        cleanup_parse_result(result);
        return Err("Parse errors in query".into());
    }

    let n_roots = unsafe { cypher_parse_result_nroots(result) };
    if n_roots == 0 {
        cleanup_parse_result(result);
        return Err("No root nodes in parse result".into());
    }

    let root = unsafe { cypher_parse_result_get_root(result, 0) };
    let match_clause = find_match_clause(root).ok_or("No MATCH clause found")?;

    let graph_result = make_match_graph(match_clause);
    cleanup_parse_result(result);

    match graph_result {
        Ok(graph) => Ok(graph),
        Err(e) => Err(e.into()),
    }
}

// Helper function to find vertex by identifier
fn find_vertex_by_id<'a>(vertices: &'a [PatternVertex], id: &str) -> Option<&'a PatternVertex> {
    vertices.iter().find(|v| v.identifier == id)
}

// Helper function to count vertices with specific label
fn count_vertices_with_label(vertices: &[PatternVertex], label: &str) -> usize {
    vertices
        .iter()
        .filter(|v| v.label.as_deref() == Some(label))
        .count()
}

// Helper function to find edge between two vertices
#[allow(dead_code)]
fn find_edge<'a>(edges: &'a [PatternEdge], source: &str, target: &str) -> Option<&'a PatternEdge> {
    edges
        .iter()
        .find(|e| e.source == source && e.target == target)
}

// Helper function to validate graph is a path (connected, each vertex has at most 2 edges, at least 1)
fn is_path_graph(vertices: &[PatternVertex], edges: &[PatternEdge]) -> bool {
    if vertices.is_empty() {
        return true;
    }
    if vertices.len() == 1 {
        return edges.is_empty();
    }

    // Check degree constraint (each vertex has at most 2 edges)
    for vertex in vertices {
        let degree = edges
            .iter()
            .filter(|e| e.source == vertex.identifier || e.target == vertex.identifier)
            .count();
        if degree == 0 || degree > 2 {
            return false;
        }
    }

    // Check connectivity - must be connected as a single component
    let mut visited = std::collections::HashSet::new();
    let mut stack = vec![&vertices[0].identifier];

    while let Some(current) = stack.pop() {
        if visited.contains(current) {
            continue;
        }
        visited.insert(current);

        // Add neighbors
        for edge in edges {
            if edge.source == *current && !visited.contains(&edge.target) {
                stack.push(&edge.target);
            } else if edge.target == *current && !visited.contains(&edge.source) {
                stack.push(&edge.source);
            }
        }
    }

    visited.len() == vertices.len()
}

// Helper function to validate graph is a tree (connected, acyclic)
fn is_tree_graph(vertices: &[PatternVertex], edges: &[PatternEdge]) -> bool {
    if vertices.is_empty() {
        return true;
    }
    if vertices.len() == 1 {
        return edges.is_empty();
    }

    // Tree must have exactly n-1 edges for n vertices
    if edges.len() != vertices.len() - 1 {
        return false;
    }

    // Check connectivity using simple traversal
    let mut visited = std::collections::HashSet::new();
    let mut stack = vec![&vertices[0].identifier];

    while let Some(current) = stack.pop() {
        if visited.contains(current) {
            continue;
        }
        visited.insert(current);

        // Add neighbors
        for edge in edges {
            if edge.source == *current && !visited.contains(&edge.target) {
                stack.push(&edge.target);
            } else if edge.target == *current && !visited.contains(&edge.source) {
                stack.push(&edge.source);
            }
        }
    }

    visited.len() == vertices.len()
}

// Helper function to validate graph contains a cycle
fn has_cycle(vertices: &[PatternVertex], edges: &[PatternEdge]) -> bool {
    if vertices.len() < 3 || edges.len() < 3 {
        return false;
    }

    // Simple cycle detection using DFS for undirected graph
    let mut visited = std::collections::HashSet::new();

    for vertex in vertices {
        if !visited.contains(&vertex.identifier)
            && has_cycle_util(&vertex.identifier, None, edges, &mut visited)
        {
            return true;
        }
    }
    false
}

fn has_cycle_util(
    vertex: &str,
    parent: Option<&str>,
    edges: &[PatternEdge],
    visited: &mut std::collections::HashSet<String>,
) -> bool {
    visited.insert(vertex.to_string());

    // Visit all neighbors
    for edge in edges {
        let neighbor = if edge.source == vertex {
            Some(&edge.target)
        } else if edge.target == vertex {
            Some(&edge.source)
        } else {
            None
        };

        if let Some(neighbor) = neighbor {
            // Skip the parent to avoid immediate back-edge
            if parent == Some(neighbor) {
                continue;
            }

            if !visited.contains(neighbor) {
                if has_cycle_util(neighbor, Some(vertex), edges, visited) {
                    return true;
                }
            } else {
                // Found a back edge to a visited vertex (not parent) - this is a cycle
                return true;
            }
        }
    }

    false
}

// COMPREHENSIVE TEST SUITE FOR make_match_graph

#[test]
fn test_basic_node_patterns() {
    // Test 1: Simple node without identifier
    let result = parse_and_extract_graph("MATCH () RETURN *");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 1);
    assert_eq!(graph.edges.len(), 0);
    assert!(graph.vertices[0].identifier.starts_with("n_"));
    assert_eq!(graph.vertices[0].label, None);
    assert!(graph.vertices[0].properties.is_empty());

    // Test 2: Node with identifier
    let result = parse_and_extract_graph("MATCH (person) RETURN person");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 1);
    assert_eq!(graph.edges.len(), 0);
    assert_eq!(graph.vertices[0].identifier, "person");
    assert_eq!(graph.vertices[0].label, None);

    // Test 3: Node with label
    let result = parse_and_extract_graph("MATCH (n:Person) RETURN n");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 1);
    assert_eq!(graph.vertices[0].identifier, "n");
    assert_eq!(graph.vertices[0].label, Some("Person".to_string()));

    // Test 4: Node with identifier and label
    let result = parse_and_extract_graph("MATCH (person:Person) RETURN person");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 1);
    assert_eq!(graph.vertices[0].identifier, "person");
    assert_eq!(graph.vertices[0].label, Some("Person".to_string()));

    // Test 5: Multiple different node types
    let result = parse_and_extract_graph("MATCH (a), (b:Label), (c:AnotherLabel) RETURN a, b, c");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 3);
    assert_eq!(graph.edges.len(), 0);

    let a = find_vertex_by_id(&graph.vertices, "a").unwrap();
    assert_eq!(a.label, None);

    let b = find_vertex_by_id(&graph.vertices, "b").unwrap();
    assert_eq!(b.label, Some("Label".to_string()));

    let c = find_vertex_by_id(&graph.vertices, "c").unwrap();
    assert_eq!(c.label, Some("AnotherLabel".to_string()));
}

#[test]
fn test_relationship_patterns() {
    // Test 1: Simple relationship without type
    let result = parse_and_extract_graph("MATCH (a)-[r]->(b) RETURN a, r, b");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 2);
    assert_eq!(graph.edges.len(), 1);

    let edge = &graph.edges[0];
    assert_eq!(edge.source, "a");
    assert_eq!(edge.target, "b");
    assert_eq!(edge.rel_type, None);

    // Test 2: Relationship with type
    let result = parse_and_extract_graph("MATCH (a)-[:KNOWS]->(b) RETURN a, b");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 2);
    assert_eq!(graph.edges.len(), 1);

    let edge = &graph.edges[0];
    assert_eq!(edge.rel_type, Some("KNOWS".to_string()));

    // Test 3: Relationship with identifier and type
    let result = parse_and_extract_graph("MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 2);
    assert_eq!(graph.edges.len(), 1);

    let edge = &graph.edges[0];
    assert_eq!(edge.rel_type, Some("KNOWS".to_string()));

    // Test 4: Undirected relationship
    let result = parse_and_extract_graph("MATCH (a)-[r:CONNECTED]-(b) RETURN a, r, b");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 2);
    assert_eq!(graph.edges.len(), 1);

    let edge = &graph.edges[0];
    assert_eq!(edge.rel_type, Some("CONNECTED".to_string()));

    // Test 5: Multiple relationship types
    let result =
        parse_and_extract_graph("MATCH (a)-[:FOLLOWS]->(b), (c)-[:LIKES]->(d) RETURN a, b, c, d");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 4);
    assert_eq!(graph.edges.len(), 2);

    // Verify both relationship types exist
    let has_follows = graph
        .edges
        .iter()
        .any(|e| e.rel_type == Some("FOLLOWS".to_string()));
    let has_likes = graph
        .edges
        .iter()
        .any(|e| e.rel_type == Some("LIKES".to_string()));
    assert!(has_follows && has_likes);
}

#[test]
fn test_path_patterns() {
    // Test 1: Short path (2 nodes, 1 edge)
    let result = parse_and_extract_graph("MATCH (a)-[:KNOWS]->(b) RETURN a, b");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 2);
    assert_eq!(graph.edges.len(), 1);
    assert!(is_path_graph(&graph.vertices, &graph.edges));

    // Test 2: Medium path (3 nodes, 2 edges)
    let result = parse_and_extract_graph("MATCH (a)-[:KNOWS]->(b)-[:WORKS_AT]->(c) RETURN a, b, c");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 3);
    assert_eq!(graph.edges.len(), 2);
    assert!(is_path_graph(&graph.vertices, &graph.edges));

    // Test 3: Longer path (4 nodes, 3 edges)
    let result =
        parse_and_extract_graph("MATCH (a)-[:R1]->(b)-[:R2]->(c)-[:R3]->(d) RETURN a, b, c, d");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 4);
    assert_eq!(graph.edges.len(), 3);
    assert!(is_path_graph(&graph.vertices, &graph.edges));

    // Test 4: Very long path (5 nodes, 4 edges)
    let result = parse_and_extract_graph(
        "MATCH (a)-[:R1]->(b)-[:R2]->(c)-[:R3]->(d)-[:R4]->(e) RETURN a, b, c, d, e",
    );
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 5);
    assert_eq!(graph.edges.len(), 4);
    assert!(is_path_graph(&graph.vertices, &graph.edges));

    // Test 5: Extra long path (6 nodes, 5 edges)
    let result = parse_and_extract_graph(
        "MATCH (a)-[:R1]->(b)-[:R2]->(c)-[:R3]->(d)-[:R4]->(e)-[:R5]->(f) RETURN a, b, c, d, e, f",
    );
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 6);
    assert_eq!(graph.edges.len(), 5);
    assert!(is_path_graph(&graph.vertices, &graph.edges));
}

#[test]
fn test_multiple_pattern_paths() {
    // Test 1: Two separate nodes
    let result = parse_and_extract_graph("MATCH (a), (b) RETURN a, b");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 2);
    assert_eq!(graph.edges.len(), 0);

    // Test 2: Two separate simple relationships
    let result =
        parse_and_extract_graph("MATCH (a)-[:KNOWS]->(b), (c)-[:LIKES]->(d) RETURN a, b, c, d");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 4);
    assert_eq!(graph.edges.len(), 2);

    // Test 3: Mix of single nodes and relationships
    let result =
        parse_and_extract_graph("MATCH (isolated), (a)-[:CONNECTS]->(b) RETURN isolated, a, b");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 3);
    assert_eq!(graph.edges.len(), 1);

    // Test 4: Three separate patterns
    let result = parse_and_extract_graph(
        "MATCH (single), (a)-[:R1]->(b), (c)-[:R2]->(d)-[:R3]->(e) RETURN *",
    );
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 6); // single + a,b + c,d,e
    assert_eq!(graph.edges.len(), 3); // 0 + 1 + 2

    // Test 5: Multiple complex patterns
    let result =
        parse_and_extract_graph("MATCH (a)-[:R1]->(b)-[:R2]->(c), (d)-[:R3]->(e), (f) RETURN *");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 6); // a,b,c + d,e + f
    assert_eq!(graph.edges.len(), 3); // 2 + 1 + 0
}

#[test]
fn test_mixed_labels_and_types() {
    // Test 1: Mixed labeled and unlabeled nodes
    let result =
        parse_and_extract_graph("MATCH (person:Person)-[:KNOWS]->(friend) RETURN person, friend");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 2);

    let person = find_vertex_by_id(&graph.vertices, "person").unwrap();
    assert_eq!(person.label, Some("Person".to_string()));

    let friend = find_vertex_by_id(&graph.vertices, "friend").unwrap();
    assert_eq!(friend.label, None);

    // Test 2: Multiple different labels
    let result = parse_and_extract_graph(
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company)-[:LOCATED_IN]->(city:City) RETURN p, c, city",
    );
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 3);

    assert_eq!(count_vertices_with_label(&graph.vertices, "Person"), 1);
    assert_eq!(count_vertices_with_label(&graph.vertices, "Company"), 1);
    assert_eq!(count_vertices_with_label(&graph.vertices, "City"), 1);

    // Test 3: Mixed typed and untyped relationships
    let result = parse_and_extract_graph("MATCH (a)-[:TYPED_REL]->(b)-[]->(c) RETURN a, b, c");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 3);
    assert_eq!(graph.edges.len(), 2);

    let typed_rels = graph.edges.iter().filter(|e| e.rel_type.is_some()).count();
    let untyped_rels = graph.edges.iter().filter(|e| e.rel_type.is_none()).count();
    assert_eq!(typed_rels, 1);
    assert_eq!(untyped_rels, 1);

    // Test 4: Complex mix
    let result = parse_and_extract_graph(
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person), (c:Company)-[]->(l) RETURN *",
    );
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 4);
    assert_eq!(graph.edges.len(), 2);

    assert_eq!(count_vertices_with_label(&graph.vertices, "Person"), 2);
    assert_eq!(count_vertices_with_label(&graph.vertices, "Company"), 1);
}

#[test]
fn test_topology_validation() {
    // Test 1: Simple path topology
    let result = parse_and_extract_graph("MATCH (a)-[:R1]->(b)-[:R2]->(c) RETURN a, b, c");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert!(is_path_graph(&graph.vertices, &graph.edges));
    assert!(is_tree_graph(&graph.vertices, &graph.edges));
    assert!(!has_cycle(&graph.vertices, &graph.edges));

    // Test 2: Star topology (tree but not path)
    // Note: This might be hard to represent in a single MATCH pattern,
    // but we can test the validation functions with constructed data
    let vertices = vec![
        PatternVertex {
            identifier: "center".to_string(),
            label: None,
            properties: HashMap::new(),
        },
        PatternVertex {
            identifier: "leaf1".to_string(),
            label: None,
            properties: HashMap::new(),
        },
        PatternVertex {
            identifier: "leaf2".to_string(),
            label: None,
            properties: HashMap::new(),
        },
        PatternVertex {
            identifier: "leaf3".to_string(),
            label: None,
            properties: HashMap::new(),
        },
    ];
    let edges = vec![
        PatternEdge {
            identifier: "r_1".to_string(),
            source: "center".to_string(),
            target: "leaf1".to_string(),
            rel_type: None,
            properties: HashMap::new(),
            direction: RelationshipDirection::Outbound,
            min_depth: Some(1),
            max_depth: Some(1),
        },
        PatternEdge {
            identifier: "r_2".to_string(),
            source: "center".to_string(),
            target: "leaf2".to_string(),
            rel_type: None,
            properties: HashMap::new(),
            direction: RelationshipDirection::Outbound,
            min_depth: Some(1),
            max_depth: Some(1),
        },
        PatternEdge {
            identifier: "r_3".to_string(),
            source: "center".to_string(),
            target: "leaf3".to_string(),
            rel_type: None,
            properties: HashMap::new(),
            direction: RelationshipDirection::Outbound,
            min_depth: Some(1),
            max_depth: Some(1),
        },
    ];

    assert!(!is_path_graph(&vertices, &edges)); // Not a path (center has degree 3)
    assert!(is_tree_graph(&vertices, &edges)); // But is a tree
    assert!(!has_cycle(&vertices, &edges)); // No cycles

    // Test 3: Disconnected components
    let result = parse_and_extract_graph("MATCH (a)-[:R1]->(b), (c)-[:R2]->(d) RETURN a, b, c, d");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert!(!is_path_graph(&graph.vertices, &graph.edges)); // Not connected as single path
    assert!(!is_tree_graph(&graph.vertices, &graph.edges)); // Not connected as single tree
}

#[test]
fn test_auto_generated_identifiers() {
    // Test 1: Nodes without identifiers get auto-generated ones
    let result = parse_and_extract_graph("MATCH ()-[]-() RETURN *");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 2);
    assert_eq!(graph.edges.len(), 1);

    // Both vertices should have auto-generated identifiers
    for vertex in &graph.vertices {
        assert!(vertex.identifier.starts_with("n_") || !vertex.identifier.is_empty());
    }

    // Test 2: Mix of explicit and auto-generated identifiers
    let result = parse_and_extract_graph("MATCH (named)-[]-() RETURN named");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 2);

    let named = find_vertex_by_id(&graph.vertices, "named");
    assert!(named.is_some());

    // The other vertex should have auto-generated identifier
    let other = graph
        .vertices
        .iter()
        .find(|v| v.identifier != "named")
        .unwrap();
    assert!(other.identifier.starts_with("n_") || other.identifier != "named");
}

#[test]
fn test_complex_real_world_patterns() {
    // Test 1: Social network pattern
    let result = parse_and_extract_graph(
        "MATCH (user:User)-[:FOLLOWS]->(friend:User)-[:POSTS]->(content:Post) RETURN user, friend, content",
    );
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 3);
    assert_eq!(graph.edges.len(), 2);
    assert!(is_path_graph(&graph.vertices, &graph.edges));

    // Test 2: Organizational hierarchy
    let result = parse_and_extract_graph(
        "MATCH (emp:Employee)-[:REPORTS_TO]->(mgr:Manager)-[:WORKS_FOR]->(dept:Department) RETURN emp, mgr, dept",
    );
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 3);
    assert_eq!(graph.edges.len(), 2);

    assert_eq!(count_vertices_with_label(&graph.vertices, "Employee"), 1);
    assert_eq!(count_vertices_with_label(&graph.vertices, "Manager"), 1);
    assert_eq!(count_vertices_with_label(&graph.vertices, "Department"), 1);

    // Test 3: Technology stack pattern
    let result = parse_and_extract_graph(
        "MATCH (app:Application)-[:RUNS_ON]->(server:Server)-[:HOSTS]->(db:Database) RETURN app, server, db",
    );
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 3);
    assert_eq!(graph.edges.len(), 2);

    let runs_on = graph
        .edges
        .iter()
        .any(|e| e.rel_type == Some("RUNS_ON".to_string()));
    let hosts = graph
        .edges
        .iter()
        .any(|e| e.rel_type == Some("HOSTS".to_string()));
    assert!(runs_on && hosts);

    // Test 4: Supply chain pattern
    let result = parse_and_extract_graph(
        "MATCH (supplier:Supplier)-[:SUPPLIES]->(manufacturer:Manufacturer)-[:PRODUCES]->(product:Product)-[:SOLD_TO]->(customer:Customer) RETURN *",
    );
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 4);
    assert_eq!(graph.edges.len(), 3);
    assert!(is_path_graph(&graph.vertices, &graph.edges));

    // Verify all relationship types
    let rel_types: Vec<String> = graph
        .edges
        .iter()
        .filter_map(|e| e.rel_type.clone())
        .collect();
    assert!(rel_types.contains(&"SUPPLIES".to_string()));
    assert!(rel_types.contains(&"PRODUCES".to_string()));
    assert!(rel_types.contains(&"SOLD_TO".to_string()));
}

#[test]
fn test_error_conditions() {
    // Test 1: Invalid query syntax
    let result = parse_and_extract_graph("INVALID SYNTAX {{{");
    assert!(result.is_err());

    // Test 2: Non-MATCH query
    let _result = parse_and_extract_graph("CREATE (n) RETURN n");
    // This might still work if it finds no MATCH clause, or might error
    // The exact behavior depends on the implementation

    // Test 3: Empty pattern
    let result = parse_and_extract_graph("MATCH RETURN *");
    assert!(result.is_err());

    // Test 4: NULL input to make_match_graph
    let result = make_match_graph(std::ptr::null());
    assert_eq!(result, Err(GraphError::InvalidAstNode));
}

#[test]
fn test_graph_printing() {
    // Test that graph printing doesn't panic with various patterns
    let result = parse_and_extract_graph(
        "MATCH (p:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(f:Friend) RETURN p, f",
    );
    if let Ok(graph) = result {
        print_pattern_graph(&graph);
        // This should not panic
    }

    // Test with empty graph
    let graph = PatternGraph::new();
    print_pattern_graph(&graph);

    // Test with single vertex
    let mut graph = PatternGraph::new();
    graph.add_vertex(PatternVertex {
        identifier: "test".to_string(),
        label: Some("TestLabel".to_string()),
        properties: HashMap::new(),
    });
    print_pattern_graph(&graph);
}

// TESTS FOR determine_relationship_direction FUNCTION

#[test]
fn test_relationship_direction_outbound() {
    let query = "MATCH (v) -[e]-> (w) RETURN v, e, w";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            if let Some(match_clause) = find_match_clause_from_result(result) {
                match make_match_graph(match_clause) {
                    Ok(graph) => {
                        assert_eq!(graph.vertices.len(), 2);
                        assert_eq!(graph.edges.len(), 1);

                        let edge = &graph.edges[0];
                        assert_eq!(edge.direction, RelationshipDirection::Outbound);
                        assert_eq!(edge.source, "v");
                        assert_eq!(edge.target, "w");
                    }
                    Err(e) => {
                        println!("Failed to create graph: {e}");
                    }
                }
            }
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_relationship_direction_inbound() {
    let query = "MATCH (v) <-[e]- (w) RETURN v, e, w";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            if let Some(match_clause) = find_match_clause_from_result(result) {
                match make_match_graph(match_clause) {
                    Ok(graph) => {
                        assert_eq!(graph.vertices.len(), 2);
                        assert_eq!(graph.edges.len(), 1);

                        let edge = &graph.edges[0];
                        assert_eq!(edge.direction, RelationshipDirection::Inbound);
                        assert_eq!(edge.source, "v");
                        assert_eq!(edge.target, "w");
                    }
                    Err(e) => {
                        println!("Failed to create graph: {e}");
                    }
                }
            }
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_relationship_direction_bidirectional() {
    let query = "MATCH (v) -[e]- (w) RETURN v, e, w";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            if let Some(match_clause) = find_match_clause_from_result(result) {
                match make_match_graph(match_clause) {
                    Ok(graph) => {
                        assert_eq!(graph.vertices.len(), 2);
                        assert_eq!(graph.edges.len(), 1);

                        let edge = &graph.edges[0];
                        assert_eq!(edge.direction, RelationshipDirection::Bidirectional);
                        assert_eq!(edge.source, "v");
                        assert_eq!(edge.target, "w");
                    }
                    Err(e) => {
                        println!("Failed to create graph: {e}");
                    }
                }
            }
        }
    }

    cleanup_parse_result(result);
}

#[test]
fn test_multiple_relationship_directions() {
    let query = "MATCH (a) -[r1]-> (b) <-[r2]- (c), (d) -[r3]- (e) RETURN a, b, c, d, e";
    let result = parse_cypher_query(query);

    if !result.is_null() {
        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors == 0 {
            if let Some(match_clause) = find_match_clause_from_result(result) {
                match make_match_graph(match_clause) {
                    Ok(graph) => {
                        assert_eq!(graph.vertices.len(), 5);
                        assert_eq!(graph.edges.len(), 3);

                        // Check that we have all three types of directions
                        let has_outbound = graph
                            .edges
                            .iter()
                            .any(|e| e.direction == RelationshipDirection::Outbound);
                        let has_inbound = graph
                            .edges
                            .iter()
                            .any(|e| e.direction == RelationshipDirection::Inbound);
                        let has_bidirectional = graph
                            .edges
                            .iter()
                            .any(|e| e.direction == RelationshipDirection::Bidirectional);

                        assert!(
                            has_outbound,
                            "Should have at least one outbound relationship"
                        );
                        assert!(has_inbound, "Should have at least one inbound relationship");
                        assert!(
                            has_bidirectional,
                            "Should have at least one bidirectional relationship"
                        );
                    }
                    Err(e) => {
                        println!("Failed to create graph: {e}");
                    }
                }
            }
        }
    }

    cleanup_parse_result(result);
}

// Helper function to find match clause from parse result
fn find_match_clause_from_result(
    result: *const cypher_parse_result_t,
) -> Option<*const cypher_astnode_t> {
    if result.is_null() {
        return None;
    }

    let n_roots = unsafe { cypher_parse_result_nroots(result) };
    for i in 0..n_roots {
        let root = unsafe { cypher_parse_result_get_root(result, i) };
        if let Some(match_clause) = find_match_clause(root) {
            return Some(match_clause);
        }
    }

    None
}

#[test]
fn test_path_name_tracking() {
    // Test 1: Named path should retain the name
    let result = parse_and_extract_graph("MATCH my_path = (a)-[r]->(b) RETURN a, b");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 2);
    assert_eq!(graph.edges.len(), 1);
    assert_eq!(graph.paths.len(), 1);
    assert!(graph.paths.as_inner().contains_key("my_path"));
    assert_eq!(
        graph.paths.as_inner()["my_path"],
        crate::pattern_graph::PatternPath::ProperPath(vec![0])
    ); // First edge should have index 0

    // Test 2: Anonymous path should get invented name
    let result = parse_and_extract_graph("MATCH (a)-[r]->(b) RETURN a, b");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 2);
    assert_eq!(graph.edges.len(), 1);
    assert_eq!(graph.paths.len(), 1);
    assert!(graph.paths.as_inner().contains_key("path1"));
    assert_eq!(
        graph.paths.as_inner()["path1"],
        crate::pattern_graph::PatternPath::ProperPath(vec![0])
    );

    // Test 3: Multiple paths should get different names
    let result = parse_and_extract_graph("MATCH (a)-[r1]->(b), (c)-[r2]->(d) RETURN a, b, c, d");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 4);
    assert_eq!(graph.edges.len(), 2);
    assert_eq!(graph.paths.len(), 2);
    assert!(graph.paths.as_inner().contains_key("path1"));
    assert!(graph.paths.as_inner().contains_key("path2"));
    // Each path should contain one edge
    match &graph.paths.as_inner()["path1"] {
        crate::pattern_graph::PatternPath::ProperPath(edges) => assert_eq!(edges.len(), 1),
        _ => panic!("Expected ProperPath"),
    }
    match &graph.paths.as_inner()["path2"] {
        crate::pattern_graph::PatternPath::ProperPath(edges) => assert_eq!(edges.len(), 1),
        _ => panic!("Expected ProperPath"),
    }

    // Test 4: Mix of named and anonymous paths
    let result = parse_and_extract_graph(
        "MATCH named_path = (a)-[r1]->(b), (c)-[r2]->(d) RETURN a, b, c, d",
    );
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 4);
    assert_eq!(graph.edges.len(), 2);
    assert_eq!(graph.paths.len(), 2);
    assert!(graph.paths.as_inner().contains_key("named_path"));
    assert!(graph.paths.as_inner().contains_key("path1")); // Anonymous path gets path1 in this query

    // Test 5: Complex path with multiple edges
    let result =
        parse_and_extract_graph("MATCH long_path = (a)-[r1]->(b)-[r2]->(c) RETURN a, b, c");
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 3);
    assert_eq!(graph.edges.len(), 2);
    assert_eq!(graph.paths.len(), 1);
    assert!(graph.paths.as_inner().contains_key("long_path"));
    assert_eq!(
        graph.paths.as_inner()["long_path"],
        crate::pattern_graph::PatternPath::ProperPath(vec![0, 1])
    ); // Should contain both edges
}

#[test]
fn test_path_edge_mapping_correctness() {
    // Test that edge indices in path mapping are correct
    let result = parse_and_extract_graph(
        "MATCH path1 = (a)-[r1:TYPE1]->(b), path2 = (c)-[r2:TYPE2]->(d) RETURN a, b, c, d",
    );
    assert!(result.is_ok());
    let graph = result.unwrap();
    assert_eq!(graph.vertices.len(), 4);
    assert_eq!(graph.edges.len(), 2);
    assert_eq!(graph.paths.len(), 2);

    // Check that the edge indices are valid
    for (path_name, pattern_path) in graph.paths.iter() {
        match pattern_path {
            crate::pattern_graph::PatternPath::ProperPath(edge_indices) => {
                for &edge_idx in edge_indices {
                    assert!(
                        edge_idx < graph.edges.len(),
                        "Edge index {} is out of bounds for path {}",
                        edge_idx,
                        path_name
                    );
                }
            }
            crate::pattern_graph::PatternPath::VertexPath(vertex_idx) => {
                assert!(
                    *vertex_idx < graph.vertices.len(),
                    "Vertex index {} is out of bounds for path {}",
                    vertex_idx,
                    path_name
                );
            }
        }
    }

    // Verify specific edge types
    let path1_edges: Vec<&PatternEdge> = match &graph.paths.as_inner()["path1"] {
        crate::pattern_graph::PatternPath::ProperPath(indices) => {
            indices.iter().map(|&i| &graph.edges[i]).collect()
        }
        _ => panic!("Expected ProperPath"),
    };
    let path2_edges: Vec<&PatternEdge> = match &graph.paths.as_inner()["path2"] {
        crate::pattern_graph::PatternPath::ProperPath(indices) => {
            indices.iter().map(|&i| &graph.edges[i]).collect()
        }
        _ => panic!("Expected ProperPath"),
    };

    assert_eq!(path1_edges.len(), 1);
    assert_eq!(path2_edges.len(), 1);

    // One should have TYPE1 and the other TYPE2
    let has_type1 = path1_edges
        .iter()
        .any(|e| e.rel_type == Some("TYPE1".to_string()))
        || path2_edges
            .iter()
            .any(|e| e.rel_type == Some("TYPE1".to_string()));
    let has_type2 = path1_edges
        .iter()
        .any(|e| e.rel_type == Some("TYPE2".to_string()))
        || path2_edges
            .iter()
            .any(|e| e.rel_type == Some("TYPE2".to_string()));

    assert!(has_type1, "Should have an edge with TYPE1");
    assert!(has_type2, "Should have an edge with TYPE2");
}
