use super::*;
use crate::pattern_graph::{
    PatternEdge, PatternGraph, PatternVertex, RelationshipDirection, SpanningTreeEdge,
    TraversalDirection,
};
use std::collections::HashMap;

fn create_test_vertex(id: &str) -> PatternVertex {
    PatternVertex {
        identifier: id.to_string(),
        label: None,
        properties: HashMap::new(),
    }
}

fn create_test_edge(source: &str, target: &str, direction: RelationshipDirection) -> PatternEdge {
    PatternEdge {
        identifier: format!("{}_{}", source, target),
        source: source.to_string(),
        target: target.to_string(),
        rel_type: None,
        properties: HashMap::new(),
        direction,
        min_depth: Some(1),
        max_depth: Some(1),
    }
}

fn create_multi_hop_edge(source: &str, target: &str, direction: RelationshipDirection, min_depth: u32, max_depth: u32) -> PatternEdge {
    PatternEdge {
        identifier: format!("{}_{}", source, target),
        source: source.to_string(),
        target: target.to_string(),
        rel_type: Some("KNOWS".to_string()),
        properties: HashMap::new(),
        direction,
        min_depth: Some(min_depth),
        max_depth: Some(max_depth),
    }
}

#[test]
fn test_edge_index_outbound() {
    let vertices = vec![
        create_test_vertex("a"), // index 0
        create_test_vertex("b"), // index 1
    ];
    let edges = vec![create_test_edge("a", "b", RelationshipDirection::Outbound)];

    let pattern_graph =
        PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
    let index = pattern_graph.create_edge_index();

    assert_eq!(index.outgoing[0], vec![1usize]);
    assert_eq!(index.outgoing[1], Vec::<usize>::new());
    assert_eq!(index.incoming[0], Vec::<usize>::new());
    assert_eq!(index.incoming[1], vec![0usize]);
}

#[test]
fn test_edge_index_bidirectional() {
    let vertices = vec![
        create_test_vertex("a"), // index 0
        create_test_vertex("b"), // index 1
    ];
    let edges = vec![create_test_edge(
        "a",
        "b",
        RelationshipDirection::Bidirectional,
    )];

    let pattern_graph =
        PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
    let index = pattern_graph.create_edge_index();

    assert_eq!(index.outgoing[0], vec![1usize]);
    assert_eq!(index.outgoing[1], vec![0usize]);
    assert_eq!(index.incoming[0], vec![1usize]);
    assert_eq!(index.incoming[1], vec![0usize]);
}

#[test]
fn test_is_connected_empty_graph() {
    let pattern_graph =
        PatternGraph::from_components(vec![], vec![], crate::pattern_graph::PatternPaths::new());
    assert!(pattern_graph.is_connected());
}

#[test]
fn test_is_connected_single_vertex() {
    let vertices = vec![create_test_vertex("a")];
    let pattern_graph =
        PatternGraph::from_components(vertices, vec![], crate::pattern_graph::PatternPaths::new());
    assert!(pattern_graph.is_connected());
}

#[test]
fn test_is_connected_two_connected_vertices() {
    let vertices = vec![create_test_vertex("a"), create_test_vertex("b")];
    let edges = vec![create_test_edge("a", "b", RelationshipDirection::Outbound)];
    let pattern_graph =
        PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
    assert!(pattern_graph.is_connected());
}

#[test]
fn test_is_connected_two_disconnected_vertices() {
    let vertices = vec![create_test_vertex("a"), create_test_vertex("b")];
    let pattern_graph =
        PatternGraph::from_components(vertices, vec![], crate::pattern_graph::PatternPaths::new());
    assert!(!pattern_graph.is_connected());
}

#[test]
fn test_is_connected_three_vertex_chain() {
    let vertices = vec![
        create_test_vertex("a"),
        create_test_vertex("b"),
        create_test_vertex("c"),
    ];
    let edges = vec![
        create_test_edge("a", "b", RelationshipDirection::Outbound),
        create_test_edge("b", "c", RelationshipDirection::Outbound),
    ];
    let pattern_graph =
        PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
    assert!(pattern_graph.is_connected());
}

#[test]
fn test_is_connected_disconnected_components() {
    let vertices = vec![
        create_test_vertex("a"),
        create_test_vertex("b"),
        create_test_vertex("c"),
        create_test_vertex("d"),
    ];
    let edges = vec![
        create_test_edge("a", "b", RelationshipDirection::Outbound),
        create_test_edge("c", "d", RelationshipDirection::Outbound),
    ];
    let pattern_graph =
        PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
    assert!(!pattern_graph.is_connected());
}

fn create_test_edge_with_type(
    source: &str,
    target: &str,
    direction: RelationshipDirection,
    rel_type: Option<&str>,
) -> PatternEdge {
    PatternEdge {
        identifier: format!("{}_{}", source, target),
        source: source.to_string(),
        target: target.to_string(),
        rel_type: rel_type.map(String::from),
        properties: HashMap::new(),
        direction,
        min_depth: Some(1),
        max_depth: Some(1),
    }
}

#[test]
fn test_build_spanning_tree_simple_chain() {
    let vertices = vec![
        create_test_vertex("a"), // index 0
        create_test_vertex("b"), // index 1
        create_test_vertex("c"), // index 2
    ];
    let edges = vec![
        create_test_edge_with_type("a", "b", RelationshipDirection::Outbound, Some("FRIEND")),
        create_test_edge_with_type("b", "c", RelationshipDirection::Outbound, Some("LIKES")),
    ];
    let pattern_graph =
        PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());

    let spanning_tree = pattern_graph.build_spanning_tree(0).unwrap();

    assert_eq!(spanning_tree.len(), 2);
    assert_eq!(spanning_tree[0].from_vertex, 0);
    assert_eq!(spanning_tree[0].to_vertex, 1);
    assert_eq!(
        spanning_tree[0].traversal_direction,
        TraversalDirection::Outbound
    );
    assert_eq!(spanning_tree[1].from_vertex, 1);
    assert_eq!(spanning_tree[1].to_vertex, 2);
}

#[test]
fn test_build_spanning_tree_bidirectional() {
    let vertices = vec![
        create_test_vertex("a"), // index 0
        create_test_vertex("b"), // index 1
    ];
    let edges = vec![create_test_edge_with_type(
        "a",
        "b",
        RelationshipDirection::Bidirectional,
        Some("CONNECTED"),
    )];
    let pattern_graph =
        PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());

    let spanning_tree = pattern_graph.build_spanning_tree(0).unwrap();

    assert_eq!(spanning_tree.len(), 1);
    assert_eq!(spanning_tree[0].from_vertex, 0);
    assert_eq!(spanning_tree[0].to_vertex, 1);
    assert_eq!(
        spanning_tree[0].traversal_direction,
        TraversalDirection::Any
    );
}

#[test]
fn test_generate_edge_traversal_outbound() {
    let vertices = vec![
        create_test_vertex("a"), // index 0
        create_test_vertex("b"), // index 1
    ];
    let edges = vec![create_test_edge_with_type(
        "a",
        "b",
        RelationshipDirection::Outbound,
        Some("FRIEND"),
    )];

    let spanning_edge = SpanningTreeEdge {
        from_vertex: 0,
        to_vertex: 1,
        edge_index: 0,
        traversal_direction: TraversalDirection::Outbound,
    };

    let mut indent = 1;
    let traversal =
        generate_edge_traversal(&spanning_edge, &vertices, &edges, &mut indent).unwrap();

    assert_eq!(
        traversal.content,
        "FOR b, a_b IN 1..1 OUTBOUND a._id FRIEND"
    );
    assert_eq!(traversal.indent, 1);
    assert_eq!(traversal.exposed_variables, vec!["b", "a_b"]);
}

#[test]
fn test_generate_edge_traversal_inbound() {
    let vertices = vec![
        create_test_vertex("a"), // index 0
        create_test_vertex("b"), // index 1
    ];
    let edges = vec![create_test_edge_with_type(
        "a",
        "b",
        RelationshipDirection::Inbound,
        Some("FRIEND"),
    )];

    let spanning_edge = SpanningTreeEdge {
        from_vertex: 1,
        to_vertex: 0,
        edge_index: 0,
        traversal_direction: TraversalDirection::Inbound,
    };

    let mut indent = 1;
    let traversal =
        generate_edge_traversal(&spanning_edge, &vertices, &edges, &mut indent).unwrap();

    assert_eq!(traversal.content, "FOR a, a_b IN 1..1 INBOUND b._id FRIEND");
    assert_eq!(traversal.indent, 1);
    assert_eq!(traversal.exposed_variables, vec!["a", "a_b"]);
}

#[test]
fn test_generate_edge_traversal_any() {
    let vertices = vec![
        create_test_vertex("a"), // index 0
        create_test_vertex("b"), // index 1
    ];
    let edges = vec![create_test_edge_with_type(
        "a",
        "b",
        RelationshipDirection::Bidirectional,
        Some("FRIEND"),
    )];

    let spanning_edge = SpanningTreeEdge {
        from_vertex: 0,
        to_vertex: 1,
        edge_index: 0,
        traversal_direction: TraversalDirection::Any,
    };

    let mut indent = 1;
    let traversal =
        generate_edge_traversal(&spanning_edge, &vertices, &edges, &mut indent).unwrap();

    assert_eq!(traversal.content, "FOR b, a_b IN 1..1 ANY a._id FRIEND");
    assert_eq!(traversal.indent, 1);
    assert_eq!(traversal.exposed_variables, vec!["b", "a_b"]);
}

#[test]
fn test_match_to_aql_with_edges() {
    let vertices = vec![create_test_vertex("user"), create_test_vertex("friend")];
    let edges = vec![create_test_edge_with_type(
        "user",
        "friend",
        RelationshipDirection::Outbound,
        Some("FRIEND"),
    )];
    let pattern_graph =
        PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());

    let (aql_lines, _current_indent) = match_to_aql(&pattern_graph).unwrap();

    assert_eq!(aql_lines.len(), 2);
    assert_eq!(aql_lines[0].content, "FOR user IN vertices");
    assert_eq!(aql_lines[0].indent, 0);
    assert_eq!(aql_lines[0].exposed_variables, vec!["user"]);
    assert_eq!(
        aql_lines[1].content,
        "FOR friend, user_friend IN 1..1 OUTBOUND user._id FRIEND"
    );
    assert_eq!(aql_lines[1].indent, 1);
    assert_eq!(
        aql_lines[1].exposed_variables,
        vec!["friend", "user_friend"]
    );
}

#[test]
fn test_edge_collection_name_error() {
    let edge = PatternEdge {
        identifier: "test".to_string(),
        source: "a".to_string(),
        target: "b".to_string(),
        rel_type: None, // No type specified
        properties: HashMap::new(),
        direction: RelationshipDirection::Outbound,
        min_depth: Some(1),
        max_depth: Some(1),
    };

    let result = derive_edge_collection_name(&edge);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err(),
        "Edge type is required but not specified"
    );
}

#[test]
fn test_path_identifier_in_return() {
    use crate::cypher::{ReturnClause, ReturnProjection};

    // Create a simple pattern graph with a named path
    let vertices = vec![create_test_vertex("user"), create_test_vertex("friend")];
    let edges = vec![create_test_edge_with_type(
        "user",
        "friend",
        RelationshipDirection::Outbound,
        Some("KNOWS"),
    )];

    // Create a pattern graph with a named path
    let mut paths = crate::pattern_graph::PatternPaths::new();
    paths.insert(
        "mypath".to_string(),
        crate::pattern_graph::PatternPath::ProperPath(vec![0]),
    ); // path includes first edge
    let pattern_graph = PatternGraph::from_components(vertices, edges, paths);

    // Create return clause with path identifier
    let return_clause = ReturnClause {
        distinct: false,
        return_star: false,
        projections: vec![ReturnProjection {
            expression: "mypath".to_string(),
            alias: None,
            is_identifier: true,
        }],
    };

    // Generate complete AQL query
    let result = generate_complete_aql(&pattern_graph, &return_clause);
    assert!(result.is_ok(), "Failed to generate AQL: {:?}", result.err());

    let aql_lines = result.unwrap();
    let query = format_aql_query(&aql_lines);

    // Verify the query contains path construction
    assert!(query.contains("FOR user IN vertices"));
    assert!(query.contains("FOR friend, user_friend IN 1..1 OUTBOUND user._id KNOWS"));
    assert!(
        query.contains("RETURN {mypath: {\"vertices\": [user, friend], \"edges\": [user_friend]}}")
    );
}

#[test]
fn test_vertex_only_path_in_return() {
    use crate::cypher::{ReturnClause, ReturnProjection};

    // Create a simple pattern graph with a single vertex path
    let vertices = vec![create_test_vertex("user")];
    let edges = vec![];

    // Create a pattern graph with a vertex-only path
    let mut paths = crate::pattern_graph::PatternPaths::new();
    paths.insert(
        "mypath".to_string(),
        crate::pattern_graph::PatternPath::VertexPath(0),
    ); // path includes only vertex 0
    let pattern_graph = PatternGraph::from_components(vertices, edges, paths);

    // Create return clause with path identifier
    let return_clause = ReturnClause {
        distinct: false,
        return_star: false,
        projections: vec![ReturnProjection {
            expression: "mypath".to_string(),
            alias: None,
            is_identifier: true,
        }],
    };

    // Generate complete AQL query
    let result = generate_complete_aql(&pattern_graph, &return_clause);
    assert!(result.is_ok(), "Failed to generate AQL: {:?}", result.err());

    let aql_lines = result.unwrap();
    let query = format_aql_query(&aql_lines);

    // Verify the query contains vertex-only path construction
    assert!(query.contains("FOR user IN vertices"));
    assert!(query.contains("RETURN {mypath: {\"vertices\": [user], \"edges\": []}}"));
    // Should not contain any edge traversal
    assert!(!query.contains("OUTBOUND"));
    assert!(!query.contains("INBOUND"));
}

#[test]
fn test_generate_path_expression_vertex_only() {
    // Create a simple pattern graph with a single vertex
    let vertices = vec![create_test_vertex("node1")];
    let edges = vec![];

    // Create a pattern graph with a vertex-only path
    let mut paths = crate::pattern_graph::PatternPaths::new();
    paths.insert(
        "single_vertex".to_string(),
        crate::pattern_graph::PatternPath::VertexPath(0),
    );
    let pattern_graph = PatternGraph::from_components(vertices, edges, paths);

    // Test the path expression generation
    let result = generate_path_expression("single_vertex", &pattern_graph);
    assert!(
        result.is_ok(),
        "Failed to generate path expression: {:?}",
        result.err()
    );

    let expression = result.unwrap();
    assert_eq!(expression, "{\"vertices\": [node1], \"edges\": []}");
}

#[test]
fn test_generate_path_expression_proper_path() {
    // Create a pattern graph with edges
    let vertices = vec![create_test_vertex("node1"), create_test_vertex("node2")];
    let edges = vec![create_test_edge_with_type(
        "node1",
        "node2",
        RelationshipDirection::Outbound,
        Some("CONNECTS"),
    )];

    // Create a pattern graph with a proper path
    let mut paths = crate::pattern_graph::PatternPaths::new();
    paths.insert(
        "proper_path".to_string(),
        crate::pattern_graph::PatternPath::ProperPath(vec![0]),
    );
    let pattern_graph = PatternGraph::from_components(vertices, edges, paths);

    // Test the path expression generation
    let result = generate_path_expression("proper_path", &pattern_graph);
    assert!(
        result.is_ok(),
        "Failed to generate path expression: {:?}",
        result.err()
    );

    let expression = result.unwrap();
    assert_eq!(
        expression,
        "{\"vertices\": [node1, node2], \"edges\": [node1_node2]}"
    );
}

#[test]
fn test_generate_path_expression_invalid_vertex_index() {
    let vertices = vec![create_test_vertex("node1")];
    let edges = vec![];

    // Create a pattern graph with invalid vertex index
    let mut paths = crate::pattern_graph::PatternPaths::new();
    paths.insert(
        "invalid_vertex".to_string(),
        crate::pattern_graph::PatternPath::VertexPath(1),
    ); // Invalid index
    let pattern_graph = PatternGraph::from_components(vertices, edges, paths);

    // Test the path expression generation should fail
    let result = generate_path_expression("invalid_vertex", &pattern_graph);
    assert!(result.is_err(), "Should fail with invalid vertex index");
    assert!(result.unwrap_err().contains("Invalid vertex index 1"));
}

#[test]
fn test_multiple_vertex_paths_same_vertex() {
    use crate::cypher::{ReturnClause, ReturnProjection};

    // Create a pattern graph with a single vertex referenced by multiple paths
    let vertices = vec![create_test_vertex("shared_node")];
    let edges = vec![];

    // Create multiple vertex-only paths pointing to the same vertex
    let mut paths = crate::pattern_graph::PatternPaths::new();
    paths.insert(
        "path1".to_string(),
        crate::pattern_graph::PatternPath::VertexPath(0),
    );
    paths.insert(
        "path2".to_string(),
        crate::pattern_graph::PatternPath::VertexPath(0),
    );
    let pattern_graph = PatternGraph::from_components(vertices, edges, paths);

    // Create return clause returning both paths
    let return_clause = ReturnClause {
        distinct: false,
        return_star: false,
        projections: vec![
            ReturnProjection {
                expression: "path1".to_string(),
                alias: None,
                is_identifier: true,
            },
            ReturnProjection {
                expression: "path2".to_string(),
                alias: None,
                is_identifier: true,
            },
        ],
    };

    // Generate complete AQL query
    let result = generate_complete_aql(&pattern_graph, &return_clause);
    assert!(result.is_ok(), "Failed to generate AQL: {:?}", result.err());

    let aql_lines = result.unwrap();
    let query = format_aql_query(&aql_lines);

    // Both paths should generate the same vertex-only path structure
    assert!(query.contains("path1: {\"vertices\": [shared_node], \"edges\": []}"));
    assert!(query.contains("path2: {\"vertices\": [shared_node], \"edges\": []}"));
}

#[test]
fn test_multi_hop_edge_traversal() {
    let vertices = vec![
        create_test_vertex("a"), // index 0
        create_test_vertex("b"), // index 1
    ];
    
    let edges = vec![create_multi_hop_edge("a", "b", RelationshipDirection::Outbound, 2, 3)];
    
    let pattern_graph = PatternGraph::from_components(vertices, edges.clone(), crate::pattern_graph::PatternPaths::new());
    
    let result = match_to_aql(&pattern_graph);
    assert!(result.is_ok(), "Failed to generate AQL: {:?}", result.err());
    
    let (aql_lines, _) = result.unwrap();
    let query = format_aql_query(&aql_lines);
    
    // Should generate multi-hop traversal with path variable
    assert!(query.contains("FOR b, a_b, _p_a_b IN 2..3 OUTBOUND a._id KNOWS"));
}

#[test]
fn test_multi_hop_edge_with_properties() {
    let vertices = vec![
        create_test_vertex("a"),
        create_test_vertex("b"),
    ];
    
    let mut edge = create_multi_hop_edge("a", "b", RelationshipDirection::Outbound, 1, 5);
    edge.properties.insert("weight".to_string(), serde_json::Value::Number(serde_json::Number::from(10)));
    
    let edges = vec![edge];
    let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
    
    let result = match_to_aql(&pattern_graph);
    assert!(result.is_ok(), "Failed to generate AQL: {:?}", result.err());
    
    let (aql_lines, _) = result.unwrap();
    let query = format_aql_query(&aql_lines);
    
    // Should generate multi-hop traversal
    assert!(query.contains("FOR b, a_b, _p_a_b IN 1..5 OUTBOUND a._id KNOWS"));
    // Should generate path-based edge property filter
    assert!(query.contains("FILTER _p_a_b.edges[*].weight ALL == 10"));
}

#[test]
fn test_multi_hop_vertex_properties() {
    let vertices = vec![
        create_test_vertex("a"),
        {
            let mut vertex = create_test_vertex("b");
            vertex.properties.insert("name".to_string(), serde_json::Value::String("test".to_string()));
            vertex
        },
    ];
    
    let edges = vec![create_multi_hop_edge("a", "b", RelationshipDirection::Outbound, 2, 4)];
    let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
    
    let result = match_to_aql(&pattern_graph);
    assert!(result.is_ok(), "Failed to generate AQL: {:?}", result.err());
    
    let (aql_lines, _) = result.unwrap();
    let query = format_aql_query(&aql_lines);
    
    // The algorithm chooses b as anchor because it has properties
    // So we traverse INBOUND from b to a
    assert!(query.contains("FOR b IN vertices"));
    assert!(query.contains("FILTER b.name == 'test'"));
    assert!(query.contains("FOR a, a_b, _p_a_b IN 2..4 INBOUND b._id KNOWS"));
    // No additional FILTER needed for vertex a since it has no properties
}

#[test]
fn test_multi_hop_without_edge_identifier() {
    let vertices = vec![
        create_test_vertex("a"),
        create_test_vertex("b"),
    ];
    
    let mut edge = create_multi_hop_edge("a", "b", RelationshipDirection::Outbound, 1, 3);
    edge.identifier = "".to_string(); // Remove edge identifier
    
    let edges = vec![edge];
    let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
    
    let result = match_to_aql(&pattern_graph);
    assert!(result.is_ok(), "Failed to generate AQL: {:?}", result.err());
    
    let (aql_lines, _) = result.unwrap();
    let query = format_aql_query(&aql_lines);
    
    // Should generate multi-hop traversal without edge variable but with path variable
    assert!(query.contains("FOR b, _p_b IN 1..3 OUTBOUND a._id KNOWS"));
}

#[test]
fn test_single_hop_still_works() {
    let vertices = vec![
        create_test_vertex("a"),
        create_test_vertex("b"),
    ];
    
    let mut edge = create_test_edge("a", "b", RelationshipDirection::Outbound);
    edge.rel_type = Some("KNOWS".to_string());
    edge.properties.insert("weight".to_string(), serde_json::Value::Number(serde_json::Number::from(5)));
    
    let edges = vec![edge];
    let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
    
    let result = match_to_aql(&pattern_graph);
    assert!(result.is_ok(), "Failed to generate AQL: {:?}", result.err());
    
    let (aql_lines, _) = result.unwrap();
    let query = format_aql_query(&aql_lines);
    
    // Should still generate single-hop traversal (no path variable)
    assert!(query.contains("FOR b, a_b IN 1..1 OUTBOUND a._id KNOWS"));
    // Should use regular edge property filter
    assert!(query.contains("FILTER a_b.weight == 5"));
}

#[test]
fn test_multi_hop_with_target_vertex_filter() {
    let vertices = vec![
        {
            let mut vertex = create_test_vertex("a");
            vertex.properties.insert("id".to_string(), serde_json::Value::Number(serde_json::Number::from(1)));
            vertex.properties.insert("type".to_string(), serde_json::Value::String("source".to_string()));
            vertex
        },
        {
            let mut vertex = create_test_vertex("b");
            vertex.properties.insert("name".to_string(), serde_json::Value::String("target".to_string()));
            vertex
        },
    ];
    
    let edges = vec![create_multi_hop_edge("a", "b", RelationshipDirection::Outbound, 2, 3)];
    let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
    
    let result = match_to_aql(&pattern_graph);
    assert!(result.is_ok(), "Failed to generate AQL: {:?}", result.err());
    
    let (aql_lines, _) = result.unwrap();
    let query = format_aql_query(&aql_lines);
    
    // Vertex a has more properties, so it becomes the anchor
    assert!(query.contains("FOR a IN vertices"));
    // Check for both property filters (order may vary due to HashMap iteration)
    assert!(query.contains("a.id == 1"));
    assert!(query.contains("a.type == 'source'"));
    assert!(query.contains("FOR b, a_b, _p_a_b IN 2..3 OUTBOUND a._id KNOWS"));
    // Should generate simple vertex property filter for target vertex b  
    assert!(query.contains("FILTER b.name == 'target'"));
}

#[test]
fn test_multi_hop_with_prune_statement() {
    let vertices = vec![
        {
            let mut vertex = create_test_vertex("a");
            vertex.properties.insert("type".to_string(), serde_json::Value::String("source".to_string()));
            vertex
        },
        {
            let mut vertex = create_test_vertex("b");
            vertex.properties.insert("name".to_string(), serde_json::Value::String("target".to_string()));
            vertex.properties.insert("active".to_string(), serde_json::Value::Bool(true));
            vertex
        },
    ];
    
    let edges = vec![create_multi_hop_edge("a", "b", RelationshipDirection::Outbound, 2, 4)];
    let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
    
    let result = match_to_aql(&pattern_graph);
    assert!(result.is_ok(), "Failed to generate AQL: {:?}", result.err());
    
    let (aql_lines, _) = result.unwrap();
    let query = format_aql_query(&aql_lines);
    
    // The algorithm should choose b as anchor because it has more properties (2 vs 1)
    // So we traverse INBOUND from b to a
    assert!(query.contains("FOR b IN vertices"));
    assert!(query.contains("FOR a, a_b, _p_a_b IN 2..4 INBOUND b._id KNOWS"));
    // Should generate PRUNE statement with inverted edge null check for target vertex a
    assert!(query.contains("PRUNE a_b != null && (a.type != 'source')"));
    // Should generate property filters for anchor b
    assert!(query.contains("b.name == 'target'"));
    assert!(query.contains("b.active == true"));
    // Should generate simple vertex property filter for target vertex a
    assert!(query.contains("FILTER a.type == 'source'"));
}

#[test]
fn test_multi_hop_prune_without_edge_identifier() {
    let vertices = vec![
        {
            let mut vertex = create_test_vertex("a");
            vertex.properties.insert("id".to_string(), serde_json::Value::Number(serde_json::Number::from(1)));
            vertex.properties.insert("type".to_string(), serde_json::Value::String("source".to_string()));
            vertex.properties.insert("priority".to_string(), serde_json::Value::Number(serde_json::Number::from(5)));
            vertex
        },
        {
            let mut vertex = create_test_vertex("b");
            vertex.properties.insert("status".to_string(), serde_json::Value::String("active".to_string()));
            vertex
        },
    ];
    
    let mut edge = create_multi_hop_edge("a", "b", RelationshipDirection::Outbound, 1, 3);
    edge.identifier = "".to_string(); // Remove edge identifier
    
    let edges = vec![edge];
    let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
    
    let result = match_to_aql(&pattern_graph);
    assert!(result.is_ok(), "Failed to generate AQL: {:?}", result.err());
    
    let (aql_lines, _) = result.unwrap();
    let query = format_aql_query(&aql_lines);
    
    // The algorithm chooses a as anchor because it has more properties (3 vs 1)
    // So we traverse OUTBOUND from a to b (no edge variable)
    assert!(query.contains("FOR a IN vertices"));
    assert!(query.contains("FOR b, _p_b IN 1..3 OUTBOUND a._id KNOWS"));
    // Should generate PRUNE statement without edge null check for vertex b (inverted condition)
    assert!(query.contains("PRUNE b.status != 'active'"));
    // Should still generate simple vertex property filter
    assert!(query.contains("FILTER b.status == 'active'"));
}

#[test]
fn test_single_hop_no_prune() {
    let vertices = vec![
        create_test_vertex("a"),
        {
            let mut vertex = create_test_vertex("b");
            vertex.properties.insert("name".to_string(), serde_json::Value::String("test".to_string()));
            vertex
        },
    ];
    
    let mut edge = create_test_edge("a", "b", RelationshipDirection::Outbound);
    edge.rel_type = Some("KNOWS".to_string());
    
    let edges = vec![edge];
    let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
    
    let result = match_to_aql(&pattern_graph);
    assert!(result.is_ok(), "Failed to generate AQL: {:?}", result.err());
    
    let (aql_lines, _) = result.unwrap();
    let query = format_aql_query(&aql_lines);
    
    // The algorithm chooses b as anchor because it has properties
    // So we traverse INBOUND from b to a
    assert!(query.contains("FOR b IN vertices"));
    assert!(query.contains("FILTER b.name == 'test'"));
    assert!(query.contains("FOR a, a_b IN 1..1 INBOUND b._id KNOWS"));
    // Should NOT generate PRUNE statement for single-hop
    assert!(!query.contains("PRUNE"));
}

#[test]
fn test_final_multi_hop_output_example() {
    let vertices = vec![
        {
            let mut vertex = create_test_vertex("start");
            vertex.properties.insert("id".to_string(), serde_json::Value::Number(serde_json::Number::from(1)));
            vertex
        },
        {
            let mut vertex = create_test_vertex("end");
            vertex.properties.insert("name".to_string(), serde_json::Value::String("target".to_string()));
            vertex.properties.insert("active".to_string(), serde_json::Value::Bool(true));
            vertex
        },
    ];
    
    let edges = vec![create_multi_hop_edge("start", "end", RelationshipDirection::Outbound, 1, 4)];
    let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
    
    let result = match_to_aql(&pattern_graph);
    assert!(result.is_ok());
    
    let (aql_lines, _) = result.unwrap();
    let query = format_aql_query(&aql_lines);
    
    // Print the final generated AQL to show the simplified syntax
    println!("Final generated AQL with simplified vertex filters:");
    println!("{}", query);
    
    // Algorithm chooses end as anchor (2 properties vs 1), so it traverses INBOUND
    assert!(query.contains("FOR end IN vertices"));
    assert!(query.contains("FOR start, start_end, _p_start_end IN 1..4 INBOUND end._id KNOWS"));
    assert!(query.contains("PRUNE start_end != null && (start.id != 1)"));
    assert!(query.contains("FILTER start.id == 1"));
    // Check properties individually since HashMap iteration order can vary
    assert!(query.contains("end.active == true"));
    assert!(query.contains("end.name == 'target'"));
}
