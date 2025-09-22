use crate::cypher::{ReturnClause, ReturnProjection};
use crate::pattern_graph::{PatternVertex, PatternEdge, PatternGraph, TraversalDirection, SpanningTreeEdge};
use std::collections::HashMap;
use serde_json::Value;



/// Represents a line in an AQL query with indentation
#[derive(Debug, Clone, PartialEq)]
pub struct AQLLine {
    pub content: String,
    pub indent: usize,
    pub exposed_variables: Vec<String>,
}

/// Generate FILTER conditions from a property map
/// Returns a string with all property conditions connected by AND
/// 
/// # Arguments
/// * `properties` - Property map with key-value pairs
/// * `variable_name` - Name of the variable to apply conditions to
/// 
/// # Returns
/// * `String` with FILTER conditions, empty if no properties
fn generate_filter_conditions(properties: &HashMap<String, Value>, variable_name: &str) -> String {
    if properties.is_empty() {
        return String::new();
    }
    
    let conditions: Vec<String> = properties
        .iter()
        .map(|(key, value)| {
            match value {
                Value::String(s) => format!("{variable_name}.{key} == '{s}'"),
                Value::Number(n) => format!("{variable_name}.{key} == {n}"),
                Value::Bool(b) => format!("{variable_name}.{key} == {b}"),
                Value::Null => format!("{variable_name}.{key} == null"),
                _ => format!("{variable_name}.{key} == {value}"),
            }
        })
        .collect();
    
    conditions.join(" AND ")
}

/// Derive edge collection name from relationship type or use default
/// 
/// # Arguments
/// * `edge` - The pattern edge to derive collection name for
/// 
/// # Returns
/// * Collection name string, or error if no rel_type specified
fn derive_edge_collection_name(edge: &PatternEdge) -> Result<String, String> {
    match &edge.rel_type {
        Some(rel_type) => Ok(rel_type.clone()),
        None => Err("Edge type is required but not specified".to_string()),
    }
}

/// Generate AQL graph traversal statement for a spanning tree edge
/// 
/// # Arguments
/// * `spanning_edge` - The spanning tree edge to generate traversal for
/// * `vertices` - Vector of pattern vertices
/// * `edges` - Vector of pattern edges
/// * `indent` - Mutable reference to the current indentation level
/// 
/// # Returns
/// * `Result<AQLLine, String>` - AQL traversal line or error message
pub fn generate_edge_traversal(
    spanning_edge: &SpanningTreeEdge,
    vertices: &[PatternVertex],
    edges: &[PatternEdge],
    indent: &mut usize,
) -> Result<AQLLine, String> {
    let edge = &edges[spanning_edge.edge_index];
    let from_vertex = &vertices[spanning_edge.from_vertex];
    let to_vertex = &vertices[spanning_edge.to_vertex];
    
    // Get edge collection name
    let edge_collection = derive_edge_collection_name(edge)?;
    
    // Determine AQL direction keyword
    let direction_keyword = match spanning_edge.traversal_direction {
        TraversalDirection::Outbound => "OUTBOUND",
        TraversalDirection::Inbound => "INBOUND", 
        TraversalDirection::Any => "ANY",
    };
    
    // Generate FOR statement
    let for_statement = if edge.identifier.is_empty() {
        // No edge variable needed
        format!(
            "FOR {} IN 1..1 {} {}._id {}",
            to_vertex.identifier,
            direction_keyword,
            from_vertex.identifier,
            edge_collection
        )
    } else {
        // Include edge variable
        format!(
            "FOR {}, {} IN 1..1 {} {}._id {}",
            to_vertex.identifier,
            edge.identifier,
            direction_keyword,
            from_vertex.identifier,
            edge_collection
        )
    };
    
    let current_indent = *indent;
    *indent += 1; // Increase indentation for subsequent statements
    
    // Generate exposed variables list
    let mut exposed_vars = vec![to_vertex.identifier.clone()];
    if !edge.identifier.is_empty() {
        exposed_vars.push(edge.identifier.clone());
    }
    
    Ok(AQLLine {
        content: for_statement,
        indent: current_indent,
        exposed_variables: exposed_vars,
    })
}

/// Generate FILTER conditions for edge properties
/// 
/// # Arguments
/// * `edge` - The pattern edge with properties
/// * `indent` - Current indentation level
/// 
/// # Returns
/// * `Option<AQLLine>` - FILTER line if edge has properties, None otherwise
pub fn generate_edge_filter(edge: &PatternEdge, indent: usize) -> Option<AQLLine> {
    if edge.properties.is_empty() {
        return None;
    }
    
    let filter_conditions = generate_filter_conditions(&edge.properties, &edge.identifier);
    if filter_conditions.is_empty() {
        return None;
    }
    
    Some(AQLLine {
        content: format!("FILTER {filter_conditions}"),
        indent,
        exposed_variables: vec![], // FILTER statements don't expose new variables
    })
}

/// Derive collection name from vertex label or use default
/// 
/// # Arguments
/// * `vertex` - The vertex to derive collection name for
/// 
/// # Returns
/// * Collection name string
fn derive_collection_name(vertex: &PatternVertex) -> String {
    if let Some(label) = &vertex.label {
        label.clone()
    } else {
        "vertices".to_string()
    }
}

/// Find the vertex with the most prescribed properties
/// In case of a tie, select the vertex with the smallest index
/// 
/// # Arguments
/// * `vertices` - Vector of pattern vertices
/// 
/// # Returns
/// * Index of the anchor vertex, or None if vertices is empty
fn find_anchor_vertex(vertices: &[PatternVertex]) -> Option<usize> {
    if vertices.is_empty() {
        return None;
    }
    
    let mut best_index = 0;
    let mut max_properties = vertices[0].properties.len();
    
    for (index, vertex) in vertices.iter().enumerate().skip(1) {
        let prop_count = vertex.properties.len();
        if prop_count > max_properties {
            max_properties = prop_count;
            best_index = index;
        }
    }
    
    Some(best_index)
}

/// Generate AQL query from a pattern graph match statement
/// Uses breadth-first search to build a spanning tree and generates one-hop traversals
/// 
/// # Arguments
/// * `pattern_graph` - The pattern graph containing vertices and edges
/// 
/// # Returns
/// * `Result<(Vec<AQLLine>, usize), String>` with AQL lines and current indentation level, or error message
pub fn match_to_aql(pattern_graph: &PatternGraph) -> Result<(Vec<AQLLine>, usize), String> {
    let vertices = &pattern_graph.vertices;
    let edges = &pattern_graph.edges;
    if vertices.is_empty() {
        return Err("No vertices in pattern graph".to_string());
    }
    
    // Validate that all edges have constant depth 1
    for edge in edges {
        if edge.min_depth != Some(1) || edge.max_depth != Some(1) {
            return Err(format!(
                "Edge '{}' does not have constant depth 1 (min: {:?}, max: {:?}). Variable length relationships are not supported.", 
                edge.identifier, 
                edge.min_depth, 
                edge.max_depth
            ));
        }
    }
    
    // Find the anchor vertex (most properties, smallest index for ties)
    let anchor_index = find_anchor_vertex(vertices)
        .ok_or("Failed to find anchor vertex".to_string())?;
    
    let anchor_vertex = &vertices[anchor_index];
    let collection_name = derive_collection_name(anchor_vertex);
    let variable_name = &anchor_vertex.identifier;
    
    let mut aql_lines = Vec::new();
    let mut current_indent = 0; // Track current indentation level
    
    // Generate anchor FOR statement
    let for_line = AQLLine {
        content: format!("FOR {variable_name} IN {collection_name}"),
        indent: current_indent,
        exposed_variables: vec![variable_name.clone()], // Anchor exposes only the vertex identifier
    };
    aql_lines.push(for_line);
    current_indent += 1; // Increase indentation after FOR statement
    
    // Generate FILTER conditions for anchor vertex if properties exist
    let filter_conditions = generate_filter_conditions(&anchor_vertex.properties, variable_name);
    if !filter_conditions.is_empty() {
        let filter_line = AQLLine {
            content: format!("FILTER {filter_conditions}"),
            indent: current_indent,
            exposed_variables: vec![], // FILTER statements don't expose new variables
        };
        aql_lines.push(filter_line);
    }
    
    // If there are edges, build spanning tree and generate edge traversals
    if !edges.is_empty() {
        let spanning_tree = pattern_graph.build_spanning_tree(anchor_index)?;
        
        // Generate traversal statements for each edge in the spanning tree
        for spanning_edge in &spanning_tree {
            // Generate the edge traversal FOR statement
            let traversal_line = generate_edge_traversal(spanning_edge, vertices, edges, &mut current_indent)?;
            aql_lines.push(traversal_line);
            
            // Generate FILTER conditions for edge properties if they exist
            let edge = &edges[spanning_edge.edge_index];
            if let Some(edge_filter) = generate_edge_filter(edge, current_indent) {
                aql_lines.push(edge_filter);
            }
        }
    }
    
    Ok((aql_lines, current_indent))
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::pattern_graph::RelationshipDirection;

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

    #[test]
    fn test_edge_index_outbound() {
        let vertices = vec![
            create_test_vertex("a"),  // index 0
            create_test_vertex("b"),  // index 1
        ];
        let edges = vec![
            create_test_edge("a", "b", RelationshipDirection::Outbound),
        ];

        let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
        let index = pattern_graph.create_edge_index();

        assert_eq!(index.outgoing[0], vec![1usize]);
        assert_eq!(index.outgoing[1], Vec::<usize>::new());
        assert_eq!(index.incoming[0], Vec::<usize>::new());
        assert_eq!(index.incoming[1], vec![0usize]);
    }

    #[test]
    fn test_edge_index_bidirectional() {
        let vertices = vec![
            create_test_vertex("a"),  // index 0
            create_test_vertex("b"),  // index 1
        ];
        let edges = vec![
            create_test_edge("a", "b", RelationshipDirection::Bidirectional),
        ];

        let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
        let index = pattern_graph.create_edge_index();

        assert_eq!(index.outgoing[0], vec![1usize]);
        assert_eq!(index.outgoing[1], vec![0usize]);
        assert_eq!(index.incoming[0], vec![1usize]);
        assert_eq!(index.incoming[1], vec![0usize]);
    }

    #[test]
    fn test_is_connected_empty_graph() {
        let pattern_graph = PatternGraph::from_components(vec![], vec![], crate::pattern_graph::PatternPaths::new());
        assert!(pattern_graph.is_connected());
    }

    #[test]
    fn test_is_connected_single_vertex() {
        let vertices = vec![create_test_vertex("a")];
        let pattern_graph = PatternGraph::from_components(vertices, vec![], crate::pattern_graph::PatternPaths::new());
        assert!(pattern_graph.is_connected());
    }

    #[test]
    fn test_is_connected_two_connected_vertices() {
        let vertices = vec![
            create_test_vertex("a"),
            create_test_vertex("b"),
        ];
        let edges = vec![
            create_test_edge("a", "b", RelationshipDirection::Outbound),
        ];
        let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
        assert!(pattern_graph.is_connected());
    }

    #[test]
    fn test_is_connected_two_disconnected_vertices() {
        let vertices = vec![
            create_test_vertex("a"),
            create_test_vertex("b"),
        ];
        let pattern_graph = PatternGraph::from_components(vertices, vec![], crate::pattern_graph::PatternPaths::new());
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
        let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
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
        let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
        assert!(!pattern_graph.is_connected());
    }

    fn create_test_edge_with_type(
        source: &str, 
        target: &str, 
        direction: RelationshipDirection,
        rel_type: Option<&str>
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
            create_test_vertex("a"),  // index 0
            create_test_vertex("b"),  // index 1
            create_test_vertex("c"),  // index 2
        ];
        let edges = vec![
            create_test_edge_with_type("a", "b", RelationshipDirection::Outbound, Some("FRIEND")),
            create_test_edge_with_type("b", "c", RelationshipDirection::Outbound, Some("LIKES")),
        ];
        let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
        
        let spanning_tree = pattern_graph.build_spanning_tree(0).unwrap();
        
        assert_eq!(spanning_tree.len(), 2);
        assert_eq!(spanning_tree[0].from_vertex, 0);
        assert_eq!(spanning_tree[0].to_vertex, 1);
        assert_eq!(spanning_tree[0].traversal_direction, TraversalDirection::Outbound);
        assert_eq!(spanning_tree[1].from_vertex, 1);
        assert_eq!(spanning_tree[1].to_vertex, 2);
    }

    #[test]
    fn test_build_spanning_tree_bidirectional() {
        let vertices = vec![
            create_test_vertex("a"),  // index 0
            create_test_vertex("b"),  // index 1
        ];
        let edges = vec![
            create_test_edge_with_type("a", "b", RelationshipDirection::Bidirectional, Some("CONNECTED")),
        ];
        let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
        
        let spanning_tree = pattern_graph.build_spanning_tree(0).unwrap();
        
        assert_eq!(spanning_tree.len(), 1);
        assert_eq!(spanning_tree[0].from_vertex, 0);
        assert_eq!(spanning_tree[0].to_vertex, 1);
        assert_eq!(spanning_tree[0].traversal_direction, TraversalDirection::Any);
    }

    #[test]
    fn test_generate_edge_traversal_outbound() {
        let vertices = vec![
            create_test_vertex("a"),  // index 0
            create_test_vertex("b"),  // index 1
        ];
        let edges = vec![
            create_test_edge_with_type("a", "b", RelationshipDirection::Outbound, Some("FRIEND")),
        ];
        
        let spanning_edge = SpanningTreeEdge {
            from_vertex: 0,
            to_vertex: 1,
            edge_index: 0,
            traversal_direction: TraversalDirection::Outbound,
        };
        
        let mut indent = 1;
        let traversal = generate_edge_traversal(&spanning_edge, &vertices, &edges, &mut indent).unwrap();
        
        assert_eq!(traversal.content, "FOR b, a_b IN 1..1 OUTBOUND a._id FRIEND");
        assert_eq!(traversal.indent, 1);
        assert_eq!(traversal.exposed_variables, vec!["b", "a_b"]);
    }

    #[test]
    fn test_generate_edge_traversal_inbound() {
        let vertices = vec![
            create_test_vertex("a"),  // index 0
            create_test_vertex("b"),  // index 1
        ];
        let edges = vec![
            create_test_edge_with_type("a", "b", RelationshipDirection::Inbound, Some("FRIEND")),
        ];
        
        let spanning_edge = SpanningTreeEdge {
            from_vertex: 1,
            to_vertex: 0,
            edge_index: 0,
            traversal_direction: TraversalDirection::Inbound,
        };
        
        let mut indent = 1;
        let traversal = generate_edge_traversal(&spanning_edge, &vertices, &edges, &mut indent).unwrap();
        
        assert_eq!(traversal.content, "FOR a, a_b IN 1..1 INBOUND b._id FRIEND");
        assert_eq!(traversal.indent, 1);
        assert_eq!(traversal.exposed_variables, vec!["a", "a_b"]);
    }

    #[test]
    fn test_generate_edge_traversal_any() {
        let vertices = vec![
            create_test_vertex("a"),  // index 0
            create_test_vertex("b"),  // index 1
        ];
        let edges = vec![
            create_test_edge_with_type("a", "b", RelationshipDirection::Bidirectional, Some("FRIEND")),
        ];
        
        let spanning_edge = SpanningTreeEdge {
            from_vertex: 0,
            to_vertex: 1,
            edge_index: 0,
            traversal_direction: TraversalDirection::Any,
        };
        
        let mut indent = 1;
        let traversal = generate_edge_traversal(&spanning_edge, &vertices, &edges, &mut indent).unwrap();
        
        assert_eq!(traversal.content, "FOR b, a_b IN 1..1 ANY a._id FRIEND");
        assert_eq!(traversal.indent, 1);
        assert_eq!(traversal.exposed_variables, vec!["b", "a_b"]);
    }

    #[test]
    fn test_match_to_aql_with_edges() {
        let vertices = vec![
            create_test_vertex("user"),
            create_test_vertex("friend"),
        ];
        let edges = vec![
            create_test_edge_with_type("user", "friend", RelationshipDirection::Outbound, Some("FRIEND")),
        ];
        let pattern_graph = PatternGraph::from_components(vertices, edges, crate::pattern_graph::PatternPaths::new());
        
        let (aql_lines, _current_indent) = match_to_aql(&pattern_graph).unwrap();
        
        assert_eq!(aql_lines.len(), 2);
        assert_eq!(aql_lines[0].content, "FOR user IN vertices");
        assert_eq!(aql_lines[0].indent, 0);
        assert_eq!(aql_lines[0].exposed_variables, vec!["user"]);
        assert_eq!(aql_lines[1].content, "FOR friend, user_friend IN 1..1 OUTBOUND user._id FRIEND");
        assert_eq!(aql_lines[1].indent, 1);
        assert_eq!(aql_lines[1].exposed_variables, vec!["friend", "user_friend"]);
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
        assert_eq!(result.unwrap_err(), "Edge type is required but not specified");
    }
}

/// Collects all variable names that are exported from the AQL lines
/// These are the variables that can be referenced in the RETURN clause
fn collect_exported_variables(aql_lines: &[AQLLine]) -> Vec<String> {
    let mut exported_vars = Vec::new();
    
    for line in aql_lines {
        for var in &line.exposed_variables {
            if !exported_vars.contains(var) {
                exported_vars.push(var.clone());
            }
        }
    }
    
    exported_vars
}

/// Generates AQL RETURN statement from a RETURN clause
/// Returns AQL lines for the RETURN statement
pub fn generate_return_clause(
    return_clause: &ReturnClause,
    aql_lines: &[AQLLine],
    indent: usize,
) -> Result<Vec<AQLLine>, String> {
    let mut result = Vec::new();
    
    // Collect all exported variables from the query so far
    let exported_vars = collect_exported_variables(aql_lines);
    
    if return_clause.return_star {
        // Handle RETURN * - return all exported variables
        result.push(generate_return_star(&exported_vars, return_clause.distinct, indent));
    } else if return_clause.projections.is_empty() {
        return Err("RETURN clause must specify either * or at least one projection".to_string());
    } else {
        // Handle specific projections
        result.push(generate_return_projections(&return_clause.projections, &exported_vars, return_clause.distinct, indent)?);
    }
    
    Ok(result)
}

/// Generates AQL RETURN statement for RETURN *
fn generate_return_star(exported_vars: &[String], distinct: bool, indent: usize) -> AQLLine {
    let distinct_keyword = if distinct { "DISTINCT " } else { "" };
    
    if exported_vars.is_empty() {
        // No variables to return
        AQLLine {
            content: format!("RETURN {distinct_keyword}{{}}"),
            indent,
            exposed_variables: vec![],
        }
    } else {
        // Create JSON object with all exported variables
        let json_properties: Vec<String> = exported_vars.to_vec(); // Use abbreviated syntax {x} instead of {x: x}
        let json_object = format!("{{{}}}", json_properties.join(", "));
        
        AQLLine {
            content: format!("RETURN {distinct_keyword}{json_object}"),
            indent,
            exposed_variables: vec![],
        }
    }
}

/// Generates AQL RETURN statement for specific projections
fn generate_return_projections(
    projections: &[ReturnProjection],
    exported_vars: &[String],
    distinct: bool,
    indent: usize,
) -> Result<AQLLine, String> {
    let distinct_keyword = if distinct { "DISTINCT " } else { "" };
    
    let mut json_properties = Vec::new();
    
    for projection in projections {
        let column_name = projection.alias.as_ref()
            .unwrap_or(&projection.expression);
        
        // Validate that the expression refers to an exported variable
        // For now, we only support simple identifier expressions
        if projection.is_identifier && !exported_vars.contains(&projection.expression) {
            return Err(format!(
                "Variable '{}' is not available in the current scope. Available variables: {}",
                projection.expression,
                exported_vars.join(", ")
            ));
        }
        
        // Use abbreviated syntax when column name equals expression
        let json_property = if column_name == &projection.expression {
            column_name.clone()
        } else {
            format!("{}: {}", column_name, projection.expression)
        };
        json_properties.push(json_property);
    }
    
    let json_object = format!("{{{}}}", json_properties.join(", "));
    
    Ok(AQLLine {
        content: format!("RETURN {distinct_keyword}{json_object}"),
        indent,
        exposed_variables: vec![],
    })
}

/// Generates a complete AQL query from MATCH and RETURN clauses
pub fn generate_complete_aql(
    pattern_graph: &PatternGraph,
    return_clause: &ReturnClause,
) -> Result<Vec<AQLLine>, String> {
    // First generate the MATCH part
    let (mut aql_lines, current_indent) = match_to_aql(pattern_graph)?;
    
    // Then generate the RETURN part using the current indentation level
    let return_lines = generate_return_clause(return_clause, &aql_lines, current_indent)?;
    aql_lines.extend(return_lines);
    
    Ok(aql_lines)
}

/// Formats AQL lines into a complete query string
pub fn format_aql_query(aql_lines: &[AQLLine]) -> String {
    aql_lines
        .iter()
        .map(|line| format!("{}{}", "  ".repeat(line.indent), line.content))
        .collect::<Vec<_>>()
        .join("\n")
}
