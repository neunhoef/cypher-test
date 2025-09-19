use crate::cypher::{PatternVertex, PatternEdge, RelationshipDirection};
use std::collections::{HashMap, HashSet, VecDeque};
use serde_json::Value;

/// Represents an edge in the spanning tree with direction information
#[derive(Debug, Clone)]
pub struct SpanningTreeEdge {
    /// Index of the source vertex (already visited)
    pub from_vertex: usize,
    /// Index of the target vertex (newly discovered)
    pub to_vertex: usize,
    /// Reference to the original pattern edge
    pub edge_index: usize,
    /// Direction to traverse this edge (OUTBOUND, INBOUND, or ANY)
    pub traversal_direction: TraversalDirection,
}

/// Direction for AQL graph traversal
#[derive(Debug, Clone, PartialEq)]
pub enum TraversalDirection {
    Outbound,     // OUTBOUND
    Inbound,      // INBOUND  
    Any,          // ANY (for bidirectional edges)
}

/// Index structure for efficient graph traversal
/// Maps each vertex index to its neighbors in different directions
#[derive(Debug, Clone)]
pub struct EdgeIndex {
    /// Vector of outgoing neighbors for each vertex (indexed by vertex index)
    #[allow(dead_code)]
    pub outgoing: Vec<Vec<usize>>,
    /// Vector of incoming neighbors for each vertex (indexed by vertex index)
    #[allow(dead_code)]
    pub incoming: Vec<Vec<usize>>,
    /// Vector of all undirected neighbors for each vertex (union of incoming and outgoing)
    pub undirected: Vec<Vec<usize>>,
}

impl EdgeIndex {
    /// Create a new EdgeIndex from vertices and edges
    pub fn new(vertices: &[PatternVertex], edges: &[PatternEdge]) -> Self {
        let n_vertices = vertices.len();
        let mut outgoing: Vec<Vec<usize>> = vec![Vec::new(); n_vertices];
        let mut incoming: Vec<Vec<usize>> = vec![Vec::new(); n_vertices];
        let mut undirected: Vec<Vec<usize>> = vec![Vec::new(); n_vertices];

        // Create mapping from vertex identifier to index
        let mut id_to_index: HashMap<String, usize> = HashMap::new();
        for (index, vertex) in vertices.iter().enumerate() {
            id_to_index.insert(vertex.identifier.clone(), index);
        }

        // Process each edge to build neighbor lists
        for edge in edges {
            let source_idx = match id_to_index.get(&edge.source) {
                Some(idx) => *idx,
                None => continue, // Skip edges referencing non-existent vertices
            };
            let target_idx = match id_to_index.get(&edge.target) {
                Some(idx) => *idx,
                None => continue, // Skip edges referencing non-existent vertices
            };

            match edge.direction {
                RelationshipDirection::Outbound => {
                    // source -> target
                    outgoing[source_idx].push(target_idx);
                    incoming[target_idx].push(source_idx);
                    
                    // For undirected connectivity, add both directions
                    undirected[source_idx].push(target_idx);
                    undirected[target_idx].push(source_idx);
                }
                RelationshipDirection::Inbound => {
                    // target -> source (inbound from source perspective)
                    outgoing[target_idx].push(source_idx);
                    incoming[source_idx].push(target_idx);
                    
                    // For undirected connectivity, add both directions
                    undirected[source_idx].push(target_idx);
                    undirected[target_idx].push(source_idx);
                }
                RelationshipDirection::Bidirectional => {
                    // Both directions
                    outgoing[source_idx].push(target_idx);
                    outgoing[target_idx].push(source_idx);
                    incoming[source_idx].push(target_idx);
                    incoming[target_idx].push(source_idx);
                    
                    // For undirected connectivity, add both directions
                    undirected[source_idx].push(target_idx);
                    undirected[target_idx].push(source_idx);
                }
            }
        }

        EdgeIndex {
            outgoing,
            incoming,
            undirected,
        }
    }
}

/// Build a spanning tree using breadth-first search from a starting vertex
/// Returns a list of edges in the spanning tree in the order they should be traversed
/// 
/// # Arguments
/// * `vertices` - Vector of pattern vertices
/// * `edges` - Vector of pattern edges  
/// * `edge_index` - Precomputed edge index for efficient neighbor lookup
/// * `start_vertex` - Index of the starting vertex for BFS
/// 
/// # Returns
/// * `Result<Vec<SpanningTreeEdge>, String>` - Ordered list of spanning tree edges or error
pub fn build_spanning_tree(
    vertices: &[PatternVertex],
    edges: &[PatternEdge],
    edge_index: &EdgeIndex,
    start_vertex: usize,
) -> Result<Vec<SpanningTreeEdge>, String> {
    if start_vertex >= vertices.len() {
        return Err("Start vertex index out of bounds".to_string());
    }

    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    let mut spanning_tree_edges = Vec::new();

    // Create mapping from vertex identifier to index
    let mut id_to_index: HashMap<String, usize> = HashMap::new();
    for (index, vertex) in vertices.iter().enumerate() {
        id_to_index.insert(vertex.identifier.clone(), index);
    }

    // Create reverse mapping from vertex pair to edge index and direction info
    let mut edge_lookup: HashMap<(usize, usize), (usize, TraversalDirection)> = HashMap::new();
    for (edge_idx, edge) in edges.iter().enumerate() {
        let source_idx = match id_to_index.get(&edge.source) {
            Some(idx) => *idx,
            None => continue,
        };
        let target_idx = match id_to_index.get(&edge.target) {
            Some(idx) => *idx,
            None => continue,
        };

        match edge.direction {
            RelationshipDirection::Outbound => {
                // source -> target, so traversal is OUTBOUND from source to target
                edge_lookup.insert((source_idx, target_idx), (edge_idx, TraversalDirection::Outbound));
                // For reverse direction (target to source), traversal is INBOUND
                edge_lookup.insert((target_idx, source_idx), (edge_idx, TraversalDirection::Inbound));
            }
            RelationshipDirection::Inbound => {
                // source <- target, so traversal is INBOUND from target to source
                edge_lookup.insert((target_idx, source_idx), (edge_idx, TraversalDirection::Outbound));
                // For reverse direction (source to target), traversal is INBOUND  
                edge_lookup.insert((source_idx, target_idx), (edge_idx, TraversalDirection::Inbound));
            }
            RelationshipDirection::Bidirectional => {
                // Both directions are ANY
                edge_lookup.insert((source_idx, target_idx), (edge_idx, TraversalDirection::Any));
                edge_lookup.insert((target_idx, source_idx), (edge_idx, TraversalDirection::Any));
            }
        }
    }

    // Start BFS from the specified vertex
    queue.push_back(start_vertex);
    visited.insert(start_vertex);

    while let Some(current_vertex_idx) = queue.pop_front() {
        // Explore undirected neighbors
        for &neighbor_idx in &edge_index.undirected[current_vertex_idx] {
            if !visited.contains(&neighbor_idx) {
                visited.insert(neighbor_idx);
                queue.push_back(neighbor_idx);

                // Find the edge information for this traversal
                if let Some((edge_idx, traversal_direction)) = edge_lookup.get(&(current_vertex_idx, neighbor_idx)) {
                    spanning_tree_edges.push(SpanningTreeEdge {
                        from_vertex: current_vertex_idx,
                        to_vertex: neighbor_idx,
                        edge_index: *edge_idx,
                        traversal_direction: traversal_direction.clone(),
                    });
                }
            }
        }
    }

    Ok(spanning_tree_edges)
}

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
/// * `vertices` - Pattern vertices from the match statement
/// * `edges` - Pattern edges from the match statement
/// * `edge_index` - Edge index for the pattern graph
/// 
/// # Returns
/// * `Result<Vec<AQLLine>, String>` with AQL lines or error message
pub fn match_to_aql(
    vertices: &[PatternVertex], 
    edges: &[PatternEdge],
    edge_index: &EdgeIndex
) -> Result<Vec<AQLLine>, String> {
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
        let spanning_tree = build_spanning_tree(vertices, edges, edge_index, anchor_index)?;
        
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
    
    Ok(aql_lines)
}

/// Check if the pattern graph is connected when viewed as an undirected graph
/// 
/// # Arguments
/// * `vertices` - Vector of pattern vertices
/// * `edge_index` - Precomputed edge index for efficient neighbor lookup
/// 
/// # Returns
/// * `true` if the graph is connected (all vertices are reachable from any starting vertex)
/// * `false` if the graph is disconnected
/// * `true` if the graph is empty (vacuously connected)
pub fn is_connected(vertices: &[PatternVertex], edge_index: &EdgeIndex) -> bool {
    // Empty graph is considered connected
    if vertices.is_empty() {
        return true;
    }

    // Single vertex is connected
    if vertices.len() == 1 {
        return true;
    }

    // Use breadth-first search starting from the first vertex (index 0)
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();

    // Start BFS from vertex index 0
    queue.push_back(0);
    visited.insert(0);

    while let Some(current_vertex_idx) = queue.pop_front() {
        // Get undirected neighbors of the current vertex
        for &neighbor_idx in &edge_index.undirected[current_vertex_idx] {
            if !visited.contains(&neighbor_idx) {
                visited.insert(neighbor_idx);
                queue.push_back(neighbor_idx);
            }
        }
    }

    // The graph is connected if we visited all vertices
    visited.len() == vertices.len()
}

#[cfg(test)]
mod tests {
    use super::*;
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
            identifier: format!("{}-{}", source, target),
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

        let index = EdgeIndex::new(&vertices, &edges);

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

        let index = EdgeIndex::new(&vertices, &edges);

        assert_eq!(index.outgoing[0], vec![1usize]);
        assert_eq!(index.outgoing[1], vec![0usize]);
        assert_eq!(index.incoming[0], vec![1usize]);
        assert_eq!(index.incoming[1], vec![0usize]);
    }

    #[test]
    fn test_is_connected_empty_graph() {
        let vertices = vec![];
        let index = EdgeIndex::new(&vertices, &[]);
        assert!(is_connected(&vertices, &index));
    }

    #[test]
    fn test_is_connected_single_vertex() {
        let vertices = vec![create_test_vertex("a")];
        let index = EdgeIndex::new(&vertices, &[]);
        assert!(is_connected(&vertices, &index));
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
        let index = EdgeIndex::new(&vertices, &edges);
        assert!(is_connected(&vertices, &index));
    }

    #[test]
    fn test_is_connected_two_disconnected_vertices() {
        let vertices = vec![
            create_test_vertex("a"),
            create_test_vertex("b"),
        ];
        let index = EdgeIndex::new(&vertices, &[]);
        assert!(!is_connected(&vertices, &index));
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
        let index = EdgeIndex::new(&vertices, &edges);
        assert!(is_connected(&vertices, &index));
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
        let index = EdgeIndex::new(&vertices, &edges);
        assert!(!is_connected(&vertices, &index));
    }

    fn create_test_edge_with_type(
        source: &str, 
        target: &str, 
        direction: RelationshipDirection,
        rel_type: Option<&str>
    ) -> PatternEdge {
        PatternEdge {
            identifier: format!("{}-{}", source, target),
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
        let index = EdgeIndex::new(&vertices, &edges);
        
        let spanning_tree = build_spanning_tree(&vertices, &edges, &index, 0).unwrap();
        
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
        let index = EdgeIndex::new(&vertices, &edges);
        
        let spanning_tree = build_spanning_tree(&vertices, &edges, &index, 0).unwrap();
        
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
        
        assert_eq!(traversal.content, "FOR b, a-b IN 1..1 OUTBOUND a._id FRIEND");
        assert_eq!(traversal.indent, 1);
        assert_eq!(traversal.exposed_variables, vec!["b", "a-b"]);
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
        
        assert_eq!(traversal.content, "FOR a, a-b IN 1..1 INBOUND b._id FRIEND");
        assert_eq!(traversal.indent, 1);
        assert_eq!(traversal.exposed_variables, vec!["a", "a-b"]);
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
        
        assert_eq!(traversal.content, "FOR b, a-b IN 1..1 ANY a._id FRIEND");
        assert_eq!(traversal.indent, 1);
        assert_eq!(traversal.exposed_variables, vec!["b", "a-b"]);
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
        let index = EdgeIndex::new(&vertices, &edges);
        
        let aql_lines = match_to_aql(&vertices, &edges, &index).unwrap();
        
        assert_eq!(aql_lines.len(), 2);
        assert_eq!(aql_lines[0].content, "FOR user IN vertices");
        assert_eq!(aql_lines[0].indent, 0);
        assert_eq!(aql_lines[0].exposed_variables, vec!["user"]);
        assert_eq!(aql_lines[1].content, "FOR friend, user-friend IN 1..1 OUTBOUND user._id FRIEND");
        assert_eq!(aql_lines[1].indent, 1);
        assert_eq!(aql_lines[1].exposed_variables, vec!["friend", "user-friend"]);
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
