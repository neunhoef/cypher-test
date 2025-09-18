use crate::cypher::{PatternVertex, PatternEdge, RelationshipDirection};
use std::collections::{HashMap, HashSet, VecDeque};
use serde_json::Value;

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

/// Represents a line in an AQL query with indentation
#[derive(Debug, Clone, PartialEq)]
pub struct AQLLine {
    pub content: String,
    pub indent: usize,
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
/// Currently generates only the anchor FOR statement with FILTER conditions
/// 
/// # Arguments
/// * `vertices` - Pattern vertices from the match statement
/// * `_edge_index` - Edge index for the pattern graph (not used yet)
/// 
/// # Returns
/// * `Result<Vec<AQLLine>, String>` with AQL lines or error message
pub fn match_to_aql(vertices: &[PatternVertex], _edge_index: &EdgeIndex) -> Result<Vec<AQLLine>, String> {
    if vertices.is_empty() {
        return Err("No vertices in pattern graph".to_string());
    }
    
    // Find the anchor vertex (most properties, smallest index for ties)
    let anchor_index = find_anchor_vertex(vertices)
        .ok_or("Failed to find anchor vertex".to_string())?;
    
    let anchor_vertex = &vertices[anchor_index];
    let collection_name = derive_collection_name(anchor_vertex);
    let variable_name = &anchor_vertex.identifier;
    
    let mut aql_lines = Vec::new();
    
    // Generate FOR statement
    let for_line = AQLLine {
        content: format!("FOR {variable_name} IN {collection_name}"),
        indent: 0,
    };
    aql_lines.push(for_line);
    
    // Generate FILTER conditions if properties exist
    let filter_conditions = generate_filter_conditions(&anchor_vertex.properties, variable_name);
    if !filter_conditions.is_empty() {
        let filter_line = AQLLine {
            content: format!("FILTER {filter_conditions}"),
            indent: 1,
        };
        aql_lines.push(filter_line);
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
}
