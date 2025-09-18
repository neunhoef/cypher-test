use crate::cypher::{PatternVertex, PatternEdge, RelationshipDirection};
use std::collections::{HashMap, HashSet, VecDeque};

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
