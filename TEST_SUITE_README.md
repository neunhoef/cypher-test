# Cypher Test Suite

This document describes the comprehensive unit test suite for the `cypher.rs` module, which provides AST (Abstract Syntax Tree) printing functionality for Cypher queries.

## Overview

The test suite includes **15 comprehensive tests** that cover all public functions in the `cypher.rs` module, using real Cypher queries from the `queries/` directory and additional test cases.

## Test Categories

### 1. Null Pointer Handling Tests (4 tests)
- `test_print_ast_with_null_node` - Tests `print_ast()` with null nodes
- `test_print_ast_enhanced_with_null_node` - Tests `print_ast_enhanced()` with null nodes  
- `test_print_ast_from_result_with_null_result` - Tests `print_ast_from_result()` with null results
- `test_print_ast_from_result_enhanced_with_null_result` - Tests `print_ast_from_result_enhanced()` with null results

### 2. Simple Query Tests (2 tests)
- `test_simple_match_query` - Tests basic `MATCH (n) RETURN n` query
- `test_empty_query` - Tests handling of empty queries

### 3. Complex Query Tests (3 tests)
- `test_complex_match_with_properties` - Tests AWS S3 bucket query with properties
- `test_relationship_query` - Tests EC2 instance to EBS volume relationship query
- `test_complex_path_query` - Tests multi-hop path query with IAM roles and S3 access

### 4. AWS-Specific Query Tests (2 tests)
- `test_administrator_role_query` - Tests EC2 instance with administrator IAM role query
- `test_lambda_function_query` - Tests Lambda function with administrator IAM role query

### 5. Error Handling Tests (1 test)
- `test_invalid_cypher_query` - Tests handling of malformed Cypher syntax

### 6. AST Structure Tests (3 tests)
- `test_ast_node_children_count` - Tests AST node children iteration
- `test_ast_node_type_handling` - Tests AST node type information extraction
- `test_multiple_root_nodes` - Tests multiple statement parsing

## Test Features

### Helper Functions
- `parse_cypher_query(query: &str)` - Parses a Cypher query and returns parse result
- `cleanup_parse_result(result)` - Safely cleans up parse result memory

### Test Coverage
- **Function Coverage**: All 4 public functions tested
- **Edge Cases**: Null pointers, empty queries, invalid syntax
- **Real Data**: Uses actual Cypher queries from the project
- **Memory Safety**: Proper cleanup of C library resources
- **Error Handling**: Graceful handling of parsing errors

### Query Examples Used
The tests use a variety of Cypher queries including:

1. **Simple Match**: `MATCH (n) RETURN n`
2. **AWS S3 Bucket**: `MATCH path = (n_0:aws_s3_bucket {org_id: 2, to_delete: false, default_encryption_type: 'aws:kms'}) RETURN path`
3. **EC2-EBS Relationship**: `MATCH path = (n_0:aws_ec2_instance {org_id: 2, to_delete: false}) <-[r_0:ATTACHES_TO]- (n_1:aws_ebs_volume {org_id: 2, to_delete: false, encrypted:false}) RETURN path`
4. **Complex IAM Path**: Multi-hop queries involving EC2 instances, IAM profiles, roles, and S3 buckets
5. **Lambda Functions**: AWS Lambda function queries with IAM role associations

## Running the Tests

### Run All Tests
```bash
cargo test
```

### Run with Verbose Output
```bash
cargo test -- --nocapture
```

### Run Specific Test Categories
```bash
# Null pointer handling
cargo test test_print_ast_with_null_node test_print_ast_enhanced_with_null_node

# Simple queries
cargo test test_simple_match_query test_empty_query

# Complex queries
cargo test test_complex_match_with_properties test_relationship_query

# AWS-specific queries
cargo test test_administrator_role_query test_lambda_function_query

# Error handling
cargo test test_invalid_cypher_query

# AST structure tests
cargo test test_ast_node_children_count test_ast_node_type_handling
```

### Run Test Suite Script
```bash
./run_tests.sh
```

## Test Output

The tests demonstrate both basic and enhanced AST printing:

### Basic AST Output
```
=== AST Structure (1 root nodes) ===
--- Root Node 0 ---
statement (1 children)
  query (2 children)
    MATCH (1 children)
      pattern (1 children)
        pattern path (1 children)
          node pattern (1 children)
            identifier (0 children)
    RETURN (1 children)
      projection (1 children)
        identifier (0 children)
```

### Enhanced AST Output
```
=== Enhanced AST Structure (1 root nodes) ===
--- Root Node 0 ---
statement (type: 0, children: 1)
  query (type: 13, children: 2)
    MATCH (type: 28, children: 1)
      pattern (type: 95, children: 1)
        pattern path (type: 98, children: 1)
          node pattern (type: 99, children: 1)
            identifier (type: 80, children: 0)
    RETURN (type: 53, children: 1)
      projection (type: 54, children: 1)
        identifier (type: 80, children: 0)
```

## Dependencies

The test suite requires:
- `libcypher-parser-sys` crate for Cypher parsing
- Rust standard library for C string handling and memory management

## Memory Safety

All tests include proper memory management:
- Parse results are cleaned up after each test
- C strings are properly converted and managed
- Null pointer checks prevent segmentation faults
- Unsafe code is properly contained and tested

## Assertions

The tests include meaningful assertions:
- Parse result validation (non-null, error count)
- Root node count validation
- Node type validation
- Children count validation
- String content validation

This comprehensive test suite ensures the reliability and robustness of the Cypher AST printing functionality across a wide range of query types and edge cases.
