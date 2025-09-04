# Cypher Test Suite

A comprehensive Rust project for parsing and analyzing Cypher queries using the `libcypher-parser-sys` crate. This project provides AST (Abstract Syntax Tree) printing functionality and includes an extensive test suite for validating Cypher query parsing across various scenarios.

## Overview

This project demonstrates how to use the `libcypher-parser-sys` crate to parse Cypher queries and work with their Abstract Syntax Trees in Rust. It includes both basic and enhanced AST printing capabilities, comprehensive error handling, and a robust test suite covering various Cypher query patterns.

## Features

- **Cypher Query Parsing**: Parse Cypher queries using the official libcypher-parser C library
- **AST Visualization**: Print and analyze Abstract Syntax Trees with both basic and enhanced formatting
- **Comprehensive Testing**: 15+ unit tests covering various query types and edge cases
- **Error Handling**: Robust error detection and reporting for malformed queries
- **Memory Safety**: Proper memory management for C library resources
- **AWS Query Support**: Specialized support for AWS resource relationship queries

## Project Structure

```
cypher-test/
├── src/
│   ├── main.rs          # Main application entry point
│   └── cypher.rs        # AST printing and parsing utilities
├── queries/             # Sample Cypher query files
│   ├── query_1.cyp      # AWS S3 bucket queries
│   ├── query_2.cyp      # EC2-EBS relationship queries
│   └── ...              # Additional test queries
├── docs/
│   └── ast.md           # Detailed AST usage documentation
├── run_tests.sh         # Test suite runner script
├── TEST_SUITE_README.md # Comprehensive test documentation
├── Cargo.toml           # Project dependencies
└── README.md            # This file
```

## Dependencies

- **libcypher-parser-sys**: Rust bindings for the libcypher-parser C library
- **Rust Edition**: 2024

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd cypher-test
   ```

2. **Install dependencies**:
   ```bash
   cargo build
   ```

3. **Run tests**:
   ```bash
   cargo test
   ```

## Usage

### Basic Usage

Parse a Cypher query file and display its AST:

```bash
cargo run -- queries/query_1.cyp
```

### Programmatic Usage

```rust
use std::ffi::CString;
use libcypher_parser_sys::*;

// Parse a Cypher query
let query = "MATCH (n:Person) RETURN n";
let c_query = CString::new(query).unwrap();
let config = unsafe { cypher_parser_new_config() };
let result = unsafe {
    cypher_uparse(
        c_query.as_ptr().cast::<c_char>(),
        c_query.as_bytes().len() as u64,
        ptr::null_mut(),
        config,
        u64::from(CYPHER_PARSE_DEFAULT),
    )
};

// Print AST structure
print_ast_from_result_enhanced(result);

// Clean up
unsafe {
    cypher_parser_config_free(config);
    cypher_parse_result_free(result);
}
```

## API Reference

### Main Functions

#### `print_ast(node, indent)`
Prints a basic AST structure with indentation.

#### `print_ast_enhanced(node, indent)`
Prints an enhanced AST structure with type information and detailed formatting.

#### `print_ast_from_result(result)`
Prints AST structure starting from a parse result (basic version).

#### `print_ast_from_result_enhanced(result)`
Prints enhanced AST structure starting from a parse result.

### Error Handling

The parser provides comprehensive error reporting:

```rust
let n_errors = unsafe { cypher_parse_result_nerrors(result) };
if n_errors > 0 {
    for i in 0..n_errors {
        let error = unsafe { cypher_parse_result_get_error(result, i) };
        let position = unsafe { cypher_parse_error_position(error) };
        println!("Error at line {}, column {}", position.line, position.column);
    }
}
```

## Test Suite

The project includes a comprehensive test suite with 15+ tests covering:

### Test Categories

1. **Null Pointer Handling** (4 tests)
   - Tests graceful handling of null nodes and results

2. **Simple Query Tests** (2 tests)
   - Basic MATCH queries and empty query handling

3. **Complex Query Tests** (3 tests)
   - AWS resource queries with properties and relationships

4. **AWS-Specific Queries** (2 tests)
   - Administrator role and Lambda function queries

5. **Error Handling** (1 test)
   - Invalid Cypher syntax handling

6. **AST Structure Tests** (3 tests)
   - Node traversal, type handling, and multiple statements

### Running Tests

```bash
# Run all tests
cargo test

# Run with verbose output
cargo test -- --nocapture

# Run specific test categories
cargo test test_simple_match_query
cargo test test_complex_match_with_properties

# Run the comprehensive test suite
./run_tests.sh
```

## Sample Queries

The project includes various sample Cypher queries in the `queries/` directory:

### AWS S3 Bucket Query
```cypher
MATCH path = (n_0:aws_s3_bucket {org_id: 2, to_delete: false, default_encryption_type: 'aws:kms'}) 
RETURN path;
```

### EC2-EBS Relationship Query
```cypher
MATCH path = (n_0:aws_ec2_instance {org_id: 2, to_delete: false}) 
<-[r_0:ATTACHES_TO]- (n_1:aws_ebs_volume {org_id: 2, to_delete: false, encrypted:false}) 
RETURN path;
```

### Complex IAM Path Query
```cypher
MATCH path = (n_0:aws_ec2_instance {org_id: 2, to_delete: false, entity_info_status:1, is_publicly_accessible: true}) 
-[r_0:ASSOCIATES]-> (n_0_0:aws_iam_instance_profile {org_id: 2, to_delete: false, entity_info_status:1}) 
-[r_0_0:ASSUMES]-> (n_0_0_0:aws_iam_role {org_id: 2, to_delete: false, entity_info_status:1}) 
-[r_0_0_0:CAN_ACCESS]-> (n_0_0_0_0:aws_s3_bucket {org_id: 2, to_delete: false, entity_info_status:1, sensitive_data: true}) 
RETURN path;
```

## AST Output Examples

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

## Memory Management

The project follows Rust best practices for memory safety:

- **Automatic Cleanup**: Parse results are automatically freed
- **Null Pointer Checks**: All C library interactions include null checks
- **Safe String Conversion**: Proper handling of C strings to Rust strings
- **Resource Management**: Parser configurations are properly freed

## Documentation

- **AST Guide**: See `docs/ast.md` for detailed AST usage documentation
- **Test Documentation**: See `TEST_SUITE_README.md` for comprehensive test information
- **API Reference**: Inline documentation in source code

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- **libcypher-parser**: The underlying C library for Cypher parsing
- **libcypher-parser-sys**: Rust bindings for the parser
- **Neo4j**: For the Cypher query language specification

## Troubleshooting

### Common Issues

1. **Build Errors**: Ensure you have Rust 1.70+ installed
2. **Parse Errors**: Check that your Cypher syntax is valid
3. **Memory Issues**: Ensure proper cleanup of parse results

### Getting Help

- Check the test suite for usage examples
- Review the documentation in `docs/ast.md`
- Run tests with verbose output: `cargo test -- --nocapture`

## Future Enhancements

- [ ] Query optimization suggestions
- [ ] AST transformation utilities
- [ ] Query validation rules
- [ ] Performance benchmarking
- [ ] Additional query language support
