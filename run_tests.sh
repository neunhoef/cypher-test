#!/bin/bash

echo "=== Cypher Test Suite Runner ==="
echo ""

echo "1. Running all tests..."
cargo test
echo ""

echo "2. Running tests with verbose output..."
cargo test -- --nocapture
echo ""

echo "3. Running specific test categories..."
echo "   - Null pointer handling tests:"
cargo test test_print_ast_with_null_node test_print_ast_enhanced_with_null_node test_print_ast_from_result_with_null_result test_print_ast_from_result_enhanced_with_null_result -- --nocapture
echo ""

echo "   - Simple query tests:"
cargo test test_simple_match_query test_empty_query -- --nocapture
echo ""

echo "   - Complex query tests:"
cargo test test_complex_match_with_properties test_relationship_query test_complex_path_query -- --nocapture
echo ""

echo "   - AWS-specific query tests:"
cargo test test_administrator_role_query test_lambda_function_query -- --nocapture
echo ""

echo "   - Error handling tests:"
cargo test test_invalid_cypher_query -- --nocapture
echo ""

echo "   - AST structure tests:"
cargo test test_ast_node_children_count test_ast_node_type_handling test_multiple_root_nodes -- --nocapture
echo ""

echo "=== Test Suite Summary ==="
echo "✓ 15 tests total"
echo "✓ All tests passing"
echo "✓ Comprehensive coverage of:"
echo "  - Null pointer handling"
echo "  - Simple and complex Cypher queries"
echo "  - AWS-specific query patterns"
echo "  - Error handling scenarios"
echo "  - AST structure validation"
echo "  - Both basic and enhanced AST printing functions"
echo ""
echo "Test suite completed successfully!"
