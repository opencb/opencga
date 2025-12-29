# Tree Parser and Evaluator System

## Overview

The Tree Parser and Evaluator system provides a flexible way to parse and evaluate boolean query expressions with custom evaluation strategies. The system uses the **Strategy Pattern** through the `TreeEvaluator` interface, allowing different implementations for evaluating queries and combining results.

## Architecture

### Core Components

1. **TreeNode** (interface): Base interface for all tree nodes
2. **QueryTreeNode**: Leaf nodes representing individual queries
3. **OperatorTreeNode**: Internal nodes representing operators (AND, OR, NOT IN)
4. **TreeOperator** (enum): Valid operators with validation
5. **TreeParser**: Parses query strings into Abstract Syntax Trees (AST)
6. **TreeEvaluator** (interface): Strategy interface for custom evaluation logic

### Evaluator Implementations

1. **SetTreeEvaluator**: Default implementation that performs set operations (union, intersection, difference)
2. **LoggingTreeEvaluator**: Decorator that adds logging to any evaluator
3. Custom implementations: Create your own by implementing the `TreeEvaluator` interface

## TreeEvaluator Interface

The `TreeEvaluator` interface defines three methods:

```java
public interface TreeEvaluator {
    // Main evaluation method
    Set<String> evaluate(TreeNode node, Map<String, Set<String>> context);
    
    // Evaluate query nodes (leaf nodes)
    Set<String> evaluateQuery(QueryTreeNode queryNode, Map<String, Set<String>> context);
    
    // Evaluate operator nodes (combine left and right results)
    Set<String> evaluateOperator(OperatorTreeNode operatorNode, 
                                  Set<String> leftResult, 
                                  Set<String> rightResult);
}
```

## Usage

### Basic Usage with Default Evaluator

```java
// Create parser with default SetTreeEvaluator
TreeParser parser = new TreeParser();

// Parse query string
String query = "(query1 OR query2) AND query3";
TreeNode tree = parser.parse(query);

// Prepare data context
Map<String, Set<String>> data = new HashMap<>();
data.put("query1", Set.of("id1", "id2", "id3"));
data.put("query2", Set.of("id2", "id3", "id4"));
data.put("query3", Set.of("id3", "id4", "id5"));

// Evaluate
Set<String> result = parser.evaluate(tree, data);
// Result: [id3, id4] (queries 1 OR 2 gives {id1,id2,id3,id4}, AND with query3 gives {id3,id4})
```

### Using Custom Evaluator

```java
// Create parser with custom evaluator
TreeEvaluator customEvaluator = new MyCustomEvaluator();
TreeParser parser = new TreeParser(customEvaluator);

// Parse and evaluate as before
TreeNode tree = parser.parse(query);
Set<String> result = parser.evaluate(tree, data);
```

### Using LoggingTreeEvaluator

```java
// Wrap any evaluator with logging
TreeEvaluator loggingEvaluator = new LoggingTreeEvaluator(new SetTreeEvaluator());
TreeParser parser = new TreeParser(loggingEvaluator);

// Evaluation will now log each step
TreeNode tree = parser.parse(query);
Set<String> result = parser.evaluate(tree, data);
```

## Creating Custom Evaluators

To create a custom evaluator, implement the `TreeEvaluator` interface:

```java
public class CustomEvaluator implements TreeEvaluator {
    
    @Override
    public Set<String> evaluate(TreeNode node, Map<String, Set<String>> context) {
        if (node instanceof QueryTreeNode) {
            return evaluateQuery((QueryTreeNode) node, context);
        }
        
        OperatorTreeNode opNode = (OperatorTreeNode) node;
        Set<String> left = evaluate(opNode.getLeft(), context);
        Set<String> right = evaluate(opNode.getRight(), context);
        
        return evaluateOperator(opNode, left, right);
    }
    
    @Override
    public Set<String> evaluateQuery(QueryTreeNode queryNode, Map<String, Set<String>> context) {
        // Custom query evaluation logic
        String queryName = queryNode.getValue();
        // ... your implementation
    }
    
    @Override
    public Set<String> evaluateOperator(OperatorTreeNode operatorNode, 
                                         Set<String> leftResult, 
                                         Set<String> rightResult) {
        // Custom operator evaluation logic
        TreeOperator operator = operatorNode.getOperator();
        // ... your implementation
    }
}
```

## Valid Operators

- **AND**: Intersection of two sets
- **OR**: Union of two sets
- **NOT IN**: Difference (items in left but not in right)

## Query Syntax

- Parentheses for grouping: `(query1 OR query2)`
- Case-insensitive operators: `AND`, `and`, `OR`, `or`, `NOT IN`, `not in`
- Operator precedence: `NOT IN` > `AND` > `OR`
- Examples:
  - `query1 AND query2`
  - `(query1 OR query2) AND query3`
  - `query1 NOT IN query2`
  - `((query1 OR query2) AND query3) NOT IN query4`

## Benefits of the Interface Design

1. **Flexibility**: Easily swap evaluation strategies without changing the parser
2. **Testability**: Mock evaluators for unit testing
3. **Extensibility**: Create custom evaluators for different use cases (e.g., database queries, API calls)
4. **Separation of Concerns**: Parsing logic is separate from evaluation logic
5. **Decorator Pattern**: Wrap evaluators (e.g., logging, caching, metrics)

## Example Use Cases

1. **SetTreeEvaluator**: Default implementation for in-memory set operations
2. **DatabaseTreeEvaluator**: Execute queries against a database
3. **CachingTreeEvaluator**: Cache query results to improve performance
4. **LoggingTreeEvaluator**: Debug query evaluation
5. **MetricsTreeEvaluator**: Collect performance metrics
6. **SecurityTreeEvaluator**: Add authorization checks

## See Also

- `TreeParserExample.java`: Complete working example
- `TreeOperator.java`: Operator enum with validation
- `SetTreeEvaluator.java`: Default implementation reference

