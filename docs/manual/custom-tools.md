# Custom Tools

## Overview

Custom Tools in OpenCGA allow users to register and execute custom Docker-based analysis tools without writing Java code. This feature enables integration of bioinformatics tools, scripts, and pipelines through a simple configuration interface.

## Key Concepts

### Custom Tool Definition

A Custom Tool consists of:

1. **Tool Metadata**: ID, name, description, and scope
2. **Docker Container Configuration**: Container image, tag, and command line template
3. **Variables**: Optional parameters that define the tool's inputs, outputs, and options
4. **Version**: Optional version identifier for tool versioning

### Docker Container

The container definition specifies:
- **name**: Docker image name (e.g., `ubuntu`, `biocontainers/samtools`)
- **tag**: Image tag (e.g., `latest`, `1.9`)
- **digest**: Optional SHA256 digest for specific image version
- **commandLine**: Command line template with variable placeholders
- **user**: Optional Docker registry username (for private images)
- **password**: Optional Docker registry password (for private images)

### Variables

Variables define the parameters that can be passed to the tool. Each variable has:

- **id**: Variable identifier (e.g., `inputFile`, `--threads`, `-q`)
- **name**: Pretty name of the variable. Used by interactive browsers such as IVA
- **description**: Human-readable description
- **type**: Variable type (e.g., `STRING`, `FLAG`, `FILE`)
- **required**: Whether the parameter is mandatory (`true`/`false`)
- **defaultValue**: Default value if not provided by the user
- **output**: Whether this is an output parameter

## Command Line Processing

OpenCGA processes the command line in two main steps:

### Step 1: Template Variable Substitution

Variables enclosed in `${variableName}` in the command line template are directly substituted with their values.

**Example:**
```bash
# Template
cat ${inputFile} > ${outputFile}

# With params: inputFile="/data/input.txt", outputFile="/data/output.txt"
# Result
cat /data/input.txt > /data/output.txt
```

**Key points:**
- Template variables are replaced first, before any other processing
- If a variable is in the template but not provided, OpenCGA checks if it has a default value
- If no default exists and the variable is required, an error is thrown
- Variables used in template substitution are NOT appended again at the end

### Step 2: Parameter Appending

After template substitution, remaining parameters (not used in the template) are appended to the command line.

**Two scenarios:**

#### 2a. Parameter Matches a Variable Definition

When a parameter key matches a variable ID (after removing dashes), the variable's ID is used as the CLI flag.

**Example:**
```bash
# Variable definition: id="--threads"
# Parameter provided: "threads": "4"
# Appended as: --threads 4

# Variable definition: id="-q"
# Parameter provided: "q": "30"
# Appended as: -q 30
```

#### 2b. Parameter Doesn't Match Any Variable

When a parameter key doesn't match any variable, the key itself is used as the CLI flag.

**Example:**
```bash
# No variable defined for "custom-flag"
# Parameter provided: "custom-flag": "value"
# Appended as: --custom-flag value

# Parameter provided: "x": "value"
# Appended as: -x value
```

**Auto-prefix rules:**
- If the key already starts with `-`, it's used as-is
- If the key is a single character, a single dash `-` is prepended
- If the key has multiple characters, double dashes `--` are prepended

## Variable Behavior Matrix

Understanding how variables behave based on their configuration:

| Required | Has Default | Behavior |
|----------|-------------|----------|
| `true` | `true` | Uses default if not provided; appends with default value |
| `true` | `false` | Must be provided; throws error if missing |
| `false` | `true` | Uses default if not provided; NOT appended (optional with default) |
| `false` | `false` | Only appended if explicitly provided |

**Important:** Required parameters with default values are always appended to the command line (even if not provided by the user), while optional parameters with default values are NOT appended unless explicitly provided.

## Special Variables

### Output Variables

Output variables use special placeholders that are automatically resolved:

- **`$OUTPUT`**: Replaced with the job's output directory path
- **`$JOB_OUTPUT`**: Alias for `$OUTPUT`
- **`file://`**: OpenCGA file URI that gets resolved to actual file path

**Example:**
```bash
# Parameter: "outputFile": "$OUTPUT/results.txt"
# Resolved to: /path/to/job/output/results.txt
```

### File References

OpenCGA automatically handles file references:

- **`file://path/to/file`**: OpenCGA catalog file reference
- Files are automatically mounted into the Docker container
- File paths are resolved to their actual storage locations
- Files containing references to other files are processed recursively

**Example:**
```bash
# Parameter: "inputFile": "file://data/sample.vcf"
# OpenCGA resolves to actual file path and mounts it
# Result: /opencga/storage/path/sample.vcf (accessible in container)
```

## Complete Example

Here's a comprehensive example demonstrating all features:

### Tool Registration

```java
List<ExternalToolVariable> variables = Arrays.asList(
    // Required input (used in template)
    new ExternalToolVariable()
        .setId("inputBam")
        .setRequired(true)
        .setDescription("Input BAM file"),
    
    // Required output (used in template)
    new ExternalToolVariable()
        .setId("outputBam")
        .setRequired(true)
        .setDescription("Output BAM file"),
    
    // Required with default (will be appended)
    new ExternalToolVariable()
        .setId("--threads")
        .setRequired(true)
        .setDefaultValue("1")
        .setDescription("Number of threads"),
    
    // Optional with default (NOT appended unless provided)
    new ExternalToolVariable()
        .setId("--format")
        .setRequired(false)
        .setDefaultValue("BAM")
        .setDescription("Output format"),
    
    // Optional without default (only appended if provided)
    new ExternalToolVariable()
        .setId("-q")
        .setRequired(false)
        .setDescription("Minimum mapping quality")
);

Container container = new Container()
    .setName("biocontainers/samtools")
    .setTag("1.9")
    .setCommandLine("samtools view ${inputBam} -o ${outputBam}");

CustomToolCreateParams tool = new CustomToolCreateParams()
    .setId("samtools-view")
    .setName("SAMtools View")
    .setDescription("Filter and convert SAM/BAM files")
    .setScope(ExternalToolScope.OTHER)
    .setContainer(container)
    .setVariables(variables);

// Register tool
catalogManager.getExternalToolManager().createCustomTool(
    studyFqn, tool, QueryOptions.empty(), token);
```

### Tool Execution

```java
Map<String, String> params = new HashMap<>();
params.put("inputBam", "file://data/sample.bam");  // Template substitution
params.put("outputBam", "$OUTPUT/filtered.bam");   // Template substitution
params.put("threads", "8");                         // Override default, append as: --threads 8
params.put("q", "30");                              // Append as: -q 30
// "format" not provided, will NOT be appended (optional with default)

CustomToolRunParams runParams = new CustomToolRunParams(null, params);
CustomExternalToolParams execParams = new CustomExternalToolParams("samtools-view", null, runParams);

// Execute tool
CustomToolExecutor executor = new CustomToolExecutor();
executor.setUp(opencgaHome, catalogManager, storageFactory, 
    execParams.toObjectMap(), outDir, studyFqn, jobId, false, token);
executor.start();
```

### Resulting Command Line

```bash
samtools view /storage/path/sample.bam -o /job/output/filtered.bam --threads 8 -q 30
```

**Note:**
- `${inputBam}` and `${outputBam}` were substituted in the template
- `--threads 8` was appended (required with default, value provided)
- `-q 30` was appended (optional without default, value provided)
- `--format BAM` was NOT appended (optional with default, not provided)

## Advanced Features

### Custom Command Line Override

You can override the entire command line at execution time:

```java
CustomToolRunParams runParams = new CustomToolRunParams()
    .setCommandLine("samtools sort input.bam -o output.sorted.bam");
```

This completely replaces the tool's default command line template.

### Variable Matching Logic

OpenCGA matches parameter keys to variable IDs by removing dash prefixes:

- Variable ID: `--input-file` → matches key: `input-file`
- Variable ID: `-t` → matches key: `t`
- Variable ID: `output` → matches key: `output`

This allows flexible parameter specification by users.

### Docker Registry Authentication

For private Docker images:

```java
Container container = new Container()
    .setName("myregistry.com/private-tool")
    .setTag("1.0")
    .setCommandLine("tool --input ${input}")
    .setUser("registry-username")
    .setPassword("registry-password");
```

### File Type Detection

OpenCGA automatically detects and handles:
- Input files that need to be mounted
- Output directories that need to be created
- Files containing references to other files (e.g., VCF with reference genome paths)

## Best Practices

### 1. Use Descriptive Variable IDs

```java
// Good
.setId("--input-file")
.setId("--output-dir")

// Less clear
.setId("in")
.setId("out")
```

### 2. Provide Default Values for Common Parameters

```java
new ExternalToolVariable()
    .setId("--threads")
    .setRequired(false)
    .setDefaultValue("1")
    .setDescription("Number of threads")
```

### 3. Mark Required Parameters Correctly

Only mark parameters as required if they are truly mandatory for the tool to run.

### 4. Use Template Variables for Core I/O

Put essential input/output parameters in the command line template:

```bash
# Good
tool process ${inputFile} --output ${outputFile}

# Less flexible
tool process
```

### 5. Document Your Tools

Provide clear descriptions for the tool and all variables to help users understand usage.

### 6. Test with Docker Locally

Before registering a tool, test the Docker command locally:

```bash
docker run biocontainers/samtools:1.9 samtools view --help
```

### 7. Handle Paths Correctly

Use OpenCGA file URIs for catalog files:
- `file://data/sample.bam` - catalog file
- Use `$OUTPUT` for output directories

### 8. Version Your Tools

Use specific Docker tags instead of `latest`:

```java
.setTag("1.9")  // Good
.setTag("latest")  // Less predictable
```

## Troubleshooting

### Error: "Missing mandatory parameter"

**Cause:** A required variable without a default value was not provided.

**Solution:** Either provide the parameter at execution time or add a default value to the variable.

### Error: "Custom tool not found"

**Cause:** The tool ID doesn't exist in the catalog.

**Solution:** Verify the tool was registered correctly and use the exact tool ID.

### Command line doesn't include expected parameter

**Cause:** Optional parameters with default values are not appended unless explicitly provided.

**Solution:** Either:
- Provide the parameter explicitly at execution time
- Mark the parameter as required if it should always be included
- Include it in the command line template: `${variableName}`

### File not found in container

**Cause:** File path not properly resolved or mounted.

**Solution:** Use OpenCGA file URIs (`file://path`) instead of absolute paths.

### Docker image not found

**Cause:** Image name/tag incorrect or registry authentication required.

**Solution:**
- Verify image exists: `docker pull imagename:tag`
- Add registry credentials if using private images

## API Reference

### CustomToolCreateParams

```java
CustomToolCreateParams tool = new CustomToolCreateParams()
    .setId("tool-id")              // Unique tool identifier
    .setName("Tool Name")          // Display name
    .setDescription("Description") // Tool description
    .setScope(ExternalToolScope.OTHER)  // Tool scope
    .setContainer(container)       // Container configuration
    .setVariables(variables)       // List of variables
    .setVersion("1.0");           // Optional version
```

### CustomExternalToolParams

```java
CustomExternalToolParams params = new CustomExternalToolParams(
    "tool-id",                     // Tool ID
    "1.0",                        // Optional version
    new CustomToolRunParams()      // Execution parameters
        .setCommandLine("...")     // Optional CLI override
        .setParams(paramMap)       // Parameter values
);
```

### ExternalToolVariable

```java
ExternalToolVariable variable = new ExternalToolVariable()
    .setId("--parameter-name")      // Variable identifier
    .setRequired(true)              // Is required
    .setDefaultValue("default")     // Default value
    .setDescription("Description")  // Description
    .setType(WorkflowVariableType.STRING)  // Variable type
    .setOutput(false);              // Is output parameter
```

## Examples

### Example 1: Simple Echo Tool

```java
Container container = new Container()
    .setName("ubuntu")
    .setTag("latest")
    .setCommandLine("echo 'Hello World'");

CustomToolCreateParams tool = createCustomTool(
    "echo-tool", "Simple echo", container, null);
```

### Example 2: Tool with Parameters

```java
List<ExternalToolVariable> variables = Arrays.asList(
    new ExternalToolVariable()
        .setId("message")
        .setRequired(true)
        .setDescription("Message to echo")
);

Container container = new Container()
    .setName("ubuntu")
    .setTag("latest")
    .setCommandLine("echo '${message}'");

CustomToolCreateParams tool = createCustomTool(
    "echo-message", "Echo with message", container, variables);

// Execution
Map<String, String> params = new HashMap<>();
params.put("message", "Hello OpenCGA!");
```

### Example 3: File Processing Tool

```java
List<ExternalToolVariable> variables = Arrays.asList(
    new ExternalToolVariable()
        .setId("inputFile")
        .setRequired(true)
        .setDescription("Input file"),
    new ExternalToolVariable()
        .setId("outputFile")
        .setRequired(true)
        .setDescription("Output file")
);

Container container = new Container()
    .setName("ubuntu")
    .setTag("latest")
    .setCommandLine("cat ${inputFile} > ${outputFile}");

// Execution
Map<String, String> params = new HashMap<>();
params.put("inputFile", "file://data/input.txt");
params.put("outputFile", "$OUTPUT/output.txt");
```

### Example 4: Complex Bioinformatics Tool

```java
List<ExternalToolVariable> variables = Arrays.asList(
    new ExternalToolVariable()
        .setId("input")
        .setRequired(true)
        .setDescription("Input VCF file"),
    new ExternalToolVariable()
        .setId("--threads")
        .setRequired(true)
        .setDefaultValue("4")
        .setDescription("Number of threads"),
    new ExternalToolVariable()
        .setId("--min-af")
        .setRequired(false)
        .setDefaultValue("0.01")
        .setDescription("Minimum allele frequency"),
    new ExternalToolVariable()
        .setId("--quality")
        .setRequired(false)
        .setDescription("Minimum quality score")
);

Container container = new Container()
    .setName("biocontainers/bcftools")
    .setTag("1.9")
    .setCommandLine("bcftools view ${input}");

// Execution
Map<String, String> params = new HashMap<>();
params.put("input", "file://variants/sample.vcf.gz");
params.put("threads", "16");      // Override default
params.put("quality", "30");      // Optional, will be appended
// min-af not provided, will NOT be appended (optional with default)
```

## Conclusion

Custom Tools provide a powerful and flexible way to integrate external tools into OpenCGA without writing custom Java code. By understanding the command line processing logic and variable behavior, you can create sophisticated analysis pipelines that leverage the full power of Docker-based bioinformatics tools.

