#!/bin/bash
# Script to run CodeQL analysis locally

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
CODEQL_DB_PATH="./codeql-db"
RESULTS_DIR="./codeql-results"
QUERY_SUITE="security-and-quality"

echo -e "${GREEN}Starting CodeQL Analysis...${NC}"

# Check if CodeQL CLI is installed
if ! command -v codeql &>/dev/null; then
    echo -e "${RED}CodeQL CLI is not installed. Please install it first.${NC}"
    echo "Download from: https://github.com/github/codeql-cli-binaries/releases"
    exit 1
fi

# Create results directory
mkdir -p "$RESULTS_DIR"

# Step 1: Create CodeQL database
echo -e "${YELLOW}Creating CodeQL database...${NC}"
codeql database create "$CODEQL_DB_PATH" \
    --language=go \
    --source-root=. \
    --overwrite

# Step 2: Run analysis
echo -e "${YELLOW}Running CodeQL analysis...${NC}"
codeql database analyze "$CODEQL_DB_PATH" \
    --format=sarif-latest \
    --output="$RESULTS_DIR/results.sarif" \
    --download \
    "$QUERY_SUITE"

# Step 3: Generate human-readable report
echo -e "${YELLOW}Generating human-readable report...${NC}"
codeql database analyze "$CODEQL_DB_PATH" \
    --format=csv \
    --output="$RESULTS_DIR/results.csv" \
    "$QUERY_SUITE"

# Step 4: Run custom queries if they exist
if [ -d ".github/codeql" ]; then
    echo -e "${YELLOW}Running custom queries...${NC}"

    # Run individual custom queries
    codeql database analyze "$CODEQL_DB_PATH" \
        --format=sarif-latest \
        --output="$RESULTS_DIR/custom-results.sarif" \
        .github/codeql/*.ql

    # Run custom query suite if it exists
    if [ -f ".github/codeql/go-custom-security.qls" ]; then
        echo -e "${YELLOW}Running custom query suite...${NC}"
        codeql database analyze "$CODEQL_DB_PATH" \
            --format=sarif-latest \
            --output="$RESULTS_DIR/custom-suite-results.sarif" \
            .github/codeql/go-custom-security.qls
    fi
fi

echo -e "${GREEN}CodeQL analysis complete!${NC}"
echo "Results available in: $RESULTS_DIR"
echo "- SARIF format: $RESULTS_DIR/results.sarif"
echo "- CSV format: $RESULTS_DIR/results.csv"

# Optional: Open results in VS Code if available
if command -v code &>/dev/null; then
    echo -e "${YELLOW}Opening results in VS Code...${NC}"
    code "$RESULTS_DIR/results.sarif"
fi
