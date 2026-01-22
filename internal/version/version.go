// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Build version information set via ldflags.

package version

// Build information. These are set via ldflags at build time.
var (
	// Version is the semantic version (e.g., "0.1.0")
	Version = "dev"

	// Commit is the git commit SHA
	Commit = "unknown"

	// Date is the build date in ISO 8601 format
	Date = "unknown"
)

// Info returns all version information as a struct.
func Info() BuildInfo {
	return BuildInfo{
		Version: Version,
		Commit:  Commit,
		Date:    Date,
	}
}

// BuildInfo contains version and build metadata.
type BuildInfo struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
}
