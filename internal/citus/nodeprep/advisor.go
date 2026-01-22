// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Node preparation advisor for pre-flight cluster checks.

package nodeprep

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Advisor performs pre-flight checks for adding a node to Citus.
type Advisor struct {
	coordinatorPool *pgxpool.Pool
}

// NewAdvisor creates a new node preparation advisor.
func NewAdvisor(coordinatorPool *pgxpool.Pool) *Advisor {
	return &Advisor{
		coordinatorPool: coordinatorPool,
	}
}

// Run performs all prerequisite checks for adding a node.
func (a *Advisor) Run(ctx context.Context, input Input) (Output, error) {
	output := Output{
		Checks:   []CheckResult{},
		Warnings: []string{},
	}

	// Build connection string for new node
	dsn := input.ConnectionString
	if dsn == "" {
		sslMode := input.SSLMode
		if sslMode == "" {
			sslMode = "prefer"
		}
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			input.User, input.Password, input.Host, input.Port, input.Database, sslMode)
	}

	// Try to connect to new node
	newNodePool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		output.ConnectionError = fmt.Sprintf("Failed to connect to new node: %v", err)
		output.Checks = append(output.Checks, CheckResult{
			Category: CategoryConnection,
			Check:    "connection",
			Name:     "New Node Connection",
			Status:   StatusFailed,
			Message:  output.ConnectionError,
			Fix: &Fix{
				Description: "Ensure PostgreSQL is running and accessible",
				Commands: []Command{
					{Type: CommandTypeBash, Target: TargetNewNode, Command: "systemctl status postgresql", Note: "Check if PostgreSQL is running"},
					{Type: CommandTypeBash, Target: TargetNewNode, Command: "pg_isready -h localhost -p " + strconv.Itoa(input.Port), Note: "Check if accepting connections"},
				},
			},
		})
		output.Summary = a.calculateSummary(output.Checks)
		return output, nil
	}
	defer newNodePool.Close()

	// Ping to verify connection
	if err := newNodePool.Ping(ctx); err != nil {
		output.ConnectionError = fmt.Sprintf("Connection ping failed: %v", err)
		output.Checks = append(output.Checks, CheckResult{
			Category: CategoryConnection,
			Check:    "connection_ping",
			Name:     "Connection Ping",
			Status:   StatusFailed,
			Message:  output.ConnectionError,
		})
		output.Summary = a.calculateSummary(output.Checks)
		return output, nil
	}

	output.Checks = append(output.Checks, CheckResult{
		Category: CategoryConnection,
		Check:    "connection",
		Name:     "New Node Connection",
		Status:   StatusPassed,
		Message:  fmt.Sprintf("Successfully connected to %s:%d", input.Host, input.Port),
	})

	// Run all checks
	a.checkPostgresVersion(ctx, newNodePool, &output)
	a.checkPostgresConfig(ctx, newNodePool, &output)
	a.checkCitusExtension(ctx, newNodePool, &output)
	a.checkExtensions(ctx, newNodePool, &output)
	a.checkSchemas(ctx, newNodePool, &output)
	a.checkTypes(ctx, newNodePool, &output)
	a.checkFunctions(ctx, newNodePool, &output)
	a.checkRoles(ctx, newNodePool, &output)

	// Calculate summary
	output.Summary = a.calculateSummary(output.Checks)
	output.Ready = output.Summary.Failed == 0

	// Generate preparation script if requested
	if input.GenerateScript {
		output.PreparationScript = a.generateScript(output.Checks)
	}

	// Generate estimated issues prevented
	output.EstimatedIssuesPrevented = a.generateIssuesPrevented(output.Checks)

	return output, nil
}

// checkPostgresVersion compares PostgreSQL versions.
func (a *Advisor) checkPostgresVersion(ctx context.Context, newNodePool *pgxpool.Pool, output *Output) {
	var coordVersion int
	var coordVersionStr string
	err := a.coordinatorPool.QueryRow(ctx, QueryPostgresVersion).Scan(&coordVersion, &coordVersionStr)
	if err != nil {
		output.Warnings = append(output.Warnings, "Could not get coordinator PostgreSQL version: "+err.Error())
		return
	}

	var newVersion int
	var newVersionStr string
	err = newNodePool.QueryRow(ctx, QueryPostgresVersion).Scan(&newVersion, &newVersionStr)
	if err != nil {
		output.Checks = append(output.Checks, CheckResult{
			Category: CategoryPostgres,
			Check:    "postgres_version",
			Name:     "PostgreSQL Version",
			Status:   StatusFailed,
			Message:  "Could not determine PostgreSQL version on new node",
		})
		return
	}

	// Compare major versions (first 2 digits: 160000 -> 16)
	coordMajor := coordVersion / 10000
	newMajor := newVersion / 10000

	if coordMajor != newMajor {
		output.Checks = append(output.Checks, CheckResult{
			Category:         CategoryPostgres,
			Check:            "postgres_version",
			Name:             "PostgreSQL Version",
			Status:           StatusFailed,
			Message:          fmt.Sprintf("PostgreSQL major version mismatch: coordinator=%d, new_node=%d", coordMajor, newMajor),
			CoordinatorValue: fmt.Sprintf("%d (%s)", coordMajor, coordVersionStr),
			NewNodeValue:     fmt.Sprintf("%d (%s)", newMajor, newVersionStr),
			Fix: &Fix{
				Description: "Install matching PostgreSQL major version",
				Commands: []Command{
					{Type: CommandTypeBash, Target: TargetNewNode, Command: fmt.Sprintf("# Install PostgreSQL %d\napt-get install postgresql-%d", coordMajor, coordMajor), Note: "Debian/Ubuntu"},
					{Type: CommandTypeBash, Target: TargetNewNode, Command: fmt.Sprintf("# Install PostgreSQL %d\nyum install postgresql%d-server", coordMajor, coordMajor), Note: "RHEL/CentOS"},
				},
			},
		})
	} else {
		output.Checks = append(output.Checks, CheckResult{
			Category:         CategoryPostgres,
			Check:            "postgres_version",
			Name:             "PostgreSQL Version",
			Status:           StatusPassed,
			Message:          fmt.Sprintf("PostgreSQL major version matches: %d", coordMajor),
			CoordinatorValue: fmt.Sprintf("%d", coordMajor),
			NewNodeValue:     fmt.Sprintf("%d", newMajor),
		})
	}
}

// checkPostgresConfig verifies PostgreSQL configuration.
func (a *Advisor) checkPostgresConfig(ctx context.Context, newNodePool *pgxpool.Pool, output *Output) {
	// Get new node config
	rows, err := newNodePool.Query(ctx, QueryPostgresConfig)
	if err != nil {
		output.Warnings = append(output.Warnings, "Could not query PostgreSQL config: "+err.Error())
		return
	}
	defer rows.Close()

	config := make(map[string]string)
	for rows.Next() {
		var name, setting string
		if err := rows.Scan(&name, &setting); err != nil {
			continue
		}
		config[name] = setting
	}

	// Check shared_preload_libraries
	sharedLibs := config["shared_preload_libraries"]
	if !strings.Contains(sharedLibs, "citus") {
		output.Checks = append(output.Checks, CheckResult{
			Category:     CategoryPostgres,
			Check:        "shared_preload_libraries",
			Name:         "shared_preload_libraries",
			Status:       StatusFailed,
			Message:      "Citus not in shared_preload_libraries",
			NewNodeValue: sharedLibs,
			Fix: &Fix{
				Description: "Add citus to shared_preload_libraries",
				Commands: []Command{
					{Type: CommandTypeConfig, Target: TargetNewNode, Command: "shared_preload_libraries = 'citus'", Note: "Add to postgresql.conf"},
					{Type: CommandTypeBash, Target: TargetNewNode, Command: "systemctl restart postgresql", Note: "Restart PostgreSQL"},
				},
			},
		})
	} else {
		output.Checks = append(output.Checks, CheckResult{
			Category:     CategoryPostgres,
			Check:        "shared_preload_libraries",
			Name:         "shared_preload_libraries",
			Status:       StatusPassed,
			Message:      "Citus is in shared_preload_libraries",
			NewNodeValue: sharedLibs,
		})
	}

	// Check max_prepared_transactions
	maxPrepTxns, _ := strconv.Atoi(config["max_prepared_transactions"])
	if maxPrepTxns == 0 {
		output.Checks = append(output.Checks, CheckResult{
			Category:     CategoryPostgres,
			Check:        "max_prepared_transactions",
			Name:         "max_prepared_transactions",
			Status:       StatusWarning,
			Message:      "max_prepared_transactions is 0, 2PC will not work",
			NewNodeValue: "0",
			Fix: &Fix{
				Description: "Enable prepared transactions for 2PC",
				Commands: []Command{
					{Type: CommandTypeConfig, Target: TargetNewNode, Command: "max_prepared_transactions = 100", Note: "Add to postgresql.conf"},
					{Type: CommandTypeBash, Target: TargetNewNode, Command: "systemctl restart postgresql", Note: "Restart PostgreSQL"},
				},
			},
		})
	} else {
		output.Checks = append(output.Checks, CheckResult{
			Category:     CategoryPostgres,
			Check:        "max_prepared_transactions",
			Name:         "max_prepared_transactions",
			Status:       StatusPassed,
			Message:      "max_prepared_transactions is configured",
			NewNodeValue: config["max_prepared_transactions"],
		})
	}

	// Check wal_level for logical replication features
	walLevel := config["wal_level"]
	if walLevel != "logical" {
		output.Checks = append(output.Checks, CheckResult{
			Category:     CategoryPostgres,
			Check:        "wal_level",
			Name:         "wal_level",
			Status:       StatusWarning,
			Message:      "wal_level is not 'logical', some features may not work",
			NewNodeValue: walLevel,
			Fix: &Fix{
				Description: "Set wal_level to logical for full feature support",
				Commands: []Command{
					{Type: CommandTypeConfig, Target: TargetNewNode, Command: "wal_level = logical", Note: "Add to postgresql.conf"},
					{Type: CommandTypeBash, Target: TargetNewNode, Command: "systemctl restart postgresql", Note: "Restart PostgreSQL"},
				},
			},
		})
	} else {
		output.Checks = append(output.Checks, CheckResult{
			Category:     CategoryPostgres,
			Check:        "wal_level",
			Name:         "wal_level",
			Status:       StatusPassed,
			Message:      "wal_level is set to logical",
			NewNodeValue: walLevel,
		})
	}
}

// checkCitusExtension verifies Citus extension is installed and version matches.
func (a *Advisor) checkCitusExtension(ctx context.Context, newNodePool *pgxpool.Pool, output *Output) {
	// Check if Citus is installed on coordinator
	var coordCitusVersion string
	err := a.coordinatorPool.QueryRow(ctx, QueryCitusVersion).Scan(&coordCitusVersion)
	if err != nil {
		output.Warnings = append(output.Warnings, "Could not get coordinator Citus version: "+err.Error())
		return
	}

	// Check if Citus is installed on new node
	var newCitusInstalled bool
	err = newNodePool.QueryRow(ctx, QueryCitusInstalled).Scan(&newCitusInstalled)
	if err != nil {
		output.Warnings = append(output.Warnings, "Could not check Citus installation: "+err.Error())
		return
	}

	if !newCitusInstalled {
		output.Checks = append(output.Checks, CheckResult{
			Category:         CategoryCitus,
			Check:            "citus_installed",
			Name:             "Citus Extension Installed",
			Status:           StatusFailed,
			Message:          "Citus extension is not installed on new node",
			CoordinatorValue: coordCitusVersion,
			NewNodeValue:     "(not installed)",
			Fix: &Fix{
				Description: "Install and create Citus extension",
				Commands: []Command{
					{Type: CommandTypeBash, Target: TargetNewNode, Command: "apt-get install postgresql-16-citus-13.0", Note: "Install Citus package (adjust version)"},
					{Type: CommandTypeSQL, Target: TargetNewNode, Command: "CREATE EXTENSION citus;", Note: "Create extension in database"},
				},
			},
		})
		return
	}

	// Check Citus version
	var newCitusVersion string
	err = newNodePool.QueryRow(ctx, QueryCitusVersion).Scan(&newCitusVersion)
	if err != nil {
		output.Checks = append(output.Checks, CheckResult{
			Category: CategoryCitus,
			Check:    "citus_version",
			Name:     "Citus Version",
			Status:   StatusFailed,
			Message:  "Could not determine Citus version on new node",
		})
		return
	}

	if newCitusVersion != coordCitusVersion {
		output.Checks = append(output.Checks, CheckResult{
			Category:         CategoryCitus,
			Check:            "citus_version",
			Name:             "Citus Version",
			Status:           StatusFailed,
			Message:          fmt.Sprintf("Citus version mismatch: coordinator=%s, new_node=%s", coordCitusVersion, newCitusVersion),
			CoordinatorValue: coordCitusVersion,
			NewNodeValue:     newCitusVersion,
			Fix: &Fix{
				Description: "Update Citus to match coordinator version",
				Commands: []Command{
					{Type: CommandTypeSQL, Target: TargetNewNode, Command: fmt.Sprintf("ALTER EXTENSION citus UPDATE TO '%s';", coordCitusVersion), Note: "Update extension version"},
				},
			},
		})
	} else {
		output.Checks = append(output.Checks, CheckResult{
			Category:         CategoryCitus,
			Check:            "citus_version",
			Name:             "Citus Version",
			Status:           StatusPassed,
			Message:          fmt.Sprintf("Citus version matches: %s", coordCitusVersion),
			CoordinatorValue: coordCitusVersion,
			NewNodeValue:     newCitusVersion,
		})
	}
}

// checkExtensions verifies all required extensions are installed.
func (a *Advisor) checkExtensions(ctx context.Context, newNodePool *pgxpool.Pool, output *Output) {
	// Get coordinator extensions
	coordRows, err := a.coordinatorPool.Query(ctx, QueryExtensions)
	if err != nil {
		output.Warnings = append(output.Warnings, "Could not query coordinator extensions: "+err.Error())
		return
	}
	defer coordRows.Close()

	coordExtensions := make(map[string]string)
	for coordRows.Next() {
		var name, version string
		if err := coordRows.Scan(&name, &version); err != nil {
			continue
		}
		coordExtensions[name] = version
	}

	// Get new node extensions
	newRows, err := newNodePool.Query(ctx, QueryExtensions)
	if err != nil {
		output.Warnings = append(output.Warnings, "Could not query new node extensions: "+err.Error())
		return
	}
	defer newRows.Close()

	newExtensions := make(map[string]string)
	for newRows.Next() {
		var name, version string
		if err := newRows.Scan(&name, &version); err != nil {
			continue
		}
		newExtensions[name] = version
	}

	// Skip standard extensions that are always present
	skipExtensions := map[string]bool{
		"plpgsql": true,
	}

	// Find missing or mismatched extensions
	var missingExtensions []string
	var mismatchedExtensions []string

	for extName, coordVersion := range coordExtensions {
		if skipExtensions[extName] {
			continue
		}
		if extName == "citus" {
			continue // Already checked separately
		}

		newVersion, exists := newExtensions[extName]
		if !exists {
			missingExtensions = append(missingExtensions, extName)
		} else if newVersion != coordVersion {
			mismatchedExtensions = append(mismatchedExtensions, fmt.Sprintf("%s (coord=%s, new=%s)", extName, coordVersion, newVersion))
		}
	}

	if len(missingExtensions) > 0 {
		sort.Strings(missingExtensions)
		commands := []Command{}
		for _, ext := range missingExtensions {
			commands = append(commands, Command{
				Type:    CommandTypeSQL,
				Target:  TargetNewNode,
				Command: fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s;", ext),
				Note:    fmt.Sprintf("Install %s extension", ext),
			})
		}

		output.Checks = append(output.Checks, CheckResult{
			Category:         CategoryExtensions,
			Check:            "missing_extensions",
			Name:             "Missing Extensions",
			Status:           StatusFailed,
			Message:          fmt.Sprintf("%d extension(s) missing: %s", len(missingExtensions), strings.Join(missingExtensions, ", ")),
			CoordinatorValue: fmt.Sprintf("%d extensions", len(coordExtensions)),
			NewNodeValue:     fmt.Sprintf("%d extensions", len(newExtensions)),
			Fix: &Fix{
				Description: "Install missing extensions",
				Commands:    commands,
			},
		})
	} else {
		output.Checks = append(output.Checks, CheckResult{
			Category: CategoryExtensions,
			Check:    "missing_extensions",
			Name:     "Extensions Present",
			Status:   StatusPassed,
			Message:  "All coordinator extensions are present on new node",
		})
	}

	if len(mismatchedExtensions) > 0 {
		output.Checks = append(output.Checks, CheckResult{
			Category: CategoryExtensions,
			Check:    "extension_versions",
			Name:     "Extension Versions",
			Status:   StatusWarning,
			Message:  fmt.Sprintf("%d extension(s) have version mismatch: %s", len(mismatchedExtensions), strings.Join(mismatchedExtensions, ", ")),
			Fix: &Fix{
				Description: "Update extensions to match coordinator versions",
				Commands: []Command{
					{Type: CommandTypeSQL, Target: TargetNewNode, Command: "ALTER EXTENSION <name> UPDATE TO '<version>';", Note: "Update each mismatched extension"},
				},
			},
		})
	}
}

// checkSchemas verifies all schemas exist.
func (a *Advisor) checkSchemas(ctx context.Context, newNodePool *pgxpool.Pool, output *Output) {
	// Get coordinator schemas
	coordRows, err := a.coordinatorPool.Query(ctx, QueryUserSchemas)
	if err != nil {
		output.Warnings = append(output.Warnings, "Could not query coordinator schemas: "+err.Error())
		return
	}
	defer coordRows.Close()

	coordSchemas := make(map[string]string)
	for coordRows.Next() {
		var name, owner string
		if err := coordRows.Scan(&name, &owner); err != nil {
			continue
		}
		coordSchemas[name] = owner
	}

	// Get new node schemas
	newRows, err := newNodePool.Query(ctx, QueryUserSchemas)
	if err != nil {
		output.Warnings = append(output.Warnings, "Could not query new node schemas: "+err.Error())
		return
	}
	defer newRows.Close()

	newSchemas := make(map[string]bool)
	for newRows.Next() {
		var name, owner string
		if err := newRows.Scan(&name, &owner); err != nil {
			continue
		}
		newSchemas[name] = true
	}

	// Find missing schemas
	var missingSchemas []string
	for schema := range coordSchemas {
		if !newSchemas[schema] {
			missingSchemas = append(missingSchemas, schema)
		}
	}

	if len(missingSchemas) > 0 {
		sort.Strings(missingSchemas)
		commands := []Command{}
		for _, schema := range missingSchemas {
			commands = append(commands, Command{
				Type:    CommandTypeSQL,
				Target:  TargetNewNode,
				Command: fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", schema),
				Note:    fmt.Sprintf("Create schema %s", schema),
			})
		}

		output.Checks = append(output.Checks, CheckResult{
			Category: CategorySchema,
			Check:    "missing_schemas",
			Name:     "Missing Schemas",
			Status:   StatusFailed,
			Message:  fmt.Sprintf("%d schema(s) missing: %s", len(missingSchemas), strings.Join(missingSchemas, ", ")),
			Fix: &Fix{
				Description: "Create missing schemas",
				Commands:    commands,
			},
		})
	} else {
		output.Checks = append(output.Checks, CheckResult{
			Category: CategorySchema,
			Check:    "missing_schemas",
			Name:     "Schemas",
			Status:   StatusPassed,
			Message:  "All user schemas are present",
		})
	}
}

// checkTypes verifies user-defined types exist.
func (a *Advisor) checkTypes(ctx context.Context, newNodePool *pgxpool.Pool, output *Output) {
	// Get coordinator types
	coordRows, err := a.coordinatorPool.Query(ctx, QueryUserTypes)
	if err != nil {
		// May fail if no user types, that's ok
		if err != pgx.ErrNoRows {
			output.Warnings = append(output.Warnings, "Could not query coordinator types: "+err.Error())
		}
		return
	}
	defer coordRows.Close()

	coordTypes := make(map[string]UserType)
	for coordRows.Next() {
		var t UserType
		if err := coordRows.Scan(&t.Schema, &t.Name, &t.Type); err != nil {
			continue
		}
		coordTypes[t.Schema+"."+t.Name] = t
	}

	if len(coordTypes) == 0 {
		return // No user types to check
	}

	// Get new node types
	newRows, err := newNodePool.Query(ctx, QueryUserTypes)
	if err != nil {
		output.Warnings = append(output.Warnings, "Could not query new node types: "+err.Error())
		return
	}
	defer newRows.Close()

	newTypes := make(map[string]bool)
	for newRows.Next() {
		var t UserType
		if err := newRows.Scan(&t.Schema, &t.Name, &t.Type); err != nil {
			continue
		}
		newTypes[t.Schema+"."+t.Name] = true
	}

	// Find missing types
	var missingTypes []string
	for typeName := range coordTypes {
		if !newTypes[typeName] {
			missingTypes = append(missingTypes, typeName)
		}
	}

	if len(missingTypes) > 0 {
		sort.Strings(missingTypes)
		output.Checks = append(output.Checks, CheckResult{
			Category: CategorySchema,
			Check:    "missing_types",
			Name:     "Missing Types",
			Status:   StatusFailed,
			Message:  fmt.Sprintf("%d type(s) missing: %s", len(missingTypes), strings.Join(missingTypes, ", ")),
			Fix: &Fix{
				Description: "Create missing user-defined types",
				Commands: []Command{
					{Type: CommandTypeSQL, Target: TargetNewNode, Command: "-- Export type definitions from coordinator and create on new node", Note: "Manual step required"},
				},
			},
		})
	} else {
		output.Checks = append(output.Checks, CheckResult{
			Category: CategorySchema,
			Check:    "missing_types",
			Name:     "User Types",
			Status:   StatusPassed,
			Message:  "All user-defined types are present",
		})
	}
}

// checkFunctions verifies user-defined functions exist.
func (a *Advisor) checkFunctions(ctx context.Context, newNodePool *pgxpool.Pool, output *Output) {
	// Get coordinator functions
	coordRows, err := a.coordinatorPool.Query(ctx, QueryUserFunctions)
	if err != nil {
		if err != pgx.ErrNoRows {
			output.Warnings = append(output.Warnings, "Could not query coordinator functions: "+err.Error())
		}
		return
	}
	defer coordRows.Close()

	coordFuncs := make(map[string]UserFunction)
	for coordRows.Next() {
		var f UserFunction
		if err := coordRows.Scan(&f.Schema, &f.Name, &f.Arguments); err != nil {
			continue
		}
		key := fmt.Sprintf("%s.%s(%s)", f.Schema, f.Name, f.Arguments)
		coordFuncs[key] = f
	}

	if len(coordFuncs) == 0 {
		return // No user functions to check
	}

	// Get new node functions
	newRows, err := newNodePool.Query(ctx, QueryUserFunctions)
	if err != nil {
		output.Warnings = append(output.Warnings, "Could not query new node functions: "+err.Error())
		return
	}
	defer newRows.Close()

	newFuncs := make(map[string]bool)
	for newRows.Next() {
		var f UserFunction
		if err := newRows.Scan(&f.Schema, &f.Name, &f.Arguments); err != nil {
			continue
		}
		key := fmt.Sprintf("%s.%s(%s)", f.Schema, f.Name, f.Arguments)
		newFuncs[key] = true
	}

	// Find missing functions
	var missingFuncs []string
	for funcName := range coordFuncs {
		if !newFuncs[funcName] {
			missingFuncs = append(missingFuncs, funcName)
		}
	}

	if len(missingFuncs) > 0 {
		sort.Strings(missingFuncs)
		displayList := missingFuncs
		if len(displayList) > 10 {
			displayList = append(displayList[:10], fmt.Sprintf("... and %d more", len(missingFuncs)-10))
		}

		output.Checks = append(output.Checks, CheckResult{
			Category: CategorySchema,
			Check:    "missing_functions",
			Name:     "Missing Functions",
			Status:   StatusFailed,
			Message:  fmt.Sprintf("%d function(s) missing: %s", len(missingFuncs), strings.Join(displayList, ", ")),
			Fix: &Fix{
				Description: "Create missing user-defined functions",
				Commands: []Command{
					{Type: CommandTypeSQL, Target: TargetNewNode, Command: "-- Export function definitions from coordinator and create on new node\n-- pg_dump --schema-only can help extract these", Note: "Manual step required"},
				},
			},
		})
	} else if len(coordFuncs) > 0 {
		output.Checks = append(output.Checks, CheckResult{
			Category: CategorySchema,
			Check:    "missing_functions",
			Name:     "User Functions",
			Status:   StatusPassed,
			Message:  fmt.Sprintf("All %d user-defined functions are present", len(coordFuncs)),
		})
	}
}

// checkRoles verifies required roles exist.
func (a *Advisor) checkRoles(ctx context.Context, newNodePool *pgxpool.Pool, output *Output) {
	// Get coordinator roles
	coordRows, err := a.coordinatorPool.Query(ctx, QueryRoles)
	if err != nil {
		output.Warnings = append(output.Warnings, "Could not query coordinator roles: "+err.Error())
		return
	}
	defer coordRows.Close()

	coordRoles := make(map[string]Role)
	for coordRows.Next() {
		var r Role
		if err := coordRows.Scan(&r.Name, &r.IsSuperuser, &r.CanLogin, &r.CanCreateDB); err != nil {
			continue
		}
		coordRoles[r.Name] = r
	}

	// Get new node roles
	newRows, err := newNodePool.Query(ctx, QueryRoles)
	if err != nil {
		output.Warnings = append(output.Warnings, "Could not query new node roles: "+err.Error())
		return
	}
	defer newRows.Close()

	newRoles := make(map[string]bool)
	for newRows.Next() {
		var r Role
		if err := newRows.Scan(&r.Name, &r.IsSuperuser, &r.CanLogin, &r.CanCreateDB); err != nil {
			continue
		}
		newRoles[r.Name] = true
	}

	// Find missing roles
	var missingRoles []string
	for roleName := range coordRoles {
		if !newRoles[roleName] {
			missingRoles = append(missingRoles, roleName)
		}
	}

	if len(missingRoles) > 0 {
		sort.Strings(missingRoles)
		commands := []Command{}
		for _, role := range missingRoles {
			r := coordRoles[role]
			opts := []string{}
			if r.IsSuperuser {
				opts = append(opts, "SUPERUSER")
			}
			if r.CanLogin {
				opts = append(opts, "LOGIN")
			}
			if r.CanCreateDB {
				opts = append(opts, "CREATEDB")
			}
			optStr := ""
			if len(opts) > 0 {
				optStr = " " + strings.Join(opts, " ")
			}
			commands = append(commands, Command{
				Type:    CommandTypeSQL,
				Target:  TargetNewNode,
				Command: fmt.Sprintf("CREATE ROLE %s%s;", role, optStr),
				Note:    fmt.Sprintf("Create role %s", role),
			})
		}

		output.Checks = append(output.Checks, CheckResult{
			Category: CategorySecurity,
			Check:    "missing_roles",
			Name:     "Missing Roles",
			Status:   StatusFailed,
			Message:  fmt.Sprintf("%d role(s) missing: %s", len(missingRoles), strings.Join(missingRoles, ", ")),
			Fix: &Fix{
				Description: "Create missing roles",
				Commands:    commands,
			},
		})
	} else {
		output.Checks = append(output.Checks, CheckResult{
			Category: CategorySecurity,
			Check:    "missing_roles",
			Name:     "Roles",
			Status:   StatusPassed,
			Message:  "All required roles are present",
		})
	}
}

// calculateSummary generates the summary from check results.
func (a *Advisor) calculateSummary(checks []CheckResult) Summary {
	summary := Summary{Total: len(checks)}
	for _, check := range checks {
		switch check.Status {
		case StatusPassed:
			summary.Passed++
		case StatusFailed:
			summary.Failed++
		case StatusWarning:
			summary.Warnings++
		case StatusSkipped:
			summary.Skipped++
		}
	}
	return summary
}

// generateScript creates a preparation script from failed checks.
func (a *Advisor) generateScript(checks []CheckResult) *PrepScript {
	script := &PrepScript{
		BashCommands:  []ScriptCommand{},
		SQLCommands:   []ScriptCommand{},
		ConfigChanges: []ConfigChange{},
	}

	for _, check := range checks {
		if check.Fix == nil {
			continue
		}
		for _, cmd := range check.Fix.Commands {
			switch cmd.Type {
			case CommandTypeBash:
				script.BashCommands = append(script.BashCommands, ScriptCommand{
					Description: check.Name + ": " + cmd.Note,
					Command:     cmd.Command,
					Target:      cmd.Target,
				})
			case CommandTypeSQL:
				script.SQLCommands = append(script.SQLCommands, ScriptCommand{
					Description: check.Name + ": " + cmd.Note,
					Command:     cmd.Command,
					Target:      cmd.Target,
				})
			case CommandTypeConfig:
				script.ConfigChanges = append(script.ConfigChanges, ConfigChange{
					File:        "postgresql.conf",
					Parameter:   strings.Split(cmd.Command, " = ")[0],
					Value:       strings.Trim(strings.Split(cmd.Command, " = ")[1], "'"),
					Description: check.Name,
				})
			}
		}
	}

	// Add post-add verification commands
	script.PostAddCommands = []ScriptCommand{
		{
			Description: "Verify node was added successfully",
			Command:     "SELECT * FROM pg_dist_node ORDER BY nodeid;",
			Target:      TargetCoordinator,
		},
		{
			Description: "Check reference tables are replicated",
			Command:     "SELECT replicate_reference_tables();",
			Target:      TargetCoordinator,
		},
	}

	return script
}

// generateIssuesPrevented lists issues that would be caught.
func (a *Advisor) generateIssuesPrevented(checks []CheckResult) []string {
	var issues []string
	for _, check := range checks {
		if check.Status == StatusFailed {
			switch check.Check {
			case "citus_installed":
				issues = append(issues, "citus_add_node would fail: Citus extension not installed")
			case "citus_version":
				issues = append(issues, "citus_add_node would fail: Citus version mismatch")
			case "postgres_version":
				issues = append(issues, "citus_add_node would fail: PostgreSQL version incompatibility")
			case "shared_preload_libraries":
				issues = append(issues, "Citus would not function: Not in shared_preload_libraries")
			case "missing_extensions":
				issues = append(issues, "Distributed queries would fail: Missing required extensions")
			case "missing_schemas":
				issues = append(issues, "Shard creation would fail: Missing schemas")
			case "missing_types":
				issues = append(issues, "Queries would fail: Missing user-defined types")
			case "missing_functions":
				issues = append(issues, "Queries would fail: Missing user-defined functions")
			case "missing_roles":
				issues = append(issues, "Permission errors: Missing roles")
			}
		}
	}
	return issues
}
